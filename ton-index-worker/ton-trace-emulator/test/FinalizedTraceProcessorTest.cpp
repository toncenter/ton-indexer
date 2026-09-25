#include "EnginePrep.h"
#include "FinalizedTraceProcessor.h"
#include "TraceTestUtils.h"

namespace {
using namespace trace_test;

TransactionInfo transaction(td::Ref<vm::Cell> in, std::vector<td::Ref<vm::Cell>> out, ton::BlockSeqno seqno = 100,
                            std::uint64_t lt = 100, bool external = false) {
  auto source = node(in, out, FinalityState::Finalized, lt);
  TransactionInfo tx;
  tx.account = source->address;
  tx.root = source->transaction_root;
  tx.hash = tx.root->get_hash().bits();
  tx.lt = lt;
  tx.block_id = source->block_id;
  tx.mc_block_seqno = seqno;
  tx.in_msg_hash = in->get_hash().bits();
  for (auto& message : out) tx.out_msgs.push_back({message->get_hash().bits(), std::move(message)});
  if (external) tx.trace_ids = TraceIds{tx.hash, tx.in_msg_hash, ext_in_msg_get_normalized_hash(in).move_as_ok()};
  return tx;
}

ParsedFinalizedBlock parsed(ton::BlockSeqno seqno, std::uint32_t time, std::vector<TransactionInfo> txs = {}) {
  ParsedFinalizedBlock block;
  block.seqno = seqno;
  block.unix_time = time;
  block.transactions = std::move(txs);
  // A real empty basechain shard: account lookup returns a serialized nonexist state.
  block::gen::ShardStateUnsplit::Record state{};
  state.shard_id = vm::load_cell_slice_ref(vm::CellBuilder().store_zeroes(104).finalize());
  state.seq_no = seqno;
  state.gen_utime = time;
  state.gen_lt = 1000;
  state.out_msg_queue_info = vm::CellBuilder().store_zeroes(67).finalize();
  state.accounts = vm::CellBuilder().store_long(0, 11).finalize();
  auto none = vm::load_cell_slice_ref(vm::CellBuilder().store_long(0, 1).finalize());
  auto balance = vm::load_cell_slice_ref(vm::CellBuilder().store_long(0, 5).finalize());
  state.r1.total_balance = state.r1.total_validator_fees = balance;
  state.r1.libraries = state.r1.master_ref = state.custom = none;
  td::Ref<vm::Cell> root;
  ASSERT_TRUE(tlb::pack_cell(root, state));
  block.shard_states.push_back(std::move(root));
  return block;
}

std::string trace_key(const td::Ref<vm::Cell>& external) {
  return td::base64_encode(ext_in_msg_get_normalized_hash(external).move_as_ok().as_slice());
}

std::optional<std::string> field(const RedisWritePlan& plan, const std::string& name) {
  for (const auto& [key, value] : plan.fields_to_set)
    if (key == name) return value;
  return {};
}

using Inspect = std::function<void(std::size_t, const std::vector<RedisWritePlan>&, bool)>;
class BlockSequence : public td::actor::Actor {
  td::actor::ActorOwn<FinalizedTraceProcessor> processor_;
  std::vector<ParsedFinalizedBlock> blocks_;
  std::vector<RedisWritePlan> plans_;
  Inspect inspect_;
  mch::EmuClassifierConfig classifier_;
  std::size_t index_{0};
  bool& finished_;
  void start_up() override {
    processor_ = td::actor::create_actor<FinalizedTraceProcessor>("FlatFinalized", 30., std::move(classifier_));
    next();
  }
  void next() {
    if (index_ == blocks_.size()) {
      finished_ = true;
      td::actor::SchedulerContext::get().stop();
      stop();
      return;
    }
    plans_.clear();
    auto self = actor_id(this);
    std::function<void(RedisWritePlan)> on_plan = [self](RedisWritePlan plan) {
      td::actor::send_closure(self, &BlockSequence::received, std::move(plan));
    };
    auto done = td::PromiseCreator::lambda([self](td::Result<td::Unit> result) mutable {
      td::actor::send_closure(self, &BlockSequence::prepared, std::move(result));
    });
    td::actor::send_closure(processor_, &FinalizedTraceProcessor::prepare_block, std::move(blocks_[index_]),
                            std::move(on_plan), std::move(done));
  }
  void received(RedisWritePlan plan) { plans_.push_back(std::move(plan)); }
  void prepared(td::Result<td::Unit> result) {
    inspect_(index_++, plans_, result.is_ok());
    next();
  }

 public:
  BlockSequence(std::vector<ParsedFinalizedBlock> blocks, Inspect inspect, mch::EmuClassifierConfig classifier,
                bool& finished)
      : blocks_(std::move(blocks)),
        inspect_(std::move(inspect)),
        classifier_(std::move(classifier)),
        finished_(finished) {}
};

void run_blocks(std::vector<ParsedFinalizedBlock> blocks, Inspect inspect, mch::EmuClassifierConfig classifier = {}) {
  bool finished = false;
  td::actor::Scheduler scheduler({1});
  scheduler.run_in_context([&] {
    td::actor::create_actor<BlockSequence>("BlockSequence", std::move(blocks), std::move(inspect),
                                           std::move(classifier), finished)
        .release();
  });
  scheduler.run();
  ASSERT_TRUE(finished);
}
}  // namespace

TEST(FinalizedTraceProcessor, unordered_flat_transactions_form_one_complete_snapshot) {
  auto external = message(801, true), b = message(802), c = message(803);
  auto root = transaction(external, {b}, 100, 100, true);
  auto child = transaction(b, {c}, 100, 110);
  auto grandchild = transaction(c, {}, 100, 120);
  run_blocks({parsed(100, 1000, {grandchild, root, child})}, [&](auto, const auto& plans, bool ok) {
    ASSERT_TRUE(ok);
    ASSERT_EQ(1u, plans.size());
    const auto& plan = plans[0];
    ASSERT_TRUE(plan.replace_trace && !plan.erase_trace);
    ASSERT_EQ(trace_key(external), plan.trace_key);
    ASSERT_TRUE(field(plan, key(external)) && field(plan, key(b)) && field(plan, key(c)));
    ASSERT_EQ("1", *field(plan, "trace_complete"));
    ASSERT_EQ("100", *field(plan, "update_seq"));
    ASSERT_EQ(3u, plan.indexes_to_add.size());
    ASSERT_EQ(2u, plan.publications.size());
    ASSERT_EQ(1u, plan.account_states.size());
    ASSERT_EQ(0u, plan.account_states[0].lt);
  });
}

TEST(FinalizedTraceProcessor, disconnected_continuations_merge_across_blocks_and_expire_only_after_completion) {
  auto external = message(811, true), b = message(812), c = message(813), d = message(814);
  run_blocks({parsed(100, 1000, {transaction(external, {b, c}, 100, 100, true), transaction(c, {d}, 100, 110)}),
              parsed(101, 2000, {transaction(d, {}, 101, 130), transaction(b, {}, 101, 120)}), parsed(102, 2031)},
             [&](auto step, const auto& plans, bool ok) {
               ASSERT_TRUE(ok);
               ASSERT_EQ(1u, plans.size());
               const auto& plan = plans[0];
               if (step == 2) {
                 ASSERT_TRUE(plan.erase_trace && !plan.replace_trace);
                 ASSERT_EQ(4u, plan.indexes_to_remove.size());
                 ASSERT_TRUE(plan.publications.empty());
               } else {
                 ASSERT_TRUE(field(plan, key(external)) && field(plan, key(c)));
                 ASSERT_EQ(step == 0 ? "0" : "1", *field(plan, "trace_complete"));
                 ASSERT_EQ(step == 0 ? "100" : "101", *field(plan, "update_seq"));
                 ASSERT_EQ(step == 0 ? 2u : 4u, plan.indexes_to_add.size());
                 if (step == 1) ASSERT_TRUE(field(plan, key(b)) && field(plan, key(d)));
               }
             });
}

TEST(FinalizedTraceProcessor, message_links_are_owned_by_each_processor) {
  auto external = message(821, true), outgoing = message(822), fresh = message(823, true);
  run_blocks({parsed(100, 1000, {transaction(external, {outgoing}, 100, 100, true)})},
             [](auto, const auto& plans, bool ok) {
               ASSERT_TRUE(ok);
               ASSERT_EQ(1u, plans.size());
             });
  // A different processor must not pick up the first one's interblock mappings.
  run_blocks({parsed(101, 1001, {transaction(outgoing, {}, 101, 110), transaction(fresh, {}, 101, 120, true)})},
             [&](auto, const auto& plans, bool ok) {
               ASSERT_TRUE(ok);
               ASSERT_EQ(1u, plans.size());
               ASSERT_EQ(trace_key(fresh), plans[0].trace_key);
             });
}

TEST(FinalizedTraceProcessor, oversized_trace_keeps_routing_late_continuations_and_does_not_stop_other_traces) {
  auto external = message(10000, true), late = message(12000), fresh = message(13000, true);
  std::vector<td::Ref<vm::Cell>> out;
  for (unsigned i = 0; i < 1000; ++i) out.push_back(message(10001 + i));
  std::vector<TransactionInfo> first{transaction(external, out, 100, 100, true)};
  for (unsigned i = 0; i < 999; ++i) first.push_back(transaction(out[i], {}, 100, 101 + i));
  run_blocks({parsed(100, 1000, std::move(first)),
              parsed(101, 1001, {transaction(out.back(), {late}, 101, 2000), transaction(fresh, {}, 101, 2100, true)}),
              parsed(102, 100000, {transaction(late, {}, 102, 3000)})},
             [&](auto step, const auto& plans, bool ok) {
               ASSERT_TRUE(ok);
               ASSERT_EQ(step == 0 ? 1u : 2u, plans.size());
               bool found = false;
               for (const auto& plan : plans) {
                 if (plan.trace_key != trace_key(external)) continue;
                 found = true;
                 if (step == 0) {
                   ASSERT_TRUE(plan.replace_trace);
                   ASSERT_EQ(1000u, plan.indexes_to_add.size());
                   ASSERT_EQ("0", *field(plan, "trace_complete"));
                 } else {
                   ASSERT_TRUE(plan.erase_trace && !plan.replace_trace);
                   ASSERT_EQ(step == 1 ? 1000u : 0u, plan.indexes_to_remove.size());
                   ASSERT_TRUE(plan.fields_to_set.empty() && plan.publications.empty());
                   ASSERT_EQ(key(external), plan.raw_external_message_hash);
                   ASSERT_EQ(1u, plan.account_states.size());
                   ASSERT_EQ(0u, plan.account_states[0].lt);
                 }
               }
               ASSERT_TRUE(found);
             });
}

TEST(FinalizedTraceProcessor, updating_trace_is_not_cleaned_up_in_the_same_block) {
  auto external = message(831, true);
  run_blocks({parsed(100, 1000, {transaction(external, {}, 100, 100, true)}),
              parsed(101, 2000, {transaction(external, {}, 101, 100, true)})},
             [&](auto step, const auto& plans, bool ok) {
               ASSERT_TRUE(ok);
               ASSERT_EQ(1u, plans.size());
               ASSERT_TRUE(plans[0].replace_trace && !plans[0].erase_trace);
               ASSERT_EQ(step == 0 ? "100" : "101", *field(plans[0], "update_seq"));
             });
}

TEST(FinalizedTraceProcessor, oversized_first_update_still_emits_committed_accounts) {
  auto external = message(14000, true);
  std::vector<td::Ref<vm::Cell>> out;
  for (unsigned i = 0; i < 1000; ++i) out.push_back(message(14001 + i));
  std::vector<TransactionInfo> txs{transaction(external, out, 100, 100, true)};
  for (unsigned i = 0; i < 1000; ++i) txs.push_back(transaction(out[i], {}, 100, 101 + i));
  run_blocks({parsed(100, 1000, std::move(txs))}, [&](auto, const auto& plans, bool ok) {
    ASSERT_TRUE(ok);
    ASSERT_EQ(1u, plans.size());
    const auto& plan = plans[0];
    ASSERT_TRUE(plan.erase_trace && !plan.replace_trace);
    ASSERT_TRUE(plan.fields_to_set.empty() && plan.publications.empty());
    ASSERT_TRUE(plan.indexes_to_remove.empty());
    ASSERT_EQ(key(external), plan.raw_external_message_hash);
    ASSERT_EQ(1u, plan.account_states.size());
    ASSERT_EQ(0u, plan.account_states[0].lt);
  });
}

TEST(FinalizedTraceProcessor, external_out_messages_do_not_keep_a_trace_open) {
  auto external = message(841, true);
  vm::CellBuilder builder;
  builder.store_long(3, 2);  // ext_out
  address(builder);
  auto log = builder.store_long(0, 2).store_long(100, 64).store_long(1000, 32).store_long(0, 2).finalize();
  run_blocks({parsed(100, 1000, {transaction(external, {log}, 100, 100, true)})}, [](auto, const auto& plans, bool ok) {
    ASSERT_TRUE(ok);
    ASSERT_EQ(1u, plans.size());
    ASSERT_EQ("1", *field(plans[0], "trace_complete"));
  });
}

TEST(FinalizedTraceProcessor, invalid_block_does_not_create_message_links) {
  auto external = message(851, true), internal = message(852);
  auto duplicate = transaction(external, {internal}, 100, 100, true);
  run_blocks({parsed(100, 1000, {duplicate, duplicate}), parsed(101, 1001, {transaction(internal, {}, 101, 110)})},
             [](auto step, const auto& plans, bool ok) {
               ASSERT_EQ(step != 0, ok);
               ASSERT_TRUE(plans.empty());
             });
}

TEST(FinalizedTraceProcessor, uses_shared_classifier_for_the_assembled_trace) {
  mch::EmuClassifierConfig classifier;
  classifier.prep = mch::make_engine_prep().move_as_ok();
  classifier.workers = 2;
  auto a = message(861, true), b = message(862, true), internal = message(863);
  run_blocks(
      {parsed(100, 1000,
              {transaction(a, {internal}, 100, 100, true), transaction(b, {}, 100, 105, true),
               transaction(internal, {}, 100, 110)})},
      [](auto, const auto& plans, bool ok) {
        ASSERT_TRUE(ok);
        ASSERT_EQ(2u, plans.size());
        for (const auto& plan : plans) {
          ASSERT_TRUE(field(plan, "mch_classify_state"));
          ASSERT_TRUE(field(plan, "actions"));
          ASSERT_EQ("1", *field(plan, "streaming_actions_updated"));
          ASSERT_EQ("100", *field(plan, "update_seq"));
        }
      },
      std::move(classifier));
}

TEST(FinalizedTraceProcessor, flat_and_legacy_accounts_use_the_same_encoder) {
  auto external = message(871, true);
  auto legacy = trace(node(external, {}, FinalityState::Finalized), external);
  const auto address = legacy.root->address;
  block::Account account(address.workchain, address.addr.cbits());
  ASSERT_TRUE(account.init_new(1000));
  account.last_trans_lt_ = 123;
  legacy.committed_accounts.emplace(address, account);
  legacy.committed_interfaces[address] = {};
  DetectedAccounts flat;
  flat.states.emplace(address, std::move(account));
  flat.interfaces[address] = {};
  RedisWritePlan a, b;
  ASSERT_TRUE(trace_materialization::append_account_state_writes(a, legacy).is_ok());
  ASSERT_TRUE(trace_materialization::append_account_state_writes(b, flat, FinalityState::Finalized).is_ok());
  ASSERT_EQ(1u, a.account_states.size());
  ASSERT_EQ(1u, b.account_states.size());
  ASSERT_EQ(a.account_states[0].account, b.account_states[0].account);
  ASSERT_EQ(a.account_states[0].lt, b.account_states[0].lt);
  ASSERT_EQ(a.account_states[0].state, b.account_states[0].state);
  ASSERT_EQ(a.account_states[0].interfaces, b.account_states[0].interfaces);
}

TEST(FinalizedTraceProcessor, flat_snapshots_match_the_legacy_fragment_assembler) {
  auto external = message(881, true), internal = message(882);
  const auto root_tx = transaction(external, {internal}, 100, 100, true);
  const auto child_tx = transaction(internal, {}, 101, 110);
  ActiveTrace legacy_state;
  run_blocks({parsed(100, 1000, {root_tx}), parsed(101, 1001, {child_tx})}, [&](auto step, const auto& plans, bool ok) {
    ASSERT_TRUE(ok);
    ASSERT_EQ(1u, plans.size());
    auto n = step == 0 ? node(external, {internal}, FinalityState::Finalized, 100)
                       : node(internal, {}, FinalityState::Finalized, 110);
    n->mc_block_seqno = 100 + step;
    auto fragment = trace(std::move(n), external, root_tx.hash);
    fragment.ext_in_msg_hash_norm = root_tx.trace_ids->ext_in_msg_hash_norm;
    const auto address = fragment.root->address;
    block::Account account(address.workchain, address.addr.cbits());
    ASSERT_TRUE(account.init_new(1000 + step));
    fragment.committed_accounts.emplace(address, account);
    fragment.committed_interfaces[address] = {};
    fragment.interfaces[address] = {};
    auto update = make_trace_update(std::move(fragment), {});
    auto transition = TraceAssembler().apply_update(legacy_state, update, trace_key(external)).move_as_ok();
    transition.next_trace.update_seq = 100 + step;
    mch::EmuActionPayload payload;
    payload.finality = 2;
    payload.update_seq = 100 + step;
    auto prepared = trace_materialization::prepare_trace_materialization(legacy_state, std::move(transition), update,
                                                                         payload, trace_key(external), {})
                        .move_as_ok();
    auto expected = trace_materialization::build_finalized_snapshot(trace_key(external), prepared.next_trace, 30);
    expected.account_states = std::move(prepared.redis.account_states);
    expected.indexes_to_remove = trace_materialization::collect_trace_index_refs(legacy_state);
    const auto& actual = plans[0];
    ASSERT_TRUE(expected.fields_to_set == actual.fields_to_set);
    ASSERT_TRUE(expected.publications == actual.publications);
    ASSERT_TRUE(expected.indexes_to_add == actual.indexes_to_add);
    ASSERT_TRUE(expected.indexes_to_remove == actual.indexes_to_remove);
    ASSERT_EQ(expected.account_states[0].state, actual.account_states[0].state);
    ASSERT_EQ(expected.account_states[0].interfaces, actual.account_states[0].interfaces);
    legacy_state = std::move(prepared.next_trace);
  });
}
