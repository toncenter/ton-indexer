#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>
#include <sw/redis++/redis++.h>

#include "../src/TraceProcessor.cpp"
#include "td/utils/tests.h"

#include "TraceTestUtils.h"

namespace {

std::string actions_blob(std::uint8_t finality, const std::string& suffix = {}) {
  msgpack::sbuffer buffer;
  msgpack::packer<msgpack::sbuffer> packer(&buffer);
  packer.pack_array(2);

  packer.pack_map(4);
  packer.pack(std::string("action_id"));
  packer.pack(std::string("first") + suffix);
  packer.pack(std::string("finality"));
  packer.pack(finality);
  packer.pack(std::string("success"));
  packer.pack(true);
  packer.pack(std::string("amount"));
  packer.pack(std::uint64_t{17});

  packer.pack_map(4);
  packer.pack(std::string("action_id"));
  packer.pack(std::string("second") + suffix);
  packer.pack(std::string("finality"));
  packer.pack(finality);
  packer.pack(std::string("success"));
  packer.pack(false);
  packer.pack(std::string("amount"));
  packer.pack(std::uint64_t{29});

  return {buffer.data(), buffer.size()};
}

const msgpack::object& map_field(const msgpack::object& row, std::string_view name) {
  for (std::uint32_t index = 0; index < row.via.map.size; ++index) {
    const auto& field = row.via.map.ptr[index];
    if (field.key.type == msgpack::type::STR &&
        std::string_view(field.key.via.str.ptr, field.key.via.str.size) == name) {
      return field.val;
    }
  }
  UNREACHABLE();
}

std::optional<std::string> redis_field(const RedisWritePlan& plan, std::string_view name) {
  for (const auto& [field, value] : plan.fields_to_set) {
    if (field == name) {
      return value;
    }
  }
  return std::nullopt;
}

td::Bits256 trace_hash(char digit) {
  td::Bits256 result;
  ASSERT_EQ(256, result.from_hex(std::string(64, digit)));
  return result;
}

Trace trace_fragment(char trace_digit, FinalityState finality, ton::BlockSeqno mc_seqno) {
  Trace trace;
  trace.ext_in_msg_hash_norm = trace_hash(trace_digit);
  trace.ext_in_msg_hash = trace_hash(trace_digit);
  trace.root_tx_hash = trace_hash(trace_digit);
  trace.root = std::make_unique<TraceNode>();
  trace.root->node_id = trace_hash(trace_digit);
  trace.root->finality_state = finality;
  trace.root->mc_block_seqno = mc_seqno;
  return trace;
}

ActiveTrace trace_with_actions(FinalityState trace_finality, std::optional<std::uint8_t> action_finality) {
  ActiveTrace trace;
  trace.finality = trace_finality;
  trace.metadata.emplace("root_node", "root");
  trace.nodes.apply(trace.nodes.upsert_nodes({TraceStateNode{
      .key = "root", .finality = static_cast<TraceStateFinality>(trace_finality), .fingerprint = "root-tx",
  }}));
  if (action_finality) {
    trace.actions.blob = actions_blob(*action_finality);
    trace.actions.blob_finality = *action_finality;
    trace.actions.classify_state = "ok";
    trace.actions.routes = {mch::EmuActionRoute{
        .type = "ton_transfer",
        .accounts = {"0:AAAA"},
    }};
    trace.actions.blob_is_current = true;
  }
  return trace;
}

PreparedTraceUpdate promote(ActiveTrace current) {
  ConfirmedTraceSnapshotData snapshot{
      .trace_key = "trace",
      .root_key = *trace_metadata_value(current, "root_node"),
      .root_transaction_hash = std::string(current.root()->transaction_hash()),
  };
  auto result = prepare_confirmed_promotion(current, CachedConfirmedTrace{}, "trace", 100, snapshot);
  ASSERT_TRUE(result.is_ok());
  return result.move_as_ok();
}

ConfirmedTraceSnapshot snapshot_of(const ActiveTrace& trace) {
  auto snapshot = std::make_shared<ConfirmedTraceSnapshotData>();
  snapshot->trace_key = "trace";
  snapshot->root_key = *trace_metadata_value(trace, "root_node");
  snapshot->root_transaction_hash = std::string(trace.root()->transaction_hash());
  for (const auto& [key, node] : trace.nodes.nodes()) {
    if (node.finality == TraceStateFinality::Confirmed) {
      snapshot->confirmed.nodes.emplace(key, node);
    }
  }
  return snapshot;
}

void attach_actions(ActiveTrace& trace, const std::string& action_id) {
  trace.actions.blob = actions_blob(trace_finality(trace), action_id);
  trace.actions.blob_finality = trace_finality(trace);
  trace.actions.classify_state = "ok-" + action_id;
  trace.actions.blob_is_current = true;
  trace.actions.aai_refs = {{"_aai:account", "trace:" + action_id, 10}};
}

void commit_to_redis(const char* uri, RedisWritePlan plan) {
  auto options = parse_redis_connection_options(uri).move_as_ok();
  td::actor::Scheduler scheduler({1});
  td::actor::ActorOwn<RedisMaterializer> materializer;
  scheduler.run_in_context([&] {
    materializer = td::actor::create_actor<RedisMaterializer>("RootReplacementTest", std::move(options), 1);
    td::actor::send_closure(
        materializer, &RedisMaterializer::write, RedisWriteBatch{{std::move(plan)}},
        [](td::Status status, RedisWriteBatch) {
          status.ensure();
          td::actor::SchedulerContext::get().stop();
        },
        td::Timer());
  });
  scheduler.run();
  scheduler.run_in_context([&] { materializer.reset(); });
}

}  // namespace

TEST(TraceProcessor, promotion_declines_a_missing_or_different_root_without_mutation) {
  using namespace trace_test;
  auto a = message(1, true), b = message(2, true);
  ActiveTrace canonical;
  apply(canonical, trace(node(a, {}, FinalityState::Confirmed, 100), a));
  auto snapshot = snapshot_of(canonical);

  for (auto finality : {FinalityState::Confirmed, FinalityState::Finalized}) {
    for (auto raw : {a, b}) {
      ActiveTrace current;
      apply(current, trace(node(raw, {}, finality, 200), raw));
      attach_actions(current, "current");
      auto before = current;
      auto result = prepare_confirmed_promotion(current, snapshot->confirmed, "trace", 300, *snapshot);
      ASSERT_TRUE(result.is_error());
      ASSERT_EQ(kConfirmedPromotionUnavailable, result.error().code());
      ASSERT_TRUE(current.nodes.delta_to(before.nodes).empty());
      ASSERT_EQ(before.metadata, current.metadata);
      ASSERT_EQ(before.actions.blob, current.actions.blob);
      ASSERT_EQ(before.update_seq, current.update_seq);
    }
  }
  for (bool root_hint : {false, true}) {
    ActiveTrace evicted;
    if (root_hint) {
      evicted.metadata.emplace("root_node", key(a));
    }
    auto result = prepare_confirmed_promotion(evicted, snapshot->confirmed, "trace", 300, *snapshot);
    ASSERT_TRUE(result.is_error());
    ASSERT_EQ(kConfirmedPromotionUnavailable, result.error().code());
    ASSERT_TRUE(evicted.nodes.nodes().empty());
  }
}

TEST(TraceProcessor, promotion_declines_missing_or_changed_children_without_partial_finalization) {
  using namespace trace_test;
  auto a = message(1, true), c = message(2), t = message(3);
  ActiveTrace canonical;
  auto root = node(a, {c}, FinalityState::Confirmed);
  root->children.push_back(node(c, {t}, FinalityState::Confirmed, 200));
  apply(canonical, trace(std::move(root), a));
  auto snapshot = snapshot_of(canonical);

  for (bool missing : {true, false}) {
    ActiveTrace current;
    auto existing = node(a, {c}, FinalityState::Confirmed);
    if (!missing) {
      existing->children.push_back(node(c, {t}, FinalityState::Confirmed, 300));
    }
    apply(current, trace(std::move(existing), a));
    const auto before = current;
    auto result = prepare_confirmed_promotion(current, snapshot->confirmed, "trace", 400, *snapshot);
    ASSERT_TRUE(result.is_error());
    ASSERT_EQ(kConfirmedPromotionUnavailable, result.error().code());
    ASSERT_TRUE(current.nodes.delta_to(before.nodes).empty());
    ASSERT_EQ(TraceStateFinality::Confirmed, current.root()->finality);
  }
}

TEST(TraceProcessor, promotion_only_changes_finality_and_preserves_pending_tail) {
  using namespace trace_test;
  auto a = message(1, true), c = message(2), t = message(3);
  ActiveTrace current;
  auto root = node(a, {c}, FinalityState::Confirmed);
  auto child = node(c, {t}, FinalityState::Confirmed, 200);
  child->children.push_back(node(t, {}, FinalityState::Emulated, 300));
  root->children.push_back(std::move(child));
  apply(current, trace(std::move(root), a));
  attach_actions(current, "current");
  auto snapshot = snapshot_of(current);
  auto prepared = prepare_confirmed_promotion(current, snapshot->confirmed, "trace", 400, *snapshot).move_as_ok();
  ASSERT_EQ(current.nodes.nodes().size(), prepared.next_trace.nodes.nodes().size());
  ASSERT_EQ(current.metadata, prepared.next_trace.metadata);
  ASSERT_EQ(current.actions.aai_refs, prepared.next_trace.actions.aai_refs);
  ASSERT_EQ(current.update_seq + 1, prepared.next_trace.update_seq);
  ASSERT_EQ(0u, trace_finality(prepared.next_trace));
  ASSERT_TRUE(prepared.redis.node_fields_to_delete.empty());
  ASSERT_TRUE(prepared.redis.indexes_to_remove.empty());
  ASSERT_TRUE(prepared.redis.indexes_to_add.empty());
  ASSERT_TRUE(!redis_field(prepared.redis, "root_node"));
  for (auto raw : {a, c}) {
    const auto* before = current.nodes.find(key(raw));
    const auto* after = prepared.next_trace.nodes.find(key(raw));
    ASSERT_EQ(before->transaction_hash(), after->transaction_hash());
    ASSERT_EQ(before->child_keys, after->child_keys);
    ASSERT_EQ(before->index_refs, after->index_refs);
    ASSERT_TRUE(before->transaction_boc == after->transaction_boc);
    ASSERT_EQ(TraceStateFinality::Finalized, after->finality);
    ASSERT_EQ(400u, after->mc_seqno);
  }
  ASSERT_TRUE(*current.nodes.find(key(t)) == *prepared.next_trace.nodes.find(key(t)));
  ASSERT_TRUE(!redis_field(prepared.redis, key(t)));
  ASSERT_EQ(current.actions.blob, prepared.next_trace.actions.blob);
  for (const auto& [channel, payload] : prepared.redis.publications) {
    if (channel == kStreamingTransactionsChannel) {
      StreamingTransactionHint hint;
      msgpack::unpack(payload.data(), payload.size()).get().convert(hint);
      ASSERT_EQ(0u, hint.trace_finality);
    }
  }

  auto again = prepare_confirmed_promotion(prepared.next_trace, snapshot->confirmed, "trace", 400, *snapshot).move_as_ok();
  ASSERT_TRUE(prepared.next_trace.nodes.delta_to(again.next_trace.nodes).empty());
  ASSERT_EQ(prepared.next_trace.update_seq, again.next_trace.update_seq);
  ASSERT_TRUE(!redis_field(again.redis, key(a)));
  ASSERT_TRUE(!redis_field(again.redis, key(c)));
}

TEST(TraceProcessor, promotion_of_same_execution_keeps_a_newer_confirmed_chain) {
  using namespace trace_test;
  auto a = message(1, true), child = message(2), tail = message(3);
  ActiveTrace current;
  auto root = node(a, {child}, FinalityState::Confirmed);
  td::Bits256 root_hash = root->transaction_root->get_hash().bits();
  apply(current, trace(std::move(root), a));
  auto snapshot = snapshot_of(current);
  auto continuation = node(child, {tail}, FinalityState::Confirmed);
  continuation->children.push_back(node(tail, {}, FinalityState::Emulated));
  apply(current, trace(std::move(continuation), a, root_hash));

  auto result = prepare_confirmed_promotion(current, snapshot->confirmed, "trace", 200, *snapshot);
  ASSERT_TRUE(result.is_ok());
  auto prepared = result.move_as_ok();
  ASSERT_EQ(TraceStateFinality::Finalized, prepared.next_trace.root()->finality);
  ASSERT_EQ(TraceStateFinality::Confirmed, prepared.next_trace.nodes.find(key(child))->finality);
  ASSERT_EQ(TraceStateFinality::Emulated, prepared.next_trace.nodes.find(key(tail))->finality);
  ASSERT_TRUE(prepared.redis.node_fields_to_delete.empty());
  ASSERT_TRUE(!redis_field(prepared.redis, key(child)));
  ASSERT_TRUE(!redis_field(prepared.redis, key(tail)));
}

TEST(TraceProcessor, snapshot_rejects_a_root_from_another_confirmed_variant) {
  using namespace trace_test;
  auto a = message(1, true), b = message(2, true);
  ActiveTrace state;
  auto original = node(a, {}, FinalityState::Confirmed);
  auto original_tx = key(original->transaction_root);
  apply(state, trace(node(b, {}, FinalityState::Confirmed), b));
  std::optional<td::Result<ConfirmedTraceSnapshot>> result;
  InsertCompletion completion{
      .confirmed = true,
      .confirmed_promise = td::PromiseCreator::lambda(
          [&](td::Result<ConfirmedTraceSnapshot> value) { result.emplace(std::move(value)); }),
      .root_key = key(a),
      .root_transaction_hash = original_tx,
  };
  completion.set_snapshot("trace", state, {});
  ASSERT_TRUE(result.has_value() && result->is_error());
  ASSERT_EQ(kConfirmedSnapshotRootMismatch, result->error().code());
}

TEST(TraceProcessor, real_redis_declined_promotion_uses_regular_finalized_update) {
  const auto* uri = std::getenv("TON_REDIS_TRANSPORT_TEST_URI");
  if (!uri) {
    LOG(INFO) << "Skipping real Redis root replacement: TON_REDIS_TRANSPORT_TEST_URI is unset";
    return;
  }
  using namespace trace_test;
  auto options = sw::redis::Uri(uri).connection_options();
  options.socket_timeout = std::chrono::seconds(2);
  sw::redis::Redis redis(options);
  const auto trace_key = "root-replacement-test:" + std::to_string(td::Time::now());
  const auto action_account = trace_key + ":account";
  std::vector<StreamingTransactionHint> notices;
  auto subscriber = redis.subscriber();
  subscriber.on_message([&](std::string, std::string payload) {
    StreamingTransactionHint hint;
    msgpack::unpack(payload.data(), payload.size()).get().convert(hint);
    notices.push_back(std::move(hint));
  });
  subscriber.subscribe(kStreamingTransactionsChannel);
  subscriber.consume();

  ActiveTrace state;
  auto write = [&](Trace fragment, const std::string& suffix) {
    TraceUpdate update;
    update.fragments.push_back(std::move(fragment));
    auto transition = TraceAssembler().apply_update(state, update, trace_key).move_as_ok();
    mch::EmuActionPayload payload;
    payload.state = "ok";
    payload.finality = trace_finality(transition.next_trace);
    payload.update_seq = transition.next_trace.update_seq;
    payload.actions_blob = actions_blob(payload.finality, suffix);
    payload.aai = {{action_account, trace_key + ":first" + suffix}};
    auto prepared =
        prepare_trace_materialization(state, std::move(transition), update, payload, trace_key, {}).move_as_ok();
    state = prepared.next_trace;
    auto notifications = std::count_if(prepared.redis.publications.begin(), prepared.redis.publications.end(),
                                       [](const auto& p) { return p.first == kStreamingTransactionsChannel; });
    commit_to_redis(uri, std::move(prepared.redis));
    while (notifications-- > 0) {
      subscriber.consume();
    }
    ASSERT_EQ(state.update_seq, notices.back().update_seq);
    ASSERT_EQ(trace_finality(state), notices.back().trace_finality);
    ASSERT_EQ(std::to_string(state.update_seq), *redis.hget(trace_key, "update_seq"));
  };
  auto a = message(1, true), b = message(2, true), old = message(3), other = message(4), tail = message(5);
  auto root_a = node(a, {old}, FinalityState::Confirmed);
  root_a->children.push_back(node(old, {tail}, FinalityState::Emulated));
  write(trace(std::move(root_a), a), "A");
  auto snapshot = snapshot_of(state);
  auto root_b = node(b, {other}, FinalityState::Confirmed);
  root_b->children.push_back(node(other, {}, FinalityState::Confirmed));
  write(trace(std::move(root_b), b), "B");
  ASSERT_TRUE(!redis.hget(trace_key, key(a)));
  ASSERT_TRUE(!redis.hget(trace_key, key(old)));
  ASSERT_EQ(key(b), *redis.hget(trace_key, "root_node"));
  ASSERT_TRUE(!redis.zscore("_aai:" + action_account, trace_key + ":firstA"));
  ASSERT_TRUE(redis.zscore("_aai:" + action_account, trace_key + ":firstB").has_value());
  ASSERT_EQ(trace_key, *redis.get("tr_in_msg:" + key(a)));
  ASSERT_EQ(trace_key, *redis.get("tr_in_msg:" + key(b)));

  auto declined = prepare_confirmed_promotion(state, snapshot->confirmed, trace_key, 200, *snapshot);
  ASSERT_TRUE(declined.is_error());
  ASSERT_EQ(kConfirmedPromotionUnavailable, declined.error().code());
  ASSERT_EQ(key(b), *redis.hget(trace_key, "root_node"));
  auto finalized = node(a, {old}, FinalityState::Finalized);
  auto child = node(old, {tail}, FinalityState::Finalized);
  child->children.push_back(node(tail, {}, FinalityState::Emulated));
  finalized->children.push_back(std::move(child));
  write(trace(std::move(finalized), a), "A");
  ASSERT_EQ(5u, notices.size());
  ASSERT_EQ(state.update_seq, notices.back().update_seq);
  ASSERT_EQ(0u, notices.back().trace_finality);
  ASSERT_TRUE(redis.hget(trace_key, key(tail)).has_value());
  ASSERT_EQ(TraceLifecycle::Open, classify_trace_lifecycle(state.nodes, key(a)));
  ASSERT_EQ(key(a), *redis.hget(trace_key, "root_node"));
  ASSERT_TRUE(!redis.hget(trace_key, key(b)));
  ASSERT_TRUE(!redis.hget(trace_key, key(other)));
  ASSERT_TRUE(redis.hget(trace_key, key(old)).has_value());
  ASSERT_TRUE(!redis.zscore("_aai:" + action_account, trace_key + ":firstB"));
  ASSERT_TRUE(redis.zscore("_aai:" + action_account, trace_key + ":firstA").has_value());
  auto blob = *redis.hget(trace_key, kActionsField);
  auto actions = msgpack::unpack(blob.data(), blob.size());
  ASSERT_EQ("firstA", map_field(actions.get().via.array.ptr[0], "action_id").as<std::string>());
  ASSERT_EQ(0u, map_field(actions.get().via.array.ptr[0], "finality").as<std::uint64_t>());
}

namespace {

class QueuedPromotionTest : public td::actor::Actor {
  RedisConnectionOptions options_;
  td::actor::ActorOwn<TraceProcessor> processor_;
  ConfirmedTraceSnapshot second_;

 public:
  explicit QueuedPromotionTest(RedisConnectionOptions options) : options_(std::move(options)) {}

  void start_up() override {
    processor_ = td::actor::create_actor<TraceProcessor>("QueuedPromotionProcessor", std::move(options_), TraceRetentionConfig{});
    auto raw = trace_test::message(101, true);
    auto update = make_trace_update(trace_test::trace(trace_test::node(raw, {}, FinalityState::Confirmed), raw), {});
    td::actor::send_closure(processor_, &TraceProcessor::process_confirmed_trace_update, std::move(update),
        td::PromiseCreator::lambda([self = actor_id(this)](td::Result<ConfirmedTraceSnapshot> result) {
          td::actor::send_closure(self, &QueuedPromotionTest::first_inserted, result.move_as_ok());
        }));
    alarm_timestamp() = td::Timestamp::in(5);
  }

  void first_inserted(ConfirmedTraceSnapshot first) {
    auto raw = trace_test::message(102, true);
    auto update = make_trace_update(trace_test::trace(trace_test::node(raw, {}, FinalityState::Confirmed), raw), {});
    // Promotion is enqueued while the replacement is still being classified
    // or written. Compatibility must be checked after that write, not here.
    td::actor::send_closure(processor_, &TraceProcessor::process_confirmed_trace_update, std::move(update),
        td::PromiseCreator::lambda([self = actor_id(this)](td::Result<ConfirmedTraceSnapshot> result) {
          td::actor::send_closure(self, &QueuedPromotionTest::second_inserted, result.move_as_ok());
        }));
    td::actor::send_closure(processor_, &TraceProcessor::promote_confirmed,
        std::vector<ConfirmedTraceSnapshot>{std::move(first)}, 200,
        td::PromiseCreator::lambda([self = actor_id(this)](td::Result<td::Unit> result) {
          ASSERT_TRUE(result.is_error());
          ASSERT_EQ(kConfirmedPromotionUnavailable, result.error().code());
          td::actor::send_closure(self, &QueuedPromotionTest::promote_current);
        }));
  }

  void second_inserted(ConfirmedTraceSnapshot second) { second_ = std::move(second); }

  void promote_current() {
    ASSERT_TRUE(second_ != nullptr);
    td::actor::send_closure(processor_, &TraceProcessor::promote_confirmed,
        std::vector<ConfirmedTraceSnapshot>{second_}, 200,
        td::PromiseCreator::lambda([](td::Result<td::Unit> result) {
          result.ensure();
          td::actor::SchedulerContext::get().stop();
        }));
  }

  void alarm() override { LOG(FATAL) << "Queued promotion test timed out"; }
};

}  // namespace

TEST(TraceProcessor, real_redis_promotion_checks_state_when_its_queue_item_runs) {
  const auto* uri = std::getenv("TON_REDIS_TRANSPORT_TEST_URI");
  if (!uri) {
    LOG(INFO) << "Skipping queued promotion: TON_REDIS_TRANSPORT_TEST_URI is unset";
    return;
  }
  td::actor::Scheduler scheduler({1});
  td::actor::ActorOwn<QueuedPromotionTest> test;
  scheduler.run_in_context([&] {
    test = td::actor::create_actor<QueuedPromotionTest>("QueuedPromotionTest",
                                                       parse_redis_connection_options(uri).move_as_ok());
  });
  scheduler.run();
  scheduler.run_in_context([&] { test.reset(); });
  sw::redis::Redis redis(uri);
  const auto trace_key = td::base64_encode(trace_test::hash('f').as_slice());
  ASSERT_EQ(trace_test::key(trace_test::message(102, true)), *redis.hget(trace_key, "root_node"));
  ASSERT_TRUE(!redis.hget(trace_key, trace_test::key(trace_test::message(101, true))));
  auto serialized = *redis.hget(trace_key, trace_test::key(trace_test::message(102, true)));
  RedisTraceNode node;
  msgpack::unpack(serialized.data(), serialized.size()).get().convert(node);
  ASSERT_EQ(FinalityState::Finalized, node.finality);
}

TEST(TraceProcessor, declined_promotion_waits_for_other_trace_writes) {
  for (bool error_first : {true, false}) {
    std::optional<td::Result<td::Unit>> result;
    ConfirmedPromotionCompletion completion{
        .remaining = 2,
        .promise = td::PromiseCreator::lambda([&](td::Result<td::Unit> value) { result.emplace(std::move(value)); }),
    };
    auto error = td::Status::Error(kConfirmedPromotionUnavailable, "changed root");
    completion.one_finished(error_first ? td::Result<td::Unit>(std::move(error)) : td::Result<td::Unit>(td::Unit()));
    ASSERT_TRUE(!result.has_value());
    completion.one_finished(error_first ? td::Result<td::Unit>(td::Unit()) : td::Result<td::Unit>(std::move(error)));
    ASSERT_TRUE(result.has_value() && result->is_error());
    ASSERT_EQ(kConfirmedPromotionUnavailable, result->error().code());
  }
}

TEST(TraceProcessor, classification_telemetry_names_are_stable) {
  ASSERT_EQ(std::string("classified"), classification_outcome_name(mch::EmuClassifyOutcome::classified));
  ASSERT_EQ(std::string("classify_failed"), classification_outcome_name(mch::EmuClassifyOutcome::classify_failed));
  ASSERT_EQ(std::string("convert_failed"), classification_outcome_name(mch::EmuClassifyOutcome::convert_failed));
  ASSERT_EQ(std::string_view("emulator.trace_processor.queue_full"),
            ticker_names.at(TRACE_PROCESSOR_QUEUE_FULL));
  ASSERT_EQ(std::string_view("emulator.classify.trace.micros"), histogram_names.at(CLASSIFY_TRACE));
}

TEST(TraceProcessor, update_requires_one_trace_id_finality_and_block_update) {
  TraceUpdate valid;
  valid.fragments.push_back(trace_fragment('a', FinalityState::Finalized, 10));
  valid.fragments.push_back(trace_fragment('a', FinalityState::Finalized, 10));
  ASSERT_TRUE(validate_trace_update(valid).is_ok());

  TraceUpdate mixed_ids;
  mixed_ids.fragments.push_back(trace_fragment('a', FinalityState::Finalized, 10));
  mixed_ids.fragments.push_back(trace_fragment('b', FinalityState::Finalized, 10));
  ASSERT_TRUE(validate_trace_update(mixed_ids).is_error());

  TraceUpdate mixed_finality;
  mixed_finality.fragments.push_back(trace_fragment('a', FinalityState::Confirmed, 10));
  mixed_finality.fragments.push_back(trace_fragment('a', FinalityState::Finalized, 10));
  ASSERT_TRUE(validate_trace_update(mixed_finality).is_error());

  TraceUpdate mixed_blocks;
  mixed_blocks.fragments.push_back(trace_fragment('a', FinalityState::Finalized, 10));
  mixed_blocks.fragments.push_back(trace_fragment('a', FinalityState::Finalized, 11));
  ASSERT_TRUE(validate_trace_update(mixed_blocks).is_error());
}

TEST(TraceProcessor, update_materialization_publishes_one_logical_update) {
  ActiveTrace current;
  TraceTransition transition;
  transition.needs_redis_write = true;
  transition.raw_external_message_hash = "raw-message";
  transition.next_trace.update_seq = 9;
  transition.next_trace.finality = FinalityState::Finalized;
  transition.next_trace.metadata.emplace("root_node", "root");

  std::vector<TraceStateNode> nodes;
  for (const auto& key : {std::string("left"), std::string("right")}) {
    nodes.push_back(TraceStateNode{
        .key = key,
        .finality = TraceStateFinality::Finalized,
        .serialized = std::make_shared<const std::string>("node:" + key),
        .index_refs = {TraceStateIndexRef{
            .index_key = "account:" + key,
            .member = "trace:" + key,
            .score = 1,
        }},
    });
    transition.accepted_nodes.push_back(AcceptedNode{
        .key = key,
        .finality = FinalityState::Finalized,
    });
  }
  auto state_change = transition.next_trace.nodes.upsert_nodes(std::move(nodes));
  transition.next_trace.nodes.apply(std::move(state_change));
  transition.node_delta = current.nodes.delta_to(transition.next_trace.nodes);

  TraceUpdate update;
  update.fragments.push_back(trace_fragment('a', FinalityState::Finalized, 10));
  update.fragments.push_back(trace_fragment('a', FinalityState::Finalized, 10));
  mch::EmuActionPayload payload;
  payload.state = "ok";
  payload.finality = 2;
  payload.update_seq = 9;

  auto result = prepare_trace_materialization(current, std::move(transition), update, payload, "trace", {});
  ASSERT_TRUE(result.is_ok());
  auto prepared = result.move_as_ok();

  ASSERT_EQ(2u, prepared.redis.publications.size());
  ASSERT_EQ(std::string(kStreamingTransactionsChannel), prepared.redis.publications[0].first);
  ASSERT_EQ(std::string(kStreamingActionsChannel), prepared.redis.publications[1].first);
  ASSERT_EQ(std::string("9"), *redis_field(prepared.redis, "update_seq"));
  ASSERT_EQ(2u, prepared.accepted_nodes.size());
}

TEST(TraceProcessor, ready_work_is_routed_by_write_capacity_requirement) {
  TraceSlot slot;
  ASSERT_EQ(TraceReadyQueue::None, next_ready_queue(slot));

  slot.queued.emplace_back(InsertRequest{});
  ASSERT_EQ(TraceReadyQueue::General, next_ready_queue(slot));

  slot.queued.clear();
  slot.classification.emplace(ClassificationWork{});
  ASSERT_EQ(TraceReadyQueue::None, next_ready_queue(slot));

  slot.classification->payload.emplace();
  ASSERT_EQ(TraceReadyQueue::Write, next_ready_queue(slot));

  slot.classification.reset();
  slot.cleanup_requested = true;
  ASSERT_EQ(TraceReadyQueue::Write, next_ready_queue(slot));
}

TEST(TraceProcessor, saturated_writes_do_not_drain_the_write_ready_queue) {
  ASSERT_EQ(TraceReadyQueue::Write,
            next_queue_to_drain(kMaxConcurrentWrites - 1, true, 0, false));
  ASSERT_EQ(TraceReadyQueue::None,
            next_queue_to_drain(kMaxConcurrentWrites, true, 0, false));
  ASSERT_EQ(TraceReadyQueue::General,
            next_queue_to_drain(kMaxConcurrentWrites, true, 1, true));
}

TEST(TraceProcessor, queue_snapshot_accounts_for_pending_updates) {
  std::unordered_map<std::string, TraceSlot> traces;

  auto& queued = traces["queued"];
  queued.queued.emplace_back(InsertRequest{});
  queued.queued.emplace_back(InsertRequest{});
  queued.scheduled_queue = TraceReadyQueue::General;

  traces["classifying"].classification.emplace(ClassificationWork{});

  auto& classified = traces["classified"];
  classified.classification.emplace(ClassificationWork{});
  classified.classification->payload.emplace();
  classified.scheduled_queue = TraceReadyQueue::Write;

  traces["writing"].in_flight.emplace(InFlightWork{.counted_update = true});
  auto& cleanup = traces["cleanup"];
  cleanup.in_flight.emplace(InFlightWork{.kind = InFlightKind::Cleanup});
  cleanup.cleanup_requested = true;
  traces["promotion"].queued.emplace_back(PromoteConfirmedRequest{});

  const auto snapshot = collect_queue_snapshot(traces);
  ASSERT_EQ(2u, snapshot.queued_updates);
  ASSERT_EQ(1u, snapshot.classifying);
  ASSERT_EQ(1u, snapshot.classified_waiting_write);
  ASSERT_EQ(1u, snapshot.in_flight_updates);
  ASSERT_EQ(1u, snapshot.in_flight_cleanups);
  ASSERT_EQ(1u, snapshot.cleanup_requested);
  ASSERT_EQ(1u, snapshot.promotions_waiting_write);
  ASSERT_EQ(1u, snapshot.scheduled_general);
  ASSERT_EQ(1u, snapshot.scheduled_writes);
  ASSERT_EQ(2u, snapshot.max_slot_queue);
  ASSERT_EQ(std::string("queued"), snapshot.max_slot_trace);

  const auto formatted = format_queue_snapshot(snapshot, 5, traces.size(), 1, 1, 1);
  ASSERT_TRUE(formatted.find("pending_updates=5") != std::string::npos);
  ASSERT_TRUE(formatted.find("classifying=1") != std::string::npos);
  ASSERT_TRUE(formatted.find("classified_waiting_write=1") != std::string::npos);
  ASSERT_TRUE(formatted.find("active_writes=1") != std::string::npos);
}

TEST(TraceProcessor, promotion_rewrites_all_action_finalities_and_keeps_content) {
  auto prepared = promote(trace_with_actions(FinalityState::Finalized, 1));

  auto rewritten = redis_field(prepared.redis, kActionsField);
  ASSERT_TRUE(rewritten.has_value());
  ASSERT_EQ(std::string("2"), *redis_field(prepared.redis, kActionsFinalityField));
  ASSERT_EQ(std::uint8_t{2}, *prepared.next_trace.actions.blob_finality);
  ASSERT_EQ(*rewritten, *prepared.next_trace.actions.blob);

  auto unpacked = msgpack::unpack(rewritten->data(), rewritten->size());
  const auto& rows = unpacked.get();
  ASSERT_EQ(2u, rows.via.array.size);
  ASSERT_EQ(std::string("first"), map_field(rows.via.array.ptr[0], "action_id").as<std::string>());
  ASSERT_TRUE(map_field(rows.via.array.ptr[0], "success").as<bool>());
  ASSERT_EQ(std::uint64_t{17}, map_field(rows.via.array.ptr[0], "amount").as<std::uint64_t>());
  ASSERT_EQ(std::uint64_t{2}, map_field(rows.via.array.ptr[0], "finality").as<std::uint64_t>());
  ASSERT_EQ(std::string("second"), map_field(rows.via.array.ptr[1], "action_id").as<std::string>());
  ASSERT_TRUE(!map_field(rows.via.array.ptr[1], "success").as<bool>());
  ASSERT_EQ(std::uint64_t{29}, map_field(rows.via.array.ptr[1], "amount").as<std::uint64_t>());
  ASSERT_EQ(std::uint64_t{2}, map_field(rows.via.array.ptr[1], "finality").as<std::uint64_t>());
}

TEST(TraceProcessor, promotion_without_actions_writes_no_action_fields) {
  auto prepared = promote(trace_with_actions(FinalityState::Finalized, std::nullopt));

  ASSERT_TRUE(!redis_field(prepared.redis, kActionsField).has_value());
  ASSERT_TRUE(!redis_field(prepared.redis, kActionsFinalityField).has_value());
  ASSERT_TRUE(!redis_field(prepared.redis, kActionsStateField).has_value());
}

TEST(TraceProcessor, promotion_at_equal_finality_still_rewrites_actions) {
  auto prepared = promote(trace_with_actions(FinalityState::Confirmed, 1));

  ASSERT_TRUE(redis_field(prepared.redis, kActionsField).has_value());
  ASSERT_EQ(std::string("1"), *redis_field(prepared.redis, kActionsFinalityField));
  ASSERT_EQ(std::uint8_t{1}, *prepared.next_trace.actions.blob_finality);
}

TEST(TraceProcessor, promotion_cannot_demote_actions) {
  auto current = trace_with_actions(FinalityState::Emulated, 1);
  auto original_blob = *current.actions.blob;
  auto prepared = promote(std::move(current));

  ASSERT_TRUE(!redis_field(prepared.redis, kActionsField).has_value());
  ASSERT_TRUE(!redis_field(prepared.redis, kActionsFinalityField).has_value());
  ASSERT_EQ(std::uint8_t{1}, *prepared.next_trace.actions.blob_finality);
  ASSERT_EQ(original_blob, *prepared.next_trace.actions.blob);
  ASSERT_TRUE(!prepared.next_trace.actions.blob_is_current);
}

TEST(TraceProcessor, failed_classification_keeps_blob_but_marks_it_stale_for_streaming) {
  auto current = trace_with_actions(FinalityState::Confirmed, 1);
  mch::EmuActionPayload failed;
  failed.state = "convert_failed";
  failed.finality = 1;
  failed.update_seq = 2;

  auto prepared = prepare_action_update(current.actions, failed);

  ASSERT_TRUE(!prepared.actions_updated);
  ASSERT_TRUE(prepared.state.blob.has_value());
  ASSERT_TRUE(!prepared.state.blob_is_current);
  ASSERT_EQ(current.actions.aai_refs, prepared.state.aai_refs);
}

TEST(TraceProcessor, failed_classification_publishes_an_empty_actions_hint) {
  auto trace = trace_with_actions(FinalityState::Confirmed, 1);
  trace.update_seq = 7;
  RedisWritePlan plan;

  append_streaming_actions_hint(plan, trace, "trace", StreamingUpdateFinality::Confirmed, false);

  ASSERT_EQ(1u, plan.publications.size());
  ASSERT_EQ(std::string(kStreamingActionsChannel), plan.publications[0].first);
  StreamingActionsHint hint;
  const auto& payload = plan.publications[0].second;
  msgpack::unpack(payload.data(), payload.size()).get().convert(hint);
  ASSERT_EQ("trace", hint.trace_key);
  ASSERT_EQ(7u, hint.update_seq);
  ASSERT_EQ(1u, hint.update_finality);
  ASSERT_EQ(1u, hint.trace_finality);
  ASSERT_TRUE(!hint.actions_updated);
  ASSERT_TRUE(hint.action_types_and_accounts.empty());
}

TEST(TraceProcessor, cleanup_removes_aai_refs) {
  const TraceStateIndexRef node_ref{
      .index_key = "account:node",
      .member = "trace:node",
      .score = 11,
  };
  const TraceStateIndexRef aai_ref{
      .index_key = "_aai:account",
      .member = "trace:action",
      .score = 17,
  };
  ActiveTrace current;
  auto node_change = current.nodes.upsert_nodes({TraceStateNode{
      .key = "node",
      .index_refs = {node_ref},
  }});
  current.nodes.apply(std::move(node_change));
  current.actions.aai_refs = {aai_ref};
  TraceSlot slot;
  slot.current = std::make_shared<const ActiveTrace>(std::move(current));

  const auto refs = collect_cleanup_index_refs(slot);

  ASSERT_EQ(2u, refs.size());
  ASSERT_TRUE(std::find(refs.begin(), refs.end(), node_ref) != refs.end());
  ASSERT_TRUE(std::find(refs.begin(), refs.end(), aai_ref) != refs.end());
}

TEST(TraceProcessor, cleanup_of_actionless_trace_unchanged) {
  const TraceStateIndexRef node_ref{
      .index_key = "account:node",
      .member = "trace:node",
      .score = 11,
  };
  ActiveTrace current;
  auto node_change = current.nodes.upsert_nodes({TraceStateNode{
      .key = "node",
      .index_refs = {node_ref},
  }});
  current.nodes.apply(std::move(node_change));
  TraceSlot slot;
  slot.current = std::make_shared<const ActiveTrace>(std::move(current));

  const auto refs = collect_cleanup_index_refs(slot);

  ASSERT_EQ(1u, refs.size());
  ASSERT_EQ(node_ref, refs[0]);
}
