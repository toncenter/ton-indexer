#include <algorithm>
#include <cstdint>
#include <iterator>
#include <map>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "BlockEmulator.h"
#include "TraceInterfaceDetector.h"

namespace {
class InterblockTraceStore {
 public:
  void put(td::Bits256 key, TraceIds value) {
    std::lock_guard<std::mutex> lock(mutex_);
    trace_ids_[key] = std::move(value);
  }

  bool get(const td::Bits256& key, TraceIds& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = trace_ids_.find(key);
    if (it == trace_ids_.end()) {
      return false;
    }
    value = it->second;
    return true;
  }

 private:
  std::mutex mutex_;
  std::unordered_map<td::Bits256, TraceIds> trace_ids_;
};

InterblockTraceStore& finalized_interblock_trace_store() {
  static InterblockTraceStore store;
  return store;
}

InterblockTraceStore& confirmed_interblock_trace_store() {
  static InterblockTraceStore store;
  return store;
}

struct TraceTailUpdate {
  TraceUpdate update;
  std::vector<EmuRequest> requests;
};

using TraceTailUpdates = std::map<TraceId, TraceTailUpdate>;

Trace make_trace_fragment(const TraceIds& trace_ids, std::unique_ptr<TraceNode> root) {
  Trace trace;
  trace.ext_in_msg_hash = trace_ids.ext_in_msg_hash;
  trace.ext_in_msg_hash_norm = trace_ids.ext_in_msg_hash_norm;
  trace.root_tx_hash = trace_ids.root_tx_hash;
  trace.root = std::move(root);
  return trace;
}

void add_trace_fragment(TraceTailUpdates& updates, const TraceIds& trace_ids, std::unique_ptr<TraceNode> root,
                        std::vector<EmuRequest> requests) {
  auto& update = updates[trace_ids.ext_in_msg_hash_norm];
  update.update.fragments.push_back(make_trace_fragment(trace_ids, std::move(root)));
  update.requests.insert(update.requests.end(), std::make_move_iterator(requests.begin()),
                         std::make_move_iterator(requests.end()));
}

void collect_emulated_addresses(const TraceNode* node, std::unordered_set<block::StdAddress>& addresses) {
  if (!node) {
    return;
  }
  if (node->finality_state == FinalityState::Emulated) {
    addresses.insert(node->address);
  }
  for (const auto& child : node->children) {
    collect_emulated_addresses(child.get(), addresses);
  }
}

MeasurementPtr start_update_tail_span(const TraceTailUpdate& tail_update) {
  auto measurement = tail_update.update.measurement;
  if (measurement) {
    measurement->set_otel_attribute("ton.trace_state.update_fragments_count",
                                    static_cast<std::int64_t>(tail_update.update.size()));
    measurement->set_otel_attribute("ton.trace.update_tail_requests_count",
                                    static_cast<std::int64_t>(tail_update.requests.size()));
    measurement->start_otel_child_span("emulate_tail");
  }
  return measurement;
}

void finish_update_tail_span(const TraceUpdate& update) {
  if (!update.measurement) {
    return;
  }
  std::int64_t transactions_count = 0;
  std::int64_t emulated_transactions_count = 0;
  std::int64_t depth = 0;
  bool tx_limit_exceeded = false;
  for (const auto& trace : update.fragments) {
    transactions_count += trace.transactions_count();
    emulated_transactions_count += trace.root ? trace.root->emulated_transactions_count() : 0;
    depth = std::max(depth, static_cast<std::int64_t>(trace.depth()));
    tx_limit_exceeded = tx_limit_exceeded || trace.tx_limit_exceeded;
  }
  update.measurement->set_transactions_count(transactions_count);
  update.measurement->set_emulated_transactions_count(emulated_transactions_count);
  update.measurement->set_otel_attribute("ton.trace.depth", depth);
  update.measurement->set_otel_attribute("ton.trace.tx_limit_exceeded", tx_limit_exceeded);
  update.measurement->end_otel_child_span("emulate_tail");
}

td::Status attach_emulated_tails(TraceUpdate& update, std::vector<std::unique_ptr<TraceNode>> child_nodes,
                                 const std::vector<EmuRequest>& requests,
                                 const std::shared_ptr<EmulationContext>& context) {
  if (child_nodes.size() != requests.size()) {
    return td::Status::Error("Tail emulator returned an unexpected number of roots");
  }
  for (std::size_t index = 0; index < child_nodes.size(); ++index) {
    const auto& request = requests[index];
    if (!request.parent || request.insert_index >= request.parent->children.size()) {
      return td::Status::Error("Invalid tail attachment point");
    }
    request.parent->children[request.insert_index] = std::move(child_nodes[index]);
  }

  if (!context) {
    return td::Status::OK();
  }

  const auto rand_seed = context->get_rand_seed();
  const auto tx_limit_exceeded = context->is_limit_exceeded();
  auto account_states = context->release_account_states();
  for (auto& trace : update.fragments) {
    trace.rand_seed = rand_seed;
    trace.tx_limit_exceeded = tx_limit_exceeded;
    std::unordered_set<block::StdAddress> addresses;
    collect_emulated_addresses(trace.root.get(), addresses);
    for (const auto& [address, account] : account_states) {
      if (addresses.count(address) != 0) {
        trace.emulated_accounts.emplace(address, account);
      }
    }
  }
  return td::Status::OK();
}
}  // namespace


class BlockParser: public td::actor::Actor {
    td::Ref<ton::validator::BlockData> block_data_;
    ton::BlockSeqno mc_block_seqno_;
    td::Promise<std::vector<TransactionInfo>> promise_;
    MeasurementPtr measurement_;
public:
    BlockParser(td::Ref<ton::validator::BlockData> block_data, ton::BlockSeqno mc_block_seqno, td::Promise<std::vector<TransactionInfo>> promise, const MeasurementPtr& measurement)
        : block_data_(std::move(block_data)), mc_block_seqno_(mc_block_seqno), promise_(std::move(promise)), measurement_(measurement) {}

    void start_up() override {
        std::vector<TransactionInfo> res;

        block::gen::Block::Record blk;
        block::gen::BlockInfo::Record info;
        block::gen::BlockExtra::Record extra;
        if (!(tlb::unpack_cell(block_data_->root_cell(), blk) && tlb::unpack_cell(blk.info, info) && tlb::unpack_cell(blk.extra, extra))) {
            promise_.set_error(td::Status::Error("block data info extra unpack failed"));
            stop();
            return;
        }
        try {
            vm::AugmentedDictionary acc_dict{vm::load_cell_slice_ref(extra.account_blocks), 256, block::tlb::aug_ShardAccountBlocks};

            td::Bits256 cur_addr = td::Bits256::zero();
            bool eof = false;
            bool allow_same = true;
            while (!eof) {
                auto value = acc_dict.extract_value(
                    acc_dict.vm::DictionaryFixed::lookup_nearest_key(cur_addr.bits(), 256, true, allow_same));
                if (value.is_null()) {
                    eof = true;
                    break;
                }
                allow_same = false;
                block::gen::AccountBlock::Record acc_blk;
                if (!(tlb::csr_unpack(std::move(value), acc_blk) && acc_blk.account_addr == cur_addr)) {
                    promise_.set_error(td::Status::Error("invalid AccountBlock for account " + cur_addr.to_hex()));
                    stop();
                    return;
                }
                vm::AugmentedDictionary trans_dict{vm::DictNonEmpty(), std::move(acc_blk.transactions), 64,
                                                    block::tlb::aug_AccountTransactions};
                td::BitArray<64> cur_trans{(long long)0};
                while (true) {
                    auto tvalue = trans_dict.extract_value_ref(
                        trans_dict.vm::DictionaryFixed::lookup_nearest_key(cur_trans.bits(), 64, true));
                    if (tvalue.is_null()) {
                        break;
                    }
                    block::gen::Transaction::Record trans;
                    if (!tlb::unpack_cell(tvalue, trans)) {
                        promise_.set_error(td::Status::Error("Failed to unpack Transaction"));
                        stop();
                        return;
                    }
                    block::gen::TransactionDescr::Record_trans_ord descr;
                    if (!tlb::unpack_cell(trans.description, descr)) {
                        continue;
                    }

                    TransactionInfo tx_info;

                    tx_info.account = block::StdAddress(block_data_->block_id().id.workchain, cur_addr);
                    tx_info.hash = tvalue->get_hash().bits();
                    tx_info.root = tvalue;
                    tx_info.lt = trans.lt;
                    tx_info.block_id = block_data_->block_id().id;
                    tx_info.mc_block_seqno = mc_block_seqno_;

                    if (trans.r1.in_msg->prefetch_long(1)) {
                        auto msg = trans.r1.in_msg->prefetch_ref();
                        tx_info.in_msg_hash = msg->get_hash().bits();
                        auto message_cs = vm::load_cell_slice(trans.r1.in_msg->prefetch_ref());
                        auto msg_tag = block::gen::t_CommonMsgInfo.get_tag(message_cs);
                        if (msg_tag == block::gen::CommonMsgInfo::ext_in_msg_info) {
                            tx_info.trace_ids = TraceIds{
                                .root_tx_hash = tx_info.hash,
                                .ext_in_msg_hash = msg->get_hash().bits(),
                                .ext_in_msg_hash_norm = ext_in_msg_get_normalized_hash(msg).move_as_ok()
                            };
                        } else if (msg_tag == block::gen::CommonMsgInfo::int_msg_info) {
                            block::gen::CommonMsgInfo::Record_int_msg_info msg_info;
                            block::StdAddress source;
                            if (tlb::unpack(message_cs, msg_info) &&
                                block::tlb::t_MsgAddressInt.extract_std_address(msg_info.src, source) &&
                                source.workchain == ton::masterchainId && source.addr.is_zero()) {
                                // Protocol-generated masterchain messages have no external-message trace root.
                                continue;
                            }
                        }
                    } else {
                        LOG(ERROR) << "Ordinary transaction without in_msg, skipping";
                        continue;
                    }

                    // LOG(INFO) << "TX hash: " << tx_info.hash.to_hex();

                    if (trans.outmsg_cnt != 0) {
                        vm::Dictionary dict{trans.r1.out_msgs, 15};
                        for (int x = 0; x < trans.outmsg_cnt; x++) {
                            auto value = dict.lookup_ref(td::BitArray<15>{x});
                            OutMsgInfo out_msg_info;
                            out_msg_info.hash = value->get_hash().bits();
                            out_msg_info.root = value;
                            tx_info.out_msgs.push_back(std::move(out_msg_info));

                            // LOG(INFO) << "  out msg: " << out_msg_info.hash.to_hex();
                        }
                    }

                    res.push_back(tx_info);
                }
            }
        } catch (const vm::VmError& err) {
            promise_.set_error(td::Status::Error(PSLICE() << "error while parsing AccountBlocks : " << err.get_msg()));
            stop();
            return;
        }
        promise_.set_value(std::move(res));
        stop();
    }
};

McBlockEmulator::McBlockEmulator(schema::MasterchainBlockDataState mc_data_state,
                                 std::function<void(ton::BlockSeqno)>
                                     trace_ids_resolved,
                                 bool reuse_confirmed_state,
                                 td::Promise<FinalizedBlockResult> promise)
    : mc_data_state_(std::move(mc_data_state)),
      trace_ids_resolved_(std::move(trace_ids_resolved)),
      promise_(std::move(promise)),
      blocks_left_to_parse_(mc_data_state_.shard_blocks_diff_.size()),
      reuse_confirmed_state_(reuse_confirmed_state) {
}

void McBlockEmulator::start_up() {
    start_time_ = td::Timestamp::now();
    measurement_ = std::make_shared<Measurement>();
    measurement_->set_finality("finalized");
    measurement_->set_operation("read_finalized");
    measurement_->set_source("block");
    for (const auto& shard_state : mc_data_state_.shard_blocks_) {
        shard_states_.push_back(shard_state.block_state);
    }
    if (blocks_left_to_parse_ == 0) {
        resolve_trace_ids();
        return;
    }
    auto mc_block_seqno = mc_data_state_.shard_blocks_[0].handle->id().seqno();
    // The anchor keeps interior transaction cells readable after this actor stops.
    // Build it before spawning parsers so callbacks see a complete anchor.
    auto cell_anchor = std::make_shared<std::vector<td::Ref<vm::Cell>>>();
    cell_anchor->reserve(mc_data_state_.shard_blocks_diff_.size());
    for (auto& block_data : mc_data_state_.shard_blocks_diff_) {
        cell_anchor->push_back(block_data.block_data->root_cell());
    }
    cell_anchor_ = std::move(cell_anchor);
    for (auto& block_data : mc_data_state_.shard_blocks_diff_) {
        LOG(INFO) << "Parsing block " << block_data.block_data->block_id().to_str();
        auto block_measurement = measurement_->clone();
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), blk_id = block_data.block_data->block_id().id, block_measurement](td::Result<std::vector<TransactionInfo>> R) {
            if (R.is_error()) {
                td::actor::send_closure(SelfId, &McBlockEmulator::parse_error, blk_id, R.move_as_error(), block_measurement);
                return;
            }
            td::actor::send_closure(
                SelfId,
                &McBlockEmulator::block_parsed,
                blk_id,
                R.move_as_ok());
        });
        td::actor::create_actor<BlockParser>("BlockParser", block_data.block_data, mc_block_seqno, std::move(P), block_measurement).release();
    }
}

void McBlockEmulator::parse_error(ton::BlockId blkid, td::Status error, MeasurementPtr) {
    LOG(ERROR) << "Failed to parse block " << blkid.to_str() << ": " << error;
    promise_.set_error(std::move(error));
    stop();
}

void McBlockEmulator::block_parsed(
    ton::BlockId,
    std::vector<TransactionInfo> txs) {
    txs_.insert(txs_.end(), txs.begin(), txs.end());
    blocks_left_to_parse_--;
    if (blocks_left_to_parse_ == 0) {
        resolve_trace_ids();
    }
}

void McBlockEmulator::resolve_trace_ids() {
    std::sort(txs_.begin(), txs_.end(), [](const TransactionInfo& a, const TransactionInfo& b) {
        return a.lt < b.lt;
    });

    for (auto& tx : txs_) {
        for (const auto& out_msg : tx.out_msgs) {
            tx_by_out_msg_hash_.insert({out_msg.hash, tx});
        }
    }

    for (auto& tx : txs_) {
        if (tx.trace_ids.has_value()) {
            // we already have trace_ids for this tx
        } else if (tx_by_out_msg_hash_.find(tx.in_msg_hash) != tx_by_out_msg_hash_.end() && 
                   tx_by_out_msg_hash_[tx.in_msg_hash].trace_ids.has_value()) {
            tx.trace_ids = tx_by_out_msg_hash_[tx.in_msg_hash].trace_ids;
        } else {
            TraceIds cached_ids;
            if (finalized_interblock_trace_store().get(
                    tx.in_msg_hash, cached_ids)) {
                tx.trace_ids = cached_ids;
            } else {
                LOG(WARNING) << "Couldn't get ext_in_msg_hash_norm for tx " << tx.hash.to_hex() << ". This tx will be skipped.";
            }
        }

        // write trace_id for out_msgs for interblock chains
        if (tx.trace_ids.has_value()) {
            for (const auto& out_msg : tx.out_msgs) {
                finalized_interblock_trace_store().put(
                    out_msg.hash, tx.trace_ids.value());
                confirmed_interblock_trace_store().put(
                    out_msg.hash, tx.trace_ids.value());
            }
        }
        tx_by_in_msg_hash_.insert({tx.in_msg_hash, tx});
    }
    auto mc_seqno =
        mc_data_state_.shard_blocks_[0].handle->id().seqno();
    trace_ids_resolved_(mc_seqno);
    if (reuse_confirmed_state_) {
        const auto has_masterchain_trace =
            std::any_of(txs_.begin(), txs_.end(), [](const auto& tx) {
                return tx.block_id.is_masterchain() &&
                       tx.trace_ids.has_value();
            });
        if (!has_masterchain_trace) {
            LOG(INFO) << "Reusing confirmed shard state for mc block "
                      << mc_seqno
                      << "; finalized trace emulation is not needed";
            finish_block_if_done();
            return;
        }
        LOG(INFO) << "Cannot fully reuse confirmed shard state for mc block "
                  << mc_seqno
                  << " because the masterchain block contributes to a trace";
        reuse_confirmed_state_ = false;
    }
    emulate_traces(measurement_);
}

std::unique_ptr<TraceNode> McBlockEmulator::construct_commited_trace(const TransactionInfo& tx, std::vector<EmuRequest>& reqs, MeasurementPtr, size_t depth) {
    auto trace_node = std::make_unique<TraceNode>();
    trace_node->finality_state = FinalityState::Finalized;
    trace_node->transaction_root = tx.root;
    trace_node->node_id = tx.in_msg_hash;
    trace_node->address = tx.account;
    trace_node->block_id = tx.block_id;  
    trace_node->mc_block_seqno = tx.mc_block_seqno;

    for (const auto& out_msg : tx.out_msgs) {
        int type;
        auto destination_r = fetch_msg_dest_address(out_msg.root, type);
        if (type == block::gen::CommonMsgInfo::ext_out_msg_info) {
            continue;
        }
        if (destination_r.is_error()) {
            LOG(ERROR) << "Failed to fetch destination address for out_msg " << out_msg.hash.to_hex();
            continue;
        }

        if (auto it = tx_by_in_msg_hash_.find(out_msg.hash); it != tx_by_in_msg_hash_.end()) {
            TransactionInfo& child_tx = it->second;
            if (!child_tx.trace_ids.has_value()) {
                LOG(WARNING) << "No trace ids for child tx " << child_tx.hash.to_hex();
                child_tx.trace_ids = tx.trace_ids;
            }
            auto child = construct_commited_trace(child_tx, reqs, nullptr, depth + 1);
            trace_node->children.push_back(std::move(child));
        } else {
            // remember where to attach the emulated node
            size_t idx = trace_node->children.size();
            reqs.push_back(EmuRequest{
                trace_node.get(),
                idx,
                out_msg.root,
                out_msg.hash,
                depth + 1
            });
            // to "fill holes" later
            trace_node->children.push_back(nullptr);
        }
    }
    return trace_node;
}

void McBlockEmulator::emulate_traces(MeasurementPtr measurement) {
  // Roots disconnected inside this block may still belong to one logical
  // trace. Group their holes before emulation so account state is shared.
  std::map<TraceId, std::vector<TransactionInfo*>> roots_by_trace;
  for (auto& tx : txs_) {
    if (!tx.trace_ids.has_value()) {
      continue;
    }
    if (tx_by_out_msg_hash_.find(tx.in_msg_hash) != tx_by_out_msg_hash_.end()) {
      continue;
    }
    roots_by_trace[tx.trace_ids->ext_in_msg_hash_norm].push_back(&tx);
  }

  TraceTailUpdates tail_updates;
  for (auto& [trace_id, roots] : roots_by_trace) {
    CHECK(!roots.empty() && roots.front()->trace_ids.has_value());
    const auto& trace_ids = *roots.front()->trace_ids;
    auto& tail_update = tail_updates[trace_id];
    tail_update.update.measurement = measurement->clone();
    tail_update.update.measurement->set_ext_msg_hash(trace_ids.ext_in_msg_hash);
    tail_update.update.measurement->set_ext_msg_hash_norm(trace_ids.ext_in_msg_hash_norm);
    tail_update.update.measurement->set_trace_root_tx_hash(trace_ids.root_tx_hash);
    tail_update.update.measurement->set_otel_attribute("ton.trace_state.update_fragments_count",
                                                       static_cast<std::int64_t>(roots.size()));
    tail_update.update.measurement->start_otel_child_span("build_trace_tree");

    for (auto* tx : roots) {
      std::vector<EmuRequest> requests;
      auto root = construct_commited_trace(*tx, requests, tail_update.update.measurement);
      add_trace_fragment(tail_updates, *tx->trace_ids, std::move(root), std::move(requests));
    }

    tail_update.update.measurement->set_otel_attribute("ton.trace.tail_requests_count",
                                                       static_cast<std::int64_t>(tail_update.requests.size()));
    tail_update.update.measurement->end_otel_child_span("build_trace_tree");
  }

  in_progress_cnt_ += tail_updates.size();
  for (auto& [_, tail_update] : tail_updates) {
    auto update_measurement = start_update_tail_span(tail_update);
    if (tail_update.requests.empty()) {
      children_emulated(std::move(tail_update.update), {}, {}, nullptr);
      continue;
    }

    auto context = std::make_shared<EmulationContext>(mc_data_state_.shard_blocks_[0].handle->id().id.seqno,
                                                      mc_data_state_.config_);
    for (const auto& shard_state : mc_data_state_.shard_blocks_) {
      auto blkid = shard_state.handle->id().id;
      auto timestamp = shard_state.handle->unix_time();
      auto lt = shard_state.handle->logical_time();
      lt = lt - lt % block::ConfigInfo::get_lt_align();
      context->add_shard_state(blkid, timestamp, lt, shard_state.block_state);
    }
    context->increase_seqno(3);
    std::vector<EmulationMessage> msgs_to_emulate;
    msgs_to_emulate.reserve(tail_update.requests.size());
    for (auto& r : tail_update.requests) {
      msgs_to_emulate.push_back(EmulationMessage{r.msg, r.depth});
    }

    auto P = td::PromiseCreator::lambda(
        [SelfId = actor_id(this), update = std::move(tail_update.update), context,
         reqs = std::move(tail_update.requests)](td::Result<std::vector<std::unique_ptr<TraceNode>>> R) mutable {
          if (R.is_error()) {
            td::actor::send_closure(SelfId, &McBlockEmulator::trace_update_error, std::move(update), R.move_as_error());
            return;
          }
          td::actor::send_closure(SelfId, &McBlockEmulator::children_emulated, std::move(update), R.move_as_ok(),
                                  std::move(reqs), std::move(context));
        });

    td::actor::create_actor<MasterchainBlockEmulator>("MasterchainBlockEmulator", context, std::move(msgs_to_emulate),
                                                      std::move(P), update_measurement)
        .release();
  }
  finish_block_if_done();
}

void McBlockEmulator::children_emulated(TraceUpdate update, std::vector<std::unique_ptr<TraceNode>> child_nodes,
                                        std::vector<EmuRequest> reqs, std::shared_ptr<EmulationContext> context) {
  auto attach_status = attach_emulated_tails(update, std::move(child_nodes), reqs, context);
  if (attach_status.is_error()) {
    trace_update_error(std::move(update), std::move(attach_status));
    return;
  }

  finish_update_tail_span(update);
  for (auto& trace : update.fragments) {
    trace.cell_anchor = cell_anchor_;
    // Carry the detector's lookup context on for the classifier's tier-2 hook.
    trace.shard_states = shard_states_;
    trace.config = mc_data_state_.config_;
  }

  if constexpr (std::variant_size_v<Trace::Detector::DetectedInterface> > 0) {
    const auto trace_root_tx_hash = update.fragments.front().root_tx_hash;
    auto measurement = update.measurement;
    auto P = td::PromiseCreator::lambda(
        [SelfId = actor_id(this), trace_root_tx_hash, measurement](td::Result<TraceUpdate> result) mutable {
          if (result.is_error()) {
            td::actor::send_closure(SelfId, &McBlockEmulator::trace_interfaces_error, trace_root_tx_hash,
                                    result.move_as_error(), measurement);
            return;
          }
          td::actor::send_closure(SelfId, &McBlockEmulator::trace_emulated, result.move_as_ok());
        });
    td::actor::create_actor<TraceUpdateInterfaceDetector>("TraceUpdateInterfaceDetector", shard_states_,
                                                          mc_data_state_.config_, std::move(update), std::move(P))
        .release();
  } else {
    trace_emulated(std::move(update));
  }
}

void McBlockEmulator::trace_update_error(TraceUpdate update, td::Status error) {
  auto error_text = error.to_string();
  for (const auto& trace : update.fragments) {
    LOG(ERROR) << "Failed to emulate trace with root tx " << td::base64_encode(trace.root_tx_hash.as_slice()) << ": "
               << error_text;
  }
  if (update.measurement) {
    update.measurement->mark_otel_error("trace_emulator.processing_error", error_text);
    update.measurement->end_otel_child_span("emulate_tail");
    update.measurement->emit_otel_span();
  }
  CHECK(in_progress_cnt_ > 0);
  in_progress_cnt_--;
  finish_block_if_done();
}

void McBlockEmulator::trace_interfaces_error(td::Bits256 trace_root_tx_hash, td::Status error, MeasurementPtr measurement) {
    LOG(ERROR) << "Failed to detect interfaces on trace with root tx " << td::base64_encode(trace_root_tx_hash.as_slice()) << ": " << error;
    if (measurement) {
      measurement->mark_otel_error("trace_emulator.interface_error", error.to_string());
      measurement->emit_otel_span();
    }
    CHECK(in_progress_cnt_ > 0);
    in_progress_cnt_--;
    finish_block_if_done();
}

void McBlockEmulator::trace_emulated(TraceUpdate update) {
  traces_cnt_ += static_cast<int>(update.size());
  trace_updates_.push_back(std::move(update));
  CHECK(in_progress_cnt_ > 0);
  in_progress_cnt_--;
  finish_block_if_done();
}

void McBlockEmulator::finish_block_if_done() {
    if (finished_ || in_progress_cnt_ != 0) {
        return;
    }
    finished_ = true;
    auto blkid = mc_data_state_.shard_blocks_[0].block_data->block_id().id;
    std::sort(trace_updates_.begin(), trace_updates_.end(), [](const TraceUpdate& lhs, const TraceUpdate& rhs) {
      return lhs.fragments.front().ext_in_msg_hash_norm < rhs.fragments.front().ext_in_msg_hash_norm;
    });
    LOG(INFO) << "Finished emulating block " << blkid.to_str() << ": " << traces_cnt_ << " traces in "
              << (td::Timestamp::now().at() - start_time_.at()) * 1000 << " ms; grouped into " << trace_updates_.size()
              << " updates";
    std::vector<ton::BlockIdExt> finalized_blocks;
    finalized_blocks.reserve(mc_data_state_.shard_blocks_diff_.size());
    std::vector<td::Ref<ton::validator::BlockData>> block_data_owners;
    block_data_owners.reserve(mc_data_state_.shard_blocks_diff_.size());
    for (const auto& block : mc_data_state_.shard_blocks_diff_) {
        finalized_blocks.push_back(block.block_data->block_id());
        block_data_owners.push_back(block.block_data);
    }
    promise_.set_value(FinalizedBlockResult{
        .mc_seqno = blkid.seqno,
        .finalized_blocks = std::move(finalized_blocks),
        .trace_updates = std::move(trace_updates_),
        .trace_fragments_count = static_cast<std::size_t>(traces_cnt_),
        .reused_confirmed_state = reuse_confirmed_state_,
        .block_data_owners = std::move(block_data_owners),
    });
    stop();
}

void ConfirmedBlockEmulator::start_up() {
    start_time_ = td::Timestamp::now();
    // See McBlockEmulator::start_up: this block's traces reference interior cells
    // of its boc, and only its root cell keeps that boc alive.
    cell_anchor_ = std::make_shared<std::vector<td::Ref<vm::Cell>>>(
        std::vector<td::Ref<vm::Cell>>{block_data_state_.block_data->root_cell()});
    auto measurement = std::make_shared<Measurement>();
    measurement->set_finality(finality_ == FinalityState::Confirmed ? "confirmed" : "finalized");
    measurement->set_operation("read_finalized");
    measurement->set_source("block");
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), measurement](td::Result<std::vector<TransactionInfo>> R) {
        if (R.is_error()) {
            td::actor::send_closure(SelfId, &ConfirmedBlockEmulator::parse_error, R.move_as_error(), measurement);
        } else {
            td::actor::send_closure(SelfId, &ConfirmedBlockEmulator::block_parsed, R.move_as_ok(), measurement);
        }
    });
    auto actor_name = PSLICE() << finality_label() << "BlockParser" << static_cast<int>(block_data_state_.block_data->block_id().id.seqno);
    td::actor::create_actor<BlockParser>(actor_name, block_data_state_.block_data,
                                         config_->block_id.id.seqno + 1, // this block is not committed in mc yet, so +1
                                         std::move(P), measurement)
        .release();
}

void ConfirmedBlockEmulator::parse_error(td::Status error, MeasurementPtr) {
    LOG(ERROR) << "Failed to parse " << finality_label() << " block " << block_data_state_.block_data->block_id().to_str() << ": " << error;
    head_finished_(block_data_state_.block_data->block_id());
    promise_.set_error(std::move(error));
    stop();
}

void ConfirmedBlockEmulator::block_parsed(std::vector<TransactionInfo> txs, MeasurementPtr measurement) {
    txs_ = std::move(txs);
    resolve_trace_ids(measurement);
}

void ConfirmedBlockEmulator::resolve_trace_ids(MeasurementPtr measurement) {
    std::sort(txs_.begin(), txs_.end(), [](const TransactionInfo& a, const TransactionInfo& b) {
        return a.lt < b.lt;
    });

    for (auto& tx : txs_) {
        for (const auto& out_msg : tx.out_msgs) {
            tx_by_out_msg_hash_.insert({out_msg.hash, tx});
        }
    }

    for (auto& tx : txs_) {
        if (tx.trace_ids.has_value()) {
            // already set
        } else if (tx_by_out_msg_hash_.find(tx.in_msg_hash) != tx_by_out_msg_hash_.end() &&
                   tx_by_out_msg_hash_[tx.in_msg_hash].trace_ids.has_value()) {
            tx.trace_ids = tx_by_out_msg_hash_[tx.in_msg_hash].trace_ids;
        } else {
            TraceIds cached_ids;
            if (confirmed_interblock_trace_store().get(
                    tx.in_msg_hash, cached_ids)) {
                tx.trace_ids = cached_ids;
            } else {
                LOG(WARNING) << "Couldn't get ext_in_msg_hash_norm for confirmed tx " << tx.hash.to_hex() << ". Skipping.";
                reusable_ = false;
            }
        }

        if (tx.trace_ids.has_value()) {
            for (const auto& out_msg : tx.out_msgs) {
                confirmed_interblock_trace_store().put(
                    out_msg.hash, tx.trace_ids.value());
            }
        }
        tx_by_in_msg_hash_.insert({tx.in_msg_hash, tx});
    }
    head_finished_(block_data_state_.block_data->block_id());
    emulate_traces(measurement);
}

void ConfirmedBlockEmulator::emulate_traces(MeasurementPtr measurement) {
  // A signed block may contain several disconnected components of the same
  // trace. Their speculative tails must advance one shared account state.
  std::map<TraceId, std::vector<TransactionInfo*>> roots_by_trace;
  for (auto& tx : txs_) {
    if (!tx.trace_ids.has_value()) {
      continue;
    }
    if (tx_by_out_msg_hash_.find(tx.in_msg_hash) != tx_by_out_msg_hash_.end()) {
      continue;
    }
    roots_by_trace[tx.trace_ids->ext_in_msg_hash_norm].push_back(&tx);
  }

  TraceTailUpdates tail_updates;
  for (auto& [trace_id, roots] : roots_by_trace) {
    CHECK(!roots.empty() && roots.front()->trace_ids.has_value());
    const auto& trace_ids = *roots.front()->trace_ids;
    auto& tail_update = tail_updates[trace_id];
    tail_update.update.measurement = measurement->clone();
    tail_update.update.measurement->set_ext_msg_hash(trace_ids.ext_in_msg_hash);
    tail_update.update.measurement->set_ext_msg_hash_norm(trace_ids.ext_in_msg_hash_norm);
    tail_update.update.measurement->set_trace_root_tx_hash(trace_ids.root_tx_hash);
    tail_update.update.measurement->set_otel_attribute("ton.trace_state.update_fragments_count",
                                                       static_cast<std::int64_t>(roots.size()));
    tail_update.update.measurement->start_otel_child_span("build_trace_tree");

    for (auto* tx : roots) {
      std::vector<EmuRequest> requests;
      auto root = construct_confirmed_trace(*tx, requests, tail_update.update.measurement);
      add_trace_fragment(tail_updates, *tx->trace_ids, std::move(root), std::move(requests));
      trace_fragments_count_++;
    }

    tail_update.update.measurement->set_otel_attribute("ton.trace.tail_requests_count",
                                                       static_cast<std::int64_t>(tail_update.requests.size()));
    tail_update.update.measurement->end_otel_child_span("build_trace_tree");
  }

  trace_updates_count_ = tail_updates.size();
  in_progress_cnt_ = tail_updates.size();
  for (auto& [_, tail_update] : tail_updates) {
    auto update_measurement = start_update_tail_span(tail_update);
    if (tail_update.requests.empty()) {
      children_emulated(std::move(tail_update.update), {}, {}, nullptr);
      continue;
    }

    if (!config_ || shard_states_snapshot_.empty()) {
      LOG(ERROR) << "Missing config or shard state snapshot for " << finality_label() << " block tails";
      reusable_ = false;
      children_emulated(std::move(tail_update.update), {}, {}, nullptr);
      continue;
    }

    auto context = std::make_shared<EmulationContext>(config_->block_id.id.seqno + 1, config_);
    for (const auto& snapshot : shard_states_snapshot_) {
      auto lt = snapshot.logical_time - snapshot.logical_time % block::ConfigInfo::get_lt_align();
      context->add_shard_state(snapshot.blkid, snapshot.timestamp, lt, snapshot.state);
    }
    context->increase_seqno(3);
    std::vector<EmulationMessage> msgs_to_emulate;
    msgs_to_emulate.reserve(tail_update.requests.size());
    for (auto& r : tail_update.requests) {
      msgs_to_emulate.push_back(EmulationMessage{r.msg, r.depth});
    }

    auto P = td::PromiseCreator::lambda(
        [SelfId = actor_id(this), update = std::move(tail_update.update), context,
         reqs = std::move(tail_update.requests)](td::Result<std::vector<std::unique_ptr<TraceNode>>> R) mutable {
          if (R.is_error()) {
            td::actor::send_closure(SelfId, &ConfirmedBlockEmulator::trace_update_error, std::move(update),
                                    R.move_as_error());
            return;
          }
          td::actor::send_closure(SelfId, &ConfirmedBlockEmulator::children_emulated, std::move(update), R.move_as_ok(),
                                  std::move(reqs), std::move(context));
        });

    auto actor_name = PSLICE() << finality_label() << "TailEmulator";
    td::actor::create_actor<MasterchainBlockEmulator>(actor_name, context, std::move(msgs_to_emulate), std::move(P),
                                                      update_measurement)
        .release();
  }

  all_trace_updates_started_ = true;
  if (in_progress_cnt_ == 0) {
    finish_block();
  }
}

std::unique_ptr<TraceNode> ConfirmedBlockEmulator::construct_confirmed_trace(const TransactionInfo& tx, std::vector<EmuRequest>& reqs, MeasurementPtr, size_t depth) {
    auto trace_node = std::make_unique<TraceNode>();
    trace_node->finality_state = finality_;
    trace_node->transaction_root = tx.root;
    trace_node->node_id = tx.in_msg_hash;
    trace_node->address = tx.account;
    trace_node->block_id = tx.block_id;
    trace_node->mc_block_seqno = tx.mc_block_seqno;

    for (const auto& out_msg : tx.out_msgs) {
        int type;
        auto destination_r = fetch_msg_dest_address(out_msg.root, type);
        if (type == block::gen::CommonMsgInfo::ext_out_msg_info) {
            continue;
        }
        if (destination_r.is_error()) {
            LOG(ERROR) << "Failed to fetch destination address for out_msg " << out_msg.hash.to_hex();
            reusable_ = false;
            continue;
        }

        auto it = tx_by_in_msg_hash_.find(out_msg.hash);
        if (it != tx_by_in_msg_hash_.end()) {
            auto child = construct_confirmed_trace(it->second, reqs, nullptr, depth + 1);
            trace_node->children.push_back(std::move(child));
        } else {
            size_t idx = trace_node->children.size();
            reqs.push_back(EmuRequest{
                trace_node.get(),
                idx,
                out_msg.root,
                out_msg.hash,
                depth + 1
            });
            trace_node->children.push_back(nullptr);
        }
    }
    return trace_node;
}

void ConfirmedBlockEmulator::children_emulated(TraceUpdate update, std::vector<std::unique_ptr<TraceNode>> child_nodes,
                                               std::vector<EmuRequest> reqs,
                                               std::shared_ptr<EmulationContext> context) {
  auto attach_status = attach_emulated_tails(update, std::move(child_nodes), reqs, context);
  if (attach_status.is_error()) {
    trace_update_error(std::move(update), std::move(attach_status));
    return;
  }

  std::vector<td::Ref<vm::Cell>> shard_states;
  shard_states.reserve(shard_states_snapshot_.size());
  for (const auto& snapshot : shard_states_snapshot_) {
    shard_states.push_back(snapshot.state);
  }

  finish_update_tail_span(update);
  for (auto& trace : update.fragments) {
    trace.cell_anchor = cell_anchor_;
    // Carry the detector's lookup context on for the classifier's tier-2 hook.
    trace.shard_states = shard_states;
    trace.config = config_;
  }

  if constexpr (std::variant_size_v<Trace::Detector::DetectedInterface> > 0) {
    const auto trace_root_tx_hash = update.fragments.front().root_tx_hash;
    auto measurement = update.measurement;
    auto P = td::PromiseCreator::lambda(
        [SelfId = actor_id(this), trace_root_tx_hash, measurement](td::Result<TraceUpdate> result) mutable {
          if (result.is_error()) {
            td::actor::send_closure(SelfId, &ConfirmedBlockEmulator::trace_interfaces_error, trace_root_tx_hash,
                                    result.move_as_error(), measurement);
            return;
          }
          td::actor::send_closure(SelfId, &ConfirmedBlockEmulator::trace_emulated, result.move_as_ok());
        });
    td::actor::create_actor<TraceUpdateInterfaceDetector>(
        "ConfirmedTraceUpdateInterfaceDetector", std::move(shard_states), config_, std::move(update), std::move(P))
        .release();
  } else {
    trace_emulated(std::move(update));
  }
}

void ConfirmedBlockEmulator::trace_update_error(TraceUpdate update, td::Status error) {
  reusable_ = false;
  auto error_text = error.to_string();
  for (const auto& trace : update.fragments) {
    LOG(ERROR) << "Failed to emulate " << finality_label() << " trace with root tx "
               << td::base64_encode(trace.root_tx_hash.as_slice()) << ": " << error_text;
  }
  if (update.measurement) {
    update.measurement->mark_otel_error("trace_emulator.processing_error", error_text);
    update.measurement->end_otel_child_span("emulate_tail");
  }
  trace_update_finished({}, false, std::move(update.measurement));
}

void ConfirmedBlockEmulator::trace_interfaces_error(td::Bits256 trace_root_tx_hash, td::Status error, MeasurementPtr measurement) {
    LOG(ERROR) << "Failed to detect interfaces on " << finality_label() << " trace with root tx "
               << td::base64_encode(trace_root_tx_hash.as_slice()) << ": " << error;
    if (measurement) {
      measurement->mark_otel_error("trace_emulator.interface_error", error.to_string());
    }
    trace_update_finished({}, false, std::move(measurement));
}

void ConfirmedBlockEmulator::trace_emulated(TraceUpdate update) {
  const auto root_hash = update.fragments.front().root_tx_hash;
  auto measurement = update.measurement;
  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), root_hash, label = std::string(finality_label()),
                                       measurement](td::Result<ConfirmedTraceSnapshot> result) mutable {
    bool success = true;
    ConfirmedTraceSnapshot snapshot;
    if (result.is_error()) {
      success = false;
      auto error = result.move_as_error();
      LOG(ERROR) << "Failed to insert " << label << " trace " << td::base64_encode(root_hash.as_slice()) << ": "
                 << error;
      if (measurement) {
        measurement->mark_otel_error("trace_emulator.insert_error", error.to_string());
      }
    } else {
      snapshot = result.move_as_ok();
      // An empty successful snapshot means that this trace was
      // intentionally skipped and does not make the block unusable.
      LOG(DEBUG) << "Processed " << label << " trace " << td::base64_encode(root_hash.as_slice());
    }
    td::actor::send_closure(SelfId, &ConfirmedBlockEmulator::trace_update_finished, std::move(snapshot), success,
                            measurement);
  });
  trace_processor_(std::move(update), std::move(P));
}

void ConfirmedBlockEmulator::trace_update_finished(ConfirmedTraceSnapshot snapshot, bool success,
                                                   MeasurementPtr measurement) {
  if (in_progress_cnt_ == 0) {
    return;
  }
  reusable_ = reusable_ && success;
  if (snapshot) {
    snapshots_.push_back(std::move(snapshot));
  }
  if (measurement) {
    measurement->end_otel_child_span("insert_trace");
    measurement->emit_otel_span();
  }
  in_progress_cnt_--;
  if (in_progress_cnt_ == 0 && all_trace_updates_started_) {
    finish_block();
  }
}

void ConfirmedBlockEmulator::finish_block() {
  LOG(INFO) << "Finished " << finality_label() << " block " << block_data_state_.block_data->block_id().to_str() << ": "
            << trace_fragments_count_ << " traces in " << (td::Timestamp::now().at() - start_time_.at()) * 1000
            << " ms; grouped into " << trace_updates_count_ << " updates";
  promise_.set_value(ConfirmedBlockResult{
      .reusable = reusable_,
      .snapshots = std::move(snapshots_),
  });
  stop();
}
