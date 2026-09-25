#include "FinalizedTraceProcessor.h"

#include <algorithm>
#include <limits>

#include "Statistics.h"
#include "block/block-auto.h"
#include "td/utils/base64.h"

using namespace trace_materialization;

FinalizedTraceProcessor::FinalizedTraceProcessor(double completed_seconds, mch::EmuClassifierConfig classifier)
    : completed_seconds_(completed_seconds) {
  if (classifier.prep) {
    for (int i = 0; i < std::max(1, classifier.workers); ++i) {
      classifiers_.push_back(td::actor::create_actor<mch::EmuClassifierActor>(
          "FinalizedClassifier-" + std::to_string(i), classifier, static_cast<std::size_t>(i)));
      idle_classifiers_.push_back(i);
    }
  }
}

void FinalizedTraceProcessor::prepare_block(ParsedFinalizedBlock block, std::function<void(RedisWritePlan)> on_plan,
                                            td::Promise<td::Unit> promise) {
  if (block_) {
    promise.set_error(td::Status::Error("Finalized processor already has an active block"));
    return;
  }
  CHECK(on_plan);
  block_.emplace(BlockWork{.block = std::move(block), .on_plan = std::move(on_plan), .promise = std::move(promise)});
  td::Status status;
  try {
    status = assemble_block();
  } catch (const vm::VmError& error) {
    status = td::Status::Error("Cannot assemble finalized block: " + std::string(error.get_msg()));
  } catch (const std::exception& error) {
    status = td::Status::Error("Cannot assemble finalized block: " + std::string(error.what()));
  }
  if (status.is_error()) {
    block_->error = std::move(status);
    finish_block();
    return;
  }
  if (block_->traces.empty()) {
    finish_block();
    return;
  }
  for (auto& [key, work] : block_->traces) {
    auto done = td::PromiseCreator::lambda([self = actor_id(this), key](td::Result<DetectedAccounts> result) mutable {
      td::actor::send_closure(self, &FinalizedTraceProcessor::accounts_ready, key, std::move(result));
    });
    td::actor::create_actor<AccountStatesDetector>("FinalizedAccounts", block_->block.shard_states,
                                                   block_->block.config, work.addresses, std::move(done))
        .release();
  }
}

td::Status FinalizedTraceProcessor::assemble_block() {
  auto& pending = *block_;
  auto& block = pending.block;
  std::unordered_set<td::Bits256> seen;
  for (const auto& tx : block.transactions) {
    if (tx.mc_block_seqno != block.seqno || tx.root.is_null() || !seen.insert(tx.in_msg_hash).second) {
      return td::Status::Error("Invalid or duplicate finalized transaction");
    }
  }
  std::sort(block.transactions.begin(), block.transactions.end(),
            [](const auto& a, const auto& b) { return a.lt != b.lt ? a.lt < b.lt : a.hash < b.hash; });
  for (const auto& tx : block.transactions) {
    auto ids = tx.trace_ids;
    const bool root = ids.has_value();  // Only external-in roots carry IDs from the parser.
    auto incoming = awaiting_messages_.find(tx.in_msg_hash);
    if (!ids && incoming != awaiting_messages_.end()) ids = incoming->second;
    if (incoming != awaiting_messages_.end()) awaiting_messages_.erase(incoming);
    if (!ids) {
      // Starting at head deliberately skips executions whose root was not observed.
      continue;
    }
    const auto key = td::base64_encode(ids->ext_in_msg_hash_norm.as_slice());
    auto current = traces_.find(key);
    if (!root && current == traces_.end() && !pending.traces.count(key) && !oversized_traces_.count(key)) {
      continue;
    }
    for (const auto& msg : tx.out_msgs) {
      if (block::gen::t_CommonMsgInfo.get_tag(vm::load_cell_slice(msg.root)) ==
          block::gen::CommonMsgInfo::int_msg_info) {
        awaiting_messages_.insert_or_assign(msg.hash, *ids);
      }
    }
    auto [position, inserted] = pending.traces.try_emplace(key);
    auto& work = position->second;
    if (inserted) {
      work.ids = *ids;
      if (current != traces_.end()) work.next_trace = current->second.trace;
    } else if (work.ids.ext_in_msg_hash != ids->ext_in_msg_hash || work.ids.root_tx_hash != ids->root_tx_hash) {
      return td::Status::Error("Finalized block contains conflicting roots for one trace");
    }
    work.addresses.insert(tx.account);
    work.oversized = work.oversized || oversized_traces_.count(key);
    if (work.oversized) continue;
    TRY_RESULT(node, prepare_finalized_node(tx, key));
    const auto previous_root =
        root ? trace_metadata_value(work.next_trace, "root_node").value_or(std::string{}) : std::string{};
    auto node_key = node.key;
    work.next_trace.nodes.apply_update(TraceStateUpdate{std::move(node_key), {std::move(node)}}, previous_root);
    if (root)
      work.next_trace.metadata.insert_or_assign("root_node", td::base64_encode(ids->ext_in_msg_hash.as_slice()));
    if (work.next_trace.nodes.nodes().size() > kMaxCachedTraceNodes) {
      work.oversized = true;
      work.next_trace = ActiveTrace{};
    }
  }

  for (auto& [key, work] : pending.traces) {
    auto current = traces_.find(key);
    if (work.oversized) {
      if (current != traces_.end())
        work.cleanup = build_trace_cleanup(key, current->second.trace);
      else {
        work.cleanup.trace_key = key;
        work.cleanup.erase_trace = true;
      }
      work.cleanup.raw_external_message_hash = td::base64_encode(work.ids.ext_in_msg_hash.as_slice());
      continue;
    }
    auto& trace = work.next_trace;
    trace.metadata.insert_or_assign("depth_limit_exceeded", "0");
    trace.update_seq = block.seqno;
    trace.finality = FinalityState::Finalized;
    CHECK(trace.root());
    trace.root_account = trace.root()->index_refs.front().index_key;
  }
  // Block time makes retention deterministic. Never emit cleanup alongside an update of the same trace.
  for (auto it = traces_.begin(); it != traces_.end();) {
    if (!pending.traces.count(it->first) && block.unix_time > it->second.updated_at &&
        block.unix_time - it->second.updated_at > completed_seconds_ && !finalized_trace_is_open(it->second.trace)) {
      auto plan = build_trace_cleanup(it->first, it->second.trace);
      it = traces_.erase(it);
      pending.on_plan(std::move(plan));
    } else
      ++it;
  }
  return td::Status::OK();
}

void FinalizedTraceProcessor::accounts_ready(std::string key, td::Result<DetectedAccounts> result) {
  CHECK(block_ && block_->traces.count(key));
  if (result.is_error()) {
    // Same policy as McBlockEmulator's interface stage: omit the failed trace update.
    LOG(ERROR) << "Finalized interface detection failed for " << key << ": " << result.move_as_error();
    finish_trace(key);
    return;
  }
  auto& work = block_->traces.at(key);
  work.accounts = result.move_as_ok();
  if (work.oversized) {
    auto status = append_account_state_writes(work.cleanup, work.accounts, FinalityState::Finalized);
    if (status.is_ok()) {
      LOG(WARNING) << "Dropping oversized finalized trace " << key << "; limit=" << kMaxCachedTraceNodes;
      block_->on_plan(std::move(work.cleanup));
      traces_.erase(key);
      oversized_traces_.insert(key);
    }
    finish_trace(key, std::move(status));
    return;
  }
  apply_detected_accounts(work.next_trace, work.accounts);
  block_->ready.push_back(std::move(key));
  classify_ready();
}

void FinalizedTraceProcessor::classify_ready() {
  while (block_ && !block_->ready.empty() && (classifiers_.empty() || !idle_classifiers_.empty())) {
    auto key = std::move(block_->ready.front());
    block_->ready.pop_front();
    auto& work = block_->traces.at(key);
    auto view =
        TraceAssembler().build_full_trace(work.next_trace, key, block_->block.shard_states, block_->block.config);
    if (view.is_error()) {
      finish_trace(key, view.move_as_error());
      continue;
    }
    if (classifiers_.empty()) {
      mch::EmuActionPayload payload;
      payload.finality = static_cast<std::uint8_t>(FinalityState::Finalized);
      payload.update_seq = block_->block.seqno;
      emit_snapshot(key, std::move(payload));
      continue;
    }
    auto worker = idle_classifiers_.front();
    idle_classifiers_.pop_front();
    work.classification_timer.resume();
    auto done = td::PromiseCreator::lambda(
        [self = actor_id(this), key, worker](td::Result<mch::EmuClassifyResult> result) mutable {
          td::actor::send_closure(self, &FinalizedTraceProcessor::classified, key, worker, std::move(result));
        });
    td::actor::send_closure(classifiers_[worker], &mch::EmuClassifierActor::classify, view.move_as_ok(),
                            mch::emu_now_us(), std::move(done));
  }
}

void FinalizedTraceProcessor::classified(std::string key, std::size_t worker,
                                         td::Result<mch::EmuClassifyResult> result) {
  idle_classifiers_.push_back(worker);
  CHECK(block_ && block_->traces.count(key));
  auto& work = block_->traces.at(key);
  g_statistics.record_time(CLASSIFY_TRACE, work.classification_timer.elapsed() * 1e6);
  if (result.is_error()) {
    LOG(WARNING) << "Finalized classification response lost for " << key << ": " << result.move_as_error();
    mch::EmuActionPayload payload;
    payload.state = "response_lost";
    payload.finality = static_cast<std::uint8_t>(FinalityState::Finalized);
    payload.update_seq = block_->block.seqno;
    emit_snapshot(key, std::move(payload));
  } else {
    auto classified = result.move_as_ok();
    CHECK(classified.trace_id == key);
    emit_snapshot(key, std::move(classified.payload));
  }
  classify_ready();
}

void FinalizedTraceProcessor::emit_snapshot(const std::string& key, mch::EmuActionPayload payload) {
  auto& work = block_->traces.at(key);
  if (payload.update_seq != block_->block.seqno ||
      payload.finality != static_cast<std::uint8_t>(FinalityState::Finalized)) {
    finish_trace(key, td::Status::Error("Classifier result does not match the finalized trace"));
    return;
  }
  auto status = td::Status::OK();
  try {
    work.next_trace.actions = prepare_action_update(work.next_trace.actions, payload).state;
    auto plan = build_finalized_snapshot(key, work.next_trace, static_cast<std::uint32_t>(completed_seconds_));
    status = append_account_state_writes(plan, work.accounts, FinalityState::Finalized);
    if (status.is_ok()) {
      auto current = traces_.find(key);
      if (current != traces_.end()) plan.indexes_to_remove = collect_trace_index_refs(current->second.trace);
      block_->on_plan(std::move(plan));
      traces_.insert_or_assign(key, Entry{std::move(work.next_trace), block_->block.unix_time});
    }
  } catch (const vm::VmError& error) {
    status = td::Status::Error("Cannot prepare finalized snapshot: " + std::string(error.get_msg()));
  } catch (const std::exception& error) {
    status = td::Status::Error("Cannot prepare finalized snapshot: " + std::string(error.what()));
  }
  finish_trace(key, std::move(status));
}

void FinalizedTraceProcessor::finish_trace(const std::string& key, td::Status status) {
  CHECK(block_);
  if (status.is_error() && !block_->error) block_->error = std::move(status);
  CHECK(block_->traces.erase(key) == 1);
  if (block_->traces.empty()) finish_block();
}

void FinalizedTraceProcessor::finish_block() {
  auto finished = std::move(*block_);
  block_.reset();
  if (finished.error)
    finished.promise.set_error(std::move(*finished.error));
  else
    finished.promise.set_value(td::Unit());
}

void FinalizedTraceProcessor::tear_down() {
  if (block_) {
    block_->error = td::Status::Error("Finalized processor stopped during block preparation");
    finish_block();
  }
}
