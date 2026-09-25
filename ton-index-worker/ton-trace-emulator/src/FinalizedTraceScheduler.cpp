#include "FinalizedTraceScheduler.h"

#include "block/block.h"
#include "ton/ton-tl.hpp"

#include <algorithm>

FinalizedTraceScheduler::FinalizedTraceScheduler(td::actor::ActorId<DbScanner> scanner,
    td::actor::ActorId<FinalizedTraceProcessor> processor, RedisConnectionOptions redis,
    std::string db_event_fifo)
    : scanner_(scanner), processor_(processor),
      writer_(td::actor::create_actor<RedisMaterializer>("FinalizedRedisWriter", std::move(redis), kMaxWrites)),
      db_event_fifo_(std::move(db_event_fifo)) {}

void FinalizedTraceScheduler::start_up() {
  if (!db_event_fifo_.empty()) {
    db_event_listener_ = td::actor::create_actor<DbEventListener>("DbEventListener", db_event_fifo_,
        [self = actor_id(this)](ton::tl_object_ptr<ton::ton_api::db_Event> event) {
          td::actor::send_closure(self, &FinalizedTraceScheduler::handle_db_event, std::move(event));
        });
  }
  alarm_timestamp() = td::Timestamp::in(0.01);
}

void FinalizedTraceScheduler::handle_db_event(ton::tl_object_ptr<ton::ton_api::db_Event> event) {
  if (event->get_id() != ton::ton_api::db_event_blockApplied::ID) {
    return;
  }
  auto block = ton::create_block_id(static_cast<ton::ton_api::db_event_blockApplied&>(*event).block_id_);
  if (!block.is_masterchain() || block.seqno() <= head_) {
    return;
  }
  notified_head_ = std::max(notified_head_, block.seqno());
  if (!busy_) {
    alarm_timestamp().relax(td::Timestamp::now());
  }
}

void FinalizedTraceScheduler::alarm() {
  if (block_active_) {
    drive_block();
    return;
  }
  if (busy_) {
    return;
  }
  busy_ = true;
  if (next_ && notified_head_ > head_) {
    // Refresh archives, state and cells before fetching blocks announced by the node.
    auto done = td::PromiseCreator::lambda([self = actor_id(this), head = notified_head_](td::Result<td::Unit> r) mutable {
      td::actor::send_closure(self, &FinalizedTraceScheduler::got_head,
          r.is_error() ? td::Result<ton::BlockSeqno>(r.move_as_error()) : td::Result<ton::BlockSeqno>(head));
    });
    td::actor::send_closure(scanner_, &DbScanner::request_catch_up, std::move(done), ton::validator::CatchUpMode::Force);
    return;
  }
  auto done = td::PromiseCreator::lambda([self = actor_id(this)](td::Result<ton::BlockSeqno> result) mutable {
    td::actor::send_closure(self, &FinalizedTraceScheduler::got_head, std::move(result));
  });
  td::actor::send_closure(scanner_, &DbScanner::get_last_mc_seqno, std::move(done));
}

void FinalizedTraceScheduler::got_head(td::Result<ton::BlockSeqno> result) {
  if (result.is_error()) {
    LOG(WARNING) << "Finalized head unavailable: " << result.move_as_error();
    busy_ = false;
    alarm_timestamp() = td::Timestamp::in(0.5);
    return;
  }
  head_ = result.move_as_ok();
  if (!next_) {
    next_ = head_;
    LOG(INFO) << "Starting finalized stream at node head " << next_;
  }
  if (next_ > head_) {
    busy_ = false;
    // FIFO is a wake-up hint, not a replacement for polling after missed events.
    alarm_timestamp() = td::Timestamp::in(db_event_fifo_.empty() ? 0.1 : 1.0);
    return;
  }
  auto done = td::PromiseCreator::lambda([self = actor_id(this)](td::Result<schema::MasterchainBlockDataState> r) mutable {
    td::actor::send_closure(self, &FinalizedTraceScheduler::fetched, std::move(r));
  });
  td::actor::send_closure(scanner_, &DbScanner::fetch_seqno, next_, std::move(done));
}

void FinalizedTraceScheduler::fetched(td::Result<schema::MasterchainBlockDataState> result) {
  if (result.is_error()) {
    LOG(ERROR) << "Cannot fetch required finalized block " << next_ << ": " << result.move_as_error()
               << "; keeping the cursor";
    busy_ = false;
    alarm_timestamp() = td::Timestamp::in(1);
    return;
  }
  auto data = result.move_as_ok();
  if (data.shard_blocks_.empty() || !data.shard_blocks_.front().block_data->block_id().is_masterchain()) {
    LOG(FATAL) << "Finalized block has no masterchain data";
  }
  const auto& mc = data.shard_blocks_.front();
  const auto id = mc.block_data->block_id();
  block_time_ = mc.handle->unix_time();
  auto config = block::ConfigInfo::extract_config(mc.block_state, id,
      block::ConfigInfo::needCapabilities | block::ConfigInfo::needLibraries | block::ConfigInfo::needWorkchainInfo |
      block::ConfigInfo::needSpecialSmc);
  if (config.is_error()) {
    LOG(FATAL) << "Cannot load finalized classifier configuration: " << config.move_as_error();
  }
  parsing_block_.emplace();
  parsing_block_->seqno = id.seqno();
  parsing_block_->unix_time = block_time_;
  parsing_block_->config = config.move_as_ok();
  for (const auto& shard : data.shard_blocks_) parsing_block_->shard_states.push_back(shard.block_state);
  for (const auto& shard : data.shard_blocks_diff_) {
    parsing_block_->block_data_owners.push_back(shard.block_data);
    parsing_block_->block_roots.push_back(shard.block_data->root_cell());
  }
  blocks_left_to_parse_ = data.shard_blocks_diff_.size();
  if (!blocks_left_to_parse_) {
    blocks_left_to_parse_ = 1;
    parsed(std::vector<TransactionInfo>{});
    return;
  }
  for (const auto& shard : data.shard_blocks_diff_) {
    auto done = td::PromiseCreator::lambda([self = actor_id(this)](td::Result<std::vector<TransactionInfo>> r) mutable {
      td::actor::send_closure(self, &FinalizedTraceScheduler::parsed, std::move(r));
    });
    td::actor::create_actor<BlockParser>("FinalizedBlockParser", shard.block_data, next_, std::move(done)).release();
  }
}

void FinalizedTraceScheduler::parsed(td::Result<std::vector<TransactionInfo>> result) {
  if (result.is_error()) {
    LOG(FATAL) << "Finalized parsing failed: " << result.move_as_error();
  }
  CHECK(parsing_block_ && blocks_left_to_parse_);
  auto transactions = result.move_as_ok();
  auto& all = parsing_block_->transactions;
  all.insert(all.end(), std::make_move_iterator(transactions.begin()), std::make_move_iterator(transactions.end()));
  if (--blocks_left_to_parse_) return;
  CHECK(parsing_block_->seqno == next_);
  block_active_ = true;
  std::function<void(RedisWritePlan)> plan = [self = actor_id(this), seqno = next_](auto value) {
    td::actor::send_closure(self, &FinalizedTraceScheduler::trace_prepared, seqno, std::move(value));
  };
  auto done = td::PromiseCreator::lambda([self = actor_id(this)](td::Result<td::Unit> r) mutable {
    td::actor::send_closure(self, &FinalizedTraceScheduler::prepared, std::move(r));
  });
  td::actor::send_closure(processor_, &FinalizedTraceProcessor::prepare_block,
                         std::move(*parsing_block_), std::move(plan), std::move(done));
  parsing_block_.reset();
}

void FinalizedTraceScheduler::prepared(td::Result<td::Unit> result) {
  if (result.is_error()) {
    LOG(FATAL) << "Finalized preparation failed: " << result.move_as_error();
  }
  preparation_finished_ = true;
  drive_block();
}

void FinalizedTraceScheduler::trace_prepared(ton::BlockSeqno seqno, RedisWritePlan plan) {
  CHECK(seqno == next_ && block_active_);
  CHECK(received_traces_.insert(plan.trace_key).second);
  auto key = plan.trace_key;
  pending_traces_.emplace(std::move(key), PendingTrace{std::move(plan), false, {}});
  drive_block();
}

void FinalizedTraceScheduler::drive_block() {
  if (control_in_flight_) {
    return;
  }
  for (auto& [key, pending] : pending_traces_) {
    if (pending.in_flight) {
      continue;
    }
    if (pending.retry_at && !pending.retry_at.is_in_past()) {
      alarm_timestamp().relax(pending.retry_at);
      continue;
    }
    if (in_flight_ >= kMaxWrites) {
      break;
    }
    pending.in_flight = true;
    ++in_flight_;
    auto done = td::PromiseCreator::lambda([self = actor_id(this), key](td::Result<std::int64_t> r) mutable {
      td::actor::send_closure(self, &FinalizedTraceScheduler::trace_written, key, std::move(r));
    });
    td::actor::send_closure(writer_, &RedisMaterializer::write_finalized_trace, next_,
                           pending.plan, std::move(done));
  }
  if (!preparation_finished_ || !pending_traces_.empty()) {
    return;
  }
  CHECK(in_flight_ == 0 && completed_traces_ == received_traces_.size());
  if (control_retry_at_ && !control_retry_at_.is_in_past()) {
    alarm_timestamp().relax(control_retry_at_);
    return;
  }
  control_in_flight_ = true;
  auto done = td::PromiseCreator::lambda([self = actor_id(this)](td::Result<std::int64_t> r) mutable {
    td::actor::send_closure(self, &FinalizedTraceScheduler::committed, std::move(r));
  });
  td::actor::send_closure(writer_, &RedisMaterializer::finish_finalized, next_, block_time_, std::move(done));
}

void FinalizedTraceScheduler::trace_written(std::string trace_key, td::Result<std::int64_t> result) {
  auto it = pending_traces_.find(trace_key);
  CHECK(it != pending_traces_.end() && it->second.in_flight && in_flight_);
  --in_flight_;
  it->second.in_flight = false;
  if (result.is_error()) {
    auto error = result.move_as_error();
    LOG(ERROR) << "Finalized trace " << trace_key << " in block " << next_ << " failed, retrying: " << error;
    it->second.retry_at = td::Timestamp::in(0.5);
    alarm_timestamp().relax(it->second.retry_at);
  } else {
    ++completed_traces_;
    pending_traces_.erase(it);
  }
  drive_block();
}

void FinalizedTraceScheduler::committed(td::Result<std::int64_t> result) {
  control_in_flight_ = false;
  if (result.is_error()) {
    auto error = result.move_as_error();
    LOG(ERROR) << "Finalized completion " << next_ << " failed; retrying: " << error;
    control_retry_at_ = td::Timestamp::in(0.5);
    alarm_timestamp().relax(control_retry_at_);
    return;
  }
  LOG(INFO) << "Finalized mc block " << next_ << " processed, traces=" << completed_traces_;
  CHECK(pending_traces_.empty());
  received_traces_.clear();
  completed_traces_ = 0;
  preparation_finished_ = block_active_ = false;
  control_retry_at_ = {};
  ++next_;
  busy_ = false;
  alarm_timestamp() = td::Timestamp::in(0.001);
}
