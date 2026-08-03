#include "TraceScheduler.h"
#include "BlockEmulator.h"
#include "Statistics.h"
#include "TraceProcessor.h"
#include "common/delay.h"
#include "td/utils/Status.h"
#include "td/utils/filesystem.h"
#include "td/utils/overloaded.h"
#include "tl-utils/common-utils.hpp"
#include "ton/ton-tl.hpp"
#include "validator/interfaces/block-handle.h"
#include <chrono>
#include <cstdint>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

namespace {
constexpr const char* kHealthKey = "health:ton-trace-emulator";
constexpr auto kHealthTtl = std::chrono::seconds(20);
constexpr double kHealthIntervalSec = 1.0;
constexpr std::size_t kMaxSeenSignedBlocks = 65536;
constexpr std::size_t kMaxClosedConfirmedBlocks = 65536;
constexpr std::size_t kMaxFinalizedBlocksInFlight = 2;
constexpr std::size_t kMaxConfirmedBlocksInFlight = 8;
constexpr std::size_t kMaxSignedBlockFetchesInFlight = 64;
}  // namespace


void TraceEmulatorScheduler::handle_db_event(ton::tl_object_ptr<ton::ton_api::db_Event> event) {
    ton::ton_api::downcast_call(
        *event, td::overloaded(
                    [&](ton::ton_api::db_event_blockCandidateReceived &ev) {
                    },
                    [&](ton::ton_api::db_event_blockApplied &ev) {
                        LOG(DEBUG) << "db_event_blockApplied: "<< ton::create_block_id(ev.block_id_).to_str();
                        handle_block_applied(ton::create_block_id(ev.block_id_));
                    },
                    [&](ton::ton_api::db_event_blockSigned &ev) {
                        LOG(DEBUG) << "db_event_blockSigned: " << ton::create_block_id(ev.block_id_).to_str();
                        handle_block_signed(ton::create_block_id(ev.block_id_));
                    }));
}

void TraceEmulatorScheduler::handle_block_signed(ton::BlockIdExt block_id) {
    if (block_id.is_masterchain()) {
        return;
    }
    if (!remember_seen_signed_block(block_id)) {
        LOG(INFO) << "Skipping duplicate signed shard block " << block_id.to_str();
        return;
    }
    pending_signed_blocks_.push_back(block_id);
    request_db_catch_up();
}

void TraceEmulatorScheduler::handle_block_applied(ton::BlockIdExt block_id) {
    if (!block_id.is_masterchain()) {
        return;
    }
    if (!pending_applied_mc_seqno_ || block_id.seqno() > *pending_applied_mc_seqno_) {
        pending_applied_mc_seqno_ = block_id.seqno();
    }
    request_db_catch_up();
}

bool TraceEmulatorScheduler::has_pending_db_events() const {
    return pending_applied_mc_seqno_.has_value() || !pending_signed_blocks_.empty();
}

bool TraceEmulatorScheduler::has_ready_finalized_block() const {
    if (last_started_finalized_seqno_ == 0) {
        return false;
    }
    return blocks_to_emulate_.find(last_started_finalized_seqno_ + 1) !=
           blocks_to_emulate_.end();
}

void TraceEmulatorScheduler::request_db_catch_up() {
    if (db_catch_up_in_progress_ || !has_pending_db_events()) {
        return;
    }

    db_catch_up_in_progress_ = true;
    catch_up_applied_mc_seqno_ = pending_applied_mc_seqno_;
    pending_applied_mc_seqno_.reset();
    catch_up_signed_blocks_ = std::move(pending_signed_blocks_);
    pending_signed_blocks_.clear();

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<td::Unit> R) mutable {
        td::actor::send_closure(SelfId, &TraceEmulatorScheduler::db_catch_up_finished, std::move(R));
    });
    td::actor::send_closure(db_scanner_, &DbScanner::request_catch_up, std::move(P),
                            ton::validator::CatchUpMode::Force);
}

void TraceEmulatorScheduler::requeue_catch_up_batch() {
    if (catch_up_applied_mc_seqno_) {
        if (!pending_applied_mc_seqno_ || *catch_up_applied_mc_seqno_ > *pending_applied_mc_seqno_) {
            pending_applied_mc_seqno_ = *catch_up_applied_mc_seqno_;
        }
        catch_up_applied_mc_seqno_.reset();
    }
    while (!catch_up_signed_blocks_.empty()) {
        pending_signed_blocks_.push_front(catch_up_signed_blocks_.back());
        catch_up_signed_blocks_.pop_back();
    }
}

void TraceEmulatorScheduler::process_catch_up_batch() {
    if (catch_up_applied_mc_seqno_) {
        got_last_mc_seqno(*catch_up_applied_mc_seqno_);
        catch_up_applied_mc_seqno_.reset();
    }

    while (!catch_up_signed_blocks_.empty()) {
        auto block_id = catch_up_signed_blocks_.front();
        catch_up_signed_blocks_.pop_front();
        enqueue_signed_block(block_id);
    }
}

void TraceEmulatorScheduler::db_catch_up_finished(td::Result<td::Unit> result) {
    db_catch_up_in_progress_ = false;
    if (result.is_error()) {
        LOG(ERROR) << "Failed to catch up DB before processing events: " << result.move_as_error();
        requeue_catch_up_batch();
        alarm_timestamp() = td::Timestamp::in(0.1);
        return;
    }

    process_catch_up_batch();
    request_db_catch_up();
}


void TraceEmulatorScheduler::start_up() {
    alarm_timestamp() = td::Timestamp::in(0.1);

    if (global_config_path_.empty() || inet_addr_.empty()) {
        LOG(WARNING) << "Global config path or inet addr is empty. OverlayListener was not started.";
    } else {
        overlay_listener_ = td::actor::create_actor<OverlayListener>("OverlayListener", global_config_path_, inet_addr_,
                                                                     process_trace_patch_, external_message_admission_);
    }

    if (input_redis_channel_.empty()) {
        LOG(WARNING) << "Input redis queue name is empty. RedisListener was not started.";
    } else {
        redis_listener_ = td::actor::create_actor<RedisListener>("RedisListener", redis_dsn_, input_redis_channel_,
                                                                 process_trace_patch_, external_message_admission_);
    }

    if (db_event_fifo_path_.empty()) {
        LOG(WARNING) << "DB events FIFO path is empty. Falling back to polling (pending/finalized only).";
    } else {
        db_event_listener_ = td::actor::create_actor<DbEventListener>("DbEventListener", db_event_fifo_path_,
            [SelfId = actor_id(this)](ton::tl_object_ptr<ton::ton_api::db_Event> event) {
                td::actor::send_closure(SelfId, &TraceEmulatorScheduler::handle_db_event, std::move(event));
            });
    }
    next_health_update_ = td::Timestamp::in(0.1);
}

void TraceEmulatorScheduler::got_last_mc_seqno(ton::BlockSeqno new_last_known_seqno) {
    if (new_last_known_seqno == last_known_seqno_) {
        return;
    }

    LOG(INFO) << "New masterchain block " << new_last_known_seqno;

    if (last_known_seqno_ == 0) {
        last_known_seqno_ = new_last_known_seqno;
        last_fetched_seqno_ = new_last_known_seqno;
        return;
    }

    if (new_last_known_seqno > last_known_seqno_ + 1) {
        LOG(WARNING) << "More than one new masterchain block appeared. Skipping to the newest one, from " << last_known_seqno_ << " to " << new_last_known_seqno;
    }

    for (auto seqno = last_known_seqno_ + 1; seqno <= new_last_known_seqno; seqno++) {
        seqnos_to_fetch_.insert(seqno);
    }

    last_known_seqno_ = new_last_known_seqno;
    fetch_seqnos();
}

void TraceEmulatorScheduler::fetch_seqnos() {
    for (auto it = seqnos_to_fetch_.begin(); it != seqnos_to_fetch_.end(); ) {
        auto seqno = *it;
        LOG(INFO) << "Fetching seqno " << seqno;

        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), seqno](td::Result<schema::MasterchainBlockDataState> R) {
            if (R.is_error()) {
                td::actor::send_closure(SelfId, &TraceEmulatorScheduler::fetch_error, seqno, R.move_as_error());
                return;
            }
            auto mc_block_ds = R.move_as_ok();
            for (auto &block_ds : mc_block_ds.shard_blocks_) {
                if (block_ds.block_data->block_id().is_masterchain()) {
                    mc_block_ds.config_ = block::ConfigInfo::extract_config(block_ds.block_state,
                        block_ds.block_data->block_id(), block::ConfigInfo::needCapabilities | block::ConfigInfo::needLibraries | block::ConfigInfo::needWorkchainInfo | block::ConfigInfo::needSpecialSmc).move_as_ok();
                    break;
                }
            }
            td::actor::send_closure(SelfId, &TraceEmulatorScheduler::seqno_fetched, seqno, std::move(mc_block_ds));
        });
        td::actor::send_closure(db_scanner_, &DbScanner::fetch_seqno, seqno, std::move(P));

        it = seqnos_to_fetch_.erase(it);
    }
}

void TraceEmulatorScheduler::fetch_error(std::uint32_t seqno, td::Status error) {
    LOG(ERROR) << "Failed to fetch seqno " << seqno << ": " << error;
    seqnos_to_fetch_.insert(seqno);
    alarm_timestamp() = td::Timestamp::in(0.1);
}

void TraceEmulatorScheduler::seqno_fetched(std::uint32_t seqno, schema::MasterchainBlockDataState mc_data_state) {
    LOG(INFO) << "Fetched seqno " << seqno;

    last_finalized_mc_block_time_ = mc_data_state.shard_blocks_[0].handle->unix_time();

    if (seqno > last_fetched_seqno_) {
        LOG(INFO) << "Setting last fetched seqno to " << seqno;
        last_fetched_seqno_ = seqno;

        latest_config_ = mc_data_state.config_;
        latest_shard_states_.clear();
        for (const auto& shard_state : mc_data_state.shard_blocks_) {
            ShardStateSnapshot snapshot{
                shard_state.handle->id().id,
                shard_state.handle->unix_time(),
                shard_state.handle->logical_time(),
                shard_state.block_state
            };
            latest_shard_states_.push_back(std::move(snapshot));
        }

        if (!overlay_listener_.empty()) {
            td::actor::send_closure(overlay_listener_, &OverlayListener::set_mc_data_state, mc_data_state);
        }

        if (!redis_listener_.empty()) {
            td::actor::send_closure(redis_listener_, &RedisListener::set_mc_data_state, mc_data_state);
        }
    }

    blocks_to_emulate_[seqno] = mc_data_state;
    start_next_finalized_block();
    process_signed_blocks();
}

void TraceEmulatorScheduler::start_next_finalized_block() {
    if (last_started_finalized_seqno_ == 0) {
        last_started_finalized_seqno_ = last_fetched_seqno_;
        finalized_results_.reset(last_fetched_seqno_ + 1);
    }

    if (finalized_trace_ids_in_progress_ ||
        finalized_blocks_in_pipeline_ >= kMaxFinalizedBlocksInFlight) {
        return;
    }
    auto seqno = last_started_finalized_seqno_ + 1;
    auto it = blocks_to_emulate_.find(seqno);
    if (it == blocks_to_emulate_.end()) {
        return;
    }

    finalized_blocks_in_pipeline_++;
    finalized_trace_ids_in_progress_ = seqno;
    bool reuse_confirmed_state = false;
    bool has_shard_blocks = false;
    for (const auto& block : it->second.shard_blocks_diff_) {
        const auto block_id = block.block_data->block_id();
        close_confirmed_block(block_id.id);
        if (block_id.is_masterchain()) {
            continue;
        }
        if (!has_shard_blocks) {
            reuse_confirmed_state = true;
            has_shard_blocks = true;
        }
        if (confirmed_block_snapshots_.count(block_id) == 0) {
            reuse_confirmed_state = false;
        }
    }

    start_finalized_emulator(seqno, reuse_confirmed_state);
}

void TraceEmulatorScheduler::start_finalized_emulator(
    ton::BlockSeqno seqno,
    bool reuse_confirmed_state) {
    auto it = blocks_to_emulate_.find(seqno);
    if (it == blocks_to_emulate_.end()) {
        LOG(FATAL) << "Missing mc block selected for emulation " << seqno;
    }
    LOG(INFO) << "Emulating mc block " << seqno
              << (reuse_confirmed_state
                      ? " with reusable confirmed shard state"
                      : "");

    auto trace_ids_resolved = [
        SelfId = actor_id(this)
    ](ton::BlockSeqno resolved_seqno) {
        td::actor::send_closure(
            SelfId,
            &TraceEmulatorScheduler::finalized_trace_ids_resolved,
            resolved_seqno);
    };
    auto P = td::PromiseCreator::lambda([
        SelfId = actor_id(this),
        seqno
    ](td::Result<FinalizedBlockResult> result) mutable {
        td::actor::send_closure(
            SelfId,
            &TraceEmulatorScheduler::finalized_block_emulated,
            seqno,
            std::move(result));
    });

    auto actor_name = PSLICE() << "McBlockEmulator" << seqno;
    auto block = std::move(it->second);
    td::actor::create_actor<McBlockEmulator>(
        actor_name,
        std::move(block),
        std::move(trace_ids_resolved),
        reuse_confirmed_state,
        std::move(P))
        .release();

    blocks_to_emulate_.erase(it);
    last_started_finalized_seqno_++;
}

void TraceEmulatorScheduler::finalized_trace_ids_resolved(
    ton::BlockSeqno seqno) {
    if (!finalized_trace_ids_in_progress_ ||
        seqno != *finalized_trace_ids_in_progress_) {
        LOG(FATAL) << "Finalized trace ids resolved out of order: "
                   << seqno;
    }
    finalized_trace_ids_in_progress_.reset();
    start_next_finalized_block();
}

void TraceEmulatorScheduler::finalized_block_emulated(
    ton::BlockSeqno seqno,
    td::Result<FinalizedBlockResult> result) {
    if (result.is_error()) {
        LOG(FATAL) << "Failed to emulate finalized mc block " << seqno
                   << ": " << result.move_as_error();
        return;
    }

    auto block = result.move_as_ok();
    if (block.reused_confirmed_state) {
        for (const auto& block_id : block.finalized_blocks) {
            if (block_id.is_masterchain()) {
                continue;
            }
            auto snapshots =
                confirmed_block_snapshots_.find(block_id);
            if (snapshots == confirmed_block_snapshots_.end()) {
                LOG(FATAL) << "Reusable confirmed block snapshot disappeared: "
                           << block_id.to_str();
            }
            block.confirmed_snapshots.insert(
                block.confirmed_snapshots.end(),
                snapshots->second.begin(),
                snapshots->second.end());
        }
    }
    LOG(INFO) << "Mc block " << seqno
              << " finished computation with " << block.traces.size()
              << " trace patches";
    if (!finalized_results_.insert(seqno, std::move(block))) {
        LOG(FATAL) << "Duplicate finalized result for mc block "
                   << seqno;
    }
    try_commit_finalized_block();
}

void TraceEmulatorScheduler::try_commit_finalized_block() {
    if (finalized_commit_) {
        return;
    }

    auto outcome = finalized_results_.take_next();
    if (!outcome) {
        return;
    }

    commit_finalized_block(std::move(outcome->value));
}

void TraceEmulatorScheduler::commit_finalized_block(
    FinalizedBlockResult result) {
    auto seqno = result.mc_seqno;
    finalized_commit_.emplace(FinalizedCommitState{
        .seqno = seqno,
        .finalized_blocks = result.finalized_blocks,
        .pending_writes =
            result.reused_confirmed_state
                ? 1
                : result.traces.size(),
        .block_data_owners = std::move(result.block_data_owners),
    });

    std::map<ton::BlockId, ton::BlockIdExt> finalized_block_ids;
    for (const auto& block : result.finalized_blocks) {
        finalized_block_ids.emplace(block.id, block);
    }

    for (auto& patch : result.traces) {
        auto& trace = patch.trace;
        if (trace.contains_root_transaction()) {
            auto block = finalized_block_ids.find(trace.root->block_id);
            if (block == finalized_block_ids.end()) {
                LOG(WARNING) << "Finalized trace root belongs to unknown block "
                             << trace.root->block_id.to_str();
            } else {
                confirmed_roots_.add_finalized_root(
                    block->second, trace.ext_in_msg_hash_norm);
            }
        }

        auto trace_root_tx_hash = trace.root_tx_hash;
        auto measurement = std::move(patch.measurement);
        measurement->start_otel_child_span("insert_trace");
        auto P = td::PromiseCreator::lambda([
            SelfId = actor_id(this),
            seqno,
            trace_root_tx_hash,
            measurement
        ](td::Result<td::Unit> result) mutable {
            if (result.is_error()) {
                auto error = result.move_as_error();
                LOG(ERROR) << "Failed to insert finalized trace "
                           << td::base64_encode(
                                  trace_root_tx_hash.as_slice())
                           << ": " << error;
                measurement->mark_otel_error(
                    "trace_emulator.insert_error", error.to_string());
            } else {
                LOG(DEBUG) << "Inserted finalized trace "
                           << td::base64_encode(
                                  trace_root_tx_hash.as_slice());
            }
            measurement->end_otel_child_span("insert_trace");
            measurement->emit_otel_span();
            td::actor::send_closure(
                SelfId,
                &TraceEmulatorScheduler::finalized_trace_write_finished,
                seqno);
        });
        process_trace_patch_(
            std::move(patch.trace), std::move(P), std::move(measurement));
    }

    if (result.reused_confirmed_state) {
        auto P = td::PromiseCreator::lambda([
            SelfId = actor_id(this),
            seqno
        ](td::Result<td::Unit> result) mutable {
            if (result.is_error()) {
                LOG(ERROR) << "Failed to materialize reused confirmed state for mc block "
                           << seqno << ": " << result.move_as_error();
            }
            td::actor::send_closure(
                SelfId,
                &TraceEmulatorScheduler::finalized_trace_write_finished,
                seqno);
        });
        td::actor::send_closure(
            trace_processor_,
            &ITraceProcessor::promote_confirmed,
            std::move(result.confirmed_snapshots),
            seqno,
            std::move(P));
    }

    if (finalized_commit_->pending_writes == 0) {
        finish_finalized_commit();
    }
}

void TraceEmulatorScheduler::finalized_trace_write_finished(
    ton::BlockSeqno seqno) {
    if (!finalized_commit_ || finalized_commit_->seqno != seqno ||
        finalized_commit_->pending_writes == 0) {
        LOG(FATAL) << "Unexpected finalized trace completion for mc block "
                   << seqno;
    }
    finalized_commit_->pending_writes--;
    if (finalized_commit_->pending_writes == 0) {
        finish_finalized_commit();
    }
}

void TraceEmulatorScheduler::finish_finalized_commit() {
    auto commit = std::move(*finalized_commit_);
    finalized_commit_.reset();

    discard_confirmed_snapshots(commit.finalized_blocks);

    std::vector<td::Bits256> replaced_roots;
    for (auto& block : commit.finalized_blocks) {
        auto block_replacements =
            confirmed_roots_.finalize_block(std::move(block));
        replaced_roots.insert(
            replaced_roots.end(),
            block_replacements.begin(),
            block_replacements.end());
    }
    if (!replaced_roots.empty()) {
        td::actor::send_closure(
            trace_processor_,
            &ITraceProcessor::mark_confirmed_roots_replaced,
            std::move(replaced_roots));
    }

    LOG(INFO) << "Committed finalized mc block " << commit.seqno;
    finalized_block_done();
    try_commit_finalized_block();
}

void TraceEmulatorScheduler::finalized_block_done() {
    if (finalized_blocks_in_pipeline_ > 0) {
        finalized_blocks_in_pipeline_--;
    }
    start_next_finalized_block();
    process_signed_blocks();
}

void TraceEmulatorScheduler::close_confirmed_block(
    ton::BlockId block_id) {
    if (!closed_confirmed_blocks_.insert(block_id).second) {
        return;
    }
    closed_confirmed_block_order_.push_back(block_id);
    while (closed_confirmed_block_order_.size() >
           kMaxClosedConfirmedBlocks) {
        closed_confirmed_blocks_.erase(
            closed_confirmed_block_order_.front());
        closed_confirmed_block_order_.pop_front();
    }
}

bool TraceEmulatorScheduler::confirmed_block_is_closed(
    const ton::BlockIdExt& block_id) const {
    return closed_confirmed_blocks_.count(block_id.id) != 0;
}

void TraceEmulatorScheduler::discard_confirmed_snapshots(
    const std::vector<ton::BlockIdExt>& finalized_blocks) {
    std::set<ton::BlockId> logical_blocks;
    for (const auto& block : finalized_blocks) {
        if (!block.is_masterchain()) {
            logical_blocks.insert(block.id);
        }
    }
    for (auto it = confirmed_block_snapshots_.begin();
         it != confirmed_block_snapshots_.end();) {
        if (logical_blocks.count(it->first.id) != 0) {
            it = confirmed_block_snapshots_.erase(it);
        } else {
            ++it;
        }
    }
}

void TraceEmulatorScheduler::enqueue_signed_block(ton::BlockIdExt block_id) {
    if (confirmed_block_is_closed(block_id)) {
        return;
    }
    if (signed_blocks_inflight_.count(block_id) != 0 || signed_block_storage_.count(block_id) != 0) {
        return;
    }
    // Preserve blockSigned order even though DB fetches finish out of order.
    signed_block_queue_.push_back(block_id);
    queue_signed_block_fetch(block_id);
}

void TraceEmulatorScheduler::queue_signed_block_fetch(
    ton::BlockIdExt block_id) {
    if (confirmed_block_is_closed(block_id) ||
        signed_blocks_inflight_.count(block_id) != 0 ||
        signed_block_storage_.count(block_id) != 0) {
        process_signed_blocks();
        return;
    }
    signed_blocks_to_fetch_queue_.push_back(block_id);
    fetch_signed_blocks();
}

void TraceEmulatorScheduler::fetch_signed_blocks() {
    while (!signed_blocks_to_fetch_queue_.empty() &&
           signed_blocks_inflight_.size() < kMaxSignedBlockFetchesInFlight) {
        auto block_id = signed_blocks_to_fetch_queue_.front();
        signed_blocks_to_fetch_queue_.pop_front();
        if (confirmed_block_is_closed(block_id) ||
            signed_blocks_inflight_.count(block_id) != 0 ||
            signed_block_storage_.count(block_id) != 0) {
            continue;
        }
        signed_blocks_inflight_.insert(block_id);
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), block_id](td::Result<schema::BlockDataState> R) mutable {
            if (R.is_error()) {
                td::actor::send_closure(SelfId, &TraceEmulatorScheduler::signed_block_error, block_id, R.move_as_error());
                return;
            }
            td::actor::send_closure(SelfId, &TraceEmulatorScheduler::signed_block_fetched, block_id, R.move_as_ok());
        });
        td::actor::send_closure(db_scanner_, &DbScanner::fetch_block_by_id, block_id, std::move(P));
    }
}

void TraceEmulatorScheduler::signed_block_fetched(ton::BlockIdExt block_id, schema::BlockDataState block_data_state) {
    auto time_diff = td::Clocks::system() - block_data_state.handle->unix_time();
    LOG(INFO) << "Collected signed shard block " << block_id.to_str() << " created " << td::StringBuilder::FixedDouble(time_diff, 2) << "s ago";

    last_confirmed_block_time_ = block_data_state.handle->unix_time();

    signed_blocks_inflight_.erase(block_id);
    fetch_signed_blocks();
    if (confirmed_block_is_closed(block_id)) {
        LOG(INFO) << "Skipping signed block already covered by finalization "
                  << block_id.to_str();
        process_signed_blocks();
        return;
    }
    signed_block_storage_.emplace(block_id, std::move(block_data_state));

    process_signed_blocks();
}

void TraceEmulatorScheduler::signed_block_error(ton::BlockIdExt block_id, td::Status error) {
    signed_blocks_inflight_.erase(block_id);
    fetch_signed_blocks();
    LOG(ERROR) << "Failed to collect signed shard block " << block_id.to_str() << ": " << error;
    ton::delay_action([SelfId = actor_id(this), block_id]() {
        td::actor::send_closure(
            SelfId,
            &TraceEmulatorScheduler::queue_signed_block_fetch,
            block_id);
    }, td::Timestamp::in(0.1));
}

void TraceEmulatorScheduler::process_signed_blocks() {
    // Do not start more speculative work while finalized work is available.
    // Confirmed emulators that are already running are allowed to finish.
    if (finalized_blocks_in_pipeline_ > 0 || has_ready_finalized_block()) {
        start_next_finalized_block();
        return;
    }
    if (confirmed_head_in_progress_ ||
        confirmed_blocks_inflight_ >= kMaxConfirmedBlocksInFlight) {
        return;
    }

    while (!signed_block_queue_.empty()) {
        auto block_id = signed_block_queue_.front();
        if (confirmed_block_is_closed(block_id)) {
            signed_block_queue_.pop_front();
            signed_block_storage_.erase(block_id);
            continue;
        }
        auto it = signed_block_storage_.find(block_id);
        if (it == signed_block_storage_.end()) {
            return;
        }
        if (!latest_config_ || latest_shard_states_.empty()) {
            LOG(WARNING) << "Skipping signed shard block " << block_id.to_str() << " due to missing masterchain context";
            return;
        }

        signed_block_queue_.pop_front();
        auto block_data_state = std::move(it->second);
        signed_block_storage_.erase(it);

        auto shard_snapshot_copy = latest_shard_states_;
        for (auto& snapshot : shard_snapshot_copy) {
            if (snapshot.blkid.shard_full() == block_data_state.block_data->block_id().shard_full()) {
                snapshot.state = block_data_state.block_state;
                snapshot.timestamp = block_data_state.handle->unix_time();
                snapshot.logical_time = block_data_state.handle->logical_time();
            }
        }

        confirmed_blocks_inflight_++;
        confirmed_head_in_progress_ = block_id;
        auto head_finished = [
            SelfId = actor_id(this)
        ](ton::BlockIdExt finished_block_id) {
            td::actor::send_closure(
                SelfId,
                &TraceEmulatorScheduler::confirmed_block_head_finished,
                finished_block_id);
        };
        auto P = td::PromiseCreator::lambda([
            SelfId = actor_id(this),
            block_id
        ](td::Result<ConfirmedBlockResult> result) mutable {
            td::actor::send_closure(
                SelfId,
                &TraceEmulatorScheduler::confirmed_block_finished,
                block_id,
                std::move(result));
        });
        auto actor_name = PSLICE() << "SignedBlockEmulator" << block_id.seqno();
        auto trace_processor = make_signed_trace_processor(block_id);
        td::actor::create_actor<ConfirmedBlockEmulator>(actor_name, FinalityState::Confirmed, std::move(block_data_state), latest_config_,
                                                        std::move(shard_snapshot_copy), std::move(trace_processor),
                                                        std::move(head_finished),
                                                        std::move(P))
            .release();
        return;
    }
}

void TraceEmulatorScheduler::confirmed_block_head_finished(
    ton::BlockIdExt block_id) {
    if (!confirmed_head_in_progress_ ||
        block_id != *confirmed_head_in_progress_) {
        LOG(FATAL) << "Confirmed block head finished out of order: "
                   << block_id.to_str();
    }
    confirmed_head_in_progress_.reset();
    process_signed_blocks();
}

void TraceEmulatorScheduler::confirmed_block_finished(
    ton::BlockIdExt block_id,
    td::Result<ConfirmedBlockResult> result) {
    if (result.is_error()) {
        LOG(ERROR) << "Error processing signed shard block "
                   << block_id.to_str() << ": "
                   << result.move_as_error();
    } else {
        auto completed = result.move_as_ok();
        if (completed.reusable &&
            !confirmed_block_is_closed(block_id)) {
            auto [_, inserted] =
                confirmed_block_snapshots_.emplace(
                    block_id, std::move(completed.snapshots));
            if (!inserted) {
                LOG(FATAL) << "Confirmed block snapshot was stored twice: "
                           << block_id.to_str();
            }
        }
    }
    if (confirmed_blocks_inflight_ > 0) {
        confirmed_blocks_inflight_--;
    }
    process_signed_blocks();
}

void TraceEmulatorScheduler::process_confirmed_trace(
    ton::BlockIdExt block_id,
    Trace trace,
    td::Promise<ConfirmedTraceSnapshot> promise,
    MeasurementPtr measurement) {
    if (confirmed_block_is_closed(block_id)) {
        promise.set_value(ConfirmedTraceSnapshot{});
        return;
    }

    if (trace.contains_root_transaction()) {
        confirmed_roots_.add_confirmed_root(
            block_id, trace.ext_in_msg_hash_norm);
    }

    td::actor::send_closure(
        trace_processor_,
        &ITraceProcessor::process_confirmed_trace_patch,
        std::move(trace),
        std::move(promise),
        std::move(measurement));
}

bool TraceEmulatorScheduler::remember_seen_signed_block(ton::BlockIdExt block_id) {
    if (!seen_signed_blocks_.insert(block_id).second) {
        return false;
    }
    seen_signed_block_order_.push_back(block_id);
    while (seen_signed_block_order_.size() > kMaxSeenSignedBlocks) {
        seen_signed_blocks_.erase(seen_signed_block_order_.front());
        seen_signed_block_order_.pop_front();
    }
    return true;
}

std::function<void(
    Trace,
    td::Promise<ConfirmedTraceSnapshot>,
    MeasurementPtr)>
TraceEmulatorScheduler::make_signed_trace_processor(
    const ton::BlockIdExt& block_id_ext) {
    return [
        SelfId = actor_id(this),
        block_id_ext
    ](Trace trace,
      td::Promise<ConfirmedTraceSnapshot> promise,
      MeasurementPtr measurement) mutable {
        td::actor::send_closure(
            SelfId,
            &TraceEmulatorScheduler::process_confirmed_trace,
            block_id_ext,
            std::move(trace),
            std::move(promise),
            std::move(measurement));
    };
}

void TraceEmulatorScheduler::publish_health() {
    if (!health_redis_) {
        return;
    }

    std::vector<std::pair<std::string, std::string>> fields{
        {"finalized_mc_block_time", std::to_string(last_finalized_mc_block_time_)},
        {"confirmed_block_time", std::to_string(last_confirmed_block_time_)},
        {"updated_at", std::to_string(static_cast<std::uint32_t>(td::Clocks::system()))},
    };

    try {
        health_redis_->hset(kHealthKey, fields.begin(), fields.end());
        health_redis_->expire(kHealthKey, kHealthTtl);
    } catch (const sw::redis::Error &e) {
        LOG(ERROR) << "Failed to update Redis health state: " << e.what();
    }
}

// // debugging
// int seqno = 37600000;
// int end_seqno = 37600100;

void TraceEmulatorScheduler::alarm() {
    if (db_event_fifo_path_.empty()) {
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ton::BlockSeqno> R){
            if (R.is_error()) {
                LOG(ERROR) << "Failed to update last seqno: " << R.move_as_error();
                return;
            }
            td::actor::send_closure(SelfId, &TraceEmulatorScheduler::got_last_mc_seqno, R.move_as_ok());
            // if (seqno++ < end_seqno) {
            //     td::actor::send_closure(SelfId, &TraceEmulatorScheduler::got_last_mc_seqno, seqno); // for debugging
            // }
        });
        td::actor::send_closure(db_scanner_, &DbScanner::get_last_mc_seqno, std::move(P));
    }
    fetch_seqnos();
    if (!db_event_fifo_path_.empty()) {
        request_db_catch_up();
        fetch_signed_blocks();
        process_signed_blocks();
    }

    if (next_statistics_flush_.is_in_past()) {
        ton::delay_action([working_dir = this->working_dir_]() {
            auto stats = g_statistics.generate_report_and_reset();
            auto path = working_dir + "/" + "stats.txt";
            auto status = td::atomic_write_file(path, std::move(stats));
            if (status.is_error()) {
                LOG(ERROR) << "Failed to write statistics to " << path << ": " << status.error();
            }
        }, td::Timestamp::now());
        
        next_statistics_flush_ = td::Timestamp::in(60.0);
    }

    if (health_redis_ && next_health_update_.is_in_past()) {
        publish_health();
        next_health_update_ = td::Timestamp::in(kHealthIntervalSec);
    }

    alarm_timestamp() = td::Timestamp::in(0.3);
}
