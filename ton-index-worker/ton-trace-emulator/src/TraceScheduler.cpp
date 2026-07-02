#include "TraceScheduler.h"
#include "BlockEmulator.h"
#include "Statistics.h"
#include "TraceInserter.h"
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
    if (last_emulated_seqno_ == 0) {
        return false;
    }
    return blocks_to_emulate_.find(last_emulated_seqno_ + 1) != blocks_to_emulate_.end();
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
                                                                     insert_trace_, external_message_admission_);
    }

    if (input_redis_channel_.empty()) {
        LOG(WARNING) << "Input redis queue name is empty. RedisListener was not started.";
    } else {
        redis_listener_ = td::actor::create_actor<RedisListener>("RedisListener", redis_dsn_, input_redis_channel_,
                                                                 insert_trace_, external_message_admission_);
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
    emulate_blocks();
    process_signed_blocks();
}

void TraceEmulatorScheduler::emulate_blocks() {
    if (last_emulated_seqno_ == 0) {
        last_emulated_seqno_ = last_fetched_seqno_;
    }

    auto it = blocks_to_emulate_.find(last_emulated_seqno_ + 1);
    while(it != blocks_to_emulate_.end() && finalized_blocks_inflight_ < kMaxFinalizedBlocksInFlight) {
        auto seqno = last_emulated_seqno_ + 1;
        LOG(INFO) << "Emulating mc block " << seqno;
        finalized_blocks_inflight_++;
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), seqno, blkid = it->second.shard_blocks_[0].block_data->block_id().id](td::Result<> R) {
            if (R.is_error()) {
                LOG(ERROR) << "Error emulating mc block " << blkid.to_str();
            } else {
                LOG(INFO) << "Success emulating mc block " << blkid.to_str();
            }
            td::actor::send_closure(SelfId, &TraceEmulatorScheduler::finalized_block_finished, seqno);
        });
        auto actor_name = PSLICE() << "McBlockEmulator" << seqno;
        td::actor::create_actor<McBlockEmulator>(actor_name, it->second, insert_trace_, std::move(P)).release();

        blocks_to_emulate_.erase(it);
        last_emulated_seqno_++;
        it = blocks_to_emulate_.find(last_emulated_seqno_ + 1);
    }
}

void TraceEmulatorScheduler::finalized_block_finished(ton::BlockSeqno) {
    if (finalized_blocks_inflight_ > 0) {
        finalized_blocks_inflight_--;
    }
    emulate_blocks();
    process_signed_blocks();
}

void TraceEmulatorScheduler::enqueue_signed_block(ton::BlockIdExt block_id) {
    if (signed_blocks_inflight_.count(block_id) != 0 || signed_block_storage_.count(block_id) != 0) {
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
        if (signed_blocks_inflight_.count(block_id) != 0 || signed_block_storage_.count(block_id) != 0) {
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
    td::actor::send_closure(invalidated_trace_tracker_, &InvalidatedTraceTracker::register_pending_block, block_data_state.handle->id());
    signed_block_storage_.emplace(block_id, std::move(block_data_state));
    signed_block_queue_.push_back(block_id);

    process_signed_blocks();
}

void TraceEmulatorScheduler::signed_block_error(ton::BlockIdExt block_id, td::Status error) {
    signed_blocks_inflight_.erase(block_id);
    fetch_signed_blocks();
    LOG(ERROR) << "Failed to collect signed shard block " << block_id.to_str() << ": " << error;
    ton::delay_action([SelfId = actor_id(this), block_id]() {
        td::actor::send_closure(SelfId, &TraceEmulatorScheduler::enqueue_signed_block, block_id);
    }, td::Timestamp::in(0.1));
}

void TraceEmulatorScheduler::process_signed_blocks() {
    if (finalized_blocks_inflight_ > 0 || has_ready_finalized_block()) {
        emulate_blocks();
        return;
    }
    while (!signed_block_queue_.empty() && confirmed_blocks_inflight_ < kMaxConfirmedBlocksInFlight) {
        auto block_id = signed_block_queue_.front();
        signed_block_queue_.pop_front();
        auto it = signed_block_storage_.find(block_id);
        if (it == signed_block_storage_.end()) {
            continue;
        }
        if (!latest_config_ || latest_shard_states_.empty()) {
            LOG(WARNING) << "Skipping signed shard block " << block_id.to_str() << " due to missing masterchain context";
            signed_block_queue_.push_front(block_id);
            break;
        }

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
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), block_id](td::Result<> R) mutable {
            if (R.is_error()) {
                LOG(ERROR) << "Error processing signed shard block " << block_id.to_str() << ": " << R.move_as_error();
            }
            td::actor::send_closure(SelfId, &TraceEmulatorScheduler::confirmed_block_finished, block_id);
        });
        auto actor_name = PSLICE() << "SignedBlockEmulator" << block_id.seqno();
        auto trace_processor = make_signed_trace_processor(block_id);
        td::actor::create_actor<ConfirmedBlockEmulator>(actor_name, FinalityState::Confirmed, std::move(block_data_state), latest_config_,
                                                        std::move(shard_snapshot_copy), std::move(trace_processor),
                                                        std::move(P))
            .release();
    }
}

void TraceEmulatorScheduler::confirmed_block_finished(ton::BlockIdExt) {
    if (confirmed_blocks_inflight_ > 0) {
        confirmed_blocks_inflight_--;
    }
    process_signed_blocks();
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

std::function<void(Trace, td::Promise<td::Unit>, MeasurementPtr)> TraceEmulatorScheduler::make_signed_trace_processor(const ton::BlockIdExt& block_id_ext) {
    return [insert_trace = insert_trace_, block_id_ext, tracker = invalidated_trace_tracker_.get()](Trace trace, td::Promise<td::Unit> promise, MeasurementPtr measurement) mutable {
        td::actor::send_closure(tracker, &InvalidatedTraceTracker::add_confirmed_trace, block_id_ext, trace.ext_in_msg_hash_norm);
        insert_trace(std::move(trace), std::move(promise), measurement);
    };
}

std::function<void(Trace, td::Promise<td::Unit>, MeasurementPtr)> TraceEmulatorScheduler::make_finalized_trace_processor(const schema::MasterchainBlockDataState& mc_data_state) {
    std::unordered_map<ton::BlockId, ton::BlockIdExt, BlockIdHasher, BlockIdEq> shard_block_ids;
    for (const auto& shard_block : mc_data_state.shard_blocks_diff_) {
        shard_block_ids.emplace(shard_block.block_data->block_id().id, shard_block.block_data->block_id());
    }

    return [insert_trace = insert_trace_, shard_block_ids = std::move(shard_block_ids), tracker = invalidated_trace_tracker_.get()](Trace trace, td::Promise<td::Unit> promise, MeasurementPtr measurement) mutable {
        auto block_id_it = shard_block_ids.find(trace.root->block_id);
        if (block_id_it != shard_block_ids.end()) {
            td::actor::send_closure(tracker, &InvalidatedTraceTracker::add_finalized_trace, block_id_it->second, trace.ext_in_msg_hash_norm);
        } else {
            LOG(WARNING) << "Finalized trace belongs to unknown block " << trace.root->block_id.to_str();
        }
        insert_trace(std::move(trace), std::move(promise), measurement);
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
