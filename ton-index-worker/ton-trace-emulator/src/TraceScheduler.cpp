#include "TraceScheduler.h"
#include "BlockEmulator.h"
#include "TraceInserter.h"
#include "td/utils/filesystem.h"
#include "td/utils/Status.h"
#include "common/delay.h"
#include "validator/interfaces/block-handle.h"
#include "tl-utils/common-utils.hpp"
#include "ton/ton-tl.hpp"
#include "td/utils/overloaded.h"
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <chrono>

namespace {
constexpr const char* kHealthKey = "health:ton-trace-emulator";
constexpr auto kHealthTtl = std::chrono::seconds(20);
constexpr double kHealthIntervalSec = 1.0;
}  // namespace


void TraceEmulatorScheduler::handle_db_event(ton::tl_object_ptr<ton::ton_api::db_Event> event) {
    ton::ton_api::downcast_call(
        *event, td::overloaded(
                    [&](ton::ton_api::db_event_blockCandidateReceived &ev) {
                    },
                    [&](ton::ton_api::db_event_blockApplied &ev) {
                        LOG(WARNING) << "db_event_blockApplied: "<< ton::create_block_id(ev.block_id_).to_str();
                        handle_block_applied(ton::create_block_id(ev.block_id_));
                    },
                    [&](ton::ton_api::db_event_blockSigned &ev) {
                        LOG(WARNING) << "db_event_blockSigned: " << ton::create_block_id(ev.block_id_).to_str();
                        handle_block_signed(ton::create_block_id(ev.block_id_));
                    }));
}

void TraceEmulatorScheduler::handle_block_signed(ton::BlockIdExt block_id) {
    if (block_id.is_masterchain()) {
        return;
    }
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), block_id](td::Result<td::Unit> R) {
        R.ensure();
        td::actor::send_closure(SelfId, &TraceEmulatorScheduler::enqueue_signed_block, block_id);
    });
    td::actor::send_closure(db_scanner_, &DbScanner::request_catch_up, std::move(P));
}

void TraceEmulatorScheduler::handle_block_applied(ton::BlockIdExt block_id) {
    if (!block_id.is_masterchain()) {
        return;
    }
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), block_id](td::Result<td::Unit> R) {
        R.ensure();
        td::actor::send_closure(SelfId, &TraceEmulatorScheduler::got_last_mc_seqno, block_id.seqno());
    });
    td::actor::send_closure(db_scanner_, &DbScanner::request_catch_up, std::move(P));
}


void TraceEmulatorScheduler::start_up() {
    alarm_timestamp() = td::Timestamp::in(0.1);

    if (global_config_path_.empty() || inet_addr_.empty()) {
        LOG(WARNING) << "Global config path or inet addr is empty. OverlayListener was not started.";
    } else {
        overlay_listener_ = td::actor::create_actor<OverlayListener>("OverlayListener", global_config_path_, inet_addr_, insert_trace_);
    }

    if (input_redis_channel_.empty()) {
        LOG(WARNING) << "Input redis queue name is empty. RedisListener was not started.";
    } else {
        redis_listener_ = td::actor::create_actor<RedisListener>("RedisListener", redis_dsn_, input_redis_channel_, insert_trace_);
    }

    if (db_event_fifo_path_.empty()) {
        LOG(WARNING) << "DB events FIFO path is empty. Falling back to polling (pending/finalized only).";
    } else {
        db_event_listener_ = td::actor::create_actor<DbEventListener>("DbEventListener", db_event_fifo_path_, actor_id(this));
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

        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), seqno](td::Result<MasterchainBlockDataState> R) {
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
    LOG(ERROR) << "Failed to fetch seqno " << seqno << ": " << std::move(error);
    seqnos_to_fetch_.insert(seqno);
    alarm_timestamp() = td::Timestamp::in(0.1);
}

void TraceEmulatorScheduler::seqno_fetched(std::uint32_t seqno, MasterchainBlockDataState mc_data_state) {
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
}

void TraceEmulatorScheduler::emulate_blocks() {
    if (last_emulated_seqno_ == 0) {
        last_emulated_seqno_ = last_fetched_seqno_;
    }

    auto it = blocks_to_emulate_.find(last_emulated_seqno_ + 1);
    while(it != blocks_to_emulate_.end()) {
        LOG(ERROR) << "Emulating mc block " << last_emulated_seqno_ + 1;
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), blkid = it->second.shard_blocks_[0].block_data->block_id().id](td::Result<> R) {
            if (R.is_error()) {
                LOG(ERROR) << "Error emulating mc block " << blkid.to_str();
                return;
            }
            LOG(INFO) << "Success emulating mc block " << blkid.to_str();
        });
        auto actor_name = PSLICE() << "McBlockEmulator" << last_emulated_seqno_ + 1;
        td::actor::create_actor<McBlockEmulator>(actor_name, it->second, insert_trace_, std::move(P)).release();

        blocks_to_emulate_.erase(it);
        last_emulated_seqno_++;
        it = blocks_to_emulate_.find(last_emulated_seqno_ + 1);
    }
}

void TraceEmulatorScheduler::enqueue_signed_block(ton::BlockIdExt block_id) {
    if (signed_blocks_inflight_.count(block_id) != 0 || signed_block_storage_.count(block_id) != 0) {
        return;
    }
    signed_blocks_inflight_.insert(block_id);
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), block_id](td::Result<BlockDataState> R) mutable {
        if (R.is_error()) {
            td::actor::send_closure(SelfId, &TraceEmulatorScheduler::signed_block_error, block_id, R.move_as_error());
            return;
        }
        td::actor::send_closure(SelfId, &TraceEmulatorScheduler::signed_block_fetched, block_id, R.move_as_ok());
    });
    td::actor::send_closure(db_scanner_, &DbScanner::fetch_block_by_id, block_id, std::move(P));
}

void TraceEmulatorScheduler::signed_block_fetched(ton::BlockIdExt block_id, BlockDataState block_data_state) {
    auto time_diff = td::Clocks::system() - block_data_state.handle->unix_time();
    LOG(INFO) << "Collected signed shard block " << block_id.to_str() << " created " << td::StringBuilder::FixedDouble(time_diff, 2) << "s ago";

    last_confirmed_block_time_ = block_data_state.handle->unix_time();

    signed_blocks_inflight_.erase(block_id);
    td::actor::send_closure(invalidated_trace_tracker_, &InvalidatedTraceTracker::register_pending_block, block_data_state.handle->id());
    signed_block_storage_.emplace(block_id, std::move(block_data_state));
    signed_block_queue_.push_back(block_id);

    process_signed_blocks();
}

void TraceEmulatorScheduler::signed_block_error(ton::BlockIdExt block_id, td::Status error) {
    signed_blocks_inflight_.erase(block_id);
    LOG(ERROR) << "Failed to collect signed shard block " << block_id.to_str() << ": " << error;
    ton::delay_action([SelfId = actor_id(this), block_id]() {
        td::actor::send_closure(SelfId, &TraceEmulatorScheduler::enqueue_signed_block, block_id);
    }, td::Timestamp::in(0.1));
}

void TraceEmulatorScheduler::process_signed_blocks() {
    while (!signed_block_queue_.empty()) {
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

        auto P = td::PromiseCreator::lambda([block_id](td::Result<> R) mutable {
            if (R.is_error()) {
                LOG(ERROR) << "Error processing signed shard block " << block_id.to_str() << ": " << R.move_as_error();
            }
        });
        auto actor_name = PSLICE() << "SignedBlockEmulator" << block_id.seqno();
        auto trace_processor = make_signed_trace_processor(block_id);
        td::actor::create_actor<ConfirmedBlockEmulator>(actor_name, FinalityState::Confirmed, std::move(block_data_state), latest_config_,
                                                        std::move(shard_snapshot_copy), std::move(trace_processor),
                                                        std::move(P))
            .release();
    }
}

std::function<void(Trace, td::Promise<td::Unit>, MeasurementPtr)> TraceEmulatorScheduler::make_signed_trace_processor(const ton::BlockIdExt& block_id_ext) {
    return [insert_trace = insert_trace_, block_id_ext, tracker = invalidated_trace_tracker_.get()](Trace trace, td::Promise<td::Unit> promise, MeasurementPtr measurement) mutable {
        td::actor::send_closure(tracker, &InvalidatedTraceTracker::add_confirmed_trace, block_id_ext, trace.ext_in_msg_hash_norm);
        insert_trace(std::move(trace), std::move(promise), measurement);
    };
}

std::function<void(Trace, td::Promise<td::Unit>, MeasurementPtr)> TraceEmulatorScheduler::make_finalized_trace_processor(const MasterchainBlockDataState& mc_data_state) {
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
