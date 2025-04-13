#include "TraceScheduler.h"
#include "BlockEmulator.h"
#include "TraceInserter.h"
#include "td/utils/filesystem.h"
#include "common/delay.h"


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
}

void TraceEmulatorScheduler::fetch_seqnos() {
    for (auto it = seqnos_to_fetch_.begin(); it != seqnos_to_fetch_.end(); ) {
        auto seqno = *it;

        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), seqno](td::Result<MasterchainBlockDataState> R) {
            if (R.is_error()) {
                td::actor::send_closure(SelfId, &TraceEmulatorScheduler::fetch_error, seqno, R.move_as_error());
                return;
            }
            auto mc_block_ds = R.move_as_ok();
            for (auto &block_ds : mc_block_ds.shard_blocks_) {
                if (block_ds.block_data->block_id().is_masterchain()) {
                    mc_block_ds.config_ = block::ConfigInfo::extract_config(block_ds.block_state, block::ConfigInfo::needCapabilities | block::ConfigInfo::needLibraries | block::ConfigInfo::needWorkchainInfo | block::ConfigInfo::needSpecialSmc).move_as_ok();
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

    if (seqno > last_fetched_seqno_) {
        LOG(INFO) << "Setting last fetched seqno to " << seqno;
        last_fetched_seqno_ = seqno;

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

// int seqno = 37786481;

void TraceEmulatorScheduler::alarm() {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ton::BlockSeqno> R){
        if (R.is_error()) {
            LOG(ERROR) << "Failed to update last seqno: " << R.move_as_error();
            return;
        }
        td::actor::send_closure(SelfId, &TraceEmulatorScheduler::got_last_mc_seqno, R.move_as_ok());
        // td::actor::send_closure(SelfId, &TraceEmulatorScheduler::got_last_mc_seqno, seqno++); // for debugging
    });
    td::actor::send_closure(db_scanner_, &DbScanner::get_last_mc_seqno, std::move(P));

    fetch_seqnos();

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

    alarm_timestamp() = td::Timestamp::in(0.3);
}
