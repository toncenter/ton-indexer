#include "TraceTaskScheduler.h"
#include "TaskResultInserter.h"
#include "td/utils/filesystem.h"
#include "common/delay.h"


void TraceTaskScheduler::start_up() {
    alarm_timestamp() = td::Timestamp::in(0.1);
}

void TraceTaskScheduler::got_last_mc_seqno(ton::BlockSeqno new_last_known_seqno) {
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

void TraceTaskScheduler::fetch_seqnos() {
    for (auto it = seqnos_to_fetch_.begin(); it != seqnos_to_fetch_.end(); ) {
        auto seqno = *it;

        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), seqno](td::Result<MasterchainBlockDataState> R) {
            if (R.is_error()) {
                td::actor::send_closure(SelfId, &TraceTaskScheduler::fetch_error, seqno, R.move_as_error());
                return;
            }
            auto mc_block_ds = R.move_as_ok();
            for (auto &block_ds : mc_block_ds.shard_blocks_) {
                if (block_ds.block_data->block_id().is_masterchain()) {
                    mc_block_ds.config_ = block::ConfigInfo::extract_config(block_ds.block_state, block::ConfigInfo::needCapabilities | block::ConfigInfo::needLibraries | block::ConfigInfo::needWorkchainInfo | block::ConfigInfo::needSpecialSmc).move_as_ok();
                    break;
                }
            }
            td::actor::send_closure(SelfId, &TraceTaskScheduler::seqno_fetched, seqno, std::move(mc_block_ds));
        });
        td::actor::send_closure(db_scanner_, &DbScanner::fetch_seqno, seqno, std::move(P));

        it = seqnos_to_fetch_.erase(it);
    }
}

void TraceTaskScheduler::fetch_error(std::uint32_t seqno, td::Status error) {
    if (error.code() != ton::ErrorCode::notready) {
        LOG(ERROR) << "Failed to fetch seqno " << seqno << ": " << std::move(error);
    }
    seqnos_to_fetch_.insert(seqno);
    alarm_timestamp() = td::Timestamp::in(0.1);
}

void TraceTaskScheduler::seqno_fetched(std::uint32_t seqno, MasterchainBlockDataState mc_data_state) {
    LOG(INFO) << "Fetched seqno " << seqno;

    if (seqno > last_fetched_seqno_) {
        auto time_diff = td::Clocks::system() - mc_data_state.shard_blocks_[0].handle->unix_time();
        LOG(INFO) << "Setting last fetched seqno to " << seqno << ", created " << td::StringBuilder::FixedDouble(time_diff, 2) << "s ago";
        last_fetched_seqno_ = seqno;

        if (!redis_listener_.empty()) {
            td::actor::send_closure(redis_listener_, &RedisListener::set_mc_data_state, mc_data_state);
        }
    }
}

// static uint32_t seqno = 43074152;
// static bool changed = false;

void TraceTaskScheduler::alarm() {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ton::BlockSeqno> R){
        if (R.is_error()) {
            LOG(ERROR) << "Failed to update last seqno: " << R.move_as_error();
            std::_Exit(2);
            return;
        }
        td::actor::send_closure(SelfId, &TraceTaskScheduler::got_last_mc_seqno, R.move_as_ok());

        // td::actor::send_closure(SelfId, &TraceTaskScheduler::got_last_mc_seqno, seqno - 1);
        // if (!changed) {
        //     seqno += 1;
        //     changed = true;
        // }
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
