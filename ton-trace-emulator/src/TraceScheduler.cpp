#include "TraceScheduler.h"
#include "BlockEmulator.h"
#include "TraceInserter.h"


void TraceEmulatorScheduler::start_up() {
    alarm_timestamp() = td::Timestamp::in(0.1);

    if (global_config_path_.empty() || inet_addr_.empty()) {
        LOG(WARNING) << "Global config path or inet addr is empty. OverlayListener was not started.";
    } else {
        overlay_listener_ = td::actor::create_actor<OverlayListener>("OverlayListener", global_config_path_, inet_addr_, insert_trace_);
    }
}

void TraceEmulatorScheduler::got_last_mc_seqno(ton::BlockSeqno last_known_seqno) {
    assert(last_known_seqno >= last_known_seqno_);

    if (last_known_seqno == last_known_seqno_) {
        return;
    }
    LOG(INFO) << "New masterchain block " << last_known_seqno;
    if (last_known_seqno > last_known_seqno_ + 1) {
        LOG(WARNING) << "More than one new masterchain block appeared. Skipping to the newest one, from " << last_known_seqno_ << " to " << last_known_seqno;
    }

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), last_known_seqno](td::Result<MasterchainBlockDataState> R) {
        if (R.is_error()) {
            td::actor::send_closure(SelfId, &TraceEmulatorScheduler::fetch_error, last_known_seqno, R.move_as_error());
            return;
        }
        auto mc_block_ds = R.move_as_ok();
        for (auto &block_ds : mc_block_ds.shard_blocks_) {
            if (block_ds.block_data->block_id().is_masterchain()) {
                mc_block_ds.config_ = block::ConfigInfo::extract_config(block_ds.block_state, block::ConfigInfo::needCapabilities | block::ConfigInfo::needLibraries | block::ConfigInfo::needWorkchainInfo | block::ConfigInfo::needSpecialSmc).move_as_ok();
                break;
            }
        }
        td::actor::send_closure(SelfId, &TraceEmulatorScheduler::seqno_fetched, last_known_seqno, std::move(mc_block_ds));
    });
    td::actor::send_closure(db_scanner_, &DbScanner::fetch_seqno, last_known_seqno, std::move(P));
}

void TraceEmulatorScheduler::fetch_error(std::uint32_t seqno, td::Status error) {
    LOG(ERROR) << "Failed to fetch seqno " << seqno << ": " << std::move(error);
    alarm_timestamp() = td::Timestamp::in(0.1);
}

void TraceEmulatorScheduler::seqno_fetched(std::uint32_t seqno, MasterchainBlockDataState mc_data_state) {
    LOG(DEBUG) << "Fetched seqno " << seqno;
    last_known_seqno_ = seqno;

    if (!overlay_listener_.empty()) {
        td::actor::send_closure(overlay_listener_, &OverlayListener::set_mc_data_state, mc_data_state);
    }

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), blkid = mc_data_state.shard_blocks_[0].block_data->block_id().id](td::Result<> R) {
        if (R.is_error()) {
            LOG(ERROR) << "Error emulating mc block " << blkid.to_str();
            return;
        }
        LOG(INFO) << "Success emulating mc block " << blkid.to_str();
    });


    td::actor::create_actor<McBlockEmulator>("McBlockEmulator", mc_data_state, insert_trace_, std::move(P)).release();
}

// int seqno = 37786481;

void TraceEmulatorScheduler::alarm() {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ton::BlockSeqno> R){
        if (R.is_error()) {
            LOG(ERROR) << "Failed to update last seqno: " << R.move_as_error();
            std::_Exit(2);
            return;
        }
        td::actor::send_closure(SelfId, &TraceEmulatorScheduler::got_last_mc_seqno, R.move_as_ok());
        // td::actor::send_closure(SelfId, &TraceEmulatorScheduler::got_last_mc_seqno, seqno++); // for debugging
    });
    td::actor::send_closure(db_scanner_, &DbScanner::get_last_mc_seqno, std::move(P));

    alarm_timestamp() = td::Timestamp::in(2.0);
}
