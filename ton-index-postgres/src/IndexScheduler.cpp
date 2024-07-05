#include "IndexScheduler.h"
#include "td/utils/Time.h"
#include "td/utils/StringBuilder.h"
#include <iostream>


std::string get_time_string(double seconds) {
    int days = int(seconds / (60 * 60 * 24));
    int hours = int(seconds / (60 * 60)) % 24;
    int mins = int(seconds / 60) % 60;
    int secs = int(seconds) % 60;

    td::StringBuilder builder;
    bool flag = false;
    if (days > 0) {
        builder << days << "d ";
        flag = true;
    }
    if (flag || (hours > 0)) {
        builder << hours << "h ";
        flag = true;
    }
    if (flag || (mins > 0)) {
        builder << mins << "m ";
        flag = true;
    }
    builder << secs << "s";
    return builder.as_cslice().str();
}

void IndexScheduler::alarm() {
    alarm_timestamp() = td::Timestamp::in(1.0);
    double alpha = 0.9;
    if (last_existing_seqno_count_ == 0) 
        last_existing_seqno_count_ = existing_seqnos_.size();
    avg_tps_ = alpha * avg_tps_ + (1 - alpha) * (existing_seqnos_.size() - last_existing_seqno_count_);
    last_existing_seqno_count_ = existing_seqnos_.size();
    
    if (next_print_stats_.is_in_past()) {
        print_stats();
        next_print_stats_ = td::Timestamp::in(stats_timeout_);
    }

    auto Q = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<QueueState> R){
        R.ensure();
        td::actor::send_closure(SelfId, &IndexScheduler::got_insert_queue_state, R.move_as_ok());
    });
    td::actor::send_closure(insert_manager_, &InsertManagerInterface::get_insert_queue_state, std::move(Q));

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ton::BlockSeqno> R){
        if (R.is_error()) {
            LOG(ERROR) << "Failed to update last seqno: " << R.move_as_error();
            return;
        }
        td::actor::send_closure(SelfId, &IndexScheduler::got_last_known_seqno, R.move_as_ok());
    });
    td::actor::send_closure(db_scanner_, &DbScanner::get_last_mc_seqno, std::move(P));
}

void IndexScheduler::run() {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<std::vector<uint32_t>> R) {
        td::actor::send_closure(SelfId, &IndexScheduler::got_existing_seqnos, std::move(R));
    });
    td::actor::send_closure(insert_manager_, &InsertManagerInterface::get_existing_seqnos, std::move(P), from_seqno_, 0);
}

void IndexScheduler::got_existing_seqnos(td::Result<std::vector<uint32_t>> R) {
    if (R.is_error()) {
        LOG(ERROR) << "Error reading existing seqnos: " << R.move_as_error();
        return;
    }

    for (auto value : R.move_as_ok()) {
        existing_seqnos_.insert(value);
    }
    LOG(INFO) << "Found " << existing_seqnos_.size() << " existing seqnos";

    alarm_timestamp() = td::Timestamp::in(1.0);
    next_print_stats_ = td::Timestamp::in(stats_timeout_);
}

void IndexScheduler::got_last_known_seqno(uint32_t last_known_seqno) {
    int skipped_count_ = 0;
    for(auto seqno = last_known_seqno_ + 1; seqno <= last_known_seqno; ++seqno) {
        if (existing_seqnos_.find(seqno) != existing_seqnos_.end()) {
            ++skipped_count_;
        }
        else {
            queued_seqnos_.push(seqno);
        }
    }
    if (skipped_count_ > 0) {
        LOG(INFO) << "Skipped " << skipped_count_ << " existing seqnos";
    }
    last_known_seqno_ = last_known_seqno;
}

void IndexScheduler::schedule_seqno(uint32_t mc_seqno) {
    LOG(DEBUG) << "Scheduled seqno " << mc_seqno;

    processing_seqnos_.insert(mc_seqno);
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<MasterchainBlockDataState> R) {
        if (R.is_error()) {
            LOG(ERROR) << "Failed to fetch seqno " << mc_seqno << ": " << R.move_as_error();
            td::actor::send_closure(SelfId, &IndexScheduler::reschedule_seqno, mc_seqno);
            return;
        }
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_fetched, mc_seqno, R.move_as_ok());
    });

    td::actor::send_closure(db_scanner_, &DbScanner::fetch_seqno, mc_seqno, std::move(P));
}

void IndexScheduler::reschedule_seqno(uint32_t mc_seqno) {
    LOG(WARNING) << "Rescheduling seqno " << mc_seqno;
    processing_seqnos_.erase(mc_seqno);
    queued_seqnos_.push(mc_seqno);

    // td::actor::send_closure(actor_id(this), &IndexScheduler::schedule_next_seqnos);
}

void IndexScheduler::seqno_fetched(uint32_t mc_seqno, MasterchainBlockDataState block_data_state) {
    LOG(DEBUG) << "Fetched seqno " << mc_seqno << ": blocks=" << block_data_state.shard_blocks_diff_.size() << " shards=" << block_data_state.shard_blocks_.size();

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<ParsedBlockPtr> R) {
        if (R.is_error()) {
            LOG(ERROR) << "Failed to parse seqno " << mc_seqno << ": " << R.move_as_error();
            td::actor::send_closure(SelfId, &IndexScheduler::reschedule_seqno, mc_seqno);
            return;
        }
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_parsed, mc_seqno, R.move_as_ok());
    });
    td::actor::send_closure(parse_manager_, &ParseManager::parse, mc_seqno, std::move(block_data_state), std::move(P));
}

void IndexScheduler::seqno_parsed(uint32_t mc_seqno, ParsedBlockPtr parsed_block) {
    LOG(DEBUG) << "Parsed seqno " << mc_seqno;

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno, parsed_block](td::Result<td::Unit> R) {
        if (R.is_error()) {
            LOG(ERROR) << "Failed to process interfaces for  seqno " << mc_seqno << ": " << R.move_as_error();
            td::actor::send_closure(SelfId, &IndexScheduler::reschedule_seqno, mc_seqno);
            return;
        }
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_interfaces_processed, mc_seqno, std::move(parsed_block));
    });
    td::actor::send_closure(event_processor_, &EventProcessor::process, std::move(parsed_block), std::move(P));
}

void IndexScheduler::seqno_interfaces_processed(uint32_t mc_seqno, ParsedBlockPtr parsed_block) {
    LOG(DEBUG) << "Interfaces processed for seqno " << mc_seqno;

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<td::Unit> R) {
        if (R.is_error()) {
            LOG(ERROR) << "Failed to insert seqno " << mc_seqno << ": " << R.move_as_error();
            td::actor::send_closure(SelfId, &IndexScheduler::reschedule_seqno, mc_seqno);
            return;
        }
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_inserted, mc_seqno, R.move_as_ok());
    });
    auto Q = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<QueueState> R){
        R.ensure();
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_queued_to_insert, mc_seqno, R.move_as_ok());
    });
    td::actor::send_closure(insert_manager_, &InsertManagerInterface::insert, mc_seqno, std::move(parsed_block), std::move(Q), std::move(P));
}

void IndexScheduler::print_stats() {
    if (last_indexed_seqno_ == 0) {
        LOG(WARNING) << "No seqnos indexed yet...";
        return;
    }
    double eta = (last_known_seqno_ - last_indexed_seqno_) / avg_tps_;
    LOG(INFO) << "Last: " << last_indexed_seqno_ << " / " << last_known_seqno_ 
              << "\tBlk/s: " << avg_tps_
              << "\tETA: " << get_time_string(eta)
              << "\tQ[" << cur_queue_state_.mc_blocks_ << "M, " 
              << cur_queue_state_.blocks_ << "b, " 
              << cur_queue_state_.txs_ << "t, " 
              << cur_queue_state_.msgs_ << "m]";
}

void IndexScheduler::seqno_queued_to_insert(uint32_t mc_seqno, QueueState status) {
    LOG(DEBUG) << "Seqno queued to insert " << mc_seqno;

    processing_seqnos_.erase(mc_seqno);
    got_insert_queue_state(status);
}

void IndexScheduler::got_insert_queue_state(QueueState status) {
    cur_queue_state_ = status;
    bool accept_blocks = status < max_queue_;
    if (accept_blocks) {
        td::actor::send_closure(actor_id(this), &IndexScheduler::schedule_next_seqnos);
    }
}

void IndexScheduler::seqno_inserted(uint32_t mc_seqno, td::Unit result) {
    existing_seqnos_.insert(mc_seqno);
    if (mc_seqno > last_indexed_seqno_) {
        last_indexed_seqno_ = mc_seqno;
    }
}

void IndexScheduler::schedule_next_seqnos() {
    LOG(DEBUG) << "Scheduling next seqnos. Current tasks: " << processing_seqnos_.size();
    while (!queued_seqnos_.empty() && (processing_seqnos_.size() < max_active_tasks_)) {
        uint32_t seqno = queued_seqnos_.front();
        queued_seqnos_.pop();
        schedule_seqno(seqno);
    }
}
