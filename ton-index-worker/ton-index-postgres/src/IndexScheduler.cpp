#include "IndexScheduler.h"
#include "td/utils/Time.h"
#include "td/utils/StringBuilder.h"
#include <iostream>
#include "BlockInterfacesDetector.h"
#include "Statistics.h"
#include "td/utils/filesystem.h"
#include "common/delay.h"


void IndexScheduler::start_up() {
    trace_assembler_ = td::actor::create_actor<TraceAssembler>("trace_assembler", working_dir_ + "/trace_assembler", max_queue_.mc_blocks_);
}

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
    alarm_timestamp() = td::Timestamp::in(is_in_sync_ ? 1.0 : 0.1);

    td::Timestamp now = td::Timestamp::now();
    double dt = 0.0;
    if (last_alarm_timestamp_) {
        dt = now.at() - last_alarm_timestamp_.at();
    }
    last_alarm_timestamp_ = now;

    constexpr double alpha = 0.99; // Decay factor per second

    auto current_count = indexed_seqnos_.size();

    if (last_indexed_seqno_count_ == 0) {
        last_indexed_seqno_count_ = current_count;
    }

    if (dt > 0) {
        auto delta_count = current_count - last_indexed_seqno_count_;
        double new_tps = static_cast<double>(delta_count) / dt;
        double alpha_dt = std::pow(alpha, dt);
        avg_tps_ = alpha_dt * avg_tps_ + (1 - alpha_dt) * new_tps;
    }

    last_indexed_seqno_count_ = current_count;

    if (next_print_stats_.is_in_past()) {
        print_stats();
        next_print_stats_ = td::Timestamp::in(stats_timeout_);
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
        td::actor::send_closure(SelfId, &IndexScheduler::got_newest_mc_seqno, R.move_as_ok());
    });
    td::actor::send_closure(db_scanner_, &DbScanner::get_last_mc_seqno, std::move(P));
}

void IndexScheduler::run() {
    if (force_index_) {
        LOG(WARNING) << "Force reindexing enabled";
        td::actor::send_closure(trace_assembler_, &TraceAssembler::set_expected_seqno, from_seqno_);
        last_known_seqno_ = from_seqno_ - 1;
        alarm_timestamp() = td::Timestamp::now();
        return;
    }

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<std::vector<std::uint32_t>> R) {
        td::actor::send_closure(SelfId, &IndexScheduler::process_existing_seqnos, std::move(R));
    });
    td::actor::send_closure(insert_manager_, &InsertManagerInterface::get_existing_seqnos, std::move(P), from_seqno_, to_seqno_);
}

void IndexScheduler::process_existing_seqnos(td::Result<std::vector<std::uint32_t>> R) {
    if (R.is_error()) {
        LOG(ERROR) << "Error reading existing seqnos: " << R.move_as_error();
        return;
    }

    auto existing_db_seqnos = R.move_as_ok();
    std::sort(existing_db_seqnos.begin(), existing_db_seqnos.end());

    // Find first gap in continuous sequence from current from_seqno_
    ton::BlockSeqno next_seqno = from_seqno_;
    for (auto seqno : existing_db_seqnos) {
        if (seqno == next_seqno) {
            ++next_seqno;
        } else {
            break;
        }
    }

    LOG(INFO) << "Found continuous sequence up to " << (next_seqno - 1) 
              << " in DB (" << (next_seqno - from_seqno_) << " blocks)";

    LOG(INFO) << "Accepted " << next_seqno - from_seqno_ << " of " << existing_db_seqnos.size() 
              << " existing seqnos in DB (continuous increasing sequence)";
    LOG(INFO) << "Next seqno: " << next_seqno;

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), next_seqno](td::Result<ton::BlockSeqno> R) {
        if (R.is_error()) {
            LOG(WARNING) << "TraceAssembler state not found for seqno " << (next_seqno - 1);
            td::actor::send_closure(SelfId, &IndexScheduler::handle_missing_ta_state, next_seqno);
        } else {
            LOG(INFO) << "Restored TraceAssembler state for seqno " << R.ok();
            td::actor::send_closure(SelfId, &IndexScheduler::handle_valid_ta_state, R.move_as_ok());
        }
    });

    td::actor::send_closure(trace_assembler_, &TraceAssembler::restore_state, next_seqno - 1, std::move(P));
}

const int TRACE_BACKTRACK_LIMIT = 50;

void IndexScheduler::handle_missing_ta_state(ton::BlockSeqno next_seqno) {
    LOG(WARNING) << "Trace assembler state missing for seqno " << (next_seqno - 1);
    
    const ton::BlockSeqno backtrack_point = std::max(
        from_seqno_,
        static_cast<int32_t>(next_seqno) - TRACE_BACKTRACK_LIMIT
    );

    CHECK(backtrack_point >= from_seqno_);

    if (backtrack_point != from_seqno_) {
        LOG(WARNING) << "Backtracking " << (from_seqno_ - backtrack_point) 
                     << " blocks to capture potential ongoing traces";
    }

    from_seqno_ = backtrack_point;
    last_known_seqno_ = from_seqno_ - 1;
    LOG(INFO) << "Resetting indexing start point to " << from_seqno_;

    td::actor::send_closure(trace_assembler_, &TraceAssembler::set_expected_seqno, from_seqno_);
    alarm_timestamp() = td::Timestamp::now();
}

void IndexScheduler::handle_valid_ta_state(ton::BlockSeqno last_state_seqno) {
    if (last_state_seqno < from_seqno_) {
        LOG(WARNING) << "Trace assembler state " << last_state_seqno << " is lower than --from " << from_seqno_;
        td::actor::send_closure(trace_assembler_, &TraceAssembler::reset_state);
        last_known_seqno_ = from_seqno_ - 1;
    } else {
        last_known_seqno_ = last_state_seqno;
        from_seqno_ = last_state_seqno + 1;
    }
    LOG(INFO) << "Starting indexing from seqno: " << from_seqno_;

    td::actor::send_closure(trace_assembler_, &TraceAssembler::set_expected_seqno, from_seqno_);
    alarm_timestamp() = td::Timestamp::now();
}

const int IS_IN_SYNC_THRESHOLD = 3;

void IndexScheduler::got_newest_mc_seqno(std::uint32_t newest_mc_seqno) {
    if (to_seqno_ && last_known_seqno_ > to_seqno_)
        return;
    int skipped_count = 0;
    for (auto seqno = last_known_seqno_ + 1; seqno <= newest_mc_seqno; ++seqno) {
        const bool should_skip = indexed_seqnos_.count(seqno);
        const bool in_range = (from_seqno_ <= seqno) && (to_seqno_ == 0 || seqno <= to_seqno_);
        
        if (should_skip) {
            skipped_count++;
        } else if (in_range) {
            queued_seqnos_.push_back(seqno);
        }
    }
    if (skipped_count > 0) {
        LOG(INFO) << "Skipped " << skipped_count << " existing seqnos";
    }
    last_known_seqno_ = newest_mc_seqno;

    if (!is_in_sync_ && (last_known_seqno_ - last_indexed_seqno_ <= IS_IN_SYNC_THRESHOLD)) {
        LOG(INFO) << "Indexer is in sync with TON node";
        is_in_sync_ = true;
        td::actor::send_closure(db_scanner_, &DbScanner::set_catch_up_interval, 0.1);
    } else if (is_in_sync_ && (last_known_seqno_ - last_indexed_seqno_ > IS_IN_SYNC_THRESHOLD)) {
        LOG(WARNING) << "Indexing is out of sync with TON node";
        is_in_sync_ = false;
        td::actor::send_closure(db_scanner_, &DbScanner::set_catch_up_interval, 1.0);
    }
}

void IndexScheduler::schedule_seqno(std::uint32_t mc_seqno) {
    LOG(DEBUG) << "Scheduled seqno " << mc_seqno;

    processing_seqnos_.insert(mc_seqno);
    timers_[mc_seqno] = td::Timer();
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno, is_in_sync = is_in_sync_](td::Result<MasterchainBlockDataState> R) {
        if (R.is_error()) {
            if (!is_in_sync) {
                LOG(ERROR) << "Failed to fetch seqno " << mc_seqno << ": " << R.error();
            }
            td::actor::send_closure(SelfId, &IndexScheduler::reschedule_seqno, mc_seqno, is_in_sync);
            return;
        }
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_fetched, mc_seqno, R.move_as_ok());
    });

    td::actor::send_closure(db_scanner_, &DbScanner::fetch_seqno, mc_seqno, std::move(P));
}

void IndexScheduler::reschedule_seqno(std::uint32_t mc_seqno, bool silent) {
    if (!silent) {
        LOG(WARNING) << "Rescheduling seqno " << mc_seqno;
    }
    processing_seqnos_.erase(mc_seqno);
    queued_seqnos_.push_front(mc_seqno);
}

void IndexScheduler::seqno_fetched(std::uint32_t mc_seqno, MasterchainBlockDataState block_data_state) {
    LOG(DEBUG) << "Fetched seqno " << mc_seqno << ": blocks=" << block_data_state.shard_blocks_diff_.size() << " shards=" << block_data_state.shard_blocks_.size();

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<ParsedBlockPtr> R) {
        if (R.is_error()) {
            LOG(ERROR) << "Failed to parse seqno " << mc_seqno << ": " << R.move_as_error();
            td::actor::send_closure(SelfId, &IndexScheduler::reschedule_seqno, mc_seqno, false);
            return;
        }
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_parsed, mc_seqno, R.move_as_ok());
    });

    td::actor::send_closure(db_scanner_, &DbScanner::get_cell_db_reader, 
        [SelfId = actor_id(this), parse_manager = parse_manager_, mc_seqno, block_data_state, P = std::move(P)](td::Result<std::shared_ptr<vm::CellDbReader>> cell_db_reader) mutable {
            CHECK(cell_db_reader.is_ok());
            td::actor::send_closure(parse_manager, &ParseManager::parse, mc_seqno, std::move(block_data_state), cell_db_reader.move_as_ok(), std::move(P));
    });
}

void IndexScheduler::seqno_parsed(std::uint32_t mc_seqno, ParsedBlockPtr parsed_block) {
    LOG(DEBUG) << "Parsed seqno " << mc_seqno;

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<ParsedBlockPtr> R) {
        if (R.is_error()) {
            LOG(ERROR) << "Failed to asseble traces for seqno " << mc_seqno << ": " << R.move_as_error();
            td::actor::send_closure(SelfId, &IndexScheduler::reschedule_seqno, mc_seqno, false);
            return;
        }
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_traces_assembled, mc_seqno, R.move_as_ok());
    });
    td::actor::send_closure(trace_assembler_, &TraceAssembler::assemble, mc_seqno, std::move(parsed_block), std::move(P));
}

void IndexScheduler::seqno_traces_assembled(std::uint32_t mc_seqno, ParsedBlockPtr parsed_block) {
    LOG(DEBUG) << "Assembled traces for seqno " << mc_seqno;

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<ParsedBlockPtr> R) {
        if (R.is_error()) {
            LOG(ERROR) << "Failed to detect interfaces for seqno " << mc_seqno << ": " << R.move_as_error();
            td::actor::send_closure(SelfId, &IndexScheduler::reschedule_seqno, mc_seqno, false);
            return;
        }
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_interfaces_processed, mc_seqno, R.move_as_ok());
    });
    td::actor::create_actor<BlockInterfaceProcessor>("BlockInterfaceProcessor", std::move(parsed_block), std::move(P)).release();
}

void IndexScheduler::seqno_interfaces_processed(std::uint32_t mc_seqno, ParsedBlockPtr parsed_block) {
    LOG(DEBUG) << "Interfaces processed for seqno " << mc_seqno;

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<ParsedBlockPtr> R) {
        if (R.is_error()) {
            LOG(ERROR) << "Failed to detect actions for seqno " << mc_seqno << ": " << R.move_as_error();
            td::actor::send_closure(SelfId, &IndexScheduler::reschedule_seqno, mc_seqno, false);
            return;
        }
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_actions_processed, mc_seqno, R.move_as_ok());
    });
    td::actor::create_actor<ActionDetector>("ActionDetector", std::move(parsed_block), std::move(P)).release();
}

void IndexScheduler::seqno_actions_processed(std::uint32_t mc_seqno, ParsedBlockPtr parsed_block) {
    LOG(DEBUG) << "Actions processed for seqno " << mc_seqno;

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno, timer = td::Timer{}](td::Result<td::Unit> R) {
        if (R.is_error()) {
            LOG(ERROR) << "Failed to insert seqno " << mc_seqno << ": " << R.move_as_error();
            td::actor::send_closure(SelfId, &IndexScheduler::reschedule_seqno, mc_seqno, false);
            return;
        }
        g_statistics.record_time(INSERT_SEQNO, timer.elapsed() * 1e3);
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_inserted, mc_seqno, R.move_as_ok());
    });
    auto Q = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<QueueState> R){
        R.ensure();
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_queued_to_insert, mc_seqno, R.move_as_ok());
    });
    td::actor::send_closure(insert_manager_, &InsertManagerInterface::insert, mc_seqno, std::move(parsed_block), 
                                is_in_sync_, std::move(Q), std::move(P));
}

void IndexScheduler::print_stats() {
    double eta = (last_known_seqno_ - last_indexed_seqno_) / avg_tps_;
    td::StringBuilder sb;
    sb << last_indexed_seqno_ << " / "; 
    auto end_seqno = last_known_seqno_;
    if (to_seqno_ > 0) {
        end_seqno = to_seqno_;
    }

    sb << end_seqno;
    sb << "\t" << td::StringBuilder::FixedDouble(avg_tps_, 2) << " mb/s (" << get_time_string(eta) << ")"
       << "\tQ[" << cur_queue_state_.mc_blocks_ << "M, " 
       << cur_queue_state_.blocks_ << "b, " 
       << cur_queue_state_.txs_ << "t, " 
       << cur_queue_state_.msgs_ << "m, "
       << cur_queue_state_.traces_ << "T]";
    LOG(INFO) << sb.as_cslice().str();
}

void IndexScheduler::seqno_queued_to_insert(std::uint32_t mc_seqno, QueueState status) {
    LOG(DEBUG) << "Seqno queued to insert " << mc_seqno;

    processing_seqnos_.erase(mc_seqno);
    got_insert_queue_state(status);
}

void IndexScheduler::got_insert_queue_state(QueueState status) {
    cur_queue_state_ = status;
    bool accept_blocks = status < max_queue_;
    if (accept_blocks) {
        schedule_next_seqnos();
    }
}

void IndexScheduler::seqno_inserted(std::uint32_t mc_seqno, td::Unit result) {
    indexed_seqnos_.insert(mc_seqno);
    if (mc_seqno > last_indexed_seqno_) {
        last_indexed_seqno_ = mc_seqno;
    }
    g_statistics.record_time(PROCESS_SEQNO, timers_[mc_seqno].elapsed() * 1e3);
    timers_.erase(mc_seqno);
}

void IndexScheduler::schedule_next_seqnos() {
    LOG(DEBUG) << "Scheduling next seqnos. Current tasks: " << processing_seqnos_.size();
    while (!queued_seqnos_.empty() && (processing_seqnos_.size() < max_active_tasks_)) {
        std::uint32_t seqno = queued_seqnos_.front();
        queued_seqnos_.pop_front();
        schedule_seqno(seqno);
    }

    if(to_seqno_ > 0 && last_known_seqno_ > to_seqno_ 
       && queued_seqnos_.empty() && processing_seqnos_.empty()
       && cur_queue_state_.blocks_ == 0) {
        // stop();
        return;
    }
}
