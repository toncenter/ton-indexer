#include "IndexScheduler.h"
#include "BlockInterfacesDetector.h"
#include "Statistics.h"
#include "common/delay.h"
#include "td/utils/StringBuilder.h"
#include "td/utils/Time.h"
#include "td/utils/filesystem.h"
#include "ton/ton-tl.hpp"
#include <chrono>

namespace {

const schema::Block* find_masterchain_block(const ParsedBlock& parsed_block) {
    for (const auto& block : parsed_block.blocks_) {
        if (block.workchain == -1) {
            return &block;
        }
    }
    return parsed_block.blocks_.empty() ? nullptr : &parsed_block.blocks_.front();
}

std::int64_t processing_lag_ms(std::int32_t gen_utime) {
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    const auto block_time_ms = static_cast<std::int64_t>(gen_utime) * 1000;
    return std::max<std::int64_t>(0, now_ms - block_time_ms);
}

}  // namespace

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
    update_sync_state();
    const bool event_driven = is_in_sync_ && mc_applied_events_fresh();
    alarm_timestamp() = td::Timestamp::in(is_in_sync_ && !event_driven ? 0.1 : 1.0);

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

    request_insert_queue_state();

    if (!event_driven) {
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ton::BlockSeqno> R){
            if (R.is_error()) {
                LOG(ERROR) << "Failed to update last seqno: " << R.move_as_error();
                return;
            }
            td::actor::send_closure(SelfId, &IndexScheduler::got_newest_mc_seqno, R.move_as_ok());
        });
        td::actor::send_closure(db_scanner_, &DbScanner::get_last_mc_seqno, std::move(P));
    }
    try_event_catch_up();
}

void IndexScheduler::request_insert_queue_state() {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<QueueState> R){
        R.ensure();
        td::actor::send_closure(SelfId, &IndexScheduler::got_insert_queue_state, R.move_as_ok());
    });
    td::actor::send_closure(insert_manager_, &InsertManagerInterface::get_insert_queue_state, std::move(P));
}

const ton::BlockSeqno FIRST_INDEXABLE_MC_SEQNO = 1;

// Startup policy:
// - Bounded archive mode is selected by --from + --to. It does not use
//   _ton_indexer_progress: without --force it reads existing masterchain seqnos
//   from blocks and only writes missing ones; with --force it writes the whole
//   range. In both cases --prewarm only extends the read/TraceAssembler start,
//   not the insert range.
// - Normal mode uses _ton_indexer_progress as the durable source of truth.
//   If the row is missing, it is initialized from --from and indexing starts
//   exactly there. Once the row exists, --from is only a deployment default and
//   is ignored on restart.
// - Forced mode still starts from CLI --from, but first ensures that the
//   progress row exists so the Postgres trigger can keep advancing it.
// - Masterchain seqno 0 is not readable from the node DB, so all starts are
//   clamped to FIRST_INDEXABLE_MC_SEQNO.
// - On restart, --prewarm controls how far TraceAssembler may move the start
//   point backwards. If no suitable state exists, the assembler is reset and
//   the same bounded fallback is used.
void IndexScheduler::run() {
    if (is_bounded_archive_range()) {
        LOG(INFO) << "Bounded archive indexing enabled: from=" << from_seqno_
                  << ", to=" << to_seqno_ << ", prewarm=" << prewarm_count_;
        if (force_index_) {
            LOG(WARNING) << "Force reindexing enabled for bounded archive range";
            start_from_seqno(prewarm_start_seqno(static_cast<ton::BlockSeqno>(from_seqno_)), true);
            return;
        }

        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<std::vector<std::uint32_t>> R) {
            td::actor::send_closure(SelfId, &IndexScheduler::process_archive_existing_seqnos, std::move(R));
        });
        td::actor::send_closure(insert_manager_, &InsertManagerInterface::get_existing_seqnos, std::move(P), from_seqno_, to_seqno_);
        return;
    }

    if (force_index_) {
        LOG(WARNING) << "Force reindexing enabled";
        const auto start_seqno = from_seqno_ > 0 ? static_cast<ton::BlockSeqno>(from_seqno_) : FIRST_INDEXABLE_MC_SEQNO;
        auto P = td::PromiseCreator::lambda(
            [SelfId = actor_id(this), start_seqno](td::Result<bool> R) {
                td::actor::send_closure(
                    SelfId, &IndexScheduler::process_force_resume_state_initialized, std::move(R), start_seqno);
            });
        td::actor::send_closure(
            insert_manager_, &InsertManagerInterface::ensure_resume_state_initialized, std::move(P), start_seqno);
        return;
    }

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<InsertManagerInterface::ResumeState> R) {
        td::actor::send_closure(SelfId, &IndexScheduler::process_resume_seqno, std::move(R));
    });
    td::actor::send_closure(insert_manager_, &InsertManagerInterface::get_resume_seqno, std::move(P), from_seqno_, to_seqno_);
}

void IndexScheduler::process_force_resume_state_initialized(td::Result<bool> R, ton::BlockSeqno start_seqno) {
    if (R.is_error()) {
        LOG(ERROR) << "Error initializing resume state for forced indexing: " << R.move_as_error();
        return;
    }
    if (R.move_as_ok()) {
        LOG(INFO) << "Initialized indexer progress before forced indexing";
    }
    start_from_seqno(start_seqno, true);
}

void IndexScheduler::process_resume_seqno(td::Result<InsertManagerInterface::ResumeState> R) {
    if (R.is_error()) {
        LOG(ERROR) << "Error reading resume seqno: " << R.move_as_error();
        return;
    }

    auto resume_state = R.move_as_ok();
    auto next_seqno = resume_state.next_seqno;
    if (next_seqno < FIRST_INDEXABLE_MC_SEQNO) {
        LOG(WARNING) << "Resume seqno " << next_seqno << " is below first indexable masterchain seqno "
                     << FIRST_INDEXABLE_MC_SEQNO << "; starting from " << FIRST_INDEXABLE_MC_SEQNO;
        next_seqno = FIRST_INDEXABLE_MC_SEQNO;
    }
    LOG(INFO) << "Next seqno from indexer progress: " << next_seqno;

    if (resume_state.initialized_from_cli) {
        LOG(INFO) << "Initialized indexer progress from --from; starting exactly from " << next_seqno;
        start_from_seqno(next_seqno, true);
        return;
    }

    if (from_seqno_ > 0 && from_seqno_ != static_cast<std::int32_t>(next_seqno)) {
        LOG(INFO) << "Ignoring --from " << from_seqno_ << " because persisted indexer progress resumes from "
                  << next_seqno;
    }

    if (next_seqno == FIRST_INDEXABLE_MC_SEQNO) {
        start_from_seqno(next_seqno, true);
        return;
    }

    const auto fallback_seqno = prewarm_start_seqno(next_seqno);
    auto P = td::PromiseCreator::lambda(
        [SelfId = actor_id(this), next_seqno, fallback_seqno](td::Result<ton::BlockSeqno> R) {
        if (R.is_error()) {
            LOG(WARNING) << "TraceAssembler state not found for seqno " << (next_seqno - 1);
            td::actor::send_closure(SelfId, &IndexScheduler::start_from_seqno, fallback_seqno, true);
        } else {
            auto restored_seqno = R.move_as_ok();
            LOG(INFO) << "Restored TraceAssembler state for seqno " << restored_seqno;
            if (restored_seqno + 1 < fallback_seqno) {
                LOG(WARNING) << "TraceAssembler state " << restored_seqno << " is older than restart fallback "
                             << fallback_seqno << "; resetting state";
                td::actor::send_closure(SelfId, &IndexScheduler::start_from_seqno, fallback_seqno, true);
                return;
            }
            td::actor::send_closure(SelfId, &IndexScheduler::start_from_seqno, restored_seqno + 1, false);
        }
    });

    td::actor::send_closure(trace_assembler_, &TraceAssembler::restore_state, next_seqno - 1, std::move(P));
}

void IndexScheduler::process_archive_existing_seqnos(td::Result<std::vector<std::uint32_t>> R) {
    if (R.is_error()) {
        LOG(ERROR) << "Error reading existing archive seqnos: " << R.move_as_error();
        return;
    }

    skip_insert_seqnos_.clear();
    auto existing_seqnos = R.move_as_ok();
    skip_insert_seqnos_.reserve(existing_seqnos.size());
    for (auto seqno : existing_seqnos) {
        if (seqno >= static_cast<std::uint32_t>(from_seqno_) && seqno <= static_cast<std::uint32_t>(to_seqno_)) {
            skip_insert_seqnos_.insert(seqno);
        }
    }

    std::uint32_t first_missing_seqno = 0;
    for (auto seqno = static_cast<std::uint32_t>(from_seqno_); seqno <= static_cast<std::uint32_t>(to_seqno_); ++seqno) {
        if (skip_insert_seqnos_.count(seqno) == 0) {
            first_missing_seqno = seqno;
            break;
        }
    }

    if (first_missing_seqno == 0) {
        LOG(INFO) << "Bounded archive range " << from_seqno_ << ".." << to_seqno_
                  << " is already fully present in Postgres";
        watcher_.reset();
        stop();
        return;
    }

    const auto start_seqno = prewarm_start_seqno(first_missing_seqno);
    LOG(INFO) << "First missing archive seqno: " << first_missing_seqno
              << "; starting read pipeline from " << start_seqno;
    start_from_seqno(start_seqno, true);
}

void IndexScheduler::start_from_seqno(ton::BlockSeqno seqno, bool reset_trace_assembler) {
    if (seqno < FIRST_INDEXABLE_MC_SEQNO) {
        seqno = FIRST_INDEXABLE_MC_SEQNO;
    }
    if (reset_trace_assembler) {
        td::actor::send_closure(trace_assembler_, &TraceAssembler::reset_state);
    }
    read_from_seqno_ = static_cast<std::int32_t>(seqno);
    last_known_seqno_ = read_from_seqno_ - 1;
    contiguous_indexed_seqno_ = read_from_seqno_ - 1;
    LOG(INFO) << "Starting indexing from seqno: " << read_from_seqno_;

    td::actor::send_closure(trace_assembler_, &TraceAssembler::set_expected_seqno, seqno);
    if (!db_event_fifo_path_.empty() && db_event_listener_.empty()) {
        LOG(INFO) << "Starting DB events listener on " << db_event_fifo_path_;
        db_event_listener_ = td::actor::create_actor<DbEventListener>("DbEventListener", db_event_fifo_path_,
            [SelfId = actor_id(this)](ton::tl_object_ptr<ton::ton_api::db_Event> event) {
                td::actor::send_closure(SelfId, &IndexScheduler::got_db_event, std::move(event));
            });
    }
    alarm_timestamp() = td::Timestamp::now();
}

const double IN_SYNC_MAX_TIME_LAG = 20.0;
const double OUT_OF_SYNC_TIME_LAG = 60.0;
const double DB_EVENTS_FRESHNESS_WINDOW = 10.0;

bool IndexScheduler::mc_applied_events_fresh() const {
    return bool(last_mc_applied_event_at_) &&
           td::Timestamp::now().at() - last_mc_applied_event_at_.at() < DB_EVENTS_FRESHNESS_WINDOW;
}

void IndexScheduler::note_indexed_block_utime(std::int32_t block_gen_utime) {
    if (block_gen_utime > 0 && static_cast<std::uint32_t>(block_gen_utime) > last_indexed_block_utime_) {
        last_indexed_block_utime_ = static_cast<std::uint32_t>(block_gen_utime);
        update_sync_state();
    }
}

void IndexScheduler::update_sync_state() {
    if (last_indexed_block_utime_ == 0) {
        return;
    }
    const double lag = td::Clocks::system() - static_cast<double>(last_indexed_block_utime_);
    if (!is_in_sync_ && lag <= IN_SYNC_MAX_TIME_LAG) {
        is_in_sync_ = true;
        LOG(INFO) << "Indexer is in sync with TON node: block lag is " << td::StringBuilder::FixedDouble(lag, 1) << "s";
    } else if (is_in_sync_ && lag > OUT_OF_SYNC_TIME_LAG) {
        is_in_sync_ = false;
        LOG(WARNING) << "Indexing is out of sync with TON node: block lag is " << td::StringBuilder::FixedDouble(lag, 1) << "s";
    }
}

void IndexScheduler::got_db_event(ton::tl_object_ptr<ton::ton_api::db_Event> event) {
    ton::ton_api::downcast_call(*event, td::overloaded(
        [&](ton::ton_api::db_event_blockApplied &ev) {
            auto block_id = ton::create_block_id(ev.block_id_);
            if (!block_id.is_masterchain()) {
                return;
            }
            // Only masterchain-applied events may suppress polling fallback.
            last_mc_applied_event_at_ = td::Timestamp::now();
            g_statistics.record_count(DB_EVENT_MC_BLOCK_APPLIED);
            if (!is_in_sync_ || static_cast<std::int32_t>(block_id.seqno()) <= last_known_seqno_) {
                return;
            }
            if (!pending_event_seqno_ || *pending_event_seqno_ < block_id.seqno()) {
                pending_event_seqno_ = block_id.seqno();
            }
            try_event_catch_up();
        },
        [](auto &) {}));
}

void IndexScheduler::try_event_catch_up() {
    if (event_catch_up_active_ || !pending_event_seqno_ || !is_in_sync_) {
        return;
    }
    const auto seqno = *pending_event_seqno_;
    pending_event_seqno_.reset();
    if (static_cast<std::int32_t>(seqno) <= last_known_seqno_) {
        return;
    }
    event_catch_up_active_ = true;
    g_statistics.record_count(DB_EVENT_CATCH_UP);
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), seqno](td::Result<td::Unit> R) {
        td::actor::send_closure(SelfId, &IndexScheduler::event_catch_up_finished, seqno, std::move(R));
    });
    td::actor::send_closure(db_scanner_, &DbScanner::request_catch_up, std::move(P), ton::validator::CatchUpMode::Force);
}

void IndexScheduler::event_catch_up_finished(ton::BlockSeqno mc_seqno, td::Result<td::Unit> result) {
    event_catch_up_active_ = false;
    if (result.is_error()) {
        LOG(WARNING) << "Failed to catch up DB for event seqno " << mc_seqno << ": " << result.move_as_error();
        if (!pending_event_seqno_ || *pending_event_seqno_ < mc_seqno) {
            pending_event_seqno_ = mc_seqno;
        }
        return;
    }
    got_newest_mc_seqno(mc_seqno);
    request_insert_queue_state();
    try_event_catch_up();
}

void IndexScheduler::got_newest_mc_seqno(std::uint32_t newest_mc_seqno) {
    auto effective_newest_seqno = newest_mc_seqno;
    if (to_seqno_ > 0 && effective_newest_seqno > static_cast<std::uint32_t>(to_seqno_)) {
        effective_newest_seqno = static_cast<std::uint32_t>(to_seqno_);
    }
    if (to_seqno_ > 0 && last_known_seqno_ >= to_seqno_) {
        maybe_finish_bounded_range();
        return;
    }
    if (last_known_seqno_ >= static_cast<std::int32_t>(effective_newest_seqno)) {
        return;
    }

    int skipped_count = 0;
    for (auto seqno = last_known_seqno_ + 1; seqno <= static_cast<std::int32_t>(effective_newest_seqno); ++seqno) {
        const bool should_skip = indexed_seqnos_.count(seqno);
        const bool in_range = (read_from_seqno_ <= seqno) && (to_seqno_ == 0 || seqno <= to_seqno_);
        
        if (should_skip) {
            skipped_count++;
        } else if (in_range) {
            queued_seqnos_.push_back(seqno);
        }
    }
    if (skipped_count > 0) {
        LOG(INFO) << "Skipped " << skipped_count << " existing seqnos";
    }
    last_known_seqno_ = static_cast<std::int32_t>(effective_newest_seqno);
}

void IndexScheduler::schedule_seqno(std::uint32_t mc_seqno) {
    LOG(DEBUG) << "Scheduled seqno " << mc_seqno;

    processing_seqnos_.insert(mc_seqno);
    timers_[mc_seqno] = td::Timer();
    start_seqno_otel_trace(mc_seqno, is_in_sync_);
    start_seqno_otel_stage(mc_seqno, "fetch_seqno");
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno, is_in_sync = is_in_sync_](td::Result<schema::MasterchainBlockDataState> R) {
        if (R.is_error()) {
            auto error = R.move_as_error();
            if (!is_in_sync) {
                LOG(ERROR) << "Failed to fetch seqno " << mc_seqno << ": " << error;
            }
            td::actor::send_closure(SelfId, &IndexScheduler::handle_seqno_failure, mc_seqno,
                                    std::string("index_postgres.fetch_error"), std::move(error), is_in_sync);
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

void IndexScheduler::seqno_fetched(std::uint32_t mc_seqno, schema::MasterchainBlockDataState block_data_state) {
    LOG(DEBUG) << "Fetched seqno " << mc_seqno << ": blocks=" << block_data_state.shard_blocks_diff_.size() << " shards=" << block_data_state.shard_blocks_.size();
    finish_seqno_otel_stage(mc_seqno);
    start_seqno_otel_stage(mc_seqno, "parse_seqno");

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<ParsedBlockPtr> R) {
        if (R.is_error()) {
            auto error = R.move_as_error();
            LOG(ERROR) << "Failed to parse seqno " << mc_seqno << ": " << error;
            td::actor::send_closure(SelfId, &IndexScheduler::handle_seqno_failure, mc_seqno,
                                    std::string("index_postgres.parse_error"), std::move(error), false);
            return;
        }
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_parsed, mc_seqno, R.move_as_ok());
    });

    td::actor::send_closure(db_scanner_, &DbScanner::get_cell_db_reader, 
        [SelfId = actor_id(this), parse_manager = parse_manager_, mc_seqno, block_data_state, P = std::move(P)](td::Result<std::shared_ptr<vm::CellDbReader>> cell_db_reader) mutable {
            if (cell_db_reader.is_error()) {
                auto error = cell_db_reader.move_as_error();
                LOG(ERROR) << "Failed to get cell DB reader for seqno " << mc_seqno << ": " << error;
                td::actor::send_closure(SelfId, &IndexScheduler::handle_seqno_failure, mc_seqno,
                                        std::string("index_postgres.cell_db_reader_error"), std::move(error), false);
                return;
            }
            td::actor::send_closure(parse_manager, &ParseManager::parse, mc_seqno, std::move(block_data_state), cell_db_reader.move_as_ok(), std::move(P));
    });
}

void IndexScheduler::seqno_parsed(std::uint32_t mc_seqno, ParsedBlockPtr parsed_block) {
    LOG(DEBUG) << "Parsed seqno " << mc_seqno;
    apply_otel_processing_lag(mc_seqno, *parsed_block);
    finish_seqno_otel_stage(mc_seqno);
    start_seqno_otel_stage(mc_seqno, "assemble_traces");

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<ParsedBlockPtr> R) {
        if (R.is_error()) {
            auto error = R.move_as_error();
            LOG(ERROR) << "Failed to asseble traces for seqno " << mc_seqno << ": " << error;
            td::actor::send_closure(SelfId, &IndexScheduler::handle_seqno_failure, mc_seqno,
                                    std::string("index_postgres.trace_assembly_error"), std::move(error), false);
            return;
        }
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_traces_assembled, mc_seqno, R.move_as_ok());
    });
    td::actor::send_closure(trace_assembler_, &TraceAssembler::assemble, mc_seqno, std::move(parsed_block), std::move(P));
}

void IndexScheduler::seqno_traces_assembled(std::uint32_t mc_seqno, ParsedBlockPtr parsed_block) {
    LOG(DEBUG) << "Assembled traces for seqno " << mc_seqno;
    finish_seqno_otel_stage(mc_seqno);
    start_seqno_otel_stage(mc_seqno, "detect_interfaces");

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<ParsedBlockPtr> R) {
        if (R.is_error()) {
            auto error = R.move_as_error();
            LOG(ERROR) << "Failed to detect interfaces for seqno " << mc_seqno << ": " << error;
            td::actor::send_closure(SelfId, &IndexScheduler::handle_seqno_failure, mc_seqno,
                                    std::string("index_postgres.interface_error"), std::move(error), false);
            return;
        }
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_interfaces_processed, mc_seqno, R.move_as_ok());
    });
    td::actor::create_actor<BlockInterfaceProcessor>("BlockInterfaceProcessor", std::move(parsed_block), std::move(P)).release();
}

void IndexScheduler::seqno_interfaces_processed(std::uint32_t mc_seqno, ParsedBlockPtr parsed_block) {
    LOG(DEBUG) << "Interfaces processed for seqno " << mc_seqno;
    finish_seqno_otel_stage(mc_seqno);
    start_seqno_otel_stage(mc_seqno, "detect_actions");

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<ParsedBlockPtr> R) {
        if (R.is_error()) {
            auto error = R.move_as_error();
            LOG(ERROR) << "Failed to detect actions for seqno " << mc_seqno << ": " << error;
            td::actor::send_closure(SelfId, &IndexScheduler::handle_seqno_failure, mc_seqno,
                                    std::string("index_postgres.action_error"), std::move(error), false);
            return;
        }
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_actions_processed, mc_seqno, R.move_as_ok());
    });
    td::actor::create_actor<ActionDetector>("ActionDetector", std::move(parsed_block), std::move(P)).release();
}

void IndexScheduler::seqno_actions_processed(std::uint32_t mc_seqno, ParsedBlockPtr parsed_block) {
    LOG(DEBUG) << "Actions processed for seqno " << mc_seqno;
    finish_seqno_otel_stage(mc_seqno);
    const auto* mc_block = find_masterchain_block(*parsed_block);
    const std::int32_t block_gen_utime = mc_block ? mc_block->gen_utime : 0;
    if (!should_insert_seqno(mc_seqno)) {
        seqno_processed_without_insert(mc_seqno, mc_seqno < static_cast<std::uint32_t>(from_seqno_) ? "prewarm" : "already_present", block_gen_utime);
        return;
    }

    start_seqno_otel_stage(mc_seqno, "insert_seqno");
    set_seqno_otel_attribute(mc_seqno, "ton.indexer.insert.force", is_in_sync_);

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno, block_gen_utime, timer = td::Timer{}](td::Result<InsertManagerInterface::InsertResult> R) {
        if (R.is_error()) {
            auto error = R.move_as_error();
            LOG(ERROR) << "Failed to insert seqno " << mc_seqno << ": " << error;
            td::actor::send_closure(SelfId, &IndexScheduler::handle_seqno_failure, mc_seqno,
                                    std::string("index_postgres.insert_error"), std::move(error), false);
            return;
        }
        auto result = R.move_as_ok();
        if (result.is_leader.has_value()) {
            td::actor::send_closure(SelfId, &IndexScheduler::set_seqno_otel_attribute, mc_seqno,
                                    std::string("ton.indexer.insert.is_leader"),
                                    OtelStageSpan::AttributeValue(*result.is_leader), true);
        }
        g_statistics.record_time(INSERT_SEQNO, timer.elapsed() * 1e3);
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_inserted, mc_seqno, block_gen_utime);
    });
    inserting_seqnos_.insert(mc_seqno);
    auto Q = td::PromiseCreator::lambda([SelfId = actor_id(this), mc_seqno](td::Result<QueueState> R){
        R.ensure();
        td::actor::send_closure(SelfId, &IndexScheduler::seqno_queued_to_insert, mc_seqno, R.move_as_ok());
    });
    td::actor::send_closure(insert_manager_, &InsertManagerInterface::insert, mc_seqno, std::move(parsed_block), 
                                is_in_sync_, std::move(Q), std::move(P));
}

void IndexScheduler::seqno_processed_without_insert(std::uint32_t mc_seqno, const char* reason, std::int32_t block_gen_utime) {
    LOG(DEBUG) << "Processed seqno " << mc_seqno << " without insert: " << reason;
    set_seqno_otel_attribute(mc_seqno, "ton.indexer.insert.skipped", true, true);
    set_seqno_otel_attribute(mc_seqno, "ton.indexer.insert.skip_reason", std::string(reason), true);
    finish_seqno_otel_trace(mc_seqno);
    note_indexed_block_utime(block_gen_utime);
    processing_seqnos_.erase(mc_seqno);
    indexed_seqnos_.insert(mc_seqno);
    if (mc_seqno > static_cast<std::uint32_t>(last_indexed_seqno_)) {
        last_indexed_seqno_ = static_cast<std::int32_t>(mc_seqno);
    }
    advance_contiguous_indexed_seqno();
    g_statistics.record_time(PROCESS_SEQNO, timers_[mc_seqno].elapsed() * 1e3);
    timers_.erase(mc_seqno);
    schedule_next_seqnos();
}

bool IndexScheduler::is_bounded_archive_range() const {
    return from_seqno_ > 0 && to_seqno_ > 0;
}

bool IndexScheduler::should_insert_seqno(std::uint32_t mc_seqno) const {
    if (!is_bounded_archive_range()) {
        return true;
    }
    if (mc_seqno < static_cast<std::uint32_t>(from_seqno_) || mc_seqno > static_cast<std::uint32_t>(to_seqno_)) {
        return false;
    }
    if (!force_index_ && skip_insert_seqnos_.count(mc_seqno) > 0) {
        return false;
    }
    return true;
}

ton::BlockSeqno IndexScheduler::prewarm_start_seqno(ton::BlockSeqno seqno) const {
    if (seqno <= FIRST_INDEXABLE_MC_SEQNO || seqno - FIRST_INDEXABLE_MC_SEQNO <= prewarm_count_) {
        return FIRST_INDEXABLE_MC_SEQNO;
    }
    return seqno - prewarm_count_;
}

void IndexScheduler::maybe_finish_bounded_range() {
    if (!is_bounded_archive_range()) {
        return;
    }
    if (last_known_seqno_ < to_seqno_ || !queued_seqnos_.empty() || !processing_seqnos_.empty() || !inserting_seqnos_.empty()
        || cur_queue_state_.blocks_ != 0) {
        return;
    }
    LOG(INFO) << "Finished bounded archive range " << from_seqno_ << ".." << to_seqno_;
    watcher_.reset();
    stop();
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
    if (last_indexed_block_utime_ > 0) {
        sb << "\tlag " << td::StringBuilder::FixedDouble(td::Clocks::system() - last_indexed_block_utime_, 1) << "s";
    }
    if (!db_event_fifo_path_.empty()) {
        sb << (mc_applied_events_fresh() ? "\tev[ok]" : "\tev[stale]");
    }
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

void IndexScheduler::set_seqno_otel_attribute(std::uint32_t mc_seqno, const std::string& key,
                                              const OtelStageSpan::AttributeValue& value, bool include_stage) {
    if (auto it = seqno_otel_traces_.find(mc_seqno); it != seqno_otel_traces_.end()) {
        if (it->second.otel_root_span_) {
            std::visit([&](const auto& typed_value) {
                it->second.otel_root_span_->set_attribute(key, typed_value);
            }, value);
        }
        if (include_stage && it->second.active_otel_stage_span_) {
            std::visit([&](const auto& typed_value) {
                it->second.active_otel_stage_span_->set_attribute(key, typed_value);
            }, value);
        }
    }
}

void IndexScheduler::seqno_inserted(std::uint32_t mc_seqno, std::int32_t block_gen_utime) {
    finish_seqno_otel_stage(mc_seqno);
    finish_seqno_otel_trace(mc_seqno);
    note_indexed_block_utime(block_gen_utime);
    inserting_seqnos_.erase(mc_seqno);
    indexed_seqnos_.insert(mc_seqno);
    if (mc_seqno > last_indexed_seqno_) {
        last_indexed_seqno_ = mc_seqno;
    }
    advance_contiguous_indexed_seqno();
    g_statistics.record_time(PROCESS_SEQNO, timers_[mc_seqno].elapsed() * 1e3);
    timers_.erase(mc_seqno);
    maybe_finish_bounded_range();
}

void IndexScheduler::advance_contiguous_indexed_seqno() {
    while (indexed_seqnos_.count(static_cast<std::uint32_t>(contiguous_indexed_seqno_ + 1)) > 0) {
        ++contiguous_indexed_seqno_;
    }
}

void IndexScheduler::get_contiguous_indexed_seqno(td::Promise<std::int32_t> promise) {
    promise.set_result(std::int32_t{contiguous_indexed_seqno_});
}

void IndexScheduler::handle_seqno_failure(std::uint32_t mc_seqno, std::string error_type, td::Status error, bool silent) {
    fail_seqno_otel_trace(mc_seqno, error_type, error, silent);
    inserting_seqnos_.erase(mc_seqno);
    reschedule_seqno(mc_seqno, silent);
}

void IndexScheduler::schedule_next_seqnos() {
    LOG(DEBUG) << "Scheduling next seqnos. Current tasks: " << processing_seqnos_.size();
    while (!queued_seqnos_.empty() && (processing_seqnos_.size() < max_active_tasks_)) {
        std::uint32_t seqno = queued_seqnos_.front();
        queued_seqnos_.pop_front();
        schedule_seqno(seqno);
    }

    maybe_finish_bounded_range();
}

void IndexScheduler::start_seqno_otel_trace(std::uint32_t mc_seqno, bool is_in_sync) {
    if (!OtelSpan::tracing_enabled()) {
        return;
    }

    SeqnoOtelTraceState otel_trace_state;
    otel_trace_state.attempt_ = ++seqno_attempts_[mc_seqno];
    otel_trace_state.is_in_sync_ = is_in_sync;
    otel_trace_state.otel_root_span_ = std::make_unique<OtelSpan>(OtelSpan::Options{
        .service_name = "ton-index-postgres",
        .span_name = "ton.index_postgres.process_seqno",
        .pipeline = "seqno_to_postgres",
        .service_stage = "process_seqno",
        .kind = opentelemetry::trace::SpanKind::kInternal,
        .parent = std::nullopt,
        .start_system_time_ns = OtelSpan::system_now_ns(),
        .start_steady_time_ns = OtelSpan::steady_now_ns(),
    });
    if (otel_trace_state.otel_root_span_) {
        otel_trace_state.otel_root_span_->set_attribute("ton.mc_seqno", static_cast<std::int64_t>(mc_seqno));
        otel_trace_state.otel_root_span_->set_attribute("ton.indexer.attempt", static_cast<std::int64_t>(otel_trace_state.attempt_));
        otel_trace_state.otel_root_span_->set_attribute("ton.indexer.is_in_sync", is_in_sync);
        otel_trace_state.otel_root_span_->set_attribute("ton.scheduler.last_known_mc_seqno", static_cast<std::int64_t>(last_known_seqno_));
        otel_trace_state.otel_root_span_->set_attribute("ton.scheduler.last_indexed_mc_seqno", static_cast<std::int64_t>(last_indexed_seqno_));
        otel_trace_state.otel_root_span_->set_attribute("ton.scheduler.seqno_lag", static_cast<std::int64_t>(last_known_seqno_) - static_cast<std::int64_t>(mc_seqno));
        otel_trace_state.otel_root_span_->set_attribute("ton.scheduler.index_backlog", static_cast<std::int64_t>(last_known_seqno_) - static_cast<std::int64_t>(last_indexed_seqno_));
        otel_trace_state.otel_root_span_->set_attribute("ton.scheduler.avg_seqnos_per_sec_x1000", static_cast<std::int64_t>(avg_tps_ * 1000.0));
        otel_trace_state.otel_root_span_->set_attribute("ton.indexer.processing_seqnos", static_cast<std::int64_t>(processing_seqnos_.size()));
        otel_trace_state.otel_root_span_->set_attribute("ton.indexer.queue.pending_seqnos", static_cast<std::int64_t>(queued_seqnos_.size()));
    }
    seqno_otel_traces_[mc_seqno] = std::move(otel_trace_state);
}

void IndexScheduler::start_seqno_otel_stage(std::uint32_t mc_seqno, const std::string& stage_name) {
    auto it = seqno_otel_traces_.find(mc_seqno);
    if (it == seqno_otel_traces_.end() || !it->second.otel_root_span_) {
        return;
    }

    if (it->second.active_otel_stage_span_) {
        it->second.active_otel_stage_span_->end();
        it->second.active_otel_stage_span_.reset();
    }

    auto otel_parent_context = it->second.otel_root_span_->context();
    it->second.active_otel_stage_span_ = std::make_unique<OtelSpan>(OtelSpan::Options{
        .service_name = "ton-index-postgres",
        .span_name = std::string("ton.index_postgres.") + stage_name,
        .pipeline = "seqno_to_postgres",
        .service_stage = stage_name,
        .kind = opentelemetry::trace::SpanKind::kInternal,
        .parent = otel_parent_context,
        .start_system_time_ns = OtelSpan::system_now_ns(),
        .start_steady_time_ns = OtelSpan::steady_now_ns(),
    });
    if (it->second.active_otel_stage_span_) {
        it->second.active_otel_stage_span_->set_attribute("ton.mc_seqno", static_cast<std::int64_t>(mc_seqno));
        it->second.active_otel_stage_span_->set_attribute("ton.indexer.attempt", static_cast<std::int64_t>(it->second.attempt_));
        it->second.active_otel_stage_span_->set_attribute("ton.indexer.is_in_sync", it->second.is_in_sync_);
    }
}

void IndexScheduler::finish_seqno_otel_stage(std::uint32_t mc_seqno) {
    auto it = seqno_otel_traces_.find(mc_seqno);
    if (it == seqno_otel_traces_.end() || !it->second.active_otel_stage_span_) {
        return;
    }
    it->second.active_otel_stage_span_->end();
    it->second.active_otel_stage_span_.reset();
}

void IndexScheduler::finish_seqno_otel_trace(std::uint32_t mc_seqno) {
    auto it = seqno_otel_traces_.find(mc_seqno);
    if (it == seqno_otel_traces_.end()) {
        seqno_attempts_.erase(mc_seqno);
        return;
    }

    finish_seqno_otel_stage(mc_seqno);
    if (it->second.otel_root_span_) {
        it->second.otel_root_span_->set_attribute("ton.indexer.outcome", "completed");
        it->second.otel_root_span_->end();
    }
    seqno_otel_traces_.erase(it);
    seqno_attempts_.erase(mc_seqno);
}

void IndexScheduler::fail_seqno_otel_trace(std::uint32_t mc_seqno, const std::string& error_type, const td::Status& error, bool silent) {
    auto it = seqno_otel_traces_.find(mc_seqno);
    if (it == seqno_otel_traces_.end()) {
        return;
    }

    const auto error_message = error.to_string();
    if (it->second.active_otel_stage_span_) {
        it->second.active_otel_stage_span_->mark_error(error_type, error_message);
        it->second.active_otel_stage_span_->end();
        it->second.active_otel_stage_span_.reset();
    }
    if (it->second.otel_root_span_) {
        it->second.otel_root_span_->set_attribute("ton.indexer.outcome", "rescheduled");
        it->second.otel_root_span_->set_attribute("ton.indexer.reschedule_silent", silent);
        it->second.otel_root_span_->mark_error(error_type, error_message);
        it->second.otel_root_span_->end();
    }
    seqno_otel_traces_.erase(it);
}

void IndexScheduler::apply_otel_processing_lag(std::uint32_t mc_seqno, const ParsedBlock& parsed_block) {
    if (const auto* mc_block = find_masterchain_block(parsed_block); mc_block != nullptr) {
        set_seqno_otel_attribute(mc_seqno, "ton.block.processing_lag_ms", processing_lag_ms(mc_block->gen_utime), false);
    }
}
