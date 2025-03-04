#include <fstream>
#include <sstream>
#include <string>
#include <common/delay.h>
#include <tdutils/td/utils/filesystem.h>
#include "IntegrityChecker.h"


void IntegrityChecker::start_up() {
    uint32_t last_seqno = 0;
    if (checkpoint_path_.size() && td::stat(checkpoint_path_).is_ok()) {
        LOG(INFO) << "Checkpoint file exists. Reading last verified seqno from it.";
        auto checkpoint = td::read_file_secure(checkpoint_path_);
        if (checkpoint.is_error()) {
            LOG(ERROR) << "Failed to read checkpoint file: " << checkpoint.move_as_error();
            std::_Exit(2);
        }
        try {
            from_seqno_ = std::stoul(checkpoint.ok().as_slice().str());
        } catch (...) {
            LOG(ERROR) << "Failed to parse checkpoint file";;
            std::_Exit(2);
        }
        checkpoint_seqno_ = from_seqno_;
        LOG(INFO) << "Last verified seqno: " << from_seqno_;
    } else {
        if (checkpoint_path_.size()) {
            LOG(INFO) << "Checkpoint file does not exist. Starting from scratch.";
        } else {
            LOG(INFO) << "No checkpoint file specified. Starting from scratch.";
        }
    }

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ton::BlockSeqno> R){
        if (R.is_error()) {
            LOG(ERROR) << "Failed to update last seqno: " << R.move_as_error();
            return;
        }
        td::actor::send_closure(SelfId, &IntegrityChecker::got_last_mc_seqno, R.move_as_ok());
    });
    td::actor::send_closure(db_scanner_, &DbScanner::get_last_mc_seqno, std::move(P));
}

void IntegrityChecker::got_last_mc_seqno(ton::BlockSeqno last_known_seqno) {
    LOG(INFO) << "Newest DB known seqno: " << last_known_seqno;
    to_seqno_ = last_known_seqno;

    if (from_seqno_) {
        got_oldest_mc_seqno_with_state(from_seqno_);
    } else {
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ton::BlockSeqno> R){
            if (R.is_error()) {
                LOG(ERROR) << "Failed to update last seqno: " << R.move_as_error();
                return;
            }
            td::actor::send_closure(SelfId, &IntegrityChecker::got_min_mc_seqno, R.move_as_ok());
        });
        td::actor::send_closure(db_scanner_, &DbScanner::get_oldest_mc_seqno, std::move(P));
    }
}

void IntegrityChecker::got_min_mc_seqno(ton::BlockSeqno min_known_seqno) {
    LOG(INFO) << "Oldest DB known seqno: " << min_known_seqno;

    find_oldest_seqno_with_state(min_known_seqno, to_seqno_);
}

void IntegrityChecker::got_block_handle(ton::validator::ConstBlockHandle handle) {
    if (handle->id().id.seqno % 10 == 0) {
        LOG(INFO) << "Got block handle for seqno " << handle->id().id.seqno;
    }
    if (handle->inited_state_boc() && !handle->deleted_state_boc()) {
        LOG(INFO) << "Block " << handle->id().to_str() << " has state";
        got_oldest_mc_seqno_with_state(handle->id().id.seqno);
    } else {
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<ton::validator::ConstBlockHandle> R){
            if (R.is_error()) {
                LOG(ERROR) << "Failed to get block handle: " << R.move_as_error();
                std::_Exit(2);
            }
            td::actor::send_closure(SelfId, &IntegrityChecker::got_block_handle, R.move_as_ok());
        });
        td::actor::send_closure(db_scanner_, &DbScanner::get_mc_block_handle, handle->id().id.seqno + 1, std::move(P));
    }
}

void IntegrityChecker::find_oldest_seqno_with_state(uint32_t min_seqno, uint32_t max_seqno) {
    if (min_seqno > max_seqno) {
        LOG(ERROR) << "Invalid state in binary search for oldest seqno with state";
        std::_Exit(2);
    }

    uint32_t mid_seqno = min_seqno + (max_seqno - min_seqno) / 2;

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), min_seqno, max_seqno, mid_seqno](td::Result<ton::validator::ConstBlockHandle> R){
        if (R.is_error()) {
            LOG(ERROR) << "Failed to get block handle: " << R.move_as_error();
            std::_Exit(2);
        }

        auto handle = R.move_as_ok();
        if (handle->inited_state_boc() && !handle->deleted_state_boc()) {
            if (min_seqno == mid_seqno) {
                LOG(INFO) << "Found oldest DB known seqno with state: " << mid_seqno;
                td::actor::send_closure(SelfId, &IntegrityChecker::got_oldest_mc_seqno_with_state, mid_seqno);
            } else {
                td::actor::send_closure(SelfId, &IntegrityChecker::find_oldest_seqno_with_state, min_seqno, mid_seqno);
            }
        } else {
            td::actor::send_closure(SelfId, &IntegrityChecker::find_oldest_seqno_with_state, mid_seqno + 1, max_seqno);
        }
    });

    td::actor::send_closure(db_scanner_, &DbScanner::get_mc_block_handle, mid_seqno, std::move(P));
}

void IntegrityChecker::got_oldest_mc_seqno_with_state(ton::BlockSeqno oldest_seqno_with_state) {
    from_seqno_ = oldest_seqno_with_state + 1;
    if (from_seqno_ < 3) {
        from_seqno_ = 3; // for seqnos 0, 1, 2 we get error "not in db" when reading shard blocks.
    }
    checkpoint_seqno_ = from_seqno_;
    LOG(INFO) << "Starting DB verifying in range [" << from_seqno_ << ", " << to_seqno_ <<"]";
    if (from_seqno_ && to_seqno_) {
        for (std::uint32_t i = from_seqno_; i <= to_seqno_; i++) {
            queued_seqnos_.push(i);
        }
        fetch_next_seqnos();
        alarm_timestamp() = td::Timestamp::in(5.0);
    }
}


void IntegrityChecker::fetch_next_seqnos() {
    if (blocks_to_parse_.size() > blocks_to_parse_queue_max_size_) {
        LOG(WARNING) << "Too many seqnos to parse, waiting for the queue to be processed";
        ton::delay_action([SelfId = actor_id(this)]() { td::actor::send_closure(SelfId, &IntegrityChecker::fetch_next_seqnos); }, td::Timestamp::in(1.0));
        return;
    }
    while (queued_seqnos_.size() > 0 && seqnos_fetching_.size() < fetch_parallelism_) {
        auto seqno = queued_seqnos_.front();
        queued_seqnos_.pop();
        seqnos_fetching_.insert(seqno);
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), seqno](td::Result<MasterchainBlockDataState> R) {
            if (R.is_error()) {
                td::actor::send_closure(SelfId, &IntegrityChecker::fetch_error, seqno, R.move_as_error());
                return;
            }
            td::actor::send_closure(SelfId, &IntegrityChecker::seqno_fetched, seqno, R.move_as_ok());
        });
        td::actor::send_closure(db_scanner_, &DbScanner::fetch_seqno, seqno, std::move(P));
    }
}

void IntegrityChecker::fetch_error(std::uint32_t seqno, td::Status error) {
    LOG(ERROR) << "Failed to fetch seqno " << seqno << ": " << error;
    std::_Exit(7);
}

void IntegrityChecker::seqno_fetched(std::uint32_t seqno, MasterchainBlockDataState state) {
    LOG(DEBUG) << "Fetched seqno " << seqno;
    seqnos_fetching_.erase(seqno);
    fetch_next_seqnos();

    blocks_to_parse_.push(std::make_pair(seqno, state));
    parse_next_seqnos();
}

void IntegrityChecker::parse_next_seqnos() {
    while (blocks_to_parse_.size() > 0 && seqnos_parsing_.size() < parse_parallelism_) {
        auto [seqno, state] = blocks_to_parse_.front();
        blocks_to_parse_.pop();
        seqnos_parsing_.insert(seqno);
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), seqno = seqno](td::Result<td::Unit> R) {
            if (R.is_error()) {
                td::actor::send_closure(SelfId, &IntegrityChecker::parse_error, seqno, R.move_as_error());
                return;
            }
            td::actor::send_closure(SelfId, &IntegrityChecker::seqno_parsed, seqno);
        });
        td::actor::send_closure(parse_manager_, &IntegrityParser::parse, seqno, state, std::move(P));
    }
}

void IntegrityChecker::parse_error(std::uint32_t seqno, td::Status error) {
    LOG(ERROR) << "Failed to parse seqno " << seqno << ": " << error;
    std::_Exit(7);
}

void IntegrityChecker::seqno_parsed(std::uint32_t seqno) {
    LOG(DEBUG) << "Processed seqno " << seqno;
    seqnos_parsing_.erase(seqno);
    seqnos_processed_.insert(seqno);

    if (blocks_to_parse_.size() == 0 && queued_seqnos_.size() == 0 && seqnos_fetching_.size() == 0) {
        stop();
        return;
    }

    parse_next_seqnos();
}

void IntegrityChecker::alarm() {
    // Calculate TPS
    auto now = td::Timestamp::now();
    auto seconds_elapsed = now.at() - last_tps_calc_ts_.at();
    auto weight = 0.4;
    if (seconds_elapsed > 0) {
        double current_rate = (seqnos_processed_.size() - last_tps_calc_processed_count_) / seconds_elapsed;
        tps_ = tps_ * weight + current_rate * (1 - weight);
    }
    last_tps_calc_processed_count_ = seqnos_processed_.size();
    last_tps_calc_ts_ = now;
    
    // Print stats if needed
    if (next_print_stats_.is_in_past()) {
        print_stats();
        next_print_stats_ = td::Timestamp::in(stats_timeout_);
    }

    // Check memory usage
    fail_if_low_memory();

    // Save to checkpoint last seqno up to which we have verified the DB (seqnos_processed_ sequence is NOT guaranteed to be monotonically increasing)
    if (checkpoint_path_.size()) {
        int expected_next = checkpoint_seqno_;
        std::uint32_t next_checkpoint_seqno = 0;
        for (auto it = seqnos_processed_.lower_bound(expected_next); it != seqnos_processed_.end(); it++) {
            if (*it != expected_next) {
                checkpoint_seqno_ = expected_next;
                break;
            }
            expected_next++;
        }
        CHECK(checkpoint_seqno_ != 0);
        td::atomic_write_file(checkpoint_path_, td::to_string(checkpoint_seqno_)).ensure();
    }

    alarm_timestamp() = td::Timestamp::in(5.0);
}

td::Result<float> get_free_ram_percentage() {
    std::ifstream meminfo("/proc/meminfo");
    if (!meminfo) {
        // Handle error
        return -1;
    }

    std::string line;
    long long total_memory = -1, free_memory = -1;

    while (std::getline(meminfo, line)) {
        std::istringstream iss(line);
        std::string key;
        long long value;
        std::string unit;
        iss >> key >> value >> unit;

        if (key == "MemTotal:") {
            total_memory = value;
        } else if (key == "MemAvailable:" || key == "MemFree:") { // Prefer MemAvailable if available
            free_memory = value;
            if (key == "MemAvailable:") break; // No need to continue if we find MemAvailable
        }
    }

    if (total_memory == -1 || free_memory == -1) {
        return td::Status::Error("Failed to parse /proc/meminfo");
    }

    return (free_memory / (float)total_memory) * 100.0f;
}

void IntegrityChecker::fail_if_low_memory() {
    auto free_ram_percentage = get_free_ram_percentage();
    if (free_ram_percentage.is_error()) {
        LOG(ERROR) << "Failed to get free RAM percentage: " << free_ram_percentage.error();
        return;
    }
    if (free_ram_percentage.ok() < min_free_memory_) {
        LOG(ERROR) << "Low memory: " << free_ram_percentage.ok() << "%";
        std::_Exit(6);
    }
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

void IntegrityChecker::print_stats() {
    uint32_t total = to_seqno_ - from_seqno_ + 1;
    double eta = queued_seqnos_.size() / tps_;
    LOG(INFO) << "Processed: " << seqnos_processed_.size() << " / " << total 
              << "\tBlk/s: " << tps_
              << "\tETA: " << get_time_string(eta);
}
