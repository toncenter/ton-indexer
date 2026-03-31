#include "ShardBatchScanner.h"
#include "StateParser.h"
#include "td/utils/filesystem.h"
#include "td/utils/port/path.h"

struct AdditionResult {
    td::Bits256 result;
    bool overflow;
};

// 256-bit arithmetic implementation
AdditionResult add_bits256(const td::Bits256& a, const td::Bits256& b) {
    td::Bits256 result = a;
    bool carry = false;
    bool overflow = false;
    
    // Add from least significant bit (bit 255) to most significant (bit 0)
    for (int i = 255; i >= 0; i--) {
        bool bit_a = result[i];
        bool bit_b = b[i];
        
        // Full adder logic
        bool sum = bit_a ^ bit_b ^ carry;
        bool new_carry = (bit_a && bit_b) || (carry && (bit_a ^ bit_b));
        
        result[i] = sum;
        
        // Check for overflow on the final carry
        if (i == 0 && new_carry) {
            overflow = true;
        }
        
        carry = new_carry;
    }
    
    return {result, overflow};
}

td::Bits256 create_step(uint64_t step_size) {
    td::Bits256 step;
    step.set_zero();
    
    // Set the high 64 bits to the specified value
    for (int i = 0; i < 64; i++) {
        step[i] = (step_size >> (63 - i)) & 1;
    }
    
    return step;
}

ShardRangeScanner::ShardRangeScanner(vm::AugmentedDictionary accounts_dict, td::Bits256 start_addr, td::Bits256 end_addr,
                                   td::Promise<std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record>>> promise)
    : accounts_dict_(std::move(accounts_dict)), start_addr_(std::move(start_addr)), 
      end_addr_(std::move(end_addr)), promise_(std::move(promise)) {
}

void ShardRangeScanner::start_up() {
    // LOG(INFO) << "Starting ShardRangeScanner for range " << start_addr_.to_hex() << " - " << end_addr_.to_hex();
    std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record>> result;
    td::Bits256 cur_addr = start_addr_;
    bool allow_same = true;
    
    while (cur_addr.compare(end_addr_) < 0) {
        td::Ref<vm::CellSlice> shard_account_csr = accounts_dict_.vm::DictionaryFixed::lookup_nearest_key(cur_addr.bits(), 256, true, allow_same);
        if (cur_addr.compare(end_addr_) >= 0 || shard_account_csr.is_null()) {
            break;
        }
        allow_same = false;
        shard_account_csr = accounts_dict_.extract_value(shard_account_csr);
        block::gen::ShardAccount::Record acc_info;
        if(!tlb::csr_unpack(shard_account_csr, acc_info)) {
            LOG(ERROR) << "Failed to unpack ShardAccount for " << cur_addr.to_hex();
            continue;
        }
        
        result.emplace_back(cur_addr, std::move(acc_info));
    }
    if (result.size() == 1) {
        for (const auto &item : result) {
            LOG(INFO) << "1 batch: Found account at " << item.first.to_hex();
        }
    }
    
    // LOG(INFO) << "ShardRangeScanner found " << result.size() << " accounts in range " << start_addr_.to_hex() << " - " << end_addr_.to_hex();
    
    promise_.set_value(std::move(result));
    stop();
}


ShardStateScanner::ShardStateScanner(td::actor::ActorId<DbScanner> db_scanner,
                                     std::size_t current_shard_index,
                                     ReloadShardStateContextPtr reload_context,
                                     ShardStateDataPtr shard_state_data,
                                     Options options)
    : db_scanner_(db_scanner)
    , current_shard_index_(current_shard_index)
    , reload_context_(std::move(reload_context))
    , shard_state_data_(std::move(shard_state_data))
    , options_(options) {
}

void ShardStateScanner::start_up() {
    shard_ = ton::ShardIdFull(block::ShardId(shard_state_data_->sstate_.shard_id.write()));
    LOG(INFO) << "Shard " << shard_.to_str() << ": reload shard-state snapshot every "
              << options_.reload_shard_state_every_batches_ << " completed batches";

    if (!options_.working_dir_.empty()) {
        auto path = options_.working_dir_ + "/" + std::to_string(options_.seqno_) + "_" + shard_.to_str() + ".checkpoint";
        if (std::filesystem::exists(path)) {
            auto buffer_r = td::read_file(path);
            if (buffer_r.is_error()) {
                LOG(FATAL) << "Failed to read checkpoint from " << path << ": " << buffer_r.move_as_error();
            }
            auto buffer = buffer_r.move_as_ok();
            if (buffer.size() != 64) {
                LOG(FATAL) << "Invalid checkpoint size: " << buffer.size() << ", expected 64 bytes";
            }
            td::Bits256 checkpoint;
            if (!checkpoint.from_hex(td::Slice(buffer))) {
                LOG(FATAL) << "Failed to decode checkpoint from hex: " << td::Slice(buffer);
            }
            next_batch_start_ = std::move(checkpoint);
            LOG(INFO) << "Read checkpoint for seqno " << options_.seqno_ << " and shard" << shard_.to_str() << ": " << next_batch_start_.to_hex();
        } else {
            LOG(INFO) << "No checkpoint found for shard " << shard_.to_str() << ", starting from zero address";
            next_batch_start_ = td::Bits256::zero();
        }
    } else {
        LOG(INFO) << "No working dir provided, starting from zero address";
        next_batch_start_ = td::Bits256::zero();
    }
    schedule_next();
}

void ShardStateScanner::schedule_next() {
    if (refresh_in_progress_) {
        return;
    }
    vm::AugmentedDictionary accounts_dict{vm::load_cell_slice_ref(shard_state_data_->sstate_.accounts), 256, block::tlb::aug_ShardAccounts};

    // Launch parallel scanners up to max_parallel_batches_
    while (ranges_in_progress_.size() < options_.max_parallel_batches_ ) {
        auto batch_start = next_batch_start_;
        auto step = create_step(shard_.is_masterchain() ? options_.masterchain_batch_step_ : options_.basechain_batch_step_);
        auto plus_step = add_bits256(batch_start, step);
        auto batch_end = plus_step.result;

        // LOG(INFO) << "Scheduling range scan for " << batch_start.to_hex() << " - " << batch_end.to_hex() << " overflow: " << plus_step.overflow << " step: " << step.to_hex();
        if (plus_step.overflow) {
            batch_end = td::Bits256::ones();
        }
        if (batch_start.compare(batch_end) >= 0) {
            break;
        }

        auto range = std::make_pair(batch_start, batch_end);

        // LOG(INFO) << "Scheduling range scan for " << range.first.to_hex() << " - " << range.second.to_hex();

        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), range](td::Result<std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record>>> res) mutable {
            if (res.is_error()) {
                td::actor::send_closure(SelfId, &ShardStateScanner::fail_range, range,
                                        res.move_as_error_prefix("Failed to scan range: "));
                return;
            }
            td::actor::send_closure(SelfId, &ShardStateScanner::range_scan_completed, range, res.move_as_ok());
        });
        td::actor::create_actor<ShardRangeScanner>("range_scanner", accounts_dict, batch_start, batch_end, std::move(P)).release();
        
        ranges_in_progress_.insert({batch_start, batch_end});

        next_batch_start_ = batch_end;
    }

    if (ranges_in_progress_.empty()) {
        LOG(INFO) << "Shard " << shard_.to_str() << " finished with " << accounts_cnt_ << " accounts";
        stop();
    }
}

std::string ShardStateScanner::get_checkpoint_file_path() {
    if (options_.working_dir_.empty()) {
        LOG(ERROR) << "No working directory provided, cannot save checkpoint";
        return "";
    }
    auto path = options_.working_dir_ + "/" + std::to_string(options_.seqno_) + "_" + shard_.to_str() + ".checkpoint";
    return path;
}

void ShardStateScanner::range_scan_completed(AddrRange range, std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record>> results) {
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), range](td::Result<std::vector<InsertData>> R) mutable {
        if (R.is_error()) {
            td::actor::send_closure(SelfId, &ShardStateScanner::fail_range, range,
                                    R.move_as_error_prefix("Failed to process batch: "));
            return;
        }
        td::actor::send_closure(SelfId, &ShardStateScanner::range_parsed, range, R.move_as_ok());
    });
    
    td::actor::create_actor<StateBatchParser>("parser", std::move(results), shard_state_data_, options_, std::move(P)).release();

    accounts_cnt_ += results.size();
}

void ShardStateScanner::range_parsed(AddrRange range, std::vector<InsertData> results) {    
    // LOG(INFO) << "Parsed " << results.size() << " accstates+interfaces for range " << range.first.to_hex() << " - " << range.second.to_hex();
    if (results.empty()) {
        batch_inserted(range);
        LOG(INFO) << "Inserted 0 accstates+interfaces for range " << range.first.to_hex() << "-" << range.second.to_hex();
        return;
    }

    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), cnt = results.size(), range](td::Result<td::Unit> R) mutable {
        if (R.is_error()) {
            td::actor::send_closure(SelfId, &ShardStateScanner::fail_range, range,
                                    R.move_as_error_prefix("Failed to insert data: "));
            return;
        }
        LOG(INFO) << "Inserted " << cnt << " accstates+interfaces for range " << range.first.to_hex() << "-" << range.second.to_hex();
        td::actor::send_closure(SelfId, &ShardStateScanner::batch_inserted, range);
    });
    
    // Insert the results into the database
    td::actor::send_closure(options_.insert_manager_, &PostgreSQLInsertManager::insert_data, std::move(results), std::move(P));
}

void ShardStateScanner::update_checkpoint(td::Bits256 up_to_addr) {
    auto checkpoint_file_path = get_checkpoint_file_path();
    if (!checkpoint_file_path.empty()) {
        auto S = td::write_file(checkpoint_file_path, up_to_addr.to_hex());
        if (S.is_error()) {
            LOG(ERROR) << "Failed to write checkpoint to " << checkpoint_file_path;
        }
    }
}

void ShardStateScanner::fail_range(AddrRange range, td::Status error) {
    LOG(ERROR) << "Shard " << shard_.to_str() << " failed for range " << range.first.to_hex() << "-" << range.second.to_hex()
               << ": " << error;

    td::Bits256 new_checkpoint = range.first;
    for (const auto &in_progress_range : ranges_in_progress_) {
        if (in_progress_range.first < new_checkpoint) {
            new_checkpoint = in_progress_range.first;
        }
    }
    update_checkpoint(new_checkpoint);

    ranges_in_progress_.erase(range);
    stop();
}

void ShardStateScanner::batch_inserted(AddrRange range) {
    // LOG(INFO) << "Batch inserted for range " << range.first.to_hex() << " - " << range.second.to_hex();
    td::Bits256 new_checkpoint = td::Bits256::ones();
    for (auto it = ranges_in_progress_.begin(); it != ranges_in_progress_.end(); ) {
        if (*it == range) {
            it = ranges_in_progress_.erase(it);  // erase() returns iterator to next element
        } else {
            if (it->first < new_checkpoint) {
                new_checkpoint = it->first;
            }
            ++it;
        }
    }
    if (new_checkpoint == td::Bits256::ones()) {
        new_checkpoint = next_batch_start_;
    }
    update_checkpoint(new_checkpoint);

    completed_batches_since_reload_++;
    if (refresh_in_progress_) {
        return;
    }
    if (options_.reload_shard_state_every_batches_ != 0 &&
        completed_batches_since_reload_ >= options_.reload_shard_state_every_batches_) {
        request_shard_state_reload();
        return;
    }

    schedule_next();
}

void ShardStateScanner::request_shard_state_reload() {
    // We intentionally rebuild shard-state roots from CellDb from time to time instead of
    // reusing the same root graph for the whole shard scan.
    //
    // Why this is needed:
    // 1. The scanner keeps traversing long-lived roots:
    //    - the shard accounts dictionary in shard_state_data_->sstate_.accounts
    //    - the shared shard-state roots used by interface detectors
    //    - config/libraries roots used by execute_smc_method()
    // 2. In TON VM, loading an ExtCell materializes and stores the corresponding DataCell
    //    under that live object graph. There is no general "unload this subtree" API that
    //    turns those loaded cells back into lazy/pruned placeholders.
    // 3. As scanning continues, repeated dictionary lookups and get-method execution keep
    //    attaching more loaded cells to the same roots, so the retained live graph grows.
    // 4. The practical way to drop that accumulated VM state is to stop using the old roots
    //    and rebuild a fresh graph from CellDb. In-flight actors still finish on the old
    //    ShardStateData, but once they release it, the old graph becomes collectible.
    //
    // Reloading is therefore a bounded-lifetime root rotation mechanism: it does not change
    // scanner semantics, it only prevents one root object graph from accumulating loaded VM
    // cells for the entire duration of a long scan.
    refresh_in_progress_ = true;
    LOG(INFO) << "Reloading shard-state snapshot for " << shard_.to_str() << " after "
              << completed_batches_since_reload_ << " completed batches";
    auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<std::shared_ptr<vm::CellDbReader>> result) {
        td::actor::send_closure(SelfId, &ShardStateScanner::got_reload_cell_db_reader, std::move(result));
    });
    td::actor::send_closure(db_scanner_, &DbScanner::get_cell_db_reader, std::move(P));
}

void ShardStateScanner::got_reload_cell_db_reader(td::Result<std::shared_ptr<vm::CellDbReader>> result) {
    if (result.is_error()) {
        refresh_in_progress_ = false;
        LOG(ERROR) << "Failed to get CellDbReader for shard-state reload for " << shard_.to_str() << ": "
                   << result.move_as_error_prefix("");
        stop();
        return;
    }

    auto reader = result.move_as_ok();
    AllShardStates shard_states;
    shard_states.reserve(reload_context_->state_root_hashes_.size());
    for (const auto &state_root_hash : reload_context_->state_root_hashes_) {
        auto state_r = reader->load_cell(state_root_hash.as_slice());
        if (state_r.is_error()) {
            refresh_in_progress_ = false;
            LOG(ERROR) << "Failed to load fresh shard state for root hash " << state_root_hash.to_hex()
                       << " while reloading " << shard_.to_str() << ": "
                       << state_r.move_as_error_prefix("");
            stop();
            return;
        }
        shard_states.push_back(state_r.move_as_ok());
    }

    if (current_shard_index_ >= shard_states.size()) {
        refresh_in_progress_ = false;
        LOG(ERROR) << "Current shard index is out of range during shard-state reload for " << shard_.to_str();
        stop();
        return;
    }

    auto shard_state_data = std::make_shared<ShardStateData>();
    shard_state_data->shard_states_ = std::move(shard_states);
    if (!tlb::unpack_cell(shard_state_data->shard_states_[current_shard_index_], shard_state_data->sstate_)) {
        refresh_in_progress_ = false;
        LOG(ERROR) << "Failed to unpack reloaded ShardStateUnsplit for " << shard_.to_str();
        stop();
        return;
    }

    auto config_r = block::ConfigInfo::extract_config(
        shard_state_data->shard_states_[0],
        reload_context_->config_block_id_,
        block::ConfigInfo::needCapabilities | block::ConfigInfo::needLibraries);
    if (config_r.is_error()) {
        refresh_in_progress_ = false;
        LOG(ERROR) << "Failed to extract config from reloaded shard state for " << shard_.to_str() << ": "
                   << config_r.move_as_error_prefix("");
        stop();
        return;
    }
    shard_state_data->config_ = std::shared_ptr<block::ConfigInfo>(config_r.move_as_ok());

    refresh_in_progress_ = false;
    shard_state_data_ = std::move(shard_state_data);
    completed_batches_since_reload_ = 0;
    schedule_next();
}
