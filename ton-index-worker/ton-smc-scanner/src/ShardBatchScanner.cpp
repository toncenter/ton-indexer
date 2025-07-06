#include "ShardBatchScanner.h"
#include "StateParser.h"
#include "td/utils/filesystem.h"
#include "td/utils/port/path.h"

ShardStateScanner::ShardStateScanner(td::Ref<vm::Cell> shard_state, MasterchainBlockDataState mc_block_ds, Options options) 
    : shard_state_(shard_state), mc_block_ds_(mc_block_ds), options_(options) {
    LOG(INFO) << "Created ShardStateScanner!";
}

void ShardStateScanner::schedule_next() {
    vm::AugmentedDictionary accounts_dict{vm::load_cell_slice_ref(shard_state_data_->sstate_.accounts), 256, block::tlb::aug_ShardAccounts};

    std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record>> batch;
    batch.reserve(options_.batch_size_);

    while (batch.size() < options_.batch_size_ && !finished_) {
        td::Ref<vm::CellSlice> shard_account_csr = accounts_dict.vm::DictionaryFixed::lookup_nearest_key(cur_addr_.bits(), 256, true, allow_same_);
        if (shard_account_csr.is_null()) {
            finished_ = true;
            break;
        }
        allow_same_ = false;
        shard_account_csr = accounts_dict.extract_value(shard_account_csr);
        block::gen::ShardAccount::Record acc_info;
        if(!tlb::csr_unpack(shard_account_csr, acc_info)) {
            LOG(ERROR) << "Failed to unpack ShardAccount for " << cur_addr_.to_hex();
            continue;
        }

        batch.push_back(std::make_pair(cur_addr_, std::move(acc_info)));
    }

    LOG(INFO) << shard_.to_str() << ": Dispatched batch of " << batch.size() << " account states";
    processed_ += batch.size();
    
    in_progress_++;
    td::actor::create_actor<StateBatchParser>("parser", std::move(batch), shard_state_data_, actor_id(this), options_).release();
    

    auto checkpoint_file_path = get_checkpoint_file_path();
    if (!checkpoint_file_path.empty()) {
        auto S = td::write_file(checkpoint_file_path, cur_addr_.to_hex());
        if (S.is_error()) {
            LOG(ERROR) << "Failed to write checkpoint to " << checkpoint_file_path;
        }
    }
    if(!finished_) {
        schedule_next();
    } else {
        LOG(INFO) << "Shard " << shard_.to_str() <<  " is finished with " << processed_ << " account states";
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

void ShardStateScanner::start_up() {
    shard_state_data_ = std::make_shared<ShardStateData>();
    for (const auto &shard_ds : mc_block_ds_.shard_blocks_) {
        shard_state_data_->shard_states_.push_back(shard_ds.block_state);
    }
    auto config_r = block::ConfigInfo::extract_config(mc_block_ds_.shard_blocks_[0].block_state, block::ConfigInfo::needCapabilities | block::ConfigInfo::needLibraries);
    if (config_r.is_error()) {
        LOG(ERROR) << "Failed to extract config: " << config_r.move_as_error();
        std::_Exit(2);
        return;
    }
    shard_state_data_->config_ = config_r.move_as_ok();

    if (!tlb::unpack_cell(shard_state_, shard_state_data_->sstate_)) {
        LOG(ERROR) << "Failed to unpack ShardStateUnsplit";
        stop();
        return;
    }

    shard_ = ton::ShardIdFull(block::ShardId(shard_state_data_->sstate_.shard_id.write()));

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
            cur_addr_ = std::move(checkpoint);
            LOG(INFO) << "Read checkpoint for seqno " << options_.seqno_ << " and shard" << shard_.to_str() << ": " << cur_addr_.to_hex();
        } else {
            LOG(INFO) << "No checkpoint found for shard " << shard_.to_str() << ", starting from zero address";
            cur_addr_ = td::Bits256::zero();
        }
    } else {
        LOG(INFO) << "No working dir provided, starting from zero address";
        cur_addr_ = td::Bits256::zero();
    }
    schedule_next();
}

void ShardStateScanner::batch_inserted() {
    in_progress_--;
}