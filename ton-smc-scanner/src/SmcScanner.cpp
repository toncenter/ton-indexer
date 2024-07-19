#include "SmcScanner.h"
#include "convert-utils.h"

void SmcScanner::start_up() {
    auto P = td::PromiseCreator::lambda([=, SelfId = actor_id(this)](td::Result<MasterchainBlockDataState> R){
        if (R.is_error()) {
            LOG(ERROR) << "Failed to get seqno " << options_.seqno_ << ": " << R.move_as_error();
            stop();
            return;
        }
        td::actor::send_closure(SelfId, &SmcScanner::got_block, R.move_as_ok());
    });

    td::actor::send_closure(db_scanner_, &DbScanner::fetch_seqno, options_.seqno_, std::move(P));
}

void SmcScanner::got_block(MasterchainBlockDataState block) {
    LOG(INFO) << "Got block data state";
    for (const auto &shard_ds : block.shard_blocks_) {
        auto& shard_state = shard_ds.block_state;
        td::actor::create_actor<ShardStateScanner>("ShardStateScanner", shard_state, block, options_).release();
    }
}

ShardStateScanner::ShardStateScanner(td::Ref<vm::Cell> shard_state, MasterchainBlockDataState mc_block_ds, Options options) 
    : shard_state_(shard_state), mc_block_ds_(mc_block_ds), options_(options) {
    LOG(INFO) << "Created ShardStateScanner!";
}

void ShardStateScanner::schedule_next() {
    vm::AugmentedDictionary accounts_dict{vm::load_cell_slice_ref(shard_state_data_->sstate_.accounts), 256, block::tlb::aug_ShardAccounts};

    int count = 0;
    allow_same = true;
    while (!finished && count < 10000) {
        td::Ref<vm::CellSlice> shard_account_csr = accounts_dict.vm::DictionaryFixed::lookup_nearest_key(cur_addr_.bits(), 256, true, allow_same);
        if (shard_account_csr.is_null()) {
            finished = true;
            break;
        }
        allow_same = false;
        shard_account_csr = accounts_dict.extract_value(shard_account_csr);
        block::gen::ShardAccount::Record acc_info;
        if(!tlb::csr_unpack(shard_account_csr, acc_info)) {
            LOG(ERROR) << "Failed to unpack ShardAccount for " << cur_addr_.to_hex();
            continue;
        }

        ++count;
        queue_.push_back(std::make_pair(cur_addr_, std::move(acc_info)));
        if (queue_.size() > options_.batch_size_) {
            // LOG(INFO) << "Dispatched batch of " << queue_.size() << " account states";
            std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record>> batch_;
            std::copy(queue_.begin(), queue_.end(), std::back_inserter(batch_));
            queue_.clear();
            
            in_progress_.fetch_add(1);
            td::actor::create_actor<StateBatchParser>("parser", std::move(batch_), shard_state_data_, actor_id(this), options_).release();
        }
    }
    processed_ += count;
    
    td::actor::send_closure(options_.insert_manager_, &PostgreSQLInsertManager::checkpoint, shard_, cur_addr_);
    if(!finished) {
        alarm_timestamp() = td::Timestamp::in(0.1);
    } else {
        LOG(ERROR) << "Finished!";
        stop();
    }
}

void ShardStateScanner::start_up() {
    // cur_addr_.from_hex("012508807D259B1F3BDD2A830CF7F4591838E0A1D1474A476B20CFB540CD465B");
    // cur_addr_.from_hex("E750CF93EAEDD2EC01B5DE8F49A334622BD630A8728806ABA65F1443EB7C8FD7");
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

    if (options_.from_checkpoint) {
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), shard = shard_](td::Result<td::Bits256> R) {
            td::Bits256 cur_addr{td::Bits256::zero()};
            if (R.is_error()) {
                LOG(ERROR) << "Failed to restore state for shard (" << shard.workchain << "," << shard.shard << ")";
            } else {
                cur_addr = R.move_as_ok();
            }
            td::actor::send_closure(SelfId, &ShardStateScanner::got_checkpoint, std::move(cur_addr));
        });
        td::actor::send_closure(options_.insert_manager_, &PostgreSQLInsertManager::checkpoint_read, shard_, std::move(P));
    } else {
        td::actor::send_closure(options_.insert_manager_, &PostgreSQLInsertManager::checkpoint_reset, shard_);
        td::actor::send_closure(actor_id(this), &ShardStateScanner::got_checkpoint, td::Bits256::zero());
    }
}

void ShardStateScanner::alarm() {
    LOG(INFO) << "workchain: " << shard_.workchain << " shard: " << static_cast<std::int64_t>(shard_.shard) << " cur_addr: " << cur_addr_.to_hex();
    schedule_next();
}

void ShardStateScanner::batch_inserted() {
    in_progress_.fetch_sub(1);
}

void ShardStateScanner::got_checkpoint(td::Bits256 cur_addr) {
    cur_addr_ = std::move(cur_addr);
    alarm_timestamp() = td::Timestamp::in(0.1);
}

void StateBatchParser::interfaces_detected(std::vector<Detector::DetectedInterface> ifaces) {
    LOG(ERROR) << "Detected, but not queued! Interfaces will not be inserted!!!";
    // TODO: here should be added logic of addition to result_
}

void StateBatchParser::process_account_states(std::vector<schema::AccountState> account_states) {
    // LOG(INFO) << "Processing account state " << account.account;
    for (auto &account : account_states) {
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), addr = account.account](td::Result<std::vector<Detector::DetectedInterface>> R) {
            if (R.is_error()) {
                LOG(ERROR) << "Failed to detect interfaces of account '" << addr << "'";
                return;
            }
            td::actor::send_closure(SelfId, &StateBatchParser::interfaces_detected, R.move_as_ok());
        });
        td::actor::create_actor<Detector>("InterfacesDetector", account.account, account.code, account.data, shard_state_data_->shard_states_, shard_state_data_->config_, std::move(P)).release();
    }
}

void StateBatchParser::start_up() {
    // if (cur_addr_.to_hex() != "E753CF93EAEDD2EC01B5DE8F49A334622BD630A8728806ABA65F1443EB7C8FD7") {
    //     continue;
    // }
    std::vector<schema::AccountState> state_list_;
    for (auto &[addr_, acc_info] : data_) {
        int account_tag = block::gen::t_Account.get_tag(vm::load_cell_slice(acc_info.account));
        switch (account_tag) {
            case block::gen::Account::account_none: {
                LOG(WARNING) << "Skipping non-existing account " << addr_;
                break;
            }
            case block::gen::Account::account: {
                auto account_r = ParseQuery::parse_account(acc_info.account, shard_state_data_->sstate_.gen_utime, acc_info.last_trans_hash, acc_info.last_trans_lt);
                if (account_r.is_error()) {
                    LOG(ERROR) << "Failed to parse account " << addr_.to_hex() << ": " << account_r.move_as_error();
                    break;
                }
                auto account_state_ = account_r.move_as_ok();
                result_.push_back(account_state_);
                state_list_.push_back(account_state_);
                break;
            }
            default: LOG(ERROR) << "Unknown account tag"; break;
        }
    }
    if (options_.index_interfaces_) {
        process_account_states(state_list_);
    } else {
        std::copy(state_list_.begin(), state_list_.end(), std::back_inserter(result_));
        processing_finished();
    }
}

void StateBatchParser::processing_finished() {
    td::actor::send_closure(options_.insert_manager_, &PostgreSQLInsertManager::insert_data, std::move(result_));
    td::actor::send_closure(shard_state_scanner_, &ShardStateScanner::batch_inserted);
    stop();
}
