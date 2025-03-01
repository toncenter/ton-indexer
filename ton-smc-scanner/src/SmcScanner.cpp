#include "SmcScanner.h"
#include "convert-utils.h"

void SmcScanner::start_up() {
    auto P = td::PromiseCreator::lambda([=, this, SelfId = actor_id(this)](td::Result<MasterchainBlockDataState> R){
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
    
    td::actor::send_closure(options_.insert_manager_, &PostgreSQLInsertManager::checkpoint, shard_, cur_addr_);
    if(!finished_) {
        alarm_timestamp() = td::Timestamp::in(0.1);
    } else {
        LOG(INFO) << "Shard " << shard_.to_str() <<  " is finished with " << processed_ << " account states";
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
                LOG(ERROR) << "Failed to restore state for shard " << shard.to_str() << ": " << R.move_as_error();
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
    LOG(INFO) << "Shard " << shard_.to_str() << " cur addr: " << cur_addr_.to_hex();
    schedule_next();
}

void ShardStateScanner::batch_inserted() {
    in_progress_--;
}

void ShardStateScanner::got_checkpoint(td::Bits256 cur_addr) {
    cur_addr_ = std::move(cur_addr);
    alarm_timestamp() = td::Timestamp::in(0.1);
}

void StateBatchParser::interfaces_detected(block::StdAddress address, std::vector<typename Detector::DetectedInterface> interfaces, 
                                    td::Bits256 code_hash, td::Bits256 data_hash, uint64_t last_trans_lt, uint32_t last_trans_now, td::Promise<td::Unit> promise) {
    for (auto& interface : interfaces) {
        std::visit([&](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, JettonMasterDetectorR::Result>) {
                JettonMasterDataV2 jetton_master_data;
                jetton_master_data.address = address;
                jetton_master_data.total_supply = arg.total_supply;
                jetton_master_data.mintable = arg.mintable;
                jetton_master_data.admin_address = arg.admin_address;
                jetton_master_data.jetton_content = arg.jetton_content;
                jetton_master_data.jetton_wallet_code_hash = arg.jetton_wallet_code_hash.bits();
                jetton_master_data.data_hash = data_hash;
                jetton_master_data.code_hash = code_hash;
                jetton_master_data.last_transaction_lt = last_trans_lt;
                jetton_master_data.last_transaction_now = last_trans_now;
                interfaces_[address].push_back(jetton_master_data);
            } else if constexpr (std::is_same_v<T, JettonWalletDetectorR::Result>) {
                JettonWalletDataV2 jetton_wallet_data;
                jetton_wallet_data.balance = arg.balance;
                jetton_wallet_data.address = address;
                jetton_wallet_data.owner = arg.owner;
                jetton_wallet_data.jetton = arg.jetton;
                jetton_wallet_data.mintless_is_claimed = arg.mintless_is_claimed;
                jetton_wallet_data.last_transaction_lt = last_trans_lt;
                jetton_wallet_data.last_transaction_now = last_trans_now;
                jetton_wallet_data.code_hash = code_hash;
                jetton_wallet_data.data_hash = data_hash;
                interfaces_[address].push_back(jetton_wallet_data);
            } else if constexpr (std::is_same_v<T, NftCollectionDetectorR::Result>) {
                NFTCollectionDataV2 nft_collection_data;
                nft_collection_data.address = address;
                nft_collection_data.next_item_index = arg.next_item_index;
                nft_collection_data.owner_address = arg.owner_address;
                nft_collection_data.collection_content = arg.collection_content;
                nft_collection_data.last_transaction_lt = last_trans_lt;
                nft_collection_data.last_transaction_now = last_trans_now;
                nft_collection_data.code_hash = code_hash;
                nft_collection_data.data_hash = data_hash;
                interfaces_[address].push_back(nft_collection_data);
            } else if constexpr (std::is_same_v<T, NftItemDetectorR::Result>) {
                NFTItemDataV2 nft_item_data;
                nft_item_data.address = address;
                nft_item_data.init = arg.init;
                nft_item_data.index = arg.index;
                nft_item_data.collection_address = arg.collection_address;
                nft_item_data.owner_address = arg.owner_address;
                nft_item_data.content = arg.content;
                nft_item_data.last_transaction_lt = last_trans_lt;
                nft_item_data.last_transaction_now = last_trans_now;
                nft_item_data.code_hash = code_hash;
                nft_item_data.data_hash = data_hash;
                if (arg.dns_entry) {
                    nft_item_data.dns_entry = NFTItemDataV2::DNSEntry{arg.dns_entry->domain, 
                                                                      arg.dns_entry->wallet, 
                                                                      arg.dns_entry->next_resolver, 
                                                                      arg.dns_entry->site_adnl,
                                                                      arg.dns_entry->storage_bag_id};
                }
                interfaces_[address].push_back(nft_item_data);
            }
        }, interface);
    }
    promise.set_value(td::Unit());
}

void StateBatchParser::process_account_states(std::vector<schema::AccountState> account_states) {
    // LOG(INFO) << "Processing account state " << account.account;
    
    auto P = td::PromiseCreator::lambda([&, SelfId=actor_id(this)](td::Result<td::Unit> res) mutable {
        td::actor::send_closure(SelfId, &StateBatchParser::processing_finished);
    });

    td::MultiPromise mp;
    auto ig = mp.init_guard();
    ig.add_promise(std::move(P));

    for (auto &account : account_states) {
        if (account.code.is_null() || account.data.is_null()) {
            continue;
        }
        interfaces_[account.account] = {};
        auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), account, promise = ig.get_promise()](td::Result<std::vector<Detector::DetectedInterface>> R) mutable {
            if (R.is_error()) {
                LOG(ERROR) << "Failed to detect interfaces of account '" << account.account << "'";
                return;
            }
            td::actor::send_closure(SelfId, &StateBatchParser::interfaces_detected, account.account, R.move_as_ok(), account.code_hash.value(), account.data_hash.value(), account.last_trans_lt, account.timestamp, std::move(promise));
        });
        td::actor::create_actor<Detector>("InterfacesDetector", account.account, account.code, account.data, shard_state_data_->shard_states_, shard_state_data_->config_, std::move(P)).release();
    }
}

void StateBatchParser::start_up() {
    std::vector<schema::AccountState> state_list;
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
                auto account_state = account_r.move_as_ok();
                state_list.push_back(account_state);
                break;
            }
            default: LOG(ERROR) << "Unknown account tag"; break;
        }
    }
    
    std::copy(state_list.begin(), state_list.end(), std::back_inserter(result_));

    if (options_.index_interfaces_) {
        process_account_states(state_list);
    } else {
        processing_finished();
    }
}

void StateBatchParser::processing_finished() {
    for (auto& [addr, ifaces] : interfaces_ ) {
        std::copy(ifaces.begin(), ifaces.end(), std::back_inserter(result_));
    }
    td::actor::send_closure(options_.insert_manager_, &PostgreSQLInsertManager::insert_data, std::move(result_));
    td::actor::send_closure(shard_state_scanner_, &ShardStateScanner::batch_inserted);
    stop();
}
