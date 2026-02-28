#pragma once
#include "td/actor/actor.h"
#include "ShardBatchScanner.h"


class StateBatchParser: public td::actor::Actor {
private:
  std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record>> data_;
  ShardStateDataPtr shard_state_data_;
  Options options_;
  td::Promise<std::vector<InsertData>> promise_;

  std::unordered_map<block::StdAddress, std::vector<InsertData>> interfaces_;
  std::vector<InsertData> result_;
public:
  StateBatchParser(std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record>> data,
    ShardStateDataPtr shard_state_data, Options options, td::Promise<std::vector<InsertData>> promise)
    : data_(std::move(data)), shard_state_data_(std::move(shard_state_data)),
      options_(options), promise_(std::move(promise)) {}

  StateBatchParser(std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record>> data,
    ShardStateDataPtr shard_state_data,
    Options options)
    : data_(std::move(data)), shard_state_data_(std::move(shard_state_data)), options_(options) {}
  void start_up() override;
  void processing_finished();
private:
  void interfaces_detected(block::StdAddress address, std::vector<typename Detector::DetectedInterface> interfaces,
                                    td::Bits256 code_hash, td::Bits256 data_hash, uint64_t last_trans_lt, uint32_t last_trans_now, td::Promise<td::Unit> promise);
  void process_account_states(std::vector<schema::AccountState> account_states);
};

void StateBatchParser::interfaces_detected(block::StdAddress address, std::vector<typename Detector::DetectedInterface> interfaces,
                                    td::Bits256 code_hash, td::Bits256 data_hash, uint64_t last_trans_lt, uint32_t last_trans_now, td::Promise<td::Unit> promise) {
    for (auto& interface : interfaces) {
        std::visit([&](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, JettonMasterDetectorR::Result>) {
                schema::JettonMasterDataV2 jetton_master_data;
                jetton_master_data.address = schema::AddressStd{address};
                jetton_master_data.total_supply = arg.total_supply;
                jetton_master_data.mintable = arg.mintable;
                jetton_master_data.admin_address = convert::to_account_address(arg.admin_address);
                jetton_master_data.jetton_content = arg.jetton_content;
                jetton_master_data.jetton_wallet_code_hash = arg.jetton_wallet_code_hash.bits();
                jetton_master_data.data_hash = data_hash;
                jetton_master_data.code_hash = code_hash;
                jetton_master_data.last_transaction_lt = last_trans_lt;
                jetton_master_data.last_transaction_now = last_trans_now;
                interfaces_[address].push_back(jetton_master_data);
            } else if constexpr (std::is_same_v<T, JettonWalletDetectorR::Result>) {
                schema::JettonWalletDataV2 jetton_wallet_data;
                jetton_wallet_data.balance = arg.balance;
                jetton_wallet_data.address = schema::AddressStd{address};
                jetton_wallet_data.owner = schema::AddressStd{arg.owner};
                jetton_wallet_data.jetton = schema::AddressStd{arg.jetton};
                jetton_wallet_data.mintless_is_claimed = arg.mintless_is_claimed;
                jetton_wallet_data.last_transaction_lt = last_trans_lt;
                jetton_wallet_data.last_transaction_now = last_trans_now;
                jetton_wallet_data.code_hash = code_hash;
                jetton_wallet_data.data_hash = data_hash;
                interfaces_[address].push_back(jetton_wallet_data);
            } else if constexpr (std::is_same_v<T, NftCollectionDetectorR::Result>) {
                schema::NFTCollectionDataV2 nft_collection_data;
                nft_collection_data.address = schema::AddressStd{address};
                nft_collection_data.next_item_index = arg.next_item_index;
                nft_collection_data.owner_address = convert::to_account_address(arg.owner_address);
                nft_collection_data.collection_content = arg.collection_content;
                nft_collection_data.last_transaction_lt = last_trans_lt;
                nft_collection_data.last_transaction_now = last_trans_now;
                nft_collection_data.code_hash = code_hash;
                nft_collection_data.data_hash = data_hash;
                interfaces_[address].push_back(nft_collection_data);
            } else if constexpr (std::is_same_v<T, NftItemDetectorR::Result>) {
                schema::NFTItemDataV2 nft_item_data;
                nft_item_data.address = schema::AddressStd{address};
                nft_item_data.init = arg.init;
                nft_item_data.index = arg.index;
                nft_item_data.collection_address = convert::to_account_address(arg.collection_address);
                nft_item_data.owner_address = convert::to_account_address(arg.owner_address);
                nft_item_data.content = arg.content;
                nft_item_data.last_transaction_lt = last_trans_lt;
                nft_item_data.last_transaction_now = last_trans_now;
                nft_item_data.code_hash = code_hash;
                nft_item_data.data_hash = data_hash;
                if (arg.dns_entry) {
                    nft_item_data.dns_entry = schema::NFTItemDataV2::DNSEntry{arg.dns_entry->domain,
                                                                      convert::to_account_address(arg.dns_entry->wallet),
                                                                      convert::to_account_address(arg.dns_entry->next_resolver),
                                                                      arg.dns_entry->site_adnl,
                                                                      arg.dns_entry->storage_bag_id};
                }
                interfaces_[address].push_back(nft_item_data);
            } else if constexpr (std::is_same_v<T, GetGemsNftAuction::Result>) {
                schema::GetGemsNftAuctionData auction_data;
                auction_data.address = schema::AddressStd{address};
                auction_data.end = arg.end;
                auction_data.end_time = arg.end_time;
                auction_data.mp_addr = schema::AddressStd{arg.mp_addr};
                auction_data.nft_addr = schema::AddressStd{arg.nft_addr};
                auction_data.nft_owner = convert::to_account_address(arg.nft_owner);
                auction_data.last_bid = arg.last_bid;
                auction_data.last_member = convert::to_account_address(arg.last_member);
                auction_data.min_step = arg.min_step;
                auction_data.mp_fee_addr = schema::AddressStd{arg.mp_fee_addr};
                auction_data.mp_fee_factor = arg.mp_fee_factor;
                auction_data.mp_fee_base = arg.mp_fee_base;
                auction_data.royalty_fee_addr = schema::AddressStd{arg.royalty_fee_addr};
                auction_data.royalty_fee_factor = arg.royalty_fee_factor;
                auction_data.royalty_fee_base = arg.royalty_fee_base;
                auction_data.max_bid = arg.max_bid;
                auction_data.min_bid = arg.min_bid;
                auction_data.created_at = arg.created_at;
                auction_data.last_bid_at = arg.last_bid_at;
                auction_data.is_canceled = arg.is_canceled;
                auction_data.last_transaction_lt = last_trans_lt;
                auction_data.last_transaction_now = last_trans_now;
                auction_data.code_hash = code_hash;
                auction_data.data_hash = data_hash;
                interfaces_[address].push_back(auction_data);
            } else if constexpr (std::is_same_v<T, GetGemsNftFixPriceSale::Result>) {
                schema::GetGemsNftFixPriceSaleData fix_price_sale_data;
                fix_price_sale_data.address = schema::AddressStd{address};
                fix_price_sale_data.is_complete = arg.is_complete;
                fix_price_sale_data.created_at = arg.created_at;
                fix_price_sale_data.marketplace_address = schema::AddressStd{arg.marketplace_address};
                fix_price_sale_data.nft_address = schema::AddressStd{arg.nft_address};
                fix_price_sale_data.nft_owner_address = convert::to_account_address(arg.nft_owner_address);
                fix_price_sale_data.full_price = arg.full_price;
                fix_price_sale_data.marketplace_fee_address = schema::AddressStd{arg.marketplace_fee_address};
                fix_price_sale_data.marketplace_fee = arg.marketplace_fee;
                fix_price_sale_data.royalty_address = schema::AddressStd{arg.royalty_address};
                fix_price_sale_data.royalty_amount = arg.royalty_amount;
                fix_price_sale_data.last_transaction_lt = last_trans_lt;
                fix_price_sale_data.last_transaction_now = last_trans_now;
                fix_price_sale_data.code_hash = code_hash;
                fix_price_sale_data.data_hash = data_hash;
                interfaces_[address].push_back(fix_price_sale_data);
            } else if constexpr (std::is_same_v<T, MultisigContract::Result>) {
                schema::MultisigContractData multisig_contract_data;
                multisig_contract_data.address = schema::AddressStd{address};
                multisig_contract_data.next_order_seqno = arg.next_order_seqno;
                multisig_contract_data.threshold = arg.threshold;
                multisig_contract_data.signers = convert::to_account_address_vector(arg.signers);
                multisig_contract_data.proposers = convert::to_account_address_vector(arg.proposers);
                multisig_contract_data.last_transaction_lt = last_trans_lt;
                multisig_contract_data.last_transaction_now = last_trans_now;
                multisig_contract_data.code_hash = code_hash;
                multisig_contract_data.data_hash = data_hash;
                interfaces_[address].push_back(multisig_contract_data);
            } else if constexpr (std::is_same_v<T, MultisigOrder::Result>) {
                schema::MultisigOrderData multisig_order_data;
                multisig_order_data.address = schema::AddressStd{address};
                multisig_order_data.multisig_address = schema::AddressStd{arg.multisig_address};
                multisig_order_data.order_seqno = arg.order_seqno;
                multisig_order_data.threshold = arg.threshold;
                multisig_order_data.sent_for_execution = arg.sent_for_execution;
                multisig_order_data.approvals_mask = arg.approvals_mask;
                multisig_order_data.approvals_num = arg.approvals_num;
                multisig_order_data.expiration_date = arg.expiration_date;
                multisig_order_data.order = arg.order;
                multisig_order_data.signers = convert::to_account_address_vector(arg.signers);
                multisig_order_data.last_transaction_lt = last_trans_lt;
                multisig_order_data.last_transaction_now = last_trans_now;
                multisig_order_data.code_hash = code_hash;
                multisig_order_data.data_hash = data_hash;
                interfaces_[address].push_back(multisig_order_data);
            } else if constexpr (std::is_same_v<T, VestingContract::Result>)
            {
                schema::VestingData vesting_data;
                vesting_data.address = schema::AddressStd{address};
                vesting_data.vesting_start_time = arg.vesting_start_time;
                vesting_data.vesting_total_duration = arg.vesting_total_duration;
                vesting_data.unlock_period = arg.unlock_period;
                vesting_data.cliff_duration = arg.cliff_duration;
                vesting_data.vesting_total_amount = arg.vesting_total_amount;
                vesting_data.vesting_sender_address = schema::AddressStd{arg.vesting_sender_address};
                vesting_data.owner_address = schema::AddressStd{arg.owner_address};
                vesting_data.whitelist = convert::to_account_address_vector(arg.whitelist);
                vesting_data.last_transaction_lt = last_trans_lt;
                vesting_data.last_transaction_now = last_trans_now;
                vesting_data.code_hash = code_hash;
                vesting_data.data_hash = data_hash;
                interfaces_[address].push_back(vesting_data);
            } else if constexpr (std::is_same_v<T, DedustPoolDetector::Result>) {
                schema::DedustPoolData dedust_pool_data;
                dedust_pool_data.address = schema::AddressStd{address};
                dedust_pool_data.asset_1 = convert::to_account_address(arg.asset_1);
                dedust_pool_data.asset_2 = convert::to_account_address(arg.asset_2);
                dedust_pool_data.last_transaction_lt = last_trans_lt;
                dedust_pool_data.last_transaction_now = last_trans_now;
                dedust_pool_data.code_hash = code_hash;
                dedust_pool_data.data_hash = data_hash;
                dedust_pool_data.reserve_1 = arg.reserve_1;
                dedust_pool_data.reserve_2 = arg.reserve_2;
                dedust_pool_data.is_stable = arg.is_stable;
                dedust_pool_data.fee = arg.fee;
                interfaces_[address].push_back(dedust_pool_data);
            } else if constexpr (std::is_same_v<T, StonfiPoolV2Detector::Result>) {
                schema::StonfiPoolV2Data stonfi_pool_data;
                stonfi_pool_data.address = schema::AddressStd{address};
                stonfi_pool_data.asset_1 = convert::to_account_address(arg.asset_1);
                stonfi_pool_data.asset_2 = convert::to_account_address(arg.asset_2);
                stonfi_pool_data.last_transaction_lt = last_trans_lt;
                stonfi_pool_data.last_transaction_now = last_trans_now;
                stonfi_pool_data.code_hash = code_hash;
                stonfi_pool_data.data_hash = data_hash;
                stonfi_pool_data.reserve_1 = arg.reserve_1;
                stonfi_pool_data.reserve_2 = arg.reserve_2;
                stonfi_pool_data.pool_type = arg.pool_type;
                stonfi_pool_data.fee = arg.fee;
                interfaces_[address].push_back(stonfi_pool_data);
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
        auto std_account = std::get<schema::AddressStd>(account.account);
        interfaces_[std_account] = {};
        auto Q = td::PromiseCreator::lambda([SelfId = actor_id(this), account, std_account, promise = ig.get_promise()](td::Result<std::vector<Detector::DetectedInterface>> R) mutable {
            if (R.is_error()) {
                LOG(ERROR) << "Failed to detect interfaces of account '" << std_account << "'";
                return;
            }
            td::actor::send_closure(SelfId, &StateBatchParser::interfaces_detected, std_account, R.move_as_ok(), account.code_hash.value(), account.data_hash.value(), account.last_trans_lt, account.timestamp, std::move(promise));
        });
        td::actor::create_actor<Detector>("InterfacesDetector", std_account, account.code, account.data, shard_state_data_->shard_states_, shard_state_data_->config_, std::move(Q)).release();
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
                auto account_r = parse_account_state(acc_info.account, shard_state_data_->sstate_.gen_utime, acc_info.last_trans_hash, acc_info.last_trans_lt);
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

    if (options_.index_account_states_) {
        std::copy(state_list.begin(), state_list.end(), std::back_inserter(result_));
    }

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
    promise_.set_value(std::move(result_));
    stop();
}
