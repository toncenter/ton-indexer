#include <td/actor/actor.h>
#include <td/actor/MultiPromise.h>
#include <block/block.h>

#include "convert-utils.h"
#include "IndexData.h"
#include "Statistics.h"
#include "parse_contract_methods.h"
#include "smc-interfaces/InterfacesDetector.h"

using AllShardStates = std::vector<td::Ref<vm::Cell>>;
using FullDetector = InterfacesDetector<JettonWalletDetectorR, JettonMasterDetectorR,
                                     NftItemDetectorR, NftCollectionDetectorR,
                                     GetGemsNftAuction, GetGemsNftFixPriceSale,
                                     MultisigContract, MultisigOrder,
                                     VestingContract, DedustPoolDetector, StonfiPoolV2Detector>;

class BlockInterfaceProcessor: public td::actor::Actor {
private:
    DataContainerPtr block_;
    td::Promise<DataContainerPtr> promise_;
    std::unordered_map<block::StdAddress, std::vector<schema::BlockchainInterfaceV2>> interfaces_{};
    std::unordered_multimap<td::Bits256, uint64_t> contract_methods_{};
    td::Timer timer_{true};
public:
    BlockInterfaceProcessor(DataContainerPtr block, td::Promise<DataContainerPtr> promise) :
        block_(std::move(block)), promise_(std::move(promise)) {}

    void start_up() override {
        timer_.resume();
        std::unordered_map<block::StdAddress, schema::AccountState> account_states_to_detect;
        for (const auto& account_state : block_->account_states_) {
            if (!account_state.code_hash || !account_state.data_hash) {
                continue;
            }
            const auto& account_address = std::get<schema::AddressStd>(account_state.account);
            auto existing = account_states_to_detect.find(account_address);
            if (existing == account_states_to_detect.end() || account_state.last_trans_lt > existing->second.last_trans_lt) {
                account_states_to_detect[account_address] = account_state;
                interfaces_[account_address] = {};
                
                // check if need to parse contract methods
                if (account_state.code.not_null()) {
                    auto code_hash = account_state.code_hash.value();
                    if (contract_methods_.find(code_hash) == contract_methods_.end()) {
                        auto methods_result = parse_contract_methods(account_state.code);
                        if (methods_result.is_ok()) {
                            for (auto method_id : methods_result.move_as_ok()) {
                                contract_methods_.emplace(code_hash, method_id);
                            }
                        }
                    }
                }
            }
        }

        std::vector<td::Ref<vm::Cell>> shard_states;
        for (const auto& shard_state : block_->mc_block_.shard_blocks_) {
            shard_states.push_back(shard_state.block_state);
        }

        auto P = td::PromiseCreator::lambda([&, SelfId=actor_id(this)](td::Result<td::Unit> res) mutable {
            td::actor::send_closure(SelfId, &BlockInterfaceProcessor::finish, std::move(res));
        });

        td::MultiPromise mp;
        auto ig = mp.init_guard();
        ig.add_promise(std::move(P));

        for (const auto& [_, account_state] : account_states_to_detect) {
            if (account_state.code.is_null()) {
                continue;
            }
            const auto& account_address = std::get<schema::AddressStd>(account_state.account);
            auto code_cell = vm::std_boc_deserialize(account_state.code_boc.value()).move_as_ok();
            auto data_cell = vm::std_boc_deserialize(account_state.data_boc.value()).move_as_ok();
            td::actor::create_actor<FullDetector>("InterfacesDetector", account_address, std::move(code_cell), std::move(data_cell), shard_states, block_->mc_block_.config_,
                td::PromiseCreator::lambda([SelfId = actor_id(this), account_state, promise = ig.get_promise()](std::vector<typename FullDetector::DetectedInterface> interfaces) mutable {
                    const auto& account_address = std::get<schema::AddressStd>(account_state.account);
                    td::actor::send_closure(SelfId, &BlockInterfaceProcessor::process_address_interfaces, account_address, std::move(interfaces),
                                            account_state.code_hash.value(), account_state.data_hash.value(), account_state.last_trans_lt, account_state.timestamp, std::move(promise));
            })).release();
        }
    }

    void process_address_interfaces(block::StdAddress address, std::vector<typename FullDetector::DetectedInterface> interfaces, 
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

    void finish(td::Result<td::Unit> status) {
        if (status.is_error()) {
            promise_.set_error(status.move_as_error_prefix("Failed to detect interfaces: "));
        } else {
            block_->account_interfaces_ = std::move(interfaces_);
            block_->contract_methods_ = std::move(contract_methods_);
            promise_.set_result(std::move(block_));
        }
        g_statistics.record_time(DETECT_INTERFACES_SEQNO, timer_.elapsed() * 1e3);
        stop();
    }
};
