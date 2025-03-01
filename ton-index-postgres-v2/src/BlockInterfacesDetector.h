#include <td/actor/actor.h>
#include <td/actor/MultiPromise.h>
#include <block/block.h>
#include "IndexData.h"
#include "Statistics.h"

class BlockInterfaceProcessor: public td::actor::Actor {
private:
    ParsedBlockPtr block_;
    td::Promise<ParsedBlockPtr> promise_;
    std::unordered_map<block::StdAddress, std::vector<BlockchainInterfaceV2>> interfaces_{};
    td::Timer timer_{true};
public:
    BlockInterfaceProcessor(ParsedBlockPtr block, td::Promise<ParsedBlockPtr> promise) : 
        block_(std::move(block)), promise_(std::move(promise)) {}

    void start_up() override {
        timer_.resume();
        std::unordered_map<block::StdAddress, schema::AccountState> account_states_to_detect;
        for (const auto& account_state : block_->account_states_) {
            if (!account_state.code_hash || !account_state.data_hash) {
                continue;
            }
            auto existing = account_states_to_detect.find(account_state.account);
            if (existing == account_states_to_detect.end() || account_state.last_trans_lt > existing->second.last_trans_lt) {
                account_states_to_detect[account_state.account] = account_state;
                interfaces_[account_state.account] = {};
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
            td::actor::create_actor<Detector>("InterfacesDetector", account_state.account, account_state.code, account_state.data, shard_states, block_->mc_block_.config_, 
                td::PromiseCreator::lambda([SelfId = actor_id(this), account_state, promise = ig.get_promise()](std::vector<typename Detector::DetectedInterface> interfaces) mutable {
                    td::actor::send_closure(SelfId, &BlockInterfaceProcessor::process_address_interfaces, account_state.account, std::move(interfaces), 
                                            account_state.code_hash.value(), account_state.data_hash.value(), account_state.last_trans_lt, account_state.timestamp, std::move(promise));
            })).release();
        }
    }

    void process_address_interfaces(block::StdAddress address, std::vector<typename Detector::DetectedInterface> interfaces, 
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
                } else if constexpr (std::is_same_v<T, GetGemsNftAuction::Result>) {
                    GetGemsNftAuctionData auction_data;
                    auction_data.address = address;
                    auction_data.end = arg.end;
                    auction_data.end_time = arg.end_time;
                    auction_data.mp_addr = arg.mp_addr;
                    auction_data.nft_addr = arg.nft_addr;
                    auction_data.nft_owner = arg.nft_owner;
                    auction_data.last_bid = arg.last_bid;
                    auction_data.last_member = arg.last_member;
                    auction_data.min_step = arg.min_step;
                    auction_data.mp_fee_addr = arg.mp_fee_addr;
                    auction_data.mp_fee_factor = arg.mp_fee_factor;
                    auction_data.mp_fee_base = arg.mp_fee_base;
                    auction_data.royalty_fee_addr = arg.royalty_fee_addr;
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
                    GetGemsNftFixPriceSaleData fix_price_sale_data;
                    fix_price_sale_data.address = address;
                    fix_price_sale_data.is_complete = arg.is_complete;
                    fix_price_sale_data.created_at = arg.created_at;
                    fix_price_sale_data.marketplace_address = arg.marketplace_address;
                    fix_price_sale_data.nft_address = arg.nft_address;
                    fix_price_sale_data.nft_owner_address = arg.nft_owner_address;
                    fix_price_sale_data.full_price = arg.full_price;
                    fix_price_sale_data.marketplace_fee_address = arg.marketplace_fee_address;
                    fix_price_sale_data.marketplace_fee = arg.marketplace_fee;
                    fix_price_sale_data.royalty_address = arg.royalty_address;
                    fix_price_sale_data.royalty_amount = arg.royalty_amount;
                    fix_price_sale_data.last_transaction_lt = last_trans_lt;
                    fix_price_sale_data.last_transaction_now = last_trans_now;
                    fix_price_sale_data.code_hash = code_hash;
                    fix_price_sale_data.data_hash = data_hash;
                    interfaces_[address].push_back(fix_price_sale_data);
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
            promise_.set_result(std::move(block_));
        }
        g_statistics.record_time(DETECT_INTERFACES_SEQNO, timer_.elapsed() * 1e3);
        stop();
    }
};