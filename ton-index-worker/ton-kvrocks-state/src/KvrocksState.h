#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <sw/redis++/redis++.h>

#include "IndexData.h"

namespace kvrocks_state {

struct KvrocksConfig {
  bool enabled{false};

  std::string uri;
  std::vector<std::pair<std::string, int>> sentinel_nodes;
  std::string sentinel_master_name;

  std::optional<std::string> user;
  std::optional<std::string> password;
  std::optional<int> db;

  std::optional<std::string> sentinel_user;
  std::optional<std::string> sentinel_password;

  std::size_t pool_size{4};
  std::chrono::milliseconds connect_timeout{1000};
  std::chrono::milliseconds socket_timeout{1000};
  std::chrono::milliseconds wait_timeout{1000};
  std::chrono::milliseconds sentinel_retry_interval{100};
  std::size_t sentinel_max_retry{2};

  bool use_sentinel() const;
  std::string describe() const;
};

std::vector<std::pair<std::string, int>> parse_kvrocks_sentinel_nodes(const std::string& nodes);

class KvrocksClient {
public:
  explicit KvrocksClient(KvrocksConfig config);

  const KvrocksConfig& config() const;
  std::shared_ptr<sw::redis::Redis> make_redis() const;
  void ping() const;

private:
  sw::redis::ConnectionOptions build_connection_options() const;
  sw::redis::ConnectionPoolOptions build_pool_options() const;
  sw::redis::SentinelOptions build_sentinel_options() const;

  KvrocksConfig config_;
};

#define KVROCKS_PREPARED_ROW_AS_TUPLE(...) \
  auto as_tuple() const { \
    return std::tie(__VA_ARGS__); \
  }

struct PreparedMessageContentRow {
  td::Bits256 hash;
  std::string body;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(hash, body);
};

struct PreparedAccountStateRow {
  td::Bits256 hash;
  block::StdAddress account;
  td::RefInt256 balance;
  std::string balance_extra_currencies;
  std::string account_status;
  std::optional<td::Bits256> frozen_hash;
  std::optional<td::Bits256> code_hash;
  std::optional<td::Bits256> data_hash;
  std::uint32_t source_mc_seqno;
};

struct PreparedLatestAccountStateRow {
  block::StdAddress account;
  td::Bits256 hash;
  td::RefInt256 balance;
  std::string balance_extra_currencies;
  std::string account_status;
  std::uint32_t timestamp;
  td::Bits256 last_trans_hash;
  std::uint64_t last_trans_lt;
  std::optional<td::Bits256> frozen_hash;
  std::optional<td::Bits256> data_hash;
  std::optional<td::Bits256> code_hash;
  std::optional<std::string> data_boc;
  std::optional<std::string> code_boc;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(account, hash, balance, balance_extra_currencies, account_status,
                                timestamp, last_trans_hash, last_trans_lt, frozen_hash, data_hash, code_hash,
                                data_boc, code_boc);
};

struct PreparedJettonMasterRow {
  block::StdAddress address;
  td::RefInt256 total_supply;
  bool mintable;
  std::optional<block::StdAddress> admin_address;
  std::optional<std::string> jetton_content;
  td::Bits256 jetton_wallet_code_hash;
  std::uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(address, total_supply, mintable, admin_address, jetton_content, jetton_wallet_code_hash,
                                last_transaction_lt, code_hash, data_hash, destroyed);
};

struct PreparedJettonWalletRow {
  td::RefInt256 balance;
  block::StdAddress address;
  block::StdAddress owner;
  block::StdAddress jetton;
  std::uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  std::optional<bool> mintless_is_claimed;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(balance, address, owner, jetton, last_transaction_lt, code_hash, data_hash,
                                mintless_is_claimed, destroyed);
};

struct PreparedMintlessMasterRow {
  block::StdAddress address;
  bool is_indexed;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(address, is_indexed);
};

struct PreparedNftCollectionRow {
  block::StdAddress address;
  td::RefInt256 next_item_index;
  std::optional<block::StdAddress> owner_address;
  std::optional<std::string> collection_content;
  std::uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(address, next_item_index, owner_address, collection_content, last_transaction_lt,
                                code_hash, data_hash, destroyed);
};

struct PreparedNftItemRow {
  block::StdAddress address;
  bool init;
  td::RefInt256 index;
  std::optional<block::StdAddress> collection_address;
  std::optional<block::StdAddress> owner_address;
  std::optional<std::string> content;
  std::uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  std::optional<block::StdAddress> real_owner;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(address, init, index, collection_address, owner_address, content, last_transaction_lt,
                                code_hash, data_hash, real_owner, destroyed);
};

struct PreparedDnsEntryRow {
  block::StdAddress nft_item_address;
  std::optional<block::StdAddress> nft_item_owner;
  std::string domain;
  std::optional<block::StdAddress> dns_next_resolver;
  std::optional<block::StdAddress> dns_wallet;
  std::optional<td::Bits256> dns_site_adnl;
  std::optional<td::Bits256> dns_storage_bag_id;
  std::uint64_t last_transaction_lt;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(nft_item_address, nft_item_owner, domain, dns_next_resolver, dns_wallet,
                                dns_site_adnl, dns_storage_bag_id, last_transaction_lt, destroyed);
};

struct PreparedGetgemsSaleRow {
  block::StdAddress address;
  bool is_complete;
  std::uint32_t created_at;
  block::StdAddress marketplace_address;
  block::StdAddress nft_address;
  std::optional<block::StdAddress> nft_owner_address;
  td::RefInt256 full_price;
  block::StdAddress marketplace_fee_address;
  td::RefInt256 marketplace_fee;
  block::StdAddress royalty_address;
  td::RefInt256 royalty_amount;
  std::optional<std::uint32_t> sold_at;
  std::optional<std::uint64_t> sold_query_id;
  std::optional<std::string> jetton_price_dict;
  std::uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(address, is_complete, created_at, marketplace_address, nft_address, nft_owner_address,
                                full_price, marketplace_fee_address, marketplace_fee, royalty_address, royalty_amount,
                                sold_at, sold_query_id, jetton_price_dict, last_transaction_lt, code_hash, data_hash,
                                destroyed);
};

struct PreparedGetgemsAuctionRow {
  block::StdAddress address;
  bool end_flag;
  std::uint32_t end_time;
  block::StdAddress mp_addr;
  block::StdAddress nft_addr;
  std::optional<block::StdAddress> nft_owner;
  td::RefInt256 last_bid;
  std::optional<block::StdAddress> last_member;
  std::uint32_t min_step;
  block::StdAddress mp_fee_addr;
  std::uint32_t mp_fee_factor;
  std::uint32_t mp_fee_base;
  block::StdAddress royalty_fee_addr;
  std::uint32_t royalty_fee_factor;
  std::uint32_t royalty_fee_base;
  td::RefInt256 max_bid;
  td::RefInt256 min_bid;
  std::uint32_t created_at;
  std::uint32_t last_bid_at;
  bool is_canceled;
  std::optional<bool> activated;
  std::optional<std::uint32_t> step_time;
  std::optional<std::uint64_t> last_query_id;
  std::optional<block::StdAddress> jetton_wallet;
  std::optional<block::StdAddress> jetton_master;
  std::optional<bool> is_broken_state;
  std::optional<std::string> public_key;
  std::uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(address, end_flag, end_time, mp_addr, nft_addr, nft_owner, last_bid, last_member,
                                min_step, mp_fee_addr, mp_fee_factor, mp_fee_base, royalty_fee_addr, royalty_fee_factor,
                                royalty_fee_base, max_bid, min_bid, created_at, last_bid_at, is_canceled, activated,
                                step_time, last_query_id, jetton_wallet, jetton_master, is_broken_state, public_key,
                                last_transaction_lt, code_hash, data_hash, destroyed);
};

struct PreparedMultisigContractRow {
  block::StdAddress address;
  td::RefInt256 next_order_seqno;
  std::uint32_t threshold;
  std::vector<block::StdAddress> signers;
  std::vector<block::StdAddress> proposers;
  std::uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(address, next_order_seqno, threshold, signers, proposers, last_transaction_lt,
                                code_hash, data_hash, destroyed);
};

struct PreparedMultisigOrderRow {
  block::StdAddress address;
  block::StdAddress multisig_address;
  td::RefInt256 order_seqno;
  std::uint32_t threshold;
  bool sent_for_execution;
  td::RefInt256 approvals_mask;
  std::uint32_t approvals_num;
  td::RefInt256 expiration_date;
  std::optional<std::string> order_boc;
  std::vector<block::StdAddress> signers;
  std::uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(address, multisig_address, order_seqno, threshold, sent_for_execution, approvals_mask,
                                approvals_num, expiration_date, order_boc, signers, last_transaction_lt, code_hash,
                                data_hash, destroyed);
};

struct PreparedDedustPoolRow {
  block::StdAddress address;
  std::optional<block::StdAddress> asset_1;
  std::optional<block::StdAddress> asset_2;
  td::RefInt256 reserve_1;
  td::RefInt256 reserve_2;
  std::string pool_type;
  std::string dex;
  double fee;
  std::uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(address, asset_1, asset_2, reserve_1, reserve_2, pool_type, dex, fee,
                                last_transaction_lt, code_hash, data_hash, destroyed);
};

struct PreparedVestingContractRow {
  block::StdAddress address;
  std::uint32_t vesting_start_time;
  std::uint32_t vesting_total_duration;
  std::uint32_t unlock_period;
  std::uint32_t cliff_duration;
  td::RefInt256 vesting_total_amount;
  block::StdAddress vesting_sender_address;
  block::StdAddress owner_address;
  std::uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(address, vesting_start_time, vesting_total_duration, unlock_period, cliff_duration,
                                vesting_total_amount, vesting_sender_address, owner_address, last_transaction_lt,
                                code_hash, data_hash, destroyed);
};

struct PreparedVestingWhitelistRow {
  block::StdAddress vesting_contract_address;
  block::StdAddress wallet_address;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(vesting_contract_address, wallet_address);
};

struct PreparedNominatorPoolRow {
  block::StdAddress address;
  std::int32_t state;
  std::int32_t nominators_count;
  td::RefInt256 stake_amount_sent;
  td::RefInt256 validator_amount;
  std::int32_t validator_reward_share;
  std::int32_t max_nominators_count;
  td::RefInt256 min_validator_stake;
  td::RefInt256 min_nominator_stake;
  std::string active_nominators;
  std::uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(address, state, nominators_count, stake_amount_sent, validator_amount,
                                validator_reward_share, max_nominators_count, min_validator_stake,
                                min_nominator_stake, active_nominators, last_transaction_lt,
                                code_hash, data_hash, destroyed);
};

struct PreparedTelemintRow {
  block::StdAddress address;
  std::string token_name;
  std::optional<block::StdAddress> bidder_address;
  td::RefInt256 bid;
  std::uint32_t bid_ts;
  td::RefInt256 min_bid;
  std::uint32_t end_time;
  std::optional<block::StdAddress> beneficiary_address;
  td::RefInt256 initial_min_bid;
  td::RefInt256 max_bid;
  td::RefInt256 min_bid_step;
  std::uint32_t min_extend_time;
  std::uint32_t duration;
  int royalty_numerator;
  int royalty_denominator;
  block::StdAddress royalty_destination;
  std::uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(address, token_name, bidder_address, bid, bid_ts, min_bid, end_time,
                                beneficiary_address, initial_min_bid, max_bid, min_bid_step, min_extend_time,
                                duration, royalty_numerator, royalty_denominator, royalty_destination,
                                last_transaction_lt, code_hash, data_hash, destroyed);
};

struct PreparedContractMethodsRow {
  td::Bits256 code_hash;
  std::string methods;
  std::uint32_t source_mc_seqno;

  KVROCKS_PREPARED_ROW_AS_TUPLE(code_hash, methods);
};

#undef KVROCKS_PREPARED_ROW_AS_TUPLE

struct StateBatch {
  std::vector<PreparedMessageContentRow> message_contents;
  std::vector<PreparedAccountStateRow> account_states;
  std::vector<PreparedContractMethodsRow> contract_methods;

  std::vector<PreparedLatestAccountStateRow> latest_account_states;
  std::vector<PreparedJettonMasterRow> jetton_masters;
  std::vector<PreparedJettonWalletRow> jetton_wallets;
  std::vector<PreparedMintlessMasterRow> mintless_jetton_masters;
  std::vector<PreparedNftCollectionRow> nft_collections;
  std::vector<PreparedNftItemRow> nft_items;
  std::vector<PreparedDnsEntryRow> dns_entries;
  std::vector<PreparedGetgemsSaleRow> getgems_nft_sales;
  std::vector<PreparedGetgemsAuctionRow> getgems_nft_auctions;
  std::vector<PreparedMultisigContractRow> multisig_contracts;
  std::vector<PreparedMultisigOrderRow> multisig_orders;
  std::vector<PreparedDedustPoolRow> dedust_pools;
  std::vector<PreparedVestingContractRow> vesting_contracts;
  std::vector<PreparedVestingWhitelistRow> vesting_whitelist;
  std::vector<PreparedNominatorPoolRow> nominator_pools;
  std::vector<PreparedTelemintRow> telemint_nft_items;
};

struct KvrocksIndexEntry {
  std::string key;
  std::string member;
  std::string score;
};

using PointStateData = std::variant<schema::AccountState,
                                    schema::JettonMasterDataV2,
                                    schema::JettonWalletDataV2,
                                    schema::NFTItemDataV2,
                                    schema::NFTCollectionDataV2,
                                    schema::GetGemsNftFixPriceSaleData,
                                    schema::GetGemsNftFixPriceSaleV4Data,
                                    schema::GetGemsNftAuctionData,
                                    schema::MultisigContractData,
                                    schema::MultisigOrderData,
                                    schema::VestingData,
                                    schema::NominatorPoolData,
                                    schema::TelemintData,
                                    schema::DedustPoolData>;

struct LatestAccountStateSourceRow {
  schema::AccountState account_state;
  std::string raw_account;
  std::uint32_t source_mc_seqno;
};

std::string content_to_json_string(const std::map<std::string, std::string>& content);
std::string extra_currencies_to_json_string(const std::map<std::uint32_t, td::RefInt256>& extra_currencies);
std::optional<std::string> serialize_cell_to_base64(td::Ref<vm::Cell> cell);
PreparedLatestAccountStateRow prepare_latest_account_state_row(const LatestAccountStateSourceRow& source,
                                                               std::int32_t max_data_depth);
StateBatch prepare_point_state_batch(const std::vector<PointStateData>& data,
                                     std::uint32_t source_mc_seqno,
                                     std::int32_t max_data_depth);

void load_kvrocks_scripts(sw::redis::Redis& redis);
bool is_kvrocks_no_script_error(const std::exception& e);
void repair_nft_real_owners_with_script_reload(sw::redis::Redis& redis, const StateBatch& batch);

class KvrocksBatchWriter {
public:
  KvrocksBatchWriter(sw::redis::Redis& redis, const StateBatch& batch);

  void write();
  double exec_elapsed_millis() const;

private:
  void queue_set_once(const std::string& table, const std::string& id, std::uint32_t source_mc_seqno,
                      const std::string& payload);
  void queue_set_current(const std::string& table, const std::string& id, std::uint32_t source_mc_seqno,
                         const std::string& payload);
  void queue_set_current_existing(const std::string& table, const std::string& id, std::uint32_t source_mc_seqno,
                                  const std::string& payload);
  void queue_set_indexed_current(const std::string& table, const std::string& id, std::uint32_t source_mc_seqno,
                                 const std::string& payload, const std::vector<KvrocksIndexEntry>& indexes);
  void queue_set_indexed_current_existing(const std::string& table, const std::string& id,
                                          std::uint32_t source_mc_seqno, const std::string& payload,
                                          const std::vector<KvrocksIndexEntry>& indexes);
  void queue_set_indexed_once(const std::string& table, const std::string& id, std::uint32_t source_mc_seqno,
                              const std::string& payload, const std::vector<KvrocksIndexEntry>& indexes);
  void queue_set_indexed(const std::string& script_sha, const std::string& table, const std::string& id,
                         std::uint32_t source_mc_seqno, const std::string& payload,
                         const std::vector<KvrocksIndexEntry>& indexes);
  void flush_if_needed();
  void flush();

  sw::redis::Redis& redis_;
  sw::redis::Pipeline pipeline_;
  const StateBatch& batch_;
  std::size_t pending_{0};
  std::size_t queued_{0};
  double exec_elapsed_millis_{0.0};
};

void write_batch_with_script_reload(sw::redis::Redis& redis, const StateBatch& batch);

}  // namespace kvrocks_state
