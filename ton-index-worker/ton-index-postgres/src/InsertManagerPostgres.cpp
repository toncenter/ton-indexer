#include "td/utils/JsonBuilder.h"
#include "td/utils/crypto.h"
#include "td/utils/misc.h"
#include "InsertManagerPostgres.h"
#include "convert-utils.h"
#include "Statistics.h"
#include "version.h"
#include "postgresql_tools.h"
#include "KvrocksState.h"

#include <algorithm>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string_view>
#include <tuple>
#include <unordered_set>
#include <vector>


class PostgresLeaderHeartbeat: public td::actor::Actor {
public:
  PostgresLeaderHeartbeat(std::string connection_string, std::string worker_id) :
    connection_string_(std::move(connection_string)), worker_id_(std::move(worker_id)) {}

  void hold();
  void release();
  void alarm() override;

private:
  void refresh_heartbeat();
  void schedule_next_refresh();

  std::string connection_string_;
  std::string worker_id_;
  std::size_t active_leader_batches_{0};
};

class LeaderHeartbeatGuard {
public:
  LeaderHeartbeatGuard() = default;
  explicit LeaderHeartbeatGuard(td::actor::ActorId<PostgresLeaderHeartbeat> heartbeat) : heartbeat_(heartbeat) {
    if (!heartbeat_.empty() && heartbeat_.is_alive()) {
      td::actor::send_closure(heartbeat_, &PostgresLeaderHeartbeat::hold);
    }
  }

  LeaderHeartbeatGuard(const LeaderHeartbeatGuard&) = delete;
  LeaderHeartbeatGuard& operator=(const LeaderHeartbeatGuard&) = delete;

  LeaderHeartbeatGuard(LeaderHeartbeatGuard&& other) noexcept : heartbeat_(other.heartbeat_) {
    other.heartbeat_ = {};
  }

  LeaderHeartbeatGuard& operator=(LeaderHeartbeatGuard&& other) noexcept {
    if (this != &other) {
      reset();
      heartbeat_ = other.heartbeat_;
      other.heartbeat_ = {};
    }
    return *this;
  }

  ~LeaderHeartbeatGuard() {
    reset();
  }

  void reset() {
    if (!heartbeat_.empty() && heartbeat_.is_alive()) {
      td::actor::send_closure(heartbeat_, &PostgresLeaderHeartbeat::release);
    }
    heartbeat_ = {};
  }

private:
  td::actor::ActorId<PostgresLeaderHeartbeat> heartbeat_;
};

struct PreparedBatchPostgres;
struct InsertBatchPostgresPrepareState;
struct LatestAccountStatesChunkPreparedResult;

class InsertBatchPostgres: public td::actor::Actor {
public:
  InsertBatchPostgres(InsertManagerPostgres::Credential credential,
                      std::shared_ptr<sw::redis::Redis> kvrocks,
                      td::actor::ActorId<PostgresLeaderHeartbeat> leader_heartbeat,
                      std::string worker_id,
                      std::vector<InsertTaskStruct> insert_tasks,
                      td::Promise<InsertManagerInterface::InsertResult> promise,
                      std::int32_t max_data_depth = 12,
                      std::int32_t latest_states_prepare_parallelism = 4,
                      std::int32_t latest_states_prepare_chunk_size = 128,
                      PartitionManagerConfig partition_config = {},
                      bool no_leader = false,
                      bool disable_progress_advance = false) :
    credential_(std::move(credential)), kvrocks_(std::move(kvrocks)), leader_heartbeat_(leader_heartbeat), worker_id_(std::move(worker_id)),
    insert_tasks_(std::move(insert_tasks)), promise_(std::move(promise)), max_data_depth_(max_data_depth),
    latest_states_prepare_parallelism_(latest_states_prepare_parallelism),
    latest_states_prepare_chunk_size_(latest_states_prepare_chunk_size),
    partition_config_(partition_config),
    no_leader_(no_leader),
    disable_progress_advance_(disable_progress_advance) {
      // sorting in descending seqno order for easier processing of interfaces
      std::sort(insert_tasks_.begin(), insert_tasks_.end(), [](const auto& a, const auto& b) {
        return a.mc_seqno_ > b.mc_seqno_;
      });
  }

  void start_up() override;
  void alarm() override;
private:
  InsertManagerPostgres::Credential credential_;
  std::shared_ptr<sw::redis::Redis> kvrocks_;
  td::actor::ActorId<PostgresLeaderHeartbeat> leader_heartbeat_;
  std::string connection_string_;
  std::string worker_id_;
  std::vector<InsertTaskStruct> insert_tasks_;
  td::Promise<InsertManagerInterface::InsertResult> promise_;
  std::int32_t max_data_depth_;
  std::int32_t latest_states_prepare_parallelism_;
  std::int32_t latest_states_prepare_chunk_size_;
  PartitionManagerConfig partition_config_;
  bool no_leader_;
  bool disable_progress_advance_;
  bool with_copy_{true};
  bool is_leader_{false};
  std::optional<LeaderHeartbeatGuard> leader_heartbeat_guard_;
  std::shared_ptr<PreparedBatchPostgres> prepared_batch_;
  std::shared_ptr<InsertBatchPostgresPrepareState> prepare_state_;
  std::uint64_t prepare_generation_{0};

  std::string stringify(schema::ComputeSkipReason compute_skip_reason);
  std::string stringify(schema::AccStatusChange acc_status_change);
  std::string stringify(schema::AccountStatus account_status);
  std::string stringify(schema::Trace::State state);

  void insert_blocks(pqxx::work &txn, bool with_copy);
  void insert_shard_state(pqxx::work &txn, bool with_copy);
  void insert_transactions(pqxx::work &txn, bool with_copy);
  void insert_messages(pqxx::work &txn, bool with_copy);
  void insert_message_contents(pqxx::work &txn);
  void insert_account_states(pqxx::work &txn, bool with_copy);
  std::string insert_latest_account_states(pqxx::work &txn);
  void insert_jetton_transfers(pqxx::work &txn, bool with_copy);
  void insert_jetton_burns(pqxx::work &txn, bool with_copy);
  void insert_nft_transfers(pqxx::work &txn, bool with_copy);
  void insert_nominator_pool_events(pqxx::work &txn, bool with_copy);
  std::string insert_jetton_masters(pqxx::work &txn);
  std::string insert_jetton_wallets(pqxx::work &txn);
  std::string insert_nft_collections(pqxx::work &txn);
  std::string insert_nft_items(pqxx::work &txn);
  std::string insert_getgems_nft_auctions(pqxx::work &txn);
  std::string insert_getgems_nft_sales(pqxx::work &txn);
  std::string insert_multisig_contracts(pqxx::work &txn);
  std::string insert_multisig_orders(pqxx::work &txn);
  std::string insert_vesting(pqxx::work &txn);
  std::string insert_nominator_pools(pqxx::work &txn);
  std::string insert_telemint(pqxx::work &txn);
  std::string insert_dedust_pools(pqxx::work &txn);
  void insert_contract_methods(pqxx::work &txn);
  void insert_traces(pqxx::work &txn, bool with_copy);
  void insert_kvrocks();

  bool try_acquire_leader_lock(pqxx::connection& c, const std::string& worker_id);
  InsertManagerInterface::InsertResult get_insert_result() const;
  void do_insert();
  void begin_prepare();
  void spawn_next_latest_account_state_chunks();
  void on_latest_account_state_chunk_prepared(std::uint64_t generation, std::size_t chunk_index,
                                              td::Result<std::shared_ptr<LatestAccountStatesChunkPreparedResult>> result);
  void maybe_commit_prepared_batch();
  void commit_prepared_batch();
  void reset_prepare_state(bool keep_prepared_batch = false);
  void acquire_leader_heartbeat();
  void release_leader_heartbeat();
  void ensure_inserted();
  std::optional<std::pair<std::uint32_t, std::uint32_t>> get_batch_seqno_range() const;
  void ensure_batch_partitions(pqxx::connection& c);
  void drop_old_partitions_after_commit(pqxx::connection& c);
};

std::string content_to_json_string(const std::map<std::string, std::string> &content) {
  td::JsonBuilder jetton_content_json;
  auto obj = jetton_content_json.enter_object();
  for (auto &attr : content) {
    auto value = attr.second;
    // We erase all \0 bytes because Postgres can't contain such strings
    value.erase(std::remove(value.begin(), value.end(), '\0'), value.end());
    obj(attr.first, value);
  }
  obj.leave();

  return jetton_content_json.string_builder().as_cslice().str();
}

std::string extra_currencies_to_json_string(const std::map<uint32_t, td::RefInt256> &extra_currencies) {
  td::JsonBuilder extra_currencies_json;
  auto obj = extra_currencies_json.enter_object();
  for (auto &currency : extra_currencies) {
    obj(std::to_string(currency.first), currency.second->to_dec_string());
  }
  obj.leave();

  return extra_currencies_json.string_builder().as_cslice().str();
}

std::string nominators_to_json_string(const std::vector<schema::NominatorPoolNominator>& nominators) {
  td::JsonBuilder nominators_json;
  auto arr = nominators_json.enter_array();
  for (const auto& nominator : nominators) {
    auto obj = arr.enter_value().enter_object();
    obj("address", convert::to_raw_address(nominator.address));
    obj("balance", nominator.balance->to_dec_string());
    obj("pending_balance", nominator.pending_balance->to_dec_string());
    obj.leave();
  }
  arr.leave();

  return nominators_json.string_builder().as_cslice().str();
}

void PostgresLeaderHeartbeat::hold() {
  bool was_empty = active_leader_batches_ == 0;
  ++active_leader_batches_;
  if (was_empty) {
    schedule_next_refresh();
  }
}

void PostgresLeaderHeartbeat::release() {
  if (active_leader_batches_ == 0) {
    LOG(ERROR) << "Postgres leader heartbeat released without an active leader batch for worker " << worker_id_;
    return;
  }
  --active_leader_batches_;
  if (active_leader_batches_ == 0) {
    alarm_timestamp() = td::Timestamp::never();
  }
}

void PostgresLeaderHeartbeat::alarm() {
  if (active_leader_batches_ == 0) {
    schedule_next_refresh();
    return;
  }
  refresh_heartbeat();
  schedule_next_refresh();
}

void PostgresLeaderHeartbeat::refresh_heartbeat() {
  try {
    pqxx::connection c(connection_string_);
    pqxx::work txn(c);
    auto updated = txn.exec(R"(
      UPDATE ton_indexer_leader
      SET last_heartbeat = clock_timestamp()
      WHERE id = 1 AND leader_worker_id = $1
      RETURNING 1;
    )", pqxx::params{worker_id_}).size();
    txn.commit();
    if (updated != 1) {
      LOG(WARNING) << "Postgres leader heartbeat did not update the leader row for worker " << worker_id_;
    }
  } catch (const std::exception& e) {
    LOG(ERROR) << "Error updating Postgres leader heartbeat: " << e.what();
  }
}

void PostgresLeaderHeartbeat::schedule_next_refresh() {
  alarm_timestamp() = active_leader_batches_ == 0 ? td::Timestamp::never() : td::Timestamp::in(5.0);
}

namespace {

template <typename Map, typename KeyFn>
auto get_ordered_map_values(const Map& values, KeyFn key_fn) {
  using Value = typename Map::mapped_type;
  std::vector<std::pair<std::string, const Value*>> ordered;
  ordered.reserve(values.size());
  for (const auto& [key, value] : values) {
    ordered.emplace_back(key_fn(key, value), &value);
  }
  std::sort(ordered.begin(), ordered.end(), [](const auto& lhs, const auto& rhs) {
    return lhs.first < rhs.first;
  });
  return ordered;
}

template <typename Container, typename KeyFn>
auto get_ordered_values(const Container& values, KeyFn key_fn) {
  using Value = typename Container::value_type;
  std::vector<std::pair<std::string, const Value*>> ordered;
  ordered.reserve(values.size());
  for (const auto& value : values) {
    ordered.emplace_back(key_fn(value), &value);
  }
  std::sort(ordered.begin(), ordered.end(), [](const auto& lhs, const auto& rhs) {
    return lhs.first < rhs.first;
  });
  return ordered;
}

void ensure_still_leader(pqxx::work& txn, const std::string& worker_id) {
  auto result = txn.exec(R"(
    WITH ts AS (
      SELECT clock_timestamp() AS checked_at
    )
    UPDATE ton_indexer_leader
    SET last_heartbeat = ts.checked_at
    FROM ts
    WHERE id = 1
      AND leader_worker_id = $1
      AND last_heartbeat >= ts.checked_at - INTERVAL '20 seconds'
    RETURNING 1;
  )", pqxx::params{worker_id});
  if (result.empty()) {
    throw std::runtime_error("Lost Postgres leader lease before commit");
  }
}

}  // namespace

#define PREPARED_ROW_AS_TUPLE(...) \
  auto as_tuple() const { \
    return std::tie(__VA_ARGS__); \
  }

struct PreparedMessageContentRow {
  td::Bits256 hash;
  std::string body;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(hash, body);
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
  uint32_t timestamp;
  td::Bits256 last_trans_hash;
  uint64_t last_trans_lt;
  std::optional<td::Bits256> frozen_hash;
  std::optional<td::Bits256> data_hash;
  std::optional<td::Bits256> code_hash;
  std::optional<std::string> data_boc;
  std::optional<std::string> code_boc;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(account, hash, balance, balance_extra_currencies, account_status,
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
  uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(address, total_supply, mintable, admin_address, jetton_content, jetton_wallet_code_hash,
                        last_transaction_lt, code_hash, data_hash, destroyed);
};

struct PreparedJettonWalletRow {
  td::RefInt256 balance;
  block::StdAddress address;
  block::StdAddress owner;
  block::StdAddress jetton;
  uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  std::optional<bool> mintless_is_claimed;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(balance, address, owner, jetton, last_transaction_lt, code_hash, data_hash,
                        mintless_is_claimed, destroyed);
};

struct PreparedMintlessMasterRow {
  block::StdAddress address;
  bool is_indexed;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(address, is_indexed);
};

struct PreparedNftCollectionRow {
  block::StdAddress address;
  td::RefInt256 next_item_index;
  std::optional<block::StdAddress> owner_address;
  std::optional<std::string> collection_content;
  uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(address, next_item_index, owner_address, collection_content, last_transaction_lt,
                        code_hash, data_hash, destroyed);
};

struct PreparedNftItemRow {
  block::StdAddress address;
  bool init;
  td::RefInt256 index;
  std::optional<block::StdAddress> collection_address;
  std::optional<block::StdAddress> owner_address;
  std::optional<std::string> content;
  uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  std::optional<block::StdAddress> real_owner;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(address, init, index, collection_address, owner_address, content, last_transaction_lt,
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
  uint64_t last_transaction_lt;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(nft_item_address, nft_item_owner, domain, dns_next_resolver, dns_wallet,
                        dns_site_adnl, dns_storage_bag_id, last_transaction_lt, destroyed);
};

struct PreparedGetgemsSaleRow {
  block::StdAddress address;
  bool is_complete;
  uint32_t created_at;
  block::StdAddress marketplace_address;
  block::StdAddress nft_address;
  std::optional<block::StdAddress> nft_owner_address;
  td::RefInt256 full_price;
  block::StdAddress marketplace_fee_address;
  td::RefInt256 marketplace_fee;
  block::StdAddress royalty_address;
  td::RefInt256 royalty_amount;
  std::optional<uint32_t> sold_at;
  std::optional<uint64_t> sold_query_id;
  std::optional<std::string> jetton_price_dict;
  uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(address, is_complete, created_at, marketplace_address, nft_address, nft_owner_address,
                        full_price, marketplace_fee_address, marketplace_fee, royalty_address, royalty_amount,
                        sold_at, sold_query_id, jetton_price_dict, last_transaction_lt, code_hash, data_hash,
                        destroyed);
};

struct PreparedGetgemsAuctionRow {
  block::StdAddress address;
  bool end_flag;
  uint32_t end_time;
  block::StdAddress mp_addr;
  block::StdAddress nft_addr;
  std::optional<block::StdAddress> nft_owner;
  td::RefInt256 last_bid;
  std::optional<block::StdAddress> last_member;
  uint32_t min_step;
  block::StdAddress mp_fee_addr;
  uint32_t mp_fee_factor;
  uint32_t mp_fee_base;
  block::StdAddress royalty_fee_addr;
  uint32_t royalty_fee_factor;
  uint32_t royalty_fee_base;
  td::RefInt256 max_bid;
  td::RefInt256 min_bid;
  uint32_t created_at;
  uint32_t last_bid_at;
  bool is_canceled;
  std::optional<bool> activated;
  std::optional<uint32_t> step_time;
  std::optional<uint64_t> last_query_id;
  std::optional<block::StdAddress> jetton_wallet;
  std::optional<block::StdAddress> jetton_master;
  std::optional<bool> is_broken_state;
  std::optional<std::string> public_key;
  uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(address, end_flag, end_time, mp_addr, nft_addr, nft_owner, last_bid, last_member,
                        min_step, mp_fee_addr, mp_fee_factor, mp_fee_base, royalty_fee_addr, royalty_fee_factor,
                        royalty_fee_base, max_bid, min_bid, created_at, last_bid_at, is_canceled, activated,
                        step_time, last_query_id, jetton_wallet, jetton_master, is_broken_state, public_key,
                        last_transaction_lt, code_hash, data_hash, destroyed);
};

struct PreparedMultisigContractRow {
  block::StdAddress address;
  td::RefInt256 next_order_seqno;
  uint32_t threshold;
  std::vector<block::StdAddress> signers;
  std::vector<block::StdAddress> proposers;
  uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(address, next_order_seqno, threshold, signers, proposers, last_transaction_lt,
                        code_hash, data_hash, destroyed);
};

struct PreparedMultisigOrderRow {
  block::StdAddress address;
  block::StdAddress multisig_address;
  td::RefInt256 order_seqno;
  uint32_t threshold;
  bool sent_for_execution;
  td::RefInt256 approvals_mask;
  uint32_t approvals_num;
  td::RefInt256 expiration_date;
  std::optional<std::string> order_boc;
  std::vector<block::StdAddress> signers;
  uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(address, multisig_address, order_seqno, threshold, sent_for_execution, approvals_mask,
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
  uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(address, asset_1, asset_2, reserve_1, reserve_2, pool_type, dex, fee,
                        last_transaction_lt, code_hash, data_hash, destroyed);
};

struct PreparedVestingContractRow {
  block::StdAddress address;
  uint32_t vesting_start_time;
  uint32_t vesting_total_duration;
  uint32_t unlock_period;
  uint32_t cliff_duration;
  td::RefInt256 vesting_total_amount;
  block::StdAddress vesting_sender_address;
  block::StdAddress owner_address;
  uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(address, vesting_start_time, vesting_total_duration, unlock_period, cliff_duration,
                        vesting_total_amount, vesting_sender_address, owner_address, last_transaction_lt,
                        code_hash, data_hash, destroyed);
};

struct PreparedVestingWhitelistRow {
  block::StdAddress vesting_contract_address;
  block::StdAddress wallet_address;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(vesting_contract_address, wallet_address);
};

struct PreparedNominatorPoolRow {
  block::StdAddress address;
  int32_t state;
  int32_t nominators_count;
  td::RefInt256 stake_amount_sent;
  td::RefInt256 validator_amount;
  block::StdAddress validator_address;
  int32_t validator_reward_share;
  int32_t max_nominators_count;
  td::RefInt256 min_validator_stake;
  td::RefInt256 min_nominator_stake;
  std::string active_nominators;
  uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(address, state, nominators_count, stake_amount_sent, validator_amount, validator_address,
                        validator_reward_share, max_nominators_count, min_validator_stake,
                        min_nominator_stake, active_nominators, last_transaction_lt,
                        code_hash, data_hash, destroyed);
};

struct PreparedTelemintRow {
  block::StdAddress address;
  std::string token_name;
  std::optional<block::StdAddress> bidder_address;
  td::RefInt256 bid;
  uint32_t bid_ts;
  td::RefInt256 min_bid;
  uint32_t end_time;
  std::optional<block::StdAddress> beneficiary_address;
  td::RefInt256 initial_min_bid;
  td::RefInt256 max_bid;
  td::RefInt256 min_bid_step;
  uint32_t min_extend_time;
  uint32_t duration;
  int royalty_numerator;
  int royalty_denominator;
  block::StdAddress royalty_destination;
  uint64_t last_transaction_lt;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  bool destroyed;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(address, token_name, bidder_address, bid, bid_ts, min_bid, end_time,
                        beneficiary_address, initial_min_bid, max_bid, min_bid_step, min_extend_time,
                        duration, royalty_numerator, royalty_denominator, royalty_destination,
                        last_transaction_lt, code_hash, data_hash, destroyed);
};

struct PreparedJettonTransferRow {
  td::Bits256 tx_hash;
  uint64_t tx_lt;
  uint32_t tx_now;
  bool tx_aborted;
  uint32_t mc_seqno;
  uint64_t query_id;
  td::RefInt256 amount;
  std::string source;
  std::string destination;
  std::string jetton_wallet_address;
  std::string jetton_master_address;
  std::string response_destination;
  std::optional<std::string> custom_payload;
  td::RefInt256 forward_ton_amount;
  std::optional<std::string> forward_payload;
  td::Bits256 trace_id;

  PREPARED_ROW_AS_TUPLE(tx_hash, tx_lt, tx_now, tx_aborted, mc_seqno, query_id, amount, source, destination,
                        jetton_wallet_address, jetton_master_address, response_destination, custom_payload,
                        forward_ton_amount, forward_payload, trace_id);
};

struct PreparedJettonBurnRow {
  td::Bits256 tx_hash;
  uint64_t tx_lt;
  uint32_t tx_now;
  bool tx_aborted;
  uint32_t mc_seqno;
  uint64_t query_id;
  std::string owner;
  std::string jetton_wallet_address;
  std::string jetton_master_address;
  td::RefInt256 amount;
  std::string response_destination;
  std::optional<std::string> custom_payload;
  td::Bits256 trace_id;

  PREPARED_ROW_AS_TUPLE(tx_hash, tx_lt, tx_now, tx_aborted, mc_seqno, query_id, owner, jetton_wallet_address,
                        jetton_master_address, amount, response_destination, custom_payload, trace_id);
};

struct PreparedNftTransferRow {
  td::Bits256 tx_hash;
  uint64_t tx_lt;
  uint32_t tx_now;
  bool tx_aborted;
  uint32_t mc_seqno;
  uint64_t query_id;
  block::StdAddress nft_item_address;
  td::RefInt256 nft_item_index;
  std::string nft_collection_address;
  std::string old_owner;
  std::string new_owner;
  std::string response_destination;
  std::optional<std::string> custom_payload;
  td::RefInt256 forward_amount;
  std::optional<std::string> forward_payload;
  td::Bits256 trace_id;

  PREPARED_ROW_AS_TUPLE(tx_hash, tx_lt, tx_now, tx_aborted, mc_seqno, query_id, nft_item_address,
                        nft_item_index, nft_collection_address, old_owner, new_owner, response_destination,
                        custom_payload, forward_amount, forward_payload, trace_id);
};

struct PreparedTraceRow {
  td::Bits256 trace_id;
  std::optional<td::Bits256> external_hash;
  std::optional<td::Bits256> external_hash_norm;
  std::int32_t mc_seqno_start;
  std::int32_t mc_seqno_end;
  std::uint64_t start_lt;
  std::uint32_t start_utime;
  std::uint64_t end_lt;
  std::uint32_t end_utime;
  std::string state;
  std::int64_t pending_edges;
  std::int64_t edges;
  std::int64_t nodes;

  PREPARED_ROW_AS_TUPLE(trace_id, external_hash, external_hash_norm, mc_seqno_start, mc_seqno_end, start_lt,
                        start_utime, end_lt, end_utime, state, pending_edges, edges, nodes);
};

struct PreparedContractMethodsRow {
  td::Bits256 code_hash;
  std::string methods;
  std::uint32_t source_mc_seqno;

  PREPARED_ROW_AS_TUPLE(code_hash, methods);
};

#undef PREPARED_ROW_AS_TUPLE

struct LatestAccountStateSourceRow {
  schema::AccountState account_state;
  std::string raw_account;
  std::uint32_t source_mc_seqno;
};

struct LatestAccountStatesChunkPreparedResult {
  std::vector<PreparedLatestAccountStateRow> rows;
};

struct PreparedBatchPostgres {
  std::vector<PreparedMessageContentRow> message_contents;
  std::vector<PreparedAccountStateRow> account_states;

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

  std::vector<PreparedJettonTransferRow> jetton_transfers;
  std::vector<PreparedJettonBurnRow> jetton_burns;
  std::vector<PreparedNftTransferRow> nft_transfers;

  std::vector<PreparedTraceRow> traces;
  std::vector<PreparedContractMethodsRow> contract_methods;
};

struct InsertBatchPostgresPrepareState {
  std::vector<LatestAccountStateSourceRow> latest_account_state_sources;
  std::vector<std::vector<PreparedLatestAccountStateRow>> latest_account_state_chunks;
  std::size_t latest_state_total_chunks{0};
  std::size_t latest_state_next_chunk_to_spawn{0};
  std::size_t latest_state_in_flight{0};
  std::size_t latest_state_completed{0};
};

namespace {

kvrocks_state::StateBatch make_kvrocks_state_batch(const PreparedBatchPostgres& batch) {
  kvrocks_state::StateBatch result;

  result.message_contents.reserve(batch.message_contents.size());
  for (const auto& row : batch.message_contents) {
    result.message_contents.push_back({
      .hash = row.hash,
      .body = row.body,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.account_states.reserve(batch.account_states.size());
  for (const auto& row : batch.account_states) {
    result.account_states.push_back({
      .hash = row.hash,
      .account = row.account,
      .balance = row.balance,
      .balance_extra_currencies = row.balance_extra_currencies,
      .account_status = row.account_status,
      .frozen_hash = row.frozen_hash,
      .code_hash = row.code_hash,
      .data_hash = row.data_hash,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.latest_account_states.reserve(batch.latest_account_states.size());
  for (const auto& row : batch.latest_account_states) {
    result.latest_account_states.push_back({
      .account = row.account,
      .hash = row.hash,
      .balance = row.balance,
      .balance_extra_currencies = row.balance_extra_currencies,
      .account_status = row.account_status,
      .timestamp = row.timestamp,
      .last_trans_hash = row.last_trans_hash,
      .last_trans_lt = row.last_trans_lt,
      .frozen_hash = row.frozen_hash,
      .data_hash = row.data_hash,
      .code_hash = row.code_hash,
      .data_boc = row.data_boc,
      .code_boc = row.code_boc,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.jetton_masters.reserve(batch.jetton_masters.size());
  for (const auto& row : batch.jetton_masters) {
    result.jetton_masters.push_back({
      .address = row.address,
      .total_supply = row.total_supply,
      .mintable = row.mintable,
      .admin_address = row.admin_address,
      .jetton_content = row.jetton_content,
      .jetton_wallet_code_hash = row.jetton_wallet_code_hash,
      .last_transaction_lt = row.last_transaction_lt,
      .code_hash = row.code_hash,
      .data_hash = row.data_hash,
      .destroyed = row.destroyed,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.jetton_wallets.reserve(batch.jetton_wallets.size());
  for (const auto& row : batch.jetton_wallets) {
    result.jetton_wallets.push_back({
      .balance = row.balance,
      .address = row.address,
      .owner = row.owner,
      .jetton = row.jetton,
      .last_transaction_lt = row.last_transaction_lt,
      .code_hash = row.code_hash,
      .data_hash = row.data_hash,
      .mintless_is_claimed = row.mintless_is_claimed,
      .destroyed = row.destroyed,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.mintless_jetton_masters.reserve(batch.mintless_jetton_masters.size());
  for (const auto& row : batch.mintless_jetton_masters) {
    result.mintless_jetton_masters.push_back({
      .address = row.address,
      .is_indexed = row.is_indexed,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.nft_collections.reserve(batch.nft_collections.size());
  for (const auto& row : batch.nft_collections) {
    result.nft_collections.push_back({
      .address = row.address,
      .next_item_index = row.next_item_index,
      .owner_address = row.owner_address,
      .collection_content = row.collection_content,
      .last_transaction_lt = row.last_transaction_lt,
      .code_hash = row.code_hash,
      .data_hash = row.data_hash,
      .destroyed = row.destroyed,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.nft_items.reserve(batch.nft_items.size());
  for (const auto& row : batch.nft_items) {
    result.nft_items.push_back({
      .address = row.address,
      .init = row.init,
      .index = row.index,
      .collection_address = row.collection_address,
      .owner_address = row.owner_address,
      .content = row.content,
      .last_transaction_lt = row.last_transaction_lt,
      .code_hash = row.code_hash,
      .data_hash = row.data_hash,
      .real_owner = row.real_owner,
      .destroyed = row.destroyed,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.dns_entries.reserve(batch.dns_entries.size());
  for (const auto& row : batch.dns_entries) {
    result.dns_entries.push_back({
      .nft_item_address = row.nft_item_address,
      .nft_item_owner = row.nft_item_owner,
      .domain = row.domain,
      .dns_next_resolver = row.dns_next_resolver,
      .dns_wallet = row.dns_wallet,
      .dns_site_adnl = row.dns_site_adnl,
      .dns_storage_bag_id = row.dns_storage_bag_id,
      .last_transaction_lt = row.last_transaction_lt,
      .destroyed = row.destroyed,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.getgems_nft_sales.reserve(batch.getgems_nft_sales.size());
  for (const auto& row : batch.getgems_nft_sales) {
    result.getgems_nft_sales.push_back({
      .address = row.address,
      .is_complete = row.is_complete,
      .created_at = row.created_at,
      .marketplace_address = row.marketplace_address,
      .nft_address = row.nft_address,
      .nft_owner_address = row.nft_owner_address,
      .full_price = row.full_price,
      .marketplace_fee_address = row.marketplace_fee_address,
      .marketplace_fee = row.marketplace_fee,
      .royalty_address = row.royalty_address,
      .royalty_amount = row.royalty_amount,
      .sold_at = row.sold_at,
      .sold_query_id = row.sold_query_id,
      .jetton_price_dict = row.jetton_price_dict,
      .last_transaction_lt = row.last_transaction_lt,
      .code_hash = row.code_hash,
      .data_hash = row.data_hash,
      .destroyed = row.destroyed,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.getgems_nft_auctions.reserve(batch.getgems_nft_auctions.size());
  for (const auto& row : batch.getgems_nft_auctions) {
    result.getgems_nft_auctions.push_back({
      .address = row.address,
      .end_flag = row.end_flag,
      .end_time = row.end_time,
      .mp_addr = row.mp_addr,
      .nft_addr = row.nft_addr,
      .nft_owner = row.nft_owner,
      .last_bid = row.last_bid,
      .last_member = row.last_member,
      .min_step = row.min_step,
      .mp_fee_addr = row.mp_fee_addr,
      .mp_fee_factor = row.mp_fee_factor,
      .mp_fee_base = row.mp_fee_base,
      .royalty_fee_addr = row.royalty_fee_addr,
      .royalty_fee_factor = row.royalty_fee_factor,
      .royalty_fee_base = row.royalty_fee_base,
      .max_bid = row.max_bid,
      .min_bid = row.min_bid,
      .created_at = row.created_at,
      .last_bid_at = row.last_bid_at,
      .is_canceled = row.is_canceled,
      .activated = row.activated,
      .step_time = row.step_time,
      .last_query_id = row.last_query_id,
      .jetton_wallet = row.jetton_wallet,
      .jetton_master = row.jetton_master,
      .is_broken_state = row.is_broken_state,
      .public_key = row.public_key,
      .last_transaction_lt = row.last_transaction_lt,
      .code_hash = row.code_hash,
      .data_hash = row.data_hash,
      .destroyed = row.destroyed,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.multisig_contracts.reserve(batch.multisig_contracts.size());
  for (const auto& row : batch.multisig_contracts) {
    result.multisig_contracts.push_back({
      .address = row.address,
      .next_order_seqno = row.next_order_seqno,
      .threshold = row.threshold,
      .signers = row.signers,
      .proposers = row.proposers,
      .last_transaction_lt = row.last_transaction_lt,
      .code_hash = row.code_hash,
      .data_hash = row.data_hash,
      .destroyed = row.destroyed,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.multisig_orders.reserve(batch.multisig_orders.size());
  for (const auto& row : batch.multisig_orders) {
    result.multisig_orders.push_back({
      .address = row.address,
      .multisig_address = row.multisig_address,
      .order_seqno = row.order_seqno,
      .threshold = row.threshold,
      .sent_for_execution = row.sent_for_execution,
      .approvals_mask = row.approvals_mask,
      .approvals_num = row.approvals_num,
      .expiration_date = row.expiration_date,
      .order_boc = row.order_boc,
      .signers = row.signers,
      .last_transaction_lt = row.last_transaction_lt,
      .code_hash = row.code_hash,
      .data_hash = row.data_hash,
      .destroyed = row.destroyed,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.dedust_pools.reserve(batch.dedust_pools.size());
  for (const auto& row : batch.dedust_pools) {
    result.dedust_pools.push_back({
      .address = row.address,
      .asset_1 = row.asset_1,
      .asset_2 = row.asset_2,
      .reserve_1 = row.reserve_1,
      .reserve_2 = row.reserve_2,
      .pool_type = row.pool_type,
      .dex = row.dex,
      .fee = row.fee,
      .last_transaction_lt = row.last_transaction_lt,
      .code_hash = row.code_hash,
      .data_hash = row.data_hash,
      .destroyed = row.destroyed,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.vesting_contracts.reserve(batch.vesting_contracts.size());
  for (const auto& row : batch.vesting_contracts) {
    result.vesting_contracts.push_back({
      .address = row.address,
      .vesting_start_time = row.vesting_start_time,
      .vesting_total_duration = row.vesting_total_duration,
      .unlock_period = row.unlock_period,
      .cliff_duration = row.cliff_duration,
      .vesting_total_amount = row.vesting_total_amount,
      .vesting_sender_address = row.vesting_sender_address,
      .owner_address = row.owner_address,
      .last_transaction_lt = row.last_transaction_lt,
      .code_hash = row.code_hash,
      .data_hash = row.data_hash,
      .destroyed = row.destroyed,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.vesting_whitelist.reserve(batch.vesting_whitelist.size());
  for (const auto& row : batch.vesting_whitelist) {
    result.vesting_whitelist.push_back({
      .vesting_contract_address = row.vesting_contract_address,
      .wallet_address = row.wallet_address,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.nominator_pools.reserve(batch.nominator_pools.size());
  for (const auto& row : batch.nominator_pools) {
    result.nominator_pools.push_back({
      .address = row.address,
      .state = row.state,
      .nominators_count = row.nominators_count,
      .stake_amount_sent = row.stake_amount_sent,
      .validator_amount = row.validator_amount,
      .validator_address = row.validator_address,
      .validator_reward_share = row.validator_reward_share,
      .max_nominators_count = row.max_nominators_count,
      .min_validator_stake = row.min_validator_stake,
      .min_nominator_stake = row.min_nominator_stake,
      .active_nominators = row.active_nominators,
      .last_transaction_lt = row.last_transaction_lt,
      .code_hash = row.code_hash,
      .data_hash = row.data_hash,
      .destroyed = row.destroyed,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.telemint_nft_items.reserve(batch.telemint_nft_items.size());
  for (const auto& row : batch.telemint_nft_items) {
    result.telemint_nft_items.push_back({
      .address = row.address,
      .token_name = row.token_name,
      .bidder_address = row.bidder_address,
      .bid = row.bid,
      .bid_ts = row.bid_ts,
      .min_bid = row.min_bid,
      .end_time = row.end_time,
      .beneficiary_address = row.beneficiary_address,
      .initial_min_bid = row.initial_min_bid,
      .max_bid = row.max_bid,
      .min_bid_step = row.min_bid_step,
      .min_extend_time = row.min_extend_time,
      .duration = row.duration,
      .royalty_numerator = row.royalty_numerator,
      .royalty_denominator = row.royalty_denominator,
      .royalty_destination = row.royalty_destination,
      .last_transaction_lt = row.last_transaction_lt,
      .code_hash = row.code_hash,
      .data_hash = row.data_hash,
      .destroyed = row.destroyed,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  result.contract_methods.reserve(batch.contract_methods.size());
  for (const auto& row : batch.contract_methods) {
    result.contract_methods.push_back({
      .code_hash = row.code_hash,
      .methods = row.methods,
      .source_mc_seqno = row.source_mc_seqno,
    });
  }

  return result;
}

std::string to_bits256_base64(const td::Bits256& value) {
  return td::base64_encode(value.as_slice());
}

std::optional<std::string> serialize_cell_to_base64(td::Ref<vm::Cell> cell) {
  auto res = vm::std_boc_serialize(cell);
  if (res.is_error()) {
    return std::nullopt;
  }
  return td::base64_encode(res.move_as_ok());
}

struct DestroyedAccountState {
  block::StdAddress address;
  std::uint64_t last_transaction_lt;
  std::uint32_t source_mc_seqno;
};

td::RefInt256 zero_refint() {
  return td::make_refint(0);
}

td::Bits256 zero_bits256() {
  return {};
}

block::StdAddress zero_masterchain_address() {
  block::StdAddress address;
  address.workchain = -1;
  address.addr.set_zero();
  return address;
}

PreparedJettonMasterRow make_destroyed_jetton_master_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .total_supply = zero_refint(),
    .mintable = false,
    .admin_address = std::nullopt,
    .jetton_content = std::nullopt,
    .jetton_wallet_code_hash = zero_bits256(),
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedJettonWalletRow make_destroyed_jetton_wallet_row(const DestroyedAccountState& state) {
  return {
    .balance = zero_refint(),
    .address = state.address,
    .owner = state.address,
    .jetton = state.address,
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .mintless_is_claimed = std::nullopt,
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedNftCollectionRow make_destroyed_nft_collection_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .next_item_index = zero_refint(),
    .owner_address = std::nullopt,
    .collection_content = std::nullopt,
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedNftItemRow make_destroyed_nft_item_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .init = false,
    .index = zero_refint(),
    .collection_address = std::nullopt,
    .owner_address = std::nullopt,
    .content = std::nullopt,
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .real_owner = std::nullopt,
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedDnsEntryRow make_destroyed_dns_entry_row(const DestroyedAccountState& state) {
  return {
    .nft_item_address = state.address,
    .nft_item_owner = std::nullopt,
    .domain = "",
    .dns_next_resolver = std::nullopt,
    .dns_wallet = std::nullopt,
    .dns_site_adnl = std::nullopt,
    .dns_storage_bag_id = std::nullopt,
    .last_transaction_lt = state.last_transaction_lt,
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedGetgemsSaleRow make_destroyed_getgems_sale_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .is_complete = false,
    .created_at = 0,
    .marketplace_address = state.address,
    .nft_address = state.address,
    .nft_owner_address = std::nullopt,
    .full_price = zero_refint(),
    .marketplace_fee_address = state.address,
    .marketplace_fee = zero_refint(),
    .royalty_address = state.address,
    .royalty_amount = zero_refint(),
    .sold_at = std::nullopt,
    .sold_query_id = std::nullopt,
    .jetton_price_dict = std::nullopt,
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedGetgemsAuctionRow make_destroyed_getgems_auction_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .end_flag = false,
    .end_time = 0,
    .mp_addr = state.address,
    .nft_addr = state.address,
    .nft_owner = std::nullopt,
    .last_bid = zero_refint(),
    .last_member = std::nullopt,
    .min_step = 0,
    .mp_fee_addr = state.address,
    .mp_fee_factor = 0,
    .mp_fee_base = 0,
    .royalty_fee_addr = state.address,
    .royalty_fee_factor = 0,
    .royalty_fee_base = 0,
    .max_bid = zero_refint(),
    .min_bid = zero_refint(),
    .created_at = 0,
    .last_bid_at = 0,
    .is_canceled = false,
    .activated = std::nullopt,
    .step_time = std::nullopt,
    .last_query_id = std::nullopt,
    .jetton_wallet = std::nullopt,
    .jetton_master = std::nullopt,
    .is_broken_state = std::nullopt,
    .public_key = std::nullopt,
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedMultisigContractRow make_destroyed_multisig_contract_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .next_order_seqno = zero_refint(),
    .threshold = 0,
    .signers = {},
    .proposers = {},
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedMultisigOrderRow make_destroyed_multisig_order_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .multisig_address = state.address,
    .order_seqno = zero_refint(),
    .threshold = 0,
    .sent_for_execution = false,
    .approvals_mask = zero_refint(),
    .approvals_num = 0,
    .expiration_date = zero_refint(),
    .order_boc = std::nullopt,
    .signers = {},
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedDedustPoolRow make_destroyed_dedust_pool_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .asset_1 = std::nullopt,
    .asset_2 = std::nullopt,
    .reserve_1 = zero_refint(),
    .reserve_2 = zero_refint(),
    .pool_type = "volatile",
    .dex = "dedust",
    .fee = 0.0,
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedVestingContractRow make_destroyed_vesting_contract_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .vesting_start_time = 0,
    .vesting_total_duration = 0,
    .unlock_period = 0,
    .cliff_duration = 0,
    .vesting_total_amount = zero_refint(),
    .vesting_sender_address = state.address,
    .owner_address = state.address,
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedNominatorPoolRow make_destroyed_nominator_pool_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .state = 0,
    .nominators_count = 0,
    .stake_amount_sent = zero_refint(),
    .validator_amount = zero_refint(),
    .validator_address = zero_masterchain_address(),
    .validator_reward_share = 0,
    .max_nominators_count = 0,
    .min_validator_stake = zero_refint(),
    .min_nominator_stake = zero_refint(),
    .active_nominators = "[]",
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedTelemintRow make_destroyed_telemint_row(const DestroyedAccountState& state) {
  return {
    .address = state.address,
    .token_name = "",
    .bidder_address = std::nullopt,
    .bid = zero_refint(),
    .bid_ts = 0,
    .min_bid = zero_refint(),
    .end_time = 0,
    .beneficiary_address = std::nullopt,
    .initial_min_bid = zero_refint(),
    .max_bid = zero_refint(),
    .min_bid_step = zero_refint(),
    .min_extend_time = 0,
    .duration = 0,
    .royalty_numerator = 0,
    .royalty_denominator = 0,
    .royalty_destination = state.address,
    .last_transaction_lt = state.last_transaction_lt,
    .code_hash = zero_bits256(),
    .data_hash = zero_bits256(),
    .destroyed = true,
    .source_mc_seqno = state.source_mc_seqno,
  };
}

PreparedLatestAccountStateRow prepare_latest_account_state_row(const LatestAccountStateSourceRow& source, std::int32_t max_data_depth) {
  const auto& account_state = source.account_state;
  std::optional<std::string> code_str = std::nullopt;
  std::optional<std::string> data_str = std::nullopt;

  if (max_data_depth >= 0 && account_state.data.not_null() && (max_data_depth == 0 || account_state.data->get_depth() <= max_data_depth)) {
    data_str = serialize_cell_to_base64(account_state.data);
  } else if (account_state.data.not_null()) {
    LOG(DEBUG) << "Large account data: " << account_state.account
               << " Depth: " << account_state.data->get_depth();
  }

  code_str = serialize_cell_to_base64(account_state.code);
  if (code_str && code_str->length() > 128000) {
    LOG(WARNING) << "Large account code: " << account_state.account;
  }

  return PreparedLatestAccountStateRow{
    .account = account_state.account,
    .hash = account_state.hash,
    .balance = account_state.balance.grams,
    .balance_extra_currencies = extra_currencies_to_json_string(account_state.balance.extra_currencies),
    .account_status = account_state.account_status,
    .timestamp = account_state.timestamp,
    .last_trans_hash = account_state.last_trans_hash,
    .last_trans_lt = account_state.last_trans_lt,
    .frozen_hash = account_state.frozen_hash,
    .data_hash = account_state.data_hash,
    .code_hash = account_state.code_hash,
    .data_boc = std::move(data_str),
    .code_boc = std::move(code_str),
    .source_mc_seqno = source.source_mc_seqno,
  };
}

class PrepareLatestAccountStatesChunkActor final : public td::actor::Actor {
 public:
  PrepareLatestAccountStatesChunkActor(std::vector<LatestAccountStateSourceRow> rows, std::int32_t max_data_depth,
                                       td::Promise<std::shared_ptr<LatestAccountStatesChunkPreparedResult>> promise)
      : rows_(std::move(rows)), max_data_depth_(max_data_depth), promise_(std::move(promise)) {
  }

  void start_up() override {
    try {
      auto result = std::make_shared<LatestAccountStatesChunkPreparedResult>();
      result->rows.reserve(rows_.size());
      for (const auto& row : rows_) {
        result->rows.push_back(prepare_latest_account_state_row(row, max_data_depth_));
      }
      promise_.set_result(std::move(result));
    } catch (const std::exception& e) {
      promise_.set_error(td::Status::Error(td::Slice(e.what())));
    }
    stop();
  }

 private:
  std::vector<LatestAccountStateSourceRow> rows_;
  std::int32_t max_data_depth_;
  td::Promise<std::shared_ptr<LatestAccountStatesChunkPreparedResult>> promise_;
};

void collect_and_prepare_batch_rows(const std::vector<InsertTaskStruct>& insert_tasks, PreparedBatchPostgres& prepared_batch,
                                    std::vector<LatestAccountStateSourceRow>& latest_account_state_sources) {
  std::unordered_map<block::StdAddress, DestroyedAccountState> destroyed_accounts;
  for (const auto& task : insert_tasks) {
    for (const auto& account_state : task.parsed_block_->account_states_) {
      if (account_state.account_status != "nonexist") {
        continue;
      }
      auto it = destroyed_accounts.find(account_state.account);
      if (it == destroyed_accounts.end() || it->second.last_transaction_lt < account_state.last_trans_lt) {
        destroyed_accounts[account_state.account] = DestroyedAccountState{
          .address = account_state.account,
          .last_transaction_lt = account_state.last_trans_lt,
          .source_mc_seqno = task.mc_seqno_,
        };
      }
    }
  }
  auto ordered_destroyed_accounts = get_ordered_map_values(destroyed_accounts, [](const auto& key, const auto&) {
    return convert::to_raw_address(key);
  });

  {
    struct MessageContentValue {
      std::string body;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<td::Bits256, MessageContentValue> unique_message_contents;
    for (const auto& task : insert_tasks) {
      for (const auto& blk : task.parsed_block_->blocks_) {
        for (const auto& transaction : blk.transactions) {
          if (transaction.in_msg) {
            unique_message_contents.emplace(transaction.in_msg->body->get_hash().bits(), MessageContentValue{
              .body = transaction.in_msg->body_boc,
              .source_mc_seqno = task.mc_seqno_,
            });
            if (transaction.in_msg->init_state_boc) {
              unique_message_contents.emplace(transaction.in_msg->init_state->get_hash().bits(), MessageContentValue{
                .body = transaction.in_msg->init_state_boc.value(),
                .source_mc_seqno = task.mc_seqno_,
              });
            }
          }
          for (const auto& msg : transaction.out_msgs) {
            unique_message_contents.emplace(msg.body->get_hash().bits(), MessageContentValue{
              .body = msg.body_boc,
              .source_mc_seqno = task.mc_seqno_,
            });
            if (msg.init_state_boc) {
              unique_message_contents.emplace(msg.init_state->get_hash().bits(), MessageContentValue{
                .body = msg.init_state_boc.value(),
                .source_mc_seqno = task.mc_seqno_,
              });
            }
          }
        }
      }
    }
    std::vector<std::pair<std::string, const std::pair<const td::Bits256, MessageContentValue>*>> ordered_message_contents;
    ordered_message_contents.reserve(unique_message_contents.size());
    for (const auto& entry : unique_message_contents) {
      ordered_message_contents.emplace_back(to_bits256_base64(entry.first), &entry);
    }
    std::sort(ordered_message_contents.begin(), ordered_message_contents.end(), [](const auto& lhs, const auto& rhs) {
      return lhs.first < rhs.first;
    });
    prepared_batch.message_contents.reserve(ordered_message_contents.size());
    for (const auto& [_, entry_ptr] : ordered_message_contents) {
      prepared_batch.message_contents.push_back(PreparedMessageContentRow{
        .hash = entry_ptr->first,
        .body = entry_ptr->second.body,
        .source_mc_seqno = entry_ptr->second.source_mc_seqno,
      });
    }
  }

  {
    for (const auto& task : insert_tasks) {
      for (const auto& account_state : task.parsed_block_->account_states_) {
        if (account_state.account_status == "nonexist") {
          continue;
        }
        prepared_batch.account_states.push_back(PreparedAccountStateRow{
          .hash = account_state.hash,
          .account = account_state.account,
          .balance = account_state.balance.grams,
          .balance_extra_currencies = extra_currencies_to_json_string(account_state.balance.extra_currencies),
          .account_status = account_state.account_status,
          .frozen_hash = account_state.frozen_hash,
          .code_hash = account_state.code_hash,
          .data_hash = account_state.data_hash,
          .source_mc_seqno = task.mc_seqno_,
        });
      }
    }
  }

  for (const auto& task : insert_tasks) {
    for (const auto& transfer : task.parsed_block_->get_events<schema::JettonTransfer>()) {
      auto custom_payload_boc_r = convert::to_bytes(transfer.custom_payload);
      auto custom_payload_boc = custom_payload_boc_r.is_ok() ? custom_payload_boc_r.move_as_ok() : std::nullopt;

      auto forward_payload_boc_r = convert::to_bytes(transfer.forward_payload);
      auto forward_payload_boc = forward_payload_boc_r.is_ok() ? forward_payload_boc_r.move_as_ok() : std::nullopt;

      prepared_batch.jetton_transfers.push_back(PreparedJettonTransferRow{
        .tx_hash = transfer.transaction_hash,
        .tx_lt = transfer.transaction_lt,
        .tx_now = transfer.transaction_now,
        .tx_aborted = transfer.transaction_aborted,
        .mc_seqno = transfer.mc_seqno,
        .query_id = transfer.query_id,
        .amount = transfer.amount,
        .source = transfer.source,
        .destination = transfer.destination,
        .jetton_wallet_address = transfer.jetton_wallet,
        .jetton_master_address = transfer.jetton_master,
        .response_destination = transfer.response_destination,
        .custom_payload = std::move(custom_payload_boc),
        .forward_ton_amount = transfer.forward_ton_amount,
        .forward_payload = std::move(forward_payload_boc),
        .trace_id = transfer.trace_id,
      });
    }

    for (const auto& burn : task.parsed_block_->get_events<schema::JettonBurn>()) {
      auto custom_payload_boc_r = convert::to_bytes(burn.custom_payload);
      auto custom_payload_boc = custom_payload_boc_r.is_ok() ? custom_payload_boc_r.move_as_ok() : std::nullopt;

      prepared_batch.jetton_burns.push_back(PreparedJettonBurnRow{
        .tx_hash = burn.transaction_hash,
        .tx_lt = burn.transaction_lt,
        .tx_now = burn.transaction_now,
        .tx_aborted = burn.transaction_aborted,
        .mc_seqno = burn.mc_seqno,
        .query_id = burn.query_id,
        .owner = burn.owner,
        .jetton_wallet_address = burn.jetton_wallet,
        .jetton_master_address = burn.jetton_master,
        .amount = burn.amount,
        .response_destination = burn.response_destination,
        .custom_payload = std::move(custom_payload_boc),
        .trace_id = burn.trace_id,
      });
    }

    for (const auto& transfer : task.parsed_block_->get_events<schema::NFTTransfer>()) {
      auto custom_payload_boc_r = convert::to_bytes(transfer.custom_payload);
      auto custom_payload_boc = custom_payload_boc_r.is_ok() ? custom_payload_boc_r.move_as_ok() : std::nullopt;

      auto forward_payload_boc_r = convert::to_bytes(transfer.forward_payload);
      auto forward_payload_boc = forward_payload_boc_r.is_ok() ? forward_payload_boc_r.move_as_ok() : std::nullopt;

      prepared_batch.nft_transfers.push_back(PreparedNftTransferRow{
        .tx_hash = transfer.transaction_hash,
        .tx_lt = transfer.transaction_lt,
        .tx_now = transfer.transaction_now,
        .tx_aborted = transfer.transaction_aborted,
        .mc_seqno = transfer.mc_seqno,
        .query_id = transfer.query_id,
        .nft_item_address = transfer.nft_item,
        .nft_item_index = transfer.nft_item_index,
        .nft_collection_address = transfer.nft_collection,
        .old_owner = transfer.old_owner,
        .new_owner = transfer.new_owner,
        .response_destination = transfer.response_destination,
        .custom_payload = std::move(custom_payload_boc),
        .forward_amount = transfer.forward_amount,
        .forward_payload = std::move(forward_payload_boc),
        .trace_id = transfer.trace_id,
      });
    }
  }

  {
    std::unordered_map<td::Bits256, schema::Trace> traces_map;
    for (const auto& task : insert_tasks) {
      for (auto& trace : task.parsed_block_->traces_) {
        if (trace.state == schema::Trace::State::complete) {
          auto it = traces_map.find(trace.trace_id);
          if (it != traces_map.end() && it->second.end_lt < trace.end_lt) {
            it->second = trace;
          } else {
            traces_map.insert({trace.trace_id, trace});
          }
        }
      }
    }
    auto ordered_traces = get_ordered_map_values(traces_map, [](const auto& key, const auto&) {
      return to_bits256_base64(key);
    });
    prepared_batch.traces.reserve(ordered_traces.size());
    for (const auto& [_, trace_ptr] : ordered_traces) {
      const auto& trace = *trace_ptr;
      std::string state;
      switch (trace.state) {
        case schema::Trace::State::complete: state = "complete"; break;
        case schema::Trace::State::pending: state = "pending"; break;
        case schema::Trace::State::broken: state = "broken"; break;
      }
      prepared_batch.traces.push_back(PreparedTraceRow{
        .trace_id = trace.trace_id,
        .external_hash = trace.external_hash,
        .external_hash_norm = (trace.external_hash_norm.has_value() && trace.external_hash_norm != trace.external_hash)
          ? trace.external_hash_norm
          : std::nullopt,
        .mc_seqno_start = trace.mc_seqno_start,
        .mc_seqno_end = trace.mc_seqno_end,
        .start_lt = trace.start_lt,
        .start_utime = trace.start_utime,
        .end_lt = trace.end_lt,
        .end_utime = trace.end_utime,
        .state = std::move(state),
        .pending_edges = trace.pending_edges_,
        .edges = trace.edges_,
        .nodes = trace.nodes_,
      });
    }
  }

  {
    std::unordered_multimap<td::Bits256, uint64_t> contract_methods;
    std::unordered_set<td::Bits256> unique_code_hashes;
    std::unordered_map<td::Bits256, std::uint32_t> contract_method_sources;
    for (auto i = insert_tasks.rbegin(); i != insert_tasks.rend(); ++i) {
      const auto& task = *i;
      for (auto it = task.parsed_block_->contract_methods_.begin(); it != task.parsed_block_->contract_methods_.end();) {
        const auto& code_hash = it->first;
        if (unique_code_hashes.find(code_hash) != unique_code_hashes.end()) {
          ++it;
          continue;
        }
        unique_code_hashes.insert(code_hash);
        contract_method_sources[code_hash] = task.mc_seqno_;

        auto range = task.parsed_block_->contract_methods_.equal_range(code_hash);
        for (auto vit = range.first; vit != range.second; ++vit) {
          contract_methods.emplace(code_hash, vit->second);
        }

        it = range.second;
      }
    }

    auto ordered_code_hashes = get_ordered_values(unique_code_hashes, [](const auto& code_hash) {
      return to_bits256_base64(code_hash);
    });
    prepared_batch.contract_methods.reserve(ordered_code_hashes.size());
    for (const auto& [_, code_hash_ptr] : ordered_code_hashes) {
      const auto& code_hash = *code_hash_ptr;
      std::vector<uint64_t> ordered_methods;
      auto range = contract_methods.equal_range(code_hash);
      for (auto it = range.first; it != range.second; ++it) {
        ordered_methods.push_back(it->second);
      }
      std::sort(ordered_methods.begin(), ordered_methods.end());

      std::ostringstream methods_str;
      methods_str << "{";
      bool first = true;
      for (const auto method : ordered_methods) {
        if (!first) {
          methods_str << ", ";
        }
        methods_str << method;
        first = false;
      }
      methods_str << "}";
      prepared_batch.contract_methods.push_back(PreparedContractMethodsRow{
        .code_hash = code_hash,
        .methods = methods_str.str(),
        .source_mc_seqno = contract_method_sources[code_hash],
      });
    }
  }

  {
    struct AccountStateWithSource {
      schema::AccountState account_state;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<std::string, AccountStateWithSource> latest_account_states;
    for (const auto& task : insert_tasks) {
      for (const auto& account_state : task.parsed_block_->account_states_) {
        auto account_addr = convert::to_raw_address(account_state.account);
        auto it = latest_account_states.find(account_addr);
        if (it == latest_account_states.end() || it->second.account_state.last_trans_lt < account_state.last_trans_lt) {
          latest_account_states[account_addr] = AccountStateWithSource{
            .account_state = account_state,
            .source_mc_seqno = task.mc_seqno_,
          };
        }
      }
    }
    auto ordered_latest_account_states = get_ordered_map_values(latest_account_states, [](const auto& key, const auto&) {
      return key;
    });
    latest_account_state_sources.reserve(ordered_latest_account_states.size());
    for (const auto& [raw_account, account_state_with_source_ptr] : ordered_latest_account_states) {
      latest_account_state_sources.push_back({
        .account_state = account_state_with_source_ptr->account_state,
        .raw_account = raw_account,
        .source_mc_seqno = account_state_with_source_ptr->source_mc_seqno,
      });
    }
  }

  {
    struct JettonMasterWithSource {
      schema::JettonMasterDataV2 value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, JettonMasterWithSource> jetton_masters;
    for (auto i = insert_tasks.rbegin(); i != insert_tasks.rend(); ++i) {
      const auto& task = *i;
      for (const auto& jetton_master : task.parsed_block_->get_accounts_v2<schema::JettonMasterDataV2>()) {
        auto it = jetton_masters.find(jetton_master.address);
        if (it == jetton_masters.end() || it->second.value.last_transaction_lt < jetton_master.last_transaction_lt) {
          jetton_masters[jetton_master.address] = JettonMasterWithSource{
            .value = jetton_master,
            .source_mc_seqno = task.mc_seqno_,
          };
        }
      }
    }
    auto ordered_jetton_masters = get_ordered_map_values(jetton_masters, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    prepared_batch.jetton_masters.reserve(ordered_jetton_masters.size());
    for (const auto& [_, jetton_master_with_source_ptr] : ordered_jetton_masters) {
      const auto& jetton_master = jetton_master_with_source_ptr->value;
      std::optional<std::string> jetton_content_str = std::nullopt;
      if (jetton_master.jetton_content) {
        jetton_content_str = content_to_json_string(jetton_master.jetton_content.value());
      }
      prepared_batch.jetton_masters.push_back(PreparedJettonMasterRow{
        .address = jetton_master.address,
        .total_supply = jetton_master.total_supply,
        .mintable = jetton_master.mintable,
        .admin_address = jetton_master.admin_address,
        .jetton_content = std::move(jetton_content_str),
        .jetton_wallet_code_hash = jetton_master.jetton_wallet_code_hash,
        .last_transaction_lt = jetton_master.last_transaction_lt,
        .code_hash = jetton_master.code_hash,
        .data_hash = jetton_master.data_hash,
        .destroyed = false,
        .source_mc_seqno = jetton_master_with_source_ptr->source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = jetton_masters.find(destroyed_ptr->address);
      if (it == jetton_masters.end() ||
          it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        prepared_batch.jetton_masters.push_back(make_destroyed_jetton_master_row(*destroyed_ptr));
      }
    }
  }

  {
    struct JettonWalletWithSource {
      schema::JettonWalletDataV2 value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, JettonWalletWithSource> jetton_wallets;
    std::unordered_map<block::StdAddress, std::uint32_t> known_mintless_masters;
    for (auto i = insert_tasks.rbegin(); i != insert_tasks.rend(); ++i) {
      const auto& task = *i;
      for (const auto& jetton_wallet : task.parsed_block_->get_accounts_v2<schema::JettonWalletDataV2>()) {
        auto it = jetton_wallets.find(jetton_wallet.address);
        if (it == jetton_wallets.end() || it->second.value.last_transaction_lt < jetton_wallet.last_transaction_lt) {
          jetton_wallets[jetton_wallet.address] = JettonWalletWithSource{
            .value = jetton_wallet,
            .source_mc_seqno = task.mc_seqno_,
          };
        }
      }
    }
    auto ordered_jetton_wallets = get_ordered_map_values(jetton_wallets, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    prepared_batch.jetton_wallets.reserve(ordered_jetton_wallets.size());
    for (const auto& [_, jetton_wallet_with_source_ptr] : ordered_jetton_wallets) {
      const auto& jetton_wallet = jetton_wallet_with_source_ptr->value;
      prepared_batch.jetton_wallets.push_back(PreparedJettonWalletRow{
        .balance = jetton_wallet.balance,
        .address = jetton_wallet.address,
        .owner = jetton_wallet.owner,
        .jetton = jetton_wallet.jetton,
        .last_transaction_lt = jetton_wallet.last_transaction_lt,
        .code_hash = jetton_wallet.code_hash,
        .data_hash = jetton_wallet.data_hash,
        .mintless_is_claimed = jetton_wallet.mintless_is_claimed,
        .destroyed = false,
        .source_mc_seqno = jetton_wallet_with_source_ptr->source_mc_seqno,
      });
      if (jetton_wallet.mintless_is_claimed.has_value()) {
        known_mintless_masters.emplace(jetton_wallet.jetton, jetton_wallet_with_source_ptr->source_mc_seqno);
      }
    }
    std::vector<std::pair<std::string, const std::pair<const block::StdAddress, std::uint32_t>*>> ordered_mintless_masters;
    ordered_mintless_masters.reserve(known_mintless_masters.size());
    for (const auto& entry : known_mintless_masters) {
      ordered_mintless_masters.emplace_back(convert::to_raw_address(entry.first), &entry);
    }
    std::sort(ordered_mintless_masters.begin(), ordered_mintless_masters.end(), [](const auto& lhs, const auto& rhs) {
      return lhs.first < rhs.first;
    });
    prepared_batch.mintless_jetton_masters.reserve(ordered_mintless_masters.size());
    for (const auto& [_, mintless_entry_ptr] : ordered_mintless_masters) {
      prepared_batch.mintless_jetton_masters.push_back(PreparedMintlessMasterRow{
        .address = mintless_entry_ptr->first,
        .is_indexed = false,
        .source_mc_seqno = mintless_entry_ptr->second,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = jetton_wallets.find(destroyed_ptr->address);
      if (it == jetton_wallets.end() ||
          it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        prepared_batch.jetton_wallets.push_back(make_destroyed_jetton_wallet_row(*destroyed_ptr));
      }
    }
  }

  {
    struct NftCollectionWithSource {
      schema::NFTCollectionDataV2 value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, NftCollectionWithSource> nft_collections;
    for (auto i = insert_tasks.rbegin(); i != insert_tasks.rend(); ++i) {
      const auto& task = *i;
      for (const auto& nft_collection : task.parsed_block_->get_accounts_v2<schema::NFTCollectionDataV2>()) {
        auto it = nft_collections.find(nft_collection.address);
        if (it == nft_collections.end() || it->second.value.last_transaction_lt < nft_collection.last_transaction_lt) {
          nft_collections[nft_collection.address] = NftCollectionWithSource{
            .value = nft_collection,
            .source_mc_seqno = task.mc_seqno_,
          };
        }
      }
    }
    auto ordered_nft_collections = get_ordered_map_values(nft_collections, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    prepared_batch.nft_collections.reserve(ordered_nft_collections.size());
    for (const auto& [_, nft_collection_with_source_ptr] : ordered_nft_collections) {
      const auto& nft_collection = nft_collection_with_source_ptr->value;
      std::optional<std::string> collection_content_str = std::nullopt;
      if (nft_collection.collection_content) {
        collection_content_str = content_to_json_string(nft_collection.collection_content.value());
      }
      prepared_batch.nft_collections.push_back(PreparedNftCollectionRow{
        .address = nft_collection.address,
        .next_item_index = nft_collection.next_item_index,
        .owner_address = nft_collection.owner_address,
        .collection_content = std::move(collection_content_str),
        .last_transaction_lt = nft_collection.last_transaction_lt,
        .code_hash = nft_collection.code_hash,
        .data_hash = nft_collection.data_hash,
        .destroyed = false,
        .source_mc_seqno = nft_collection_with_source_ptr->source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = nft_collections.find(destroyed_ptr->address);
      if (it == nft_collections.end() ||
          it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        prepared_batch.nft_collections.push_back(make_destroyed_nft_collection_row(*destroyed_ptr));
      }
    }
  }

  {
    struct UnifiedSaleData {
      block::StdAddress address;
      bool is_complete;
      uint32_t created_at;
      block::StdAddress marketplace_address;
      block::StdAddress nft_address;
      std::optional<block::StdAddress> nft_owner_address;
      td::RefInt256 full_price;
      block::StdAddress marketplace_fee_address;
      td::RefInt256 marketplace_fee;
      block::StdAddress royalty_address;
      td::RefInt256 royalty_amount;
      std::optional<uint32_t> sold_at;
      std::optional<uint64_t> sold_query_id;
      std::map<std::string, std::string> jetton_price_dict;
      uint64_t last_transaction_lt;
      td::Bits256 code_hash;
      td::Bits256 data_hash;
      std::uint32_t source_mc_seqno;
    };

    std::unordered_map<block::StdAddress, UnifiedSaleData> nft_sales;

    for (auto i = insert_tasks.rbegin(); i != insert_tasks.rend(); ++i) {
      const auto& task = *i;
      for (const auto& nft_sale : task.parsed_block_->get_accounts_v2<schema::GetGemsNftFixPriceSaleData>()) {
        auto it = nft_sales.find(nft_sale.address);
        if (it == nft_sales.end() || it->second.last_transaction_lt < nft_sale.last_transaction_lt) {
          nft_sales[nft_sale.address] = {
            nft_sale.address,
            nft_sale.is_complete,
            nft_sale.created_at,
            nft_sale.marketplace_address,
            nft_sale.nft_address,
            nft_sale.nft_owner_address,
            nft_sale.full_price,
            nft_sale.marketplace_fee_address,
            nft_sale.marketplace_fee,
            nft_sale.royalty_address,
            nft_sale.royalty_amount,
            std::nullopt,
            std::nullopt,
            {},
            nft_sale.last_transaction_lt,
            nft_sale.code_hash,
            nft_sale.data_hash,
            task.mc_seqno_
          };
        }
      }
    }

    for (auto i = insert_tasks.rbegin(); i != insert_tasks.rend(); ++i) {
      const auto& task = *i;
      for (const auto& nft_sale_v4 : task.parsed_block_->get_accounts_v2<schema::GetGemsNftFixPriceSaleV4Data>()) {
        auto it = nft_sales.find(nft_sale_v4.address);
        if (it == nft_sales.end() || it->second.last_transaction_lt < nft_sale_v4.last_transaction_lt) {
          nft_sales[nft_sale_v4.address] = {
            nft_sale_v4.address,
            nft_sale_v4.is_complete,
            nft_sale_v4.created_at,
            nft_sale_v4.marketplace_address,
            nft_sale_v4.nft_address,
            nft_sale_v4.nft_owner_address,
            nft_sale_v4.full_price,
            nft_sale_v4.marketplace_fee_address,
            nft_sale_v4.marketplace_fee,
            nft_sale_v4.royalty_address,
            nft_sale_v4.royalty_amount,
            nft_sale_v4.sold_at,
            nft_sale_v4.sold_query_id,
            nft_sale_v4.jetton_price_dict,
            nft_sale_v4.last_transaction_lt,
            nft_sale_v4.code_hash,
            nft_sale_v4.data_hash,
            task.mc_seqno_
          };
        }
      }
    }

    auto ordered_nft_sales = get_ordered_map_values(nft_sales, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    prepared_batch.getgems_nft_sales.reserve(ordered_nft_sales.size());
    for (const auto& [_, nft_sale_ptr] : ordered_nft_sales) {
      const auto& nft_sale = *nft_sale_ptr;
      std::optional<std::string> jetton_dict_json = std::nullopt;
      if (!nft_sale.jetton_price_dict.empty()) {
        td::JsonBuilder jb;
        auto obj = jb.enter_object();
        for (const auto& [key, value] : nft_sale.jetton_price_dict) {
          obj(key, value);
        }
        obj.leave();
        jetton_dict_json = jb.string_builder().as_cslice().str();
      }
      prepared_batch.getgems_nft_sales.push_back(PreparedGetgemsSaleRow{
        .address = nft_sale.address,
        .is_complete = nft_sale.is_complete,
        .created_at = nft_sale.created_at,
        .marketplace_address = nft_sale.marketplace_address,
        .nft_address = nft_sale.nft_address,
        .nft_owner_address = nft_sale.nft_owner_address,
        .full_price = nft_sale.full_price,
        .marketplace_fee_address = nft_sale.marketplace_fee_address,
        .marketplace_fee = nft_sale.marketplace_fee,
        .royalty_address = nft_sale.royalty_address,
        .royalty_amount = nft_sale.royalty_amount,
        .sold_at = nft_sale.sold_at,
        .sold_query_id = nft_sale.sold_query_id,
        .jetton_price_dict = std::move(jetton_dict_json),
        .last_transaction_lt = nft_sale.last_transaction_lt,
        .code_hash = nft_sale.code_hash,
        .data_hash = nft_sale.data_hash,
        .destroyed = false,
        .source_mc_seqno = nft_sale.source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = nft_sales.find(destroyed_ptr->address);
      if (it == nft_sales.end() || it->second.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        prepared_batch.getgems_nft_sales.push_back(make_destroyed_getgems_sale_row(*destroyed_ptr));
      }
    }
  }

  {
    struct NftAuctionWithSource {
      schema::GetGemsNftAuctionData value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, NftAuctionWithSource> nft_auctions;
    for (auto i = insert_tasks.rbegin(); i != insert_tasks.rend(); ++i) {
      const auto& task = *i;
      for (const auto& nft_auction : task.parsed_block_->get_accounts_v2<schema::GetGemsNftAuctionData>()) {
        auto it = nft_auctions.find(nft_auction.address);
        if (it == nft_auctions.end() || it->second.value.last_transaction_lt < nft_auction.last_transaction_lt) {
          nft_auctions[nft_auction.address] = NftAuctionWithSource{
            .value = nft_auction,
            .source_mc_seqno = task.mc_seqno_,
          };
        }
      }
    }
    auto ordered_nft_auctions = get_ordered_map_values(nft_auctions, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    prepared_batch.getgems_nft_auctions.reserve(ordered_nft_auctions.size());
    for (const auto& [_, nft_auction_with_source_ptr] : ordered_nft_auctions) {
      const auto& nft_auction = nft_auction_with_source_ptr->value;
      std::optional<std::string> public_key;
      if (nft_auction.public_key.has_value()) {
        public_key = nft_auction.public_key.value()->to_hex_string();
      }
      prepared_batch.getgems_nft_auctions.push_back(PreparedGetgemsAuctionRow{
        .address = nft_auction.address,
        .end_flag = nft_auction.end,
        .end_time = nft_auction.end_time,
        .mp_addr = nft_auction.mp_addr,
        .nft_addr = nft_auction.nft_addr,
        .nft_owner = nft_auction.nft_owner,
        .last_bid = nft_auction.last_bid,
        .last_member = nft_auction.last_member,
        .min_step = nft_auction.min_step,
        .mp_fee_addr = nft_auction.mp_fee_addr,
        .mp_fee_factor = nft_auction.mp_fee_factor,
        .mp_fee_base = nft_auction.mp_fee_base,
        .royalty_fee_addr = nft_auction.royalty_fee_addr,
        .royalty_fee_factor = nft_auction.royalty_fee_factor,
        .royalty_fee_base = nft_auction.royalty_fee_base,
        .max_bid = nft_auction.max_bid,
        .min_bid = nft_auction.min_bid,
        .created_at = nft_auction.created_at,
        .last_bid_at = nft_auction.last_bid_at,
        .is_canceled = nft_auction.is_canceled,
        .activated = nft_auction.activated,
        .step_time = nft_auction.step_time,
        .last_query_id = nft_auction.last_query_id,
        .jetton_wallet = nft_auction.jetton_wallet,
        .jetton_master = nft_auction.jetton_master,
        .is_broken_state = nft_auction.is_broken_state,
        .public_key = std::move(public_key),
        .last_transaction_lt = nft_auction.last_transaction_lt,
        .code_hash = nft_auction.code_hash,
        .data_hash = nft_auction.data_hash,
        .destroyed = false,
        .source_mc_seqno = nft_auction_with_source_ptr->source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = nft_auctions.find(destroyed_ptr->address);
      if (it == nft_auctions.end() ||
          it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        prepared_batch.getgems_nft_auctions.push_back(make_destroyed_getgems_auction_row(*destroyed_ptr));
      }
    }
  }

  {
    std::map<std::pair<ton::StdSmcAddress, ton::StdSmcAddress>, block::StdAddress> sale_real_owners;
    std::map<std::pair<ton::StdSmcAddress, ton::StdSmcAddress>, block::StdAddress> auction_real_owners;
    auto has_newer_destroyed_state = [&](const block::StdAddress& address, std::uint64_t last_transaction_lt) {
      auto destroyed_it = destroyed_accounts.find(address);
      return destroyed_it != destroyed_accounts.end() &&
             last_transaction_lt < destroyed_it->second.last_transaction_lt;
    };

    for (const auto& task : insert_tasks) {
      for (const auto& sale : task.parsed_block_->get_accounts_v2<schema::GetGemsNftFixPriceSaleData>()) {
        if (has_newer_destroyed_state(sale.address, sale.last_transaction_lt)) {
          continue;
        }
        if (sale.nft_owner_address) {
          sale_real_owners[{sale.address.addr, sale.nft_address.addr}] = sale.nft_owner_address.value();
        }
      }
      for (const auto& sale_v4 : task.parsed_block_->get_accounts_v2<schema::GetGemsNftFixPriceSaleV4Data>()) {
        if (has_newer_destroyed_state(sale_v4.address, sale_v4.last_transaction_lt)) {
          continue;
        }
        if (sale_v4.nft_owner_address) {
          sale_real_owners[{sale_v4.address.addr, sale_v4.nft_address.addr}] = sale_v4.nft_owner_address.value();
        }
      }
      for (const auto& auction : task.parsed_block_->get_accounts_v2<schema::GetGemsNftAuctionData>()) {
        if (has_newer_destroyed_state(auction.address, auction.last_transaction_lt)) {
          continue;
        }
        if (auction.nft_owner) {
          auction_real_owners[{auction.address.addr, auction.nft_addr.addr}] = auction.nft_owner.value();
        }
      }
    }

    struct NftItemWithSource {
      schema::NFTItemDataV2 value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, NftItemWithSource> nft_items;
    for (auto i = insert_tasks.rbegin(); i != insert_tasks.rend(); ++i) {
      const auto& task = *i;
      for (const auto& nft_item : task.parsed_block_->get_accounts_v2<schema::NFTItemDataV2>()) {
        auto it = nft_items.find(nft_item.address);
        if (it == nft_items.end() || it->second.value.last_transaction_lt < nft_item.last_transaction_lt) {
          nft_items[nft_item.address] = NftItemWithSource{
            .value = nft_item,
            .source_mc_seqno = task.mc_seqno_,
          };
        }
      }
    }

    auto ordered_nft_items = get_ordered_map_values(nft_items, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    prepared_batch.nft_items.reserve(ordered_nft_items.size());
    std::unordered_map<block::StdAddress, std::uint64_t> dns_entry_lts;
    for (const auto& [_, nft_item_with_source_ptr] : ordered_nft_items) {
      const auto& nft_item = nft_item_with_source_ptr->value;
      std::optional<std::string> content_str = std::nullopt;
      if (nft_item.content) {
        content_str = content_to_json_string(nft_item.content.value());
      }

      std::optional<block::StdAddress> real_owner = nft_item.owner_address;
      if (nft_item.owner_address) {
        auto sale_key = std::make_pair(nft_item.owner_address.value().addr, nft_item.address.addr);
        auto sale_it = sale_real_owners.find(sale_key);
        if (sale_it != sale_real_owners.end()) {
          real_owner = sale_it->second;
        } else {
          auto auction_key = std::make_pair(nft_item.owner_address.value().addr, nft_item.address.addr);
          auto auction_it = auction_real_owners.find(auction_key);
          if (auction_it != auction_real_owners.end()) {
            real_owner = auction_it->second;
          }
        }
      }

      prepared_batch.nft_items.push_back(PreparedNftItemRow{
        .address = nft_item.address,
        .init = nft_item.init,
        .index = nft_item.index,
        .collection_address = nft_item.collection_address,
        .owner_address = nft_item.owner_address,
        .content = std::move(content_str),
        .last_transaction_lt = nft_item.last_transaction_lt,
        .code_hash = nft_item.code_hash,
        .data_hash = nft_item.data_hash,
        .real_owner = real_owner,
        .destroyed = false,
        .source_mc_seqno = nft_item_with_source_ptr->source_mc_seqno,
      });

      if (nft_item.dns_entry) {
        dns_entry_lts[nft_item.address] = nft_item.last_transaction_lt;
        prepared_batch.dns_entries.push_back(PreparedDnsEntryRow{
          .nft_item_address = nft_item.address,
          .nft_item_owner = nft_item.owner_address,
          .domain = nft_item.dns_entry->domain,
          .dns_next_resolver = nft_item.dns_entry->next_resolver,
          .dns_wallet = nft_item.dns_entry->wallet,
          .dns_site_adnl = nft_item.dns_entry->site_adnl,
          .dns_storage_bag_id = nft_item.dns_entry->storage_bag_id,
          .last_transaction_lt = nft_item.last_transaction_lt,
          .destroyed = false,
          .source_mc_seqno = nft_item_with_source_ptr->source_mc_seqno,
        });
      }
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = nft_items.find(destroyed_ptr->address);
      if (it == nft_items.end() || it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        prepared_batch.nft_items.push_back(make_destroyed_nft_item_row(*destroyed_ptr));
      }
      auto dns_it = dns_entry_lts.find(destroyed_ptr->address);
      if (dns_it == dns_entry_lts.end() || dns_it->second < destroyed_ptr->last_transaction_lt) {
        prepared_batch.dns_entries.push_back(make_destroyed_dns_entry_row(*destroyed_ptr));
      }
    }
  }

  {
    struct MultisigContractWithSource {
      schema::MultisigContractData value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, MultisigContractWithSource> multisig_contracts;
    for (auto i = insert_tasks.rbegin(); i != insert_tasks.rend(); ++i) {
      const auto& task = *i;
      for (const auto& multisig_contract : task.parsed_block_->get_accounts_v2<schema::MultisigContractData>()) {
        auto it = multisig_contracts.find(multisig_contract.address);
        if (it == multisig_contracts.end() || it->second.value.last_transaction_lt < multisig_contract.last_transaction_lt) {
          multisig_contracts[multisig_contract.address] = MultisigContractWithSource{
            .value = multisig_contract,
            .source_mc_seqno = task.mc_seqno_,
          };
        }
      }
    }
    auto ordered_multisig_contracts = get_ordered_map_values(multisig_contracts, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    prepared_batch.multisig_contracts.reserve(ordered_multisig_contracts.size());
    for (const auto& [_, multisig_contract_with_source_ptr] : ordered_multisig_contracts) {
      const auto& multisig_contract = multisig_contract_with_source_ptr->value;
      prepared_batch.multisig_contracts.push_back(PreparedMultisigContractRow{
        .address = multisig_contract.address,
        .next_order_seqno = multisig_contract.next_order_seqno,
        .threshold = multisig_contract.threshold,
        .signers = multisig_contract.signers,
        .proposers = multisig_contract.proposers,
        .last_transaction_lt = multisig_contract.last_transaction_lt,
        .code_hash = multisig_contract.code_hash,
        .data_hash = multisig_contract.data_hash,
        .destroyed = false,
        .source_mc_seqno = multisig_contract_with_source_ptr->source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = multisig_contracts.find(destroyed_ptr->address);
      if (it == multisig_contracts.end() ||
          it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        prepared_batch.multisig_contracts.push_back(make_destroyed_multisig_contract_row(*destroyed_ptr));
      }
    }
  }

  {
    struct MultisigOrderWithSource {
      schema::MultisigOrderData value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, MultisigOrderWithSource> multisig_orders;
    for (auto i = insert_tasks.rbegin(); i != insert_tasks.rend(); ++i) {
      const auto& task = *i;
      for (const auto& multisig_order : task.parsed_block_->get_accounts_v2<schema::MultisigOrderData>()) {
        auto it = multisig_orders.find(multisig_order.address);
        if (it == multisig_orders.end() || it->second.value.last_transaction_lt < multisig_order.last_transaction_lt) {
          multisig_orders[multisig_order.address] = MultisigOrderWithSource{
            .value = multisig_order,
            .source_mc_seqno = task.mc_seqno_,
          };
        }
      }
    }
    auto ordered_multisig_orders = get_ordered_map_values(multisig_orders, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    prepared_batch.multisig_orders.reserve(ordered_multisig_orders.size());
    for (const auto& [_, multisig_order_with_source_ptr] : ordered_multisig_orders) {
      const auto& multisig_order = multisig_order_with_source_ptr->value;
      std::optional<std::string> order_boc_str = std::nullopt;
      if (multisig_order.order.not_null()) {
        order_boc_str = serialize_cell_to_base64(multisig_order.order);
      }
      prepared_batch.multisig_orders.push_back(PreparedMultisigOrderRow{
        .address = multisig_order.address,
        .multisig_address = multisig_order.multisig_address,
        .order_seqno = multisig_order.order_seqno,
        .threshold = multisig_order.threshold,
        .sent_for_execution = multisig_order.sent_for_execution,
        .approvals_mask = multisig_order.approvals_mask,
        .approvals_num = multisig_order.approvals_num,
        .expiration_date = multisig_order.expiration_date,
        .order_boc = std::move(order_boc_str),
        .signers = multisig_order.signers,
        .last_transaction_lt = multisig_order.last_transaction_lt,
        .code_hash = multisig_order.code_hash,
        .data_hash = multisig_order.data_hash,
        .destroyed = false,
        .source_mc_seqno = multisig_order_with_source_ptr->source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = multisig_orders.find(destroyed_ptr->address);
      if (it == multisig_orders.end() ||
          it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        prepared_batch.multisig_orders.push_back(make_destroyed_multisig_order_row(*destroyed_ptr));
      }
    }
  }

  {
    struct DedustPoolWithSource {
      schema::DedustPoolData value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, DedustPoolWithSource> dedust_pools;
    for (auto i = insert_tasks.rbegin(); i != insert_tasks.rend(); ++i) {
      const auto& task = *i;
      for (const auto& dedust_pool : task.parsed_block_->get_accounts_v2<schema::DedustPoolData>()) {
        auto it = dedust_pools.find(dedust_pool.address);
        if (it == dedust_pools.end() || it->second.value.last_transaction_lt < dedust_pool.last_transaction_lt) {
          dedust_pools[dedust_pool.address] = DedustPoolWithSource{
            .value = dedust_pool,
            .source_mc_seqno = task.mc_seqno_,
          };
        }
      }
    }
    auto ordered_dedust_pools = get_ordered_map_values(dedust_pools, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    prepared_batch.dedust_pools.reserve(ordered_dedust_pools.size());
    std::string dex = "dedust";
    for (const auto& [_, dedust_pool_with_source_ptr] : ordered_dedust_pools) {
      const auto& dedust_pool = dedust_pool_with_source_ptr->value;
      std::string pool_type = dedust_pool.is_stable ? "stable" : "volatile";
      prepared_batch.dedust_pools.push_back(PreparedDedustPoolRow{
        .address = dedust_pool.address,
        .asset_1 = dedust_pool.asset_1,
        .asset_2 = dedust_pool.asset_2,
        .reserve_1 = dedust_pool.reserve_1,
        .reserve_2 = dedust_pool.reserve_2,
        .pool_type = std::move(pool_type),
        .dex = dex,
        .fee = dedust_pool.fee,
        .last_transaction_lt = dedust_pool.last_transaction_lt,
        .code_hash = dedust_pool.code_hash,
        .data_hash = dedust_pool.data_hash,
        .destroyed = false,
        .source_mc_seqno = dedust_pool_with_source_ptr->source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = dedust_pools.find(destroyed_ptr->address);
      if (it == dedust_pools.end() ||
          it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        prepared_batch.dedust_pools.push_back(make_destroyed_dedust_pool_row(*destroyed_ptr));
      }
    }
  }

  {
    struct VestingWithSource {
      schema::VestingData value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, VestingWithSource> vesting_contracts;
    for (auto i = insert_tasks.rbegin(); i != insert_tasks.rend(); ++i) {
      const auto& task = *i;
      for (const auto& vesting : task.parsed_block_->get_accounts_v2<schema::VestingData>()) {
        auto it = vesting_contracts.find(vesting.address);
        if (it == vesting_contracts.end() || it->second.value.last_transaction_lt < vesting.last_transaction_lt) {
          vesting_contracts[vesting.address] = VestingWithSource{
            .value = vesting,
            .source_mc_seqno = task.mc_seqno_,
          };
        }
      }
    }
    auto ordered_vesting_contracts = get_ordered_map_values(vesting_contracts, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    prepared_batch.vesting_contracts.reserve(ordered_vesting_contracts.size());
    for (const auto& [_, vesting_with_source_ptr] : ordered_vesting_contracts) {
      const auto& vesting = vesting_with_source_ptr->value;
      prepared_batch.vesting_contracts.push_back(PreparedVestingContractRow{
        .address = vesting.address,
        .vesting_start_time = vesting.vesting_start_time,
        .vesting_total_duration = vesting.vesting_total_duration,
        .unlock_period = vesting.unlock_period,
        .cliff_duration = vesting.cliff_duration,
        .vesting_total_amount = vesting.vesting_total_amount,
        .vesting_sender_address = vesting.vesting_sender_address,
        .owner_address = vesting.owner_address,
        .last_transaction_lt = vesting.last_transaction_lt,
        .code_hash = vesting.code_hash,
        .data_hash = vesting.data_hash,
        .destroyed = false,
        .source_mc_seqno = vesting_with_source_ptr->source_mc_seqno,
      });
      auto ordered_wallets = get_ordered_values(vesting.whitelist, [](const auto& address) {
        return convert::to_raw_address(address);
      });
      for (const auto& [__, wallet_addr_ptr] : ordered_wallets) {
        prepared_batch.vesting_whitelist.push_back(PreparedVestingWhitelistRow{
          .vesting_contract_address = vesting.address,
          .wallet_address = *wallet_addr_ptr,
          .source_mc_seqno = vesting_with_source_ptr->source_mc_seqno,
        });
      }
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = vesting_contracts.find(destroyed_ptr->address);
      if (it == vesting_contracts.end() ||
          it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        prepared_batch.vesting_contracts.push_back(make_destroyed_vesting_contract_row(*destroyed_ptr));
      }
    }
  }

  {
    struct NominatorPoolWithSource {
      schema::NominatorPoolData value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, NominatorPoolWithSource> nominator_pools;
    for (auto i = insert_tasks.rbegin(); i != insert_tasks.rend(); ++i) {
      const auto& task = *i;
      for (const auto& pool : task.parsed_block_->get_accounts_v2<schema::NominatorPoolData>()) {
        auto it = nominator_pools.find(pool.address);
        if (it == nominator_pools.end() || it->second.value.last_transaction_lt < pool.last_transaction_lt) {
          nominator_pools[pool.address] = NominatorPoolWithSource{
            .value = pool,
            .source_mc_seqno = task.mc_seqno_,
          };
        }
      }
    }
    auto ordered_nominator_pools = get_ordered_map_values(nominator_pools, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    prepared_batch.nominator_pools.reserve(ordered_nominator_pools.size());
    for (const auto& [_, pool_with_source_ptr] : ordered_nominator_pools) {
      const auto& pool = pool_with_source_ptr->value;
      prepared_batch.nominator_pools.push_back(PreparedNominatorPoolRow{
        .address = pool.address,
        .state = pool.state,
        .nominators_count = pool.nominators_count,
        .stake_amount_sent = pool.stake_amount_sent,
        .validator_amount = pool.validator_amount,
        .validator_address = pool.validator_address,
        .validator_reward_share = pool.validator_reward_share,
        .max_nominators_count = pool.max_nominators_count,
        .min_validator_stake = pool.min_validator_stake,
        .min_nominator_stake = pool.min_nominator_stake,
        .active_nominators = nominators_to_json_string(pool.nominators),
        .last_transaction_lt = pool.last_transaction_lt,
        .code_hash = pool.code_hash,
        .data_hash = pool.data_hash,
        .destroyed = false,
        .source_mc_seqno = pool_with_source_ptr->source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = nominator_pools.find(destroyed_ptr->address);
      if (it == nominator_pools.end() ||
          it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        prepared_batch.nominator_pools.push_back(make_destroyed_nominator_pool_row(*destroyed_ptr));
      }
    }
  }

  {
    struct TelemintWithSource {
      schema::TelemintData value;
      std::uint32_t source_mc_seqno;
    };
    std::unordered_map<block::StdAddress, TelemintWithSource> telemint_nfts;
    std::unordered_map<block::StdAddress, schema::NFTItemDataV2> nft_items;
    for (auto i = insert_tasks.rbegin(); i != insert_tasks.rend(); ++i) {
      const auto& task = *i;
      for (const auto& nft_item : task.parsed_block_->get_accounts_v2<schema::NFTItemDataV2>()) {
        auto it = nft_items.find(nft_item.address);
        if (it == nft_items.end() || it->second.last_transaction_lt < nft_item.last_transaction_lt) {
          nft_items[nft_item.address] = nft_item;
        }
      }
    }
    for (auto i = insert_tasks.rbegin(); i != insert_tasks.rend(); ++i) {
      const auto& task = *i;
      for (const auto& telemint : task.parsed_block_->get_accounts_v2<schema::TelemintData>()) {
        if (nft_items.find(telemint.address) == nft_items.end()) {
          continue;
        }
        auto it = telemint_nfts.find(telemint.address);
        if (it == telemint_nfts.end() || it->second.value.last_transaction_lt < telemint.last_transaction_lt) {
          telemint_nfts[telemint.address] = TelemintWithSource{
            .value = telemint,
            .source_mc_seqno = task.mc_seqno_,
          };
        }
      }
    }

    auto ordered_telemint_nfts = get_ordered_map_values(telemint_nfts, [](const auto& key, const auto&) {
      return convert::to_raw_address(key);
    });
    prepared_batch.telemint_nft_items.reserve(ordered_telemint_nfts.size());
    for (const auto& [_, telemint_with_source_ptr] : ordered_telemint_nfts) {
      const auto& telemint = telemint_with_source_ptr->value;
      auto token_name = telemint.token_name;
      token_name.erase(std::remove(token_name.begin(), token_name.end(), '\0'), token_name.end());
      prepared_batch.telemint_nft_items.push_back(PreparedTelemintRow{
        .address = telemint.address,
        .token_name = std::move(token_name),
        .bidder_address = telemint.bidder_address,
        .bid = telemint.bid,
        .bid_ts = telemint.bid_ts,
        .min_bid = telemint.min_bid,
        .end_time = telemint.end_time,
        .beneficiary_address = telemint.beneficiary_address,
        .initial_min_bid = telemint.initial_min_bid,
        .max_bid = telemint.max_bid,
        .min_bid_step = telemint.min_bid_step,
        .min_extend_time = telemint.min_extend_time,
        .duration = telemint.duration,
        .royalty_numerator = telemint.royalty_numerator,
        .royalty_denominator = telemint.royalty_denominator,
        .royalty_destination = telemint.royalty_destination,
        .last_transaction_lt = telemint.last_transaction_lt,
        .code_hash = telemint.code_hash,
        .data_hash = telemint.data_hash,
        .destroyed = false,
        .source_mc_seqno = telemint_with_source_ptr->source_mc_seqno,
      });
    }
    for (const auto& [_, destroyed_ptr] : ordered_destroyed_accounts) {
      auto it = telemint_nfts.find(destroyed_ptr->address);
      if (it == telemint_nfts.end() ||
          it->second.value.last_transaction_lt < destroyed_ptr->last_transaction_lt) {
        prepared_batch.telemint_nft_items.push_back(make_destroyed_telemint_row(*destroyed_ptr));
      }
    }
  }
}

}  // namespace


std::string InsertManagerPostgres::Credential::get_connection_string() const {
  if (conn_str.length()) {
    return conn_str;
  }
  std::string result = (
    "hostaddr=" + host +
    " port=" + std::to_string(port) + 
    (user.length() ? " user=" + user : "") +
    (password.length() ? " password=" + password : "") +
    (dbname.length() ? " dbname=" + dbname : "")
  );
  // LOG(INFO) << "connection string: " << result;
  return result;
}


template <>
struct std::hash<std::pair<td::Bits256, td::Bits256>>
{
  std::size_t operator()(const std::pair<td::Bits256, td::Bits256>& k) const {
    std::size_t seed = 0;
    for(const auto& el : k.first.as_array()) {
        seed ^= std::hash<td::uint8>{}(el) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    for(const auto& el : k.second.as_array()) {
        seed ^= std::hash<td::uint8>{}(el) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
  }
};

//
// InsertBatchPostgres
//
void InsertBatchPostgres::start_up() {
  connection_string_ = credential_.get_connection_string();
  alarm();
}

std::string get_worker_id() {
#ifndef HOST_NAME_MAX
#define HOST_NAME_MAX 64
#endif

  char hostname[HOST_NAME_MAX];
  if (gethostname(hostname, HOST_NAME_MAX) != 0) {
    throw std::runtime_error("Failed to get hostname");
  }
  std::string worker_id = std::string{hostname} + "_" + std::to_string(getpid());
  return worker_id;
}

bool InsertBatchPostgres::try_acquire_leader_lock(pqxx::connection& c, const std::string& worker_id) {
  auto query = R"(
  WITH grab AS (
    INSERT INTO ton_indexer_leader(id, leader_worker_id, last_heartbeat, started_at)
    VALUES (1, $1, now(), now())
    ON CONFLICT(id) DO UPDATE SET
      leader_worker_id = excluded.leader_worker_id,
      last_heartbeat = excluded.last_heartbeat,
      started_at = CASE
        WHEN ton_indexer_leader.leader_worker_id = excluded.leader_worker_id THEN ton_indexer_leader.started_at
        ELSE excluded.started_at
      END
    WHERE ton_indexer_leader.id = 1 AND (ton_indexer_leader.last_heartbeat < NOW() - INTERVAL '20 seconds'
      OR ton_indexer_leader.leader_worker_id = $1)
    RETURNING 1
  )
  SELECT (SELECT COUNT(*) FROM grab) AS won_the_lock;
  )";
  try {
    static std::atomic_bool worker_is_leader = false;
    td::Timer timer;
    pqxx::work txn(c);
    auto [won] = txn.exec(query, pqxx::params{worker_id}).one_row().as<int>();
    txn.commit();

    is_leader_ = won == 1;
    if (is_leader_ != worker_is_leader.exchange(is_leader_)) {
      LOG(WARNING) << "Worker " << worker_id << (won ? " ACQUIRED" : " LOST") << " leader role.";
    }
    return is_leader_;
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error trying to acquire SQL leader lock: " << e.what();
    is_leader_ = false;
    return false;
  }
}

InsertManagerInterface::InsertResult InsertBatchPostgres::get_insert_result() const {
  InsertManagerInterface::InsertResult result;
  if (!no_leader_) {
    result.is_leader = is_leader_;
  }
  return result;
}

void InsertBatchPostgres::alarm() {
  do_insert();
}

void InsertBatchPostgres::ensure_inserted() {
  std::string seqnos = "(";
  for(auto& task : insert_tasks_) {
    auto mc_seqno = task.mc_seqno_;
    if (seqnos.size() > 1) {
      seqnos += ", ";
    }
    seqnos += std::to_string(mc_seqno);
  }
  seqnos += ")";
  auto query = "select count(*) from blocks where workchain = -1 and seqno in " + seqnos + ";";
  try {
    td::Timer timer;
    pqxx::connection c(connection_string_);
    pqxx::work txn(c);
    auto [inserted_cnt] = txn.query1<int>(query);
    if (inserted_cnt != insert_tasks_.size()) {
      alarm_timestamp() = td::Timestamp::in(1.0);
    } else {
      reset_prepare_state();
      for(auto& task : insert_tasks_) {
        task.promise_.set_result(get_insert_result());
      }
      promise_.set_result(get_insert_result());
      stop();
    }
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error ensuring that leader inserted blocks batch: " << e.what();
    alarm_timestamp() = td::Timestamp::in(1.0);
  }
}

std::optional<std::pair<std::uint32_t, std::uint32_t>> InsertBatchPostgres::get_batch_seqno_range() const {
  if (insert_tasks_.empty()) {
    return std::nullopt;
  }

  std::uint32_t min_seqno = insert_tasks_.front().mc_seqno_;
  std::uint32_t max_seqno = insert_tasks_.front().mc_seqno_;
  for (const auto& task : insert_tasks_) {
    min_seqno = std::min(min_seqno, task.mc_seqno_);
    max_seqno = std::max(max_seqno, task.mc_seqno_);
  }
  return std::make_pair(min_seqno, max_seqno);
}

void InsertBatchPostgres::ensure_batch_partitions(pqxx::connection& c) {
  if (!partition_config_.enabled) {
    return;
  }
  auto seqno_range = get_batch_seqno_range();
  if (!seqno_range.has_value()) {
    return;
  }
  PartitionManagerPostgres partition_manager(partition_config_);
  partition_manager.ensure_partitions(c, seqno_range->first, seqno_range->second);
}

void InsertBatchPostgres::drop_old_partitions_after_commit(pqxx::connection& c) {
  if (!partition_config_.enabled || partition_config_.retention_mc_seqnos == 0) {
    return;
  }

  try {
    PartitionManagerPostgres partition_manager(partition_config_);
    partition_manager.drop_old_partitions(c);
  } catch (const std::exception& e) {
    LOG(WARNING) << "Failed to drop old Postgres hot partitions: " << e.what();
  }
}

void InsertBatchPostgres::do_insert() {
  bool preparing = false;
  try {
    pqxx::connection c(connection_string_);
    if (!no_leader_) {
      if (!try_acquire_leader_lock(c, worker_id_)) {
        reset_prepare_state();
        release_leader_heartbeat();
        ensure_inserted();
        return;
      }
      acquire_leader_heartbeat();
    } else {
      is_leader_ = true;
    }
    ensure_batch_partitions(c);

    if (!prepared_batch_) {
      preparing = true;
      begin_prepare();
      preparing = false;
      if (prepare_state_) {
        return;
      }
    }
    commit_prepared_batch();
  } catch (const pqxx::integrity_constraint_violation &e) {
    release_leader_heartbeat();
    LOG(WARNING) << "Error COPY to PG: " << e.what();
    LOG(WARNING) << "Apparently this block already exists in the database. Nevertheless we retry with INSERT ... ON CONFLICT ...";
    with_copy_ = false;
    g_statistics.record_count(INSERT_CONFLICT);
    alarm_timestamp() = td::Timestamp::now();
  } catch (const std::exception &e) {
    release_leader_heartbeat();
    if (preparing) {
      reset_prepare_state();
      LOG(ERROR) << "Error preparing PG batch: " << e.what();
    } else {
      LOG(ERROR) << "Error inserting batch: " << e.what();
    }
    alarm_timestamp() = td::Timestamp::in(1.0);
  }
}

void InsertBatchPostgres::begin_prepare() {
  ++prepare_generation_;
  prepared_batch_ = std::make_shared<PreparedBatchPostgres>();
  prepare_state_ = std::make_shared<InsertBatchPostgresPrepareState>();

  collect_and_prepare_batch_rows(insert_tasks_, *prepared_batch_, prepare_state_->latest_account_state_sources);

  if (prepare_state_->latest_account_state_sources.empty()) {
    prepare_state_.reset();
    return;
  }

  const std::size_t chunk_size = std::max<std::size_t>(1, static_cast<std::size_t>(latest_states_prepare_chunk_size_));
  prepare_state_->latest_state_total_chunks =
      (prepare_state_->latest_account_state_sources.size() + chunk_size - 1) / chunk_size;
  prepare_state_->latest_account_state_chunks.resize(prepare_state_->latest_state_total_chunks);

  spawn_next_latest_account_state_chunks();
  maybe_commit_prepared_batch();
}

void InsertBatchPostgres::spawn_next_latest_account_state_chunks() {
  if (!prepare_state_) {
    return;
  }
  const std::size_t chunk_size = std::max<std::size_t>(1, static_cast<std::size_t>(latest_states_prepare_chunk_size_));
  const std::size_t parallelism = std::max<std::size_t>(1, static_cast<std::size_t>(latest_states_prepare_parallelism_));
  while (prepare_state_->latest_state_in_flight < parallelism &&
         prepare_state_->latest_state_next_chunk_to_spawn < prepare_state_->latest_state_total_chunks) {
    const std::size_t chunk_index = prepare_state_->latest_state_next_chunk_to_spawn++;
    const std::size_t start = chunk_index * chunk_size;
    const std::size_t end = std::min(start + chunk_size, prepare_state_->latest_account_state_sources.size());
    std::vector<LatestAccountStateSourceRow> rows(
        prepare_state_->latest_account_state_sources.begin() + static_cast<std::ptrdiff_t>(start),
        prepare_state_->latest_account_state_sources.begin() + static_cast<std::ptrdiff_t>(end));

    auto promise = td::PromiseCreator::lambda(
        [SelfId = actor_id(this), generation = prepare_generation_, chunk_index](
            td::Result<std::shared_ptr<LatestAccountStatesChunkPreparedResult>> result) mutable {
          td::actor::send_closure(SelfId, &InsertBatchPostgres::on_latest_account_state_chunk_prepared,
                                  generation, chunk_index, std::move(result));
        });
    ++prepare_state_->latest_state_in_flight;
    td::actor::create_actor<PrepareLatestAccountStatesChunkActor>(
        "prepare_latest_states_chunk", std::move(rows), max_data_depth_, std::move(promise)).release();
  }
}

void InsertBatchPostgres::on_latest_account_state_chunk_prepared(
    std::uint64_t generation, std::size_t chunk_index,
    td::Result<std::shared_ptr<LatestAccountStatesChunkPreparedResult>> result) {
  if (generation != prepare_generation_ || !prepare_state_) {
    return;
  }

  if (prepare_state_->latest_state_in_flight > 0) {
    --prepare_state_->latest_state_in_flight;
  }

  if (result.is_error()) {
    auto error = result.move_as_error();
    reset_prepare_state();
    release_leader_heartbeat();
    LOG(ERROR) << "Error preparing latest_account_states chunk: " << error;
    alarm_timestamp() = td::Timestamp::in(1.0);
    return;
  }

  prepare_state_->latest_account_state_chunks[chunk_index] = result.move_as_ok()->rows;
  ++prepare_state_->latest_state_completed;

  spawn_next_latest_account_state_chunks();
  maybe_commit_prepared_batch();
}

void InsertBatchPostgres::maybe_commit_prepared_batch() {
  if (!prepare_state_) {
    return;
  }
  if (prepare_state_->latest_state_completed != prepare_state_->latest_state_total_chunks) {
    return;
  }

  std::size_t total_rows = 0;
  for (const auto& chunk_rows : prepare_state_->latest_account_state_chunks) {
    total_rows += chunk_rows.size();
  }
  prepared_batch_->latest_account_states.clear();
  prepared_batch_->latest_account_states.reserve(total_rows);
  for (auto& chunk_rows : prepare_state_->latest_account_state_chunks) {
    std::move(chunk_rows.begin(), chunk_rows.end(), std::back_inserter(prepared_batch_->latest_account_states));
  }
  prepare_state_.reset();

  try {
    commit_prepared_batch();
  } catch (const pqxx::integrity_constraint_violation &e) {
    LOG(WARNING) << "Error COPY to PG: " << e.what();
    LOG(WARNING) << "Apparently this block already exists in the database. Nevertheless we retry with INSERT ... ON CONFLICT ...";
    with_copy_ = false;
    g_statistics.record_count(INSERT_CONFLICT);
    alarm_timestamp() = td::Timestamp::now();
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error inserting batch: " << e.what();
    alarm_timestamp() = td::Timestamp::in(1.0);
  }
}

void InsertBatchPostgres::commit_prepared_batch() {
  SCOPE_EXIT {
    release_leader_heartbeat();
  };

  // Keep Postgres as the durable progress source without holding a Postgres
  // transaction open while Kvrocks performs network writes.
  insert_kvrocks();
  const bool kvrocks_state_only = kvrocks_ != nullptr;

  td::Timer connect_timer;
  pqxx::connection c(connection_string_);
  connect_timer.pause();

  td::Timer data_timer;
  pqxx::work txn(c);
  if (disable_progress_advance_) {
    txn.exec0("SET LOCAL ton_indexer.advance_progress = 'off'");
  }

  insert_blocks(txn, with_copy_);
  insert_shard_state(txn, with_copy_);
  insert_transactions(txn, with_copy_);
  insert_messages(txn, with_copy_);
  if (!kvrocks_state_only) {
    insert_account_states(txn, with_copy_);
    insert_message_contents(txn);
  }
  insert_jetton_transfers(txn, with_copy_);
  insert_jetton_burns(txn, with_copy_);
  insert_nft_transfers(txn, with_copy_);
  insert_nominator_pool_events(txn, with_copy_);
  insert_traces(txn, with_copy_);
  if (!kvrocks_state_only) {
    insert_contract_methods(txn);
  }
  data_timer.pause();

  td::Timer states_timer;
  std::string upsert_query;
  if (!kvrocks_state_only) {
    upsert_query += insert_jetton_masters(txn);
    upsert_query += insert_jetton_wallets(txn);
    upsert_query += insert_nft_collections(txn);
    upsert_query += insert_getgems_nft_auctions(txn);
    upsert_query += insert_getgems_nft_sales(txn);
    upsert_query += insert_nft_items(txn);
    upsert_query += insert_multisig_contracts(txn);
    upsert_query += insert_multisig_orders(txn);
    upsert_query += insert_dedust_pools(txn);
    upsert_query += insert_latest_account_states(txn);
    upsert_query += insert_vesting(txn);
    upsert_query += insert_nominator_pools(txn);
    upsert_query += insert_telemint(txn);
  }

  td::Timer commit_timer{true};
  if (!upsert_query.empty()) {
    txn.exec0(upsert_query);
  }
  states_timer.pause();

  commit_timer.resume();
  if (!no_leader_) {
    ensure_still_leader(txn, worker_id_);
  }
  txn.commit();
  commit_timer.pause();

  drop_old_partitions_after_commit(c);

  for (auto& task : insert_tasks_) {
    task.promise_.set_result(get_insert_result());
  }
  promise_.set_result(get_insert_result());

  g_statistics.record_time(INSERT_BATCH_CONNECT, connect_timer.elapsed() * 1e3);
  g_statistics.record_time(INSERT_BATCH_EXEC_DATA, data_timer.elapsed() * 1e3);
  g_statistics.record_time(INSERT_BATCH_EXEC_STATES, states_timer.elapsed() * 1e3);
  g_statistics.record_time(INSERT_BATCH_COMMIT, commit_timer.elapsed() * 1e3);

  stop();
}

void InsertBatchPostgres::insert_kvrocks() {
  if (!kvrocks_) {
    return;
  }
  if (!prepared_batch_) {
    throw std::runtime_error("Kvrocks insert requested without a prepared batch");
  }

  td::Timer kvrocks_timer;
  double exec_elapsed_millis = 0.0;
  auto kvrocks_batch = make_kvrocks_state_batch(*prepared_batch_);
  try {
    kvrocks_state::KvrocksBatchWriter writer(*kvrocks_, kvrocks_batch);
    writer.write();
    exec_elapsed_millis = writer.exec_elapsed_millis();
  } catch (const sw::redis::Error& e) {
    if (!kvrocks_state::is_kvrocks_no_script_error(e)) {
      throw;
    }

    LOG(WARNING) << "Kvrocks script cache is missing, reloading scripts and retrying batch: " << e.what();
    td::Timer script_load_timer;
    kvrocks_state::load_kvrocks_scripts(*kvrocks_);
    script_load_timer.pause();

    kvrocks_state::KvrocksBatchWriter writer(*kvrocks_, kvrocks_batch);
    writer.write();
    exec_elapsed_millis = script_load_timer.elapsed() * 1e3 + writer.exec_elapsed_millis();
  }
  kvrocks_timer.pause();

  auto total_ms = static_cast<std::uint64_t>(kvrocks_timer.elapsed() * 1e3);
  auto exec_ms = static_cast<std::uint64_t>(exec_elapsed_millis);
  auto prepare_ms = total_ms > exec_ms ? total_ms - exec_ms : 0;
  g_statistics.record_time(INSERT_BATCH_KVROCKS, total_ms);
  g_statistics.record_time(INSERT_BATCH_KVROCKS_PREPARE, prepare_ms);
  g_statistics.record_time(INSERT_BATCH_KVROCKS_EXEC, exec_ms);
}

void InsertBatchPostgres::reset_prepare_state(bool keep_prepared_batch) {
  prepare_state_.reset();
  if (!keep_prepared_batch) {
    prepared_batch_.reset();
  }
}

void InsertBatchPostgres::acquire_leader_heartbeat() {
  if (!leader_heartbeat_guard_) {
    leader_heartbeat_guard_.emplace(leader_heartbeat_);
  }
}

void InsertBatchPostgres::release_leader_heartbeat() {
  leader_heartbeat_guard_.reset();
}

std::string InsertBatchPostgres::stringify(schema::ComputeSkipReason compute_skip_reason) {
  switch (compute_skip_reason) {
      case schema::ComputeSkipReason::cskip_no_state: return "no_state";
      case schema::ComputeSkipReason::cskip_bad_state: return "bad_state";
      case schema::ComputeSkipReason::cskip_no_gas: return "no_gas";
      case schema::ComputeSkipReason::cskip_suspended: return "suspended";
  };
  UNREACHABLE();
}

std::string InsertBatchPostgres::stringify(schema::AccStatusChange acc_status_change) {
  switch (acc_status_change) {
      case schema::AccStatusChange::acst_unchanged: return "unchanged";
      case schema::AccStatusChange::acst_frozen: return "frozen";
      case schema::AccStatusChange::acst_deleted: return "deleted";
  };
  UNREACHABLE();
}

std::string InsertBatchPostgres::stringify(schema::AccountStatus account_status)
{
  switch (account_status) {
      case schema::AccountStatus::frozen: return "frozen";
      case schema::AccountStatus::uninit: return "uninit";
      case schema::AccountStatus::active: return "active";
      case schema::AccountStatus::nonexist: return "nonexist";
  };
  UNREACHABLE();
}

std::string InsertBatchPostgres::stringify(schema::Trace::State state) {
  switch (state) {
    case schema::Trace::State::complete: return "complete";
    case schema::Trace::State::pending: return "pending";
    case schema::Trace::State::broken: return "broken";
  };
  UNREACHABLE();
}

void InsertBatchPostgres::insert_blocks(pqxx::work &txn, bool with_copy) {
  std::initializer_list<std::string_view> columns = {
    "workchain", "shard", "seqno", "root_hash", "file_hash", "mc_block_seqno",
    "global_id", "version", "after_merge", "before_split", "after_split", "want_merge", "want_split", "key_block",
    "vert_seqno_incr", "flags", "gen_utime", "start_lt", "end_lt", "validator_list_hash_short", "gen_catchain_seqno",
    "min_ref_mc_seqno", "prev_key_block_seqno", "vert_seqno", "master_ref_seqno", "rand_seed", "created_by", "tx_count", "prev_blocks"
  };
  PopulateTableStream stream(txn, "blocks", columns, 1000, with_copy);
  if (!with_copy) {
    stream.setConflictDoNothing();
  }

  // Prepare data
  for (const auto& task : insert_tasks_) {
    for (const auto& block : task.parsed_block_->blocks_) {
      auto tuple = std::make_tuple(
        block.workchain,
        block.shard,
        block.seqno,
        block.root_hash,
        block.file_hash,
        block.mc_block_seqno,
        block.global_id,
        block.version,
        block.after_merge,
        block.before_split,
        block.after_split,
        block.want_merge,
        block.want_split,
        block.key_block,
        block.vert_seqno_incr,
        block.flags,
        block.gen_utime,
        block.start_lt,
        block.end_lt,
        block.validator_list_hash_short,
        block.gen_catchain_seqno,
        block.min_ref_mc_seqno,
        block.prev_key_block_seqno,
        block.vert_seqno,
        block.master_ref_seqno,
        block.rand_seed,
        block.created_by,
        block.transactions.size(),
        block.prev_blocks
      );
      stream.insert_row(std::move(tuple));
    }
  }
  stream.finish();
}


void InsertBatchPostgres::insert_shard_state(pqxx::work &txn, bool with_copy) {
  std::initializer_list<std::string_view> columns = {
    "mc_seqno", "workchain", "shard", "seqno"
  };
  PopulateTableStream stream(txn, "shard_state", columns, 1000);
  if (!with_copy) {
    stream.setConflictDoNothing();
  }

  for (const auto& task : insert_tasks_) {
    for (const auto& shard : task.parsed_block_->shard_state_) {
      auto tuple = std::make_tuple(
        shard.mc_seqno,
        shard.workchain,
        shard.shard,
        shard.seqno
      );
      stream.insert_row(std::move(tuple));
    }
  }
  stream.finish();
}

template<typename... Tuples>
using tuple_cat_t = decltype(std::tuple_cat(std::declval<Tuples>()...));

void InsertBatchPostgres::insert_transactions(pqxx::work &txn, bool with_copy) {
  std::initializer_list<std::string_view> columns = {
    "account", "hash", "lt", "block_workchain", "block_shard", "block_seqno", "mc_block_seqno", "trace_id",
    "prev_trans_hash", "prev_trans_lt", "now", "orig_status", "end_status", "total_fees", "total_fees_extra_currencies",
    "account_state_hash_before", "account_state_hash_after", "descr", "aborted", "destroyed", "credit_first", "is_tock",
    "installed", "storage_fees_collected", "storage_fees_due", "storage_status_change", "credit_due_fees_collected",
    "credit", "credit_extra_currencies", "compute_skipped", "skipped_reason", "compute_success", "compute_msg_state_used",
    "compute_account_activated", "compute_gas_fees", "compute_gas_used", "compute_gas_limit", "compute_gas_credit",
    "compute_mode", "compute_exit_code", "compute_exit_arg", "compute_vm_steps", "compute_vm_init_state_hash",
    "compute_vm_final_state_hash", "action_success", "action_valid", "action_no_funds", "action_status_change",
    "action_total_fwd_fees", "action_total_action_fees", "action_result_code", "action_result_arg", "action_tot_actions",
    "action_spec_actions", "action_skipped_actions", "action_msgs_created", "action_action_list_hash",
    "action_tot_msg_size_cells", "action_tot_msg_size_bits", "bounce", "bounce_msg_size_cells", "bounce_msg_size_bits",
    "bounce_req_fwd_fees", "bounce_msg_fees", "bounce_fwd_fees", "split_info_cur_shard_pfx_len", "split_info_acc_split_depth",
    "split_info_this_addr", "split_info_sibling_addr"
  };

  PopulateTableStream stream(txn, "transactions", columns, 1000, with_copy);
  if (!with_copy) {
    stream.setConflictDoNothing();
  }

  using storage_ph_tuple = std::tuple<std::optional<td::RefInt256>, std::optional<td::RefInt256>, std::optional<std::string>>;
  auto store_storage_ph = [&](const schema::TrStoragePhase& storage_ph) -> storage_ph_tuple {
    return {storage_ph.storage_fees_collected, storage_ph.storage_fees_due, stringify(storage_ph.status_change)};
  };
  auto store_empty_storage_ph = [&]() -> storage_ph_tuple {
    return {std::nullopt, std::nullopt, std::nullopt};
  };

  using credit_ph_tuple = std::tuple<std::optional<td::RefInt256>, std::optional<td::RefInt256>, std::optional<std::string>>;
  auto store_credit_ph = [](const schema::TrCreditPhase& credit_ph) -> credit_ph_tuple {
    return {credit_ph.due_fees_collected, credit_ph.credit.grams, extra_currencies_to_json_string(credit_ph.credit.extra_currencies)};
  };
  auto store_empty_credit_ph = [&]() -> credit_ph_tuple {
    return {std::nullopt, std::nullopt, std::nullopt};
  };

  using compute_ph_tuple = std::tuple<std::optional<bool>, std::optional<std::string>, std::optional<bool>, std::optional<bool>, std::optional<bool>, 
                                      std::optional<td::RefInt256>, std::optional<uint64_t>, std::optional<uint64_t>, std::optional<size_t>, 
                                      std::optional<int32_t>, std::optional<int32_t>, std::optional<int32_t>, std::optional<uint32_t>, 
                                      std::optional<td::Bits256>, std::optional<td::Bits256>>;
  auto store_compute_ph = [&](const schema::TrComputePhase& compute_ph) -> compute_ph_tuple {
    return std::visit([&](auto&& arg) -> compute_ph_tuple {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, schema::TrComputePhase_skipped>) {
        return {true, stringify(arg.reason), std::nullopt, std::nullopt, std::nullopt, 
                std::nullopt, std::nullopt, std::nullopt, std::nullopt, 
                std::nullopt, std::nullopt, std::nullopt, std::nullopt, 
                std::nullopt, std::nullopt};
      } else if constexpr (std::is_same_v<T, schema::TrComputePhase_vm>) {
        return {false, std::nullopt, arg.success, arg.msg_state_used, arg.account_activated, 
                arg.gas_fees, arg.gas_used, arg.gas_limit, arg.gas_credit, 
                arg.mode, arg.exit_code, arg.exit_arg, arg.vm_steps, 
                arg.vm_init_state_hash, arg.vm_final_state_hash};
      } else {
        UNREACHABLE();
      }
    }, compute_ph);
  };
  auto store_empty_compute_ph = [&]() -> compute_ph_tuple {
    return {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, 
            std::nullopt, std::nullopt, std::nullopt, std::nullopt, 
            std::nullopt, std::nullopt, std::nullopt, std::nullopt, 
            std::nullopt, std::nullopt};
  };

  using action_ph_tuple = std::tuple<std::optional<bool>, std::optional<bool>, std::optional<bool>, std::optional<std::string>, 
                                     std::optional<td::RefInt256>, std::optional<td::RefInt256>, std::optional<int32_t>, std::optional<int32_t>, 
                                     std::optional<uint16_t>, std::optional<uint16_t>, std::optional<uint16_t>, std::optional<uint16_t>, 
                                     std::optional<td::Bits256>, std::optional<uint64_t>, std::optional<uint64_t>>;
  auto store_action_ph = [&](const schema::TrActionPhase& action) -> action_ph_tuple {
    return {action.success, action.valid, action.no_funds, stringify(action.status_change), 
            action.total_fwd_fees, action.total_action_fees, action.result_code, action.result_arg, 
            action.tot_actions, action.spec_actions, action.skipped_actions, action.msgs_created, 
            action.action_list_hash, action.tot_msg_size.cells, action.tot_msg_size.bits};
  };
  auto store_empty_action_ph = [&]() -> action_ph_tuple {
    return {std::nullopt, std::nullopt, std::nullopt, std::nullopt, 
            std::nullopt, std::nullopt, std::nullopt, std::nullopt, 
            std::nullopt, std::nullopt, std::nullopt, std::nullopt, 
            std::nullopt, std::nullopt, std::nullopt};
  };

  using bounce_ph_tuple = std::tuple<std::optional<std::string>, std::optional<uint64_t>, std::optional<uint64_t>, 
                                     std::optional<td::RefInt256>, std::optional<td::RefInt256>, std::optional<td::RefInt256>>;
  auto store_bounce_ph = [&](const schema::TrBouncePhase& bounce) -> bounce_ph_tuple {
    return std::visit([&](auto&& arg) -> bounce_ph_tuple {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, schema::TrBouncePhase_negfunds>) {
        return {"negfunds", std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt};
      } else if constexpr (std::is_same_v<T, schema::TrBouncePhase_nofunds>) {
        return {"nofunds", arg.msg_size.cells, arg.msg_size.bits, arg.req_fwd_fees, std::nullopt, std::nullopt};
      } else if constexpr (std::is_same_v<T, schema::TrBouncePhase_ok>) {
        return {"ok", arg.msg_size.cells, arg.msg_size.bits, std::nullopt, arg.msg_fees, arg.fwd_fees};
      } else {
        UNREACHABLE();
      }
    }, bounce);
  };
  auto store_empty_bounce_ph = [&]() -> bounce_ph_tuple {
    return {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt};
  };

  using split_info_tuple = std::tuple<std::optional<uint32_t>, std::optional<uint32_t>, std::optional<td::Bits256>, std::optional<td::Bits256>>;
  auto store_split_info = [&](const schema::SplitMergeInfo& split_info) -> split_info_tuple {
    return {split_info.cur_shard_pfx_len, split_info.acc_split_depth, split_info.this_addr, split_info.sibling_addr};
  };
  auto store_empty_split_info = [&]() -> split_info_tuple {
    return {std::nullopt, std::nullopt, std::nullopt, std::nullopt};
  };

  using transaction_descr_begin_tuple = std::tuple<std::string, std::optional<bool>, std::optional<bool>, 
                                                   std::optional<bool>, std::optional<bool>, std::optional<bool>>;

  using transaction_descr = tuple_cat_t<transaction_descr_begin_tuple, storage_ph_tuple, credit_ph_tuple, 
                                        compute_ph_tuple, action_ph_tuple, bounce_ph_tuple, split_info_tuple>;

  auto store_transaction_descr_ord = [&](const schema::TransactionDescr_ord& descr) -> transaction_descr {
    auto begin_tuple = std::make_tuple("ord", descr.aborted, descr.destroyed, descr.credit_first, std::nullopt, std::nullopt);
    auto storage_ph = descr.storage_ph ? store_storage_ph(descr.storage_ph.value()) : store_empty_storage_ph();
    auto credit_ph = descr.credit_ph ? store_credit_ph(descr.credit_ph.value()) : store_empty_credit_ph();
    auto compute_ph = store_compute_ph(descr.compute_ph);
    auto action_ph = descr.action ? store_action_ph(descr.action.value()) : store_empty_action_ph();
    auto bounce_ph = descr.bounce ? store_bounce_ph(descr.bounce.value()) : store_empty_bounce_ph();
    auto split_info = store_empty_split_info();
    return std::tuple_cat(std::move(begin_tuple), std::move(storage_ph), std::move(credit_ph), 
                          std::move(compute_ph), std::move(action_ph), std::move(bounce_ph), std::move(split_info));
  };
  auto store_transaction_descr_storage = [&](const schema::TransactionDescr_storage& descr) -> transaction_descr {
    auto begin_tuple = std::make_tuple("storage", std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt);
    auto storage_ph = store_storage_ph(descr.storage_ph);
    auto credit_ph = store_empty_credit_ph();
    auto compute_ph = store_empty_compute_ph();
    auto action_ph = store_empty_action_ph();
    auto bounce_ph = store_empty_bounce_ph();
    auto split_info = store_empty_split_info();
    return std::tuple_cat(std::move(begin_tuple), std::move(storage_ph), std::move(credit_ph), 
                          std::move(compute_ph), std::move(action_ph), std::move(bounce_ph), std::move(split_info));
  };
  auto store_transaction_descr_tick_tock = [&](const schema::TransactionDescr_tick_tock& descr) -> transaction_descr {
    auto begin_tuple = std::make_tuple("tick_tock", descr.aborted, descr.destroyed, std::nullopt, descr.is_tock, std::nullopt);
    auto storage_ph = store_storage_ph(descr.storage_ph);
    auto credit_ph = store_empty_credit_ph();
    auto compute_ph = store_compute_ph(descr.compute_ph);
    auto action_ph = descr.action ? store_action_ph(descr.action.value()) : store_empty_action_ph();
    auto bounce_ph = store_empty_bounce_ph();
    auto split_info = store_empty_split_info();
    return std::tuple_cat(std::move(begin_tuple), std::move(storage_ph), std::move(credit_ph), 
                          std::move(compute_ph), std::move(action_ph), std::move(bounce_ph), std::move(split_info));
  };
  auto store_transaction_descr_split_prepare = [&](const schema::TransactionDescr_split_prepare& descr) -> transaction_descr {
    auto begin_tuple = std::make_tuple("split_prepare", descr.aborted, descr.destroyed, std::nullopt, std::nullopt, std::nullopt);
    auto storage_ph = descr.storage_ph ? store_storage_ph(descr.storage_ph.value()) : store_empty_storage_ph();
    auto credit_ph = store_empty_credit_ph();
    auto compute_ph = store_compute_ph(descr.compute_ph);
    auto action_ph = descr.action ? store_action_ph(descr.action.value()) : store_empty_action_ph();
    auto bounce_ph = store_empty_bounce_ph();
    auto split_info = store_split_info(descr.split_info);
    return std::tuple_cat(std::move(begin_tuple), std::move(storage_ph), std::move(credit_ph), 
                          std::move(compute_ph), std::move(action_ph), std::move(bounce_ph), std::move(split_info));
  };
  auto store_transaction_descr_split_install = [&](const schema::TransactionDescr_split_install& descr) -> transaction_descr {
    auto begin_tuple = std::make_tuple("split_install", std::nullopt, std::nullopt, std::nullopt, std::nullopt, descr.installed);
    auto storage_pt = store_empty_storage_ph();
    auto credit_ph = store_empty_credit_ph();
    auto compute_ph = store_empty_compute_ph();
    auto action_ph = store_empty_action_ph();
    auto bounce_ph = store_empty_bounce_ph();
    auto split_info = store_split_info(descr.split_info);
    return std::tuple_cat(std::move(begin_tuple), std::move(storage_pt), std::move(credit_ph), 
                          std::move(compute_ph), std::move(action_ph), std::move(bounce_ph), std::move(split_info));
  };
  auto store_transaction_descr_merge_prepare = [&](const schema::TransactionDescr_merge_prepare& descr) -> transaction_descr {
    auto begin_tuple = std::make_tuple("merge_prepare", descr.aborted, std::nullopt, std::nullopt, std::nullopt, std::nullopt);
    auto storage_ph = store_storage_ph(descr.storage_ph);
    auto credit_ph = store_empty_credit_ph();
    auto compute_ph = store_empty_compute_ph();
    auto action_ph = store_empty_action_ph();
    auto bounce_ph = store_empty_bounce_ph();
    auto split_info = store_split_info(descr.split_info);
    return std::tuple_cat(std::move(begin_tuple), std::move(storage_ph), std::move(credit_ph), 
                          std::move(compute_ph), std::move(action_ph), std::move(bounce_ph), std::move(split_info));
  };
  auto store_transaction_descr_merge_install = [&](const schema::TransactionDescr_merge_install& descr) -> transaction_descr {
    auto begin_tuple = std::make_tuple("merge_install", descr.aborted, descr.destroyed, std::nullopt, std::nullopt, std::nullopt);
    auto storage_ph = descr.storage_ph ? store_storage_ph(descr.storage_ph.value()) : store_empty_storage_ph();
    auto credit_ph = descr.credit_ph ? store_credit_ph(descr.credit_ph.value()) : store_empty_credit_ph();
    auto compute_ph = store_compute_ph(descr.compute_ph);
    auto action_ph = descr.action ? store_action_ph(descr.action.value()) : store_empty_action_ph();
    auto bounce_ph = store_empty_bounce_ph();
    auto split_info = store_split_info(descr.split_info);
    return std::tuple_cat(std::move(begin_tuple), std::move(storage_ph), std::move(credit_ph), 
                          std::move(compute_ph), std::move(action_ph), std::move(bounce_ph), std::move(split_info));
  };

  for (const auto& task : insert_tasks_) {
    for (const auto &blk : task.parsed_block_->blocks_) {
      for (const auto& transaction : blk.transactions) {
        auto tx_common_tuple = std::make_tuple(
          transaction.account,
          transaction.hash,
          transaction.lt,
          blk.workchain,
          blk.shard,
          blk.seqno,
          blk.mc_block_seqno,
          transaction.trace_id,
          transaction.prev_trans_hash,
          transaction.prev_trans_lt,
          transaction.now,
          stringify(transaction.orig_status),
          stringify(transaction.end_status),
          transaction.total_fees.grams,
          extra_currencies_to_json_string(transaction.total_fees.extra_currencies),
          transaction.account_state_hash_before,
          transaction.account_state_hash_after
        );
        auto descr_tuple = std::visit([&](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, schema::TransactionDescr_ord>) {
            return store_transaction_descr_ord(arg);
          } else if constexpr (std::is_same_v<T, schema::TransactionDescr_storage>) {
            return store_transaction_descr_storage(arg);
          } else if constexpr (std::is_same_v<T, schema::TransactionDescr_tick_tock>) {
            return store_transaction_descr_tick_tock(arg);
          } else if constexpr (std::is_same_v<T, schema::TransactionDescr_split_prepare>) {
            return store_transaction_descr_split_prepare(arg);
          } else if constexpr (std::is_same_v<T, schema::TransactionDescr_split_install>) {
            return store_transaction_descr_split_install(arg);
          } else if constexpr (std::is_same_v<T, schema::TransactionDescr_merge_prepare>) {
            return store_transaction_descr_merge_prepare(arg);
          } else if constexpr (std::is_same_v<T, schema::TransactionDescr_merge_install>) {
            return store_transaction_descr_merge_install(arg);
          } else {
            UNREACHABLE();
          }
        }, transaction.description);
        auto row_tuple = std::tuple_cat(std::move(tx_common_tuple), std::move(descr_tuple));
        stream.insert_row(std::move(row_tuple));
      }
    }
  }
  stream.finish();
}

void InsertBatchPostgres::insert_messages(pqxx::work &txn, bool with_copy) {
  std::initializer_list<std::string_view> columns = {"tx_hash", "tx_lt", "mc_seqno", "msg_hash", "direction", "trace_id", "source", "destination",
                                                "value", "value_extra_currencies", "fwd_fee", "ihr_fee", "extra_flags", "created_lt", "created_at",
                                                "opcode", "ihr_disabled", "bounce", "bounced", "import_fee", "body_hash", "init_state_hash", "msg_hash_norm"};
  PopulateTableStream stream(txn, "messages", columns, 1000, with_copy);
  if (!with_copy) {
    stream.setConflictDoNothing();
  }

  auto store_message = [&](const schema::Transaction& tx, const schema::Message& msg, const std::string_view& direction) {
    // msg.import_fee is defined by user and can be too large for bigint, so we need to check it
    // and if it is too large, we will insert NULL.
    // TODO: change bigint to numeric
    std::optional<int64_t> import_fee_val;
    if (msg.import_fee) {
      import_fee_val = msg.import_fee.value()->to_long();
      if (import_fee_val.value() == (~0ULL << 63)) {
        LOG(WARNING) << "Import fee of msg " << msg.hash.to_hex() << " is too large for bigint: " << msg.import_fee.value();
        import_fee_val = std::nullopt;
      }
    }
    auto tuple = std::make_tuple(
      tx.hash,
      tx.lt,
      tx.mc_seqno,
      msg.hash,
      direction,
      msg.trace_id,
      msg.source,
      msg.destination,
      msg.value ? std::make_optional(msg.value->grams) : std::nullopt,
      msg.value ? std::make_optional(extra_currencies_to_json_string(msg.value->extra_currencies)) : std::nullopt,
      msg.fwd_fee,
      msg.ihr_fee,
      msg.extra_flags,
      msg.created_lt,
      msg.created_at,
      msg.opcode,
      msg.ihr_disabled,
      msg.bounce,
      msg.bounced,
      import_fee_val,
      msg.body->get_hash(),
      msg.init_state.not_null() ? std::make_optional(msg.init_state->get_hash()) : std::nullopt,
      (msg.hash_norm.has_value() && msg.hash_norm.value() != msg.hash) ? 
            std::make_optional(msg.hash_norm.value()) : std::nullopt
    );
    stream.insert_row(std::move(tuple));
  };

  for (const auto& task : insert_tasks_) {
    for (const auto &blk : task.parsed_block_->blocks_) {
      for (const auto& transaction : blk.transactions) {
        if(transaction.in_msg.has_value()) {
          store_message(transaction, transaction.in_msg.value(), "in");
        }
        for (const auto& msg : transaction.out_msgs) {
          store_message(transaction, msg, "out");
        }
      }
    }
  }
  stream.finish();
}

void InsertBatchPostgres::insert_message_contents(pqxx::work &txn) {
  PopulateTableStream bodies_stream(txn, "message_contents", {"hash", "body"}, 1000, false);
  bodies_stream.setConflictDoNothing();

  for (const auto& row : prepared_batch_->message_contents) {
    bodies_stream.insert_row(row);
  }
  bodies_stream.finish();
}

void InsertBatchPostgres::insert_account_states(pqxx::work &txn, bool with_copy) {
  PopulateTableStream stream(txn, "account_states", {
    "hash", "account", "balance", "balance_extra_currencies", "account_status", "frozen_hash", "code_hash", "data_hash"
  }, 1000, with_copy);
  if (!with_copy) {
    stream.setConflictDoNothing();
  }

  for (const auto& task : insert_tasks_) {
    for (const auto& account_state : task.parsed_block_->account_states_) {
      if (account_state.account_status == "nonexist") {
        // nonexist account state is inserted on DB initialization
        continue;
      }
      auto tuple = std::make_tuple(
        account_state.hash,
        account_state.account,
        account_state.balance.grams,
        extra_currencies_to_json_string(account_state.balance.extra_currencies),
        account_state.account_status,
        account_state.frozen_hash,
        account_state.code_hash,
        account_state.data_hash
      );
      stream.insert_row(std::move(tuple));
    }
  }
  
  stream.finish();
}

std::string InsertBatchPostgres::insert_latest_account_states(pqxx::work &txn) {
  std::initializer_list<std::string_view> columns = {
    "account", "hash", "balance", "balance_extra_currencies", "account_status", "timestamp",
    "last_trans_hash", "last_trans_lt", "frozen_hash", "data_hash", "code_hash", "data_boc", "code_boc"
  };
  PopulateTableStream stream(txn, "latest_account_states", columns, 1000, false);
  stream.setConflictDoUpdate({"account"}, "latest_account_states.last_trans_lt < EXCLUDED.last_trans_lt");

  for (const auto& row : prepared_batch_->latest_account_states) {
    stream.insert_row(row);
  }
  return stream.get_str();
}

template <typename Row>
std::string update_existing_destroyed_rows(pqxx::work& txn,
                                           std::string_view table_name,
                                           std::initializer_list<std::string_view> columns,
                                           std::string_view key_column,
                                           std::string_view lt_column,
                                           const std::vector<Row>& rows) {
  bool has_destroyed = false;
  for (const auto& row : rows) {
    if (row.destroyed) {
      has_destroyed = true;
      break;
    }
  }
  if (!has_destroyed) {
    return "";
  }

  std::string temp_table_name = "tmp_destroyed_" + std::string(table_name);
  std::ostringstream query;
  query << "CREATE TEMP TABLE " << temp_table_name << " ON COMMIT DROP AS SELECT ";
  bool first_select_column = true;
  for (const auto& column : columns) {
    if (!first_select_column) {
      query << ", ";
    }
    query << column;
    first_select_column = false;
  }
  query << " FROM " << table_name << " WHERE false;";

  PopulateTableStream stream(txn, temp_table_name, columns, 1000, false);
  for (const auto& row : rows) {
    if (row.destroyed) {
      stream.insert_row(row);
    }
  }
  query << stream.get_str();

  query << "UPDATE " << table_name << " AS target SET ";
  bool first_set = true;
  for (const auto& column : columns) {
    if (column == key_column) {
      continue;
    }
    if (!first_set) {
      query << ", ";
    }
    query << column << " = source." << column;
    first_set = false;
  }

  query << " FROM " << temp_table_name << " AS source"
        << " WHERE target." << key_column << " = source." << key_column
        << " AND target." << lt_column << " < source." << lt_column << ";"
        << "DROP TABLE " << temp_table_name << ";";
  return query.str();
}

template <typename Row>
std::string insert_current_rows(pqxx::work& txn,
                                std::string_view table_name,
                                std::initializer_list<std::string_view> columns,
                                std::string_view key_column,
                                std::string_view lt_column,
                                std::string_view update_condition,
                                const std::vector<Row>& rows) {
  PopulateTableStream stream(txn, table_name, columns, 1000, false);
  stream.setConflictDoUpdate({key_column}, update_condition);
  for (const auto& row : rows) {
    if (!row.destroyed) {
      stream.insert_row(row);
    }
  }
  return stream.get_str() + update_existing_destroyed_rows(txn, table_name, columns, key_column, lt_column, rows);
}

std::string InsertBatchPostgres::insert_jetton_masters(pqxx::work &txn) {
  std::initializer_list<std::string_view> columns = {
    "address", "total_supply", "mintable", "admin_address", "jetton_content", 
    "jetton_wallet_code_hash", "last_transaction_lt", "code_hash", "data_hash", "destroyed"
  };

  return insert_current_rows(txn, "jetton_masters", columns, "address", "last_transaction_lt",
                             "jetton_masters.last_transaction_lt < EXCLUDED.last_transaction_lt",
                             prepared_batch_->jetton_masters);
}

std::string InsertBatchPostgres::insert_jetton_wallets(pqxx::work &txn) {
  std::initializer_list<std::string_view> columns = {
    "balance", "address", "owner", "jetton", "last_transaction_lt", "code_hash", "data_hash", "mintless_is_claimed", "destroyed"
  };

  std::string result = insert_current_rows(txn, "jetton_wallets", columns, "address", "last_transaction_lt",
                                           "jetton_wallets.last_transaction_lt < EXCLUDED.last_transaction_lt",
                                           prepared_batch_->jetton_wallets);
  if (!prepared_batch_->mintless_jetton_masters.empty()) {
    PopulateTableStream mintless_stream(txn, "mintless_jetton_masters", {"address", "is_indexed"}, 1000, false);
    mintless_stream.setConflictDoNothing();
  
    for (const auto& row : prepared_batch_->mintless_jetton_masters) {
      mintless_stream.insert_row(row);
    }
    result += mintless_stream.get_str();
  }

  return result;
}

std::string InsertBatchPostgres::insert_nft_collections(pqxx::work &txn) {
  std::initializer_list<std::string_view> columns = {
    "address", "next_item_index", "owner_address", "collection_content", "last_transaction_lt", "code_hash", "data_hash", "destroyed"
  };

  return insert_current_rows(txn, "nft_collections", columns, "address", "last_transaction_lt",
                             "nft_collections.last_transaction_lt < EXCLUDED.last_transaction_lt",
                             prepared_batch_->nft_collections);
}

std::string InsertBatchPostgres::insert_nft_items(pqxx::work &txn) {
  std::initializer_list<std::string_view> columns = {
    "address", "init", "index", "collection_address", "owner_address", "content", "last_transaction_lt", "code_hash", "data_hash", "real_owner", "destroyed"
  };
  
  auto result = insert_current_rows(txn, "nft_items", columns, "address", "last_transaction_lt",
                                    "nft_items.last_transaction_lt < EXCLUDED.last_transaction_lt",
                                    prepared_batch_->nft_items);

  std::initializer_list<std::string_view> dns_columns = {
    "nft_item_address", "nft_item_owner", "domain", "dns_next_resolver", "dns_wallet", "dns_site_adnl", "dns_storage_bag_id", "last_transaction_lt", "destroyed"
  };
  result += insert_current_rows(txn, "dns_entries", dns_columns, "nft_item_address", "last_transaction_lt",
                                "dns_entries.last_transaction_lt < EXCLUDED.last_transaction_lt",
                                prepared_batch_->dns_entries);
  return result;
}

std::string InsertBatchPostgres::insert_getgems_nft_sales(pqxx::work &txn) {
  std::initializer_list<std::string_view> columns = {
    "address", "is_complete", "created_at", "marketplace_address", "nft_address", "nft_owner_address", "full_price",
    "marketplace_fee_address", "marketplace_fee", "royalty_address", "royalty_amount",
    "sold_at", "sold_query_id", "jetton_price_dict",
    "last_transaction_lt", "code_hash", "data_hash", "destroyed"
  };
  return insert_current_rows(txn, "getgems_nft_sales", columns, "address", "last_transaction_lt",
                             "getgems_nft_sales.last_transaction_lt < EXCLUDED.last_transaction_lt",
                             prepared_batch_->getgems_nft_sales);
}

std::string InsertBatchPostgres::insert_vesting(pqxx::work &txn) {
    std::initializer_list<std::string_view> vesting_columns = {
        "address", "vesting_start_time", "vesting_total_duration", "unlock_period", 
        "cliff_duration", "vesting_total_amount", "vesting_sender_address", "owner_address",
        "last_transaction_lt", "code_hash", "data_hash", "destroyed"
    };
    std::string result = insert_current_rows(txn, "vesting_contracts", vesting_columns, "address", "last_transaction_lt",
                                             "vesting_contracts.last_transaction_lt < EXCLUDED.last_transaction_lt",
                                             prepared_batch_->vesting_contracts);

    std::initializer_list<std::string_view> whitelist_columns = {
        "vesting_contract_address", "wallet_address"
    };
    PopulateTableStream whitelist_stream(txn, "vesting_whitelist", whitelist_columns, 1000, false);
    whitelist_stream.setConflictDoNothing();
    for (const auto& row : prepared_batch_->vesting_whitelist) {
        whitelist_stream.insert_row(row);
    }

    return result + whitelist_stream.get_str();
}

std::string InsertBatchPostgres::insert_nominator_pools(pqxx::work &txn) {
    std::initializer_list<std::string_view> columns = {
        "address", "state", "nominators_count", "stake_amount_sent", "validator_amount",
        "validator_address", "validator_reward_share", "max_nominators_count", "min_validator_stake",
        "min_nominator_stake", "active_nominators", "last_transaction_lt",
        "code_hash", "data_hash", "destroyed"
    };
    return insert_current_rows(txn, "nominator_pools", columns, "address", "last_transaction_lt",
                               "nominator_pools.last_transaction_lt < EXCLUDED.last_transaction_lt",
                               prepared_batch_->nominator_pools);
}

std::string InsertBatchPostgres::insert_telemint(pqxx::work &txn) {
    std::initializer_list<std::string_view> telemint_columns = {
        "address", "token_name", "bidder_address", "bid", "bid_ts",
        "min_bid", "end_time", "beneficiary_address", "initial_min_bid",
        "max_bid", "min_bid_step", "min_extend_time", "duration",
        "royalty_numerator", "royalty_denominator", "royalty_destination",
        "last_transaction_lt", "code_hash", "data_hash", "destroyed"
    };
    return insert_current_rows(txn, "telemint_nft_items", telemint_columns, "address", "last_transaction_lt",
                               "telemint_nft_items.last_transaction_lt < EXCLUDED.last_transaction_lt",
                               prepared_batch_->telemint_nft_items);
}

std::string InsertBatchPostgres::insert_getgems_nft_auctions(pqxx::work &txn) {
  std::initializer_list<std::string_view> columns = {"address", "end_flag", "end_time", "mp_addr", "nft_addr", "nft_owner",
    "last_bid", "last_member", "min_step", "mp_fee_addr", "mp_fee_factor", "mp_fee_base", "royalty_fee_addr", "royalty_fee_factor",
    "royalty_fee_base", "max_bid", "min_bid", "created_at", "last_bid_at", "is_canceled", "activated", "step_time", "last_query_id",
    "jetton_wallet", "jetton_master", "is_broken_state", "public_key", "last_transaction_lt", "code_hash", "data_hash", "destroyed"
  };
  return insert_current_rows(txn, "getgems_nft_auctions", columns, "address", "last_transaction_lt",
                             "getgems_nft_auctions.last_transaction_lt < EXCLUDED.last_transaction_lt",
                             prepared_batch_->getgems_nft_auctions);
}

std::string InsertBatchPostgres::insert_multisig_contracts(pqxx::work &txn) {
  std::initializer_list<std::string_view> columns = {
    "address", "next_order_seqno", "threshold", "signers", "proposers", "last_transaction_lt", "code_hash", "data_hash", "destroyed"
  };

  return insert_current_rows(txn, "multisig", columns, "address", "last_transaction_lt",
                             "multisig.last_transaction_lt < EXCLUDED.last_transaction_lt",
                             prepared_batch_->multisig_contracts);
}

std::string InsertBatchPostgres::insert_multisig_orders(pqxx::work &txn) {
  std::initializer_list<std::string_view> columns = {
    "address", "multisig_address", "order_seqno", "threshold", "sent_for_execution", "approvals_mask", "approvals_num",
    "expiration_date", "order_boc", "signers", "last_transaction_lt", "code_hash", "data_hash", "destroyed"
  };

  return insert_current_rows(txn, "multisig_orders", columns, "address", "last_transaction_lt",
                             "multisig_orders.last_transaction_lt < EXCLUDED.last_transaction_lt",
                             prepared_batch_->multisig_orders);
}

std::string InsertBatchPostgres::insert_dedust_pools(pqxx::work &txn) {
  std::initializer_list<std::string_view> pools_column = {
    "address", "asset_1", "asset_2", "reserve_1", "reserve_2", "pool_type", "dex", "fee", "last_transaction_lt", "code_hash", "data_hash", "destroyed"
  };

  return insert_current_rows(txn, "dex_pools", pools_column, "address", "last_transaction_lt",
                             "dex_pools.last_transaction_lt < EXCLUDED.last_transaction_lt",
                             prepared_batch_->dedust_pools);
}

void InsertBatchPostgres::insert_jetton_transfers(pqxx::work &txn, bool with_copy) {
  std::initializer_list<std::string_view> columns = {
    "tx_hash", "tx_lt", "tx_now", "tx_aborted", "mc_seqno", "query_id", "amount", "source", "destination", "jetton_wallet_address",
    "jetton_master_address", "response_destination", "custom_payload", "forward_ton_amount", "forward_payload", "trace_id"
  };
  PopulateTableStream stream(txn, "jetton_transfers", columns, 1000, with_copy);
  if (!with_copy) {
    stream.setConflictDoNothing();
  }

  for (const auto& row : prepared_batch_->jetton_transfers) {
    stream.insert_row(row);
  }
  stream.finish();
}

void InsertBatchPostgres::insert_nominator_pool_events(pqxx::work &txn, bool with_copy) {
  std::initializer_list<std::string_view> columns = {
    "tx_hash", "tx_lt", "tx_now", "mc_seqno", "trace_id", "pool_address", "nominator_address",
    "event_index", "event_type", "amount", "balance_delta", "pending_balance_delta",
    "balance_before", "balance_after", "pending_balance_before", "pending_balance_after",
    "withdraw_request_before", "withdraw_request_after"
  };
  PopulateTableStream stream(txn, "nominator_pool_events", columns, 1000, with_copy);
  if (!with_copy) {
    stream.setConflictDoNothing();
  }

  for (const auto& task : insert_tasks_) {
    for (const auto& event : task.parsed_block_->get_events<schema::NominatorPoolEvent>()) {
      auto tuple = std::make_tuple(
        event.transaction_hash,
        event.transaction_lt,
        event.transaction_now,
        event.mc_seqno,
        event.trace_id,
        event.pool_address,
        event.nominator_address,
        event.event_index,
        event.event_type,
        event.amount,
        event.balance_delta,
        event.pending_balance_delta,
        event.balance_before,
        event.balance_after,
        event.pending_balance_before,
        event.pending_balance_after,
        event.withdraw_request_before,
        event.withdraw_request_after
      );
      stream.insert_row(std::move(tuple));
    }
  }
  stream.finish();
}


void InsertBatchPostgres::insert_jetton_burns(pqxx::work &txn, bool with_copy) {
  std::initializer_list<std::string_view> columns = {
    "tx_hash", "tx_lt", "tx_now", "tx_aborted", "mc_seqno", "query_id", "owner", "jetton_wallet_address", "jetton_master_address",
    "amount", "response_destination", "custom_payload", "trace_id"
  };
  PopulateTableStream stream(txn, "jetton_burns", columns, 1000, with_copy);
  if (!with_copy) {
    stream.setConflictDoNothing();
  }

  for (const auto& row : prepared_batch_->jetton_burns) {
    stream.insert_row(row);
  }
  stream.finish();
}

void InsertBatchPostgres::insert_nft_transfers(pqxx::work &txn, bool with_copy) {
  std::initializer_list<std::string_view> columns = {
    "tx_hash", "tx_lt", "tx_now", "tx_aborted", "mc_seqno", "query_id", "nft_item_address", "nft_item_index", "nft_collection_address",
    "old_owner", "new_owner", "response_destination", "custom_payload", "forward_amount", "forward_payload", "trace_id"
  };
  PopulateTableStream stream(txn, "nft_transfers", columns, 1000, with_copy);
  if (!with_copy) {
    stream.setConflictDoNothing();
  }

  for (const auto& row : prepared_batch_->nft_transfers) {
    stream.insert_row(row);
  }
  stream.finish();
}

void InsertBatchPostgres::insert_traces(pqxx::work &txn, bool with_copy) {
  std::initializer_list<std::string_view> columns = { "trace_id", "external_hash", "external_hash_norm", "mc_seqno_start", "mc_seqno_end", 
    "start_lt", "start_utime", "end_lt", "end_utime", "state", "pending_edges_", "edges_", "nodes_" };

  PopulateTableStream stream(txn, "traces", columns, 1000, with_copy);
  if (!with_copy) {
    stream.setConflictDoUpdate({"trace_id", "mc_seqno_end"}, "traces.end_lt < EXCLUDED.end_lt");
  }

  for (const auto& row : prepared_batch_->traces) {
    stream.insert_row(row);
  }
  stream.finish();
}

void InsertBatchPostgres::insert_contract_methods(pqxx::work &txn) {
  std::initializer_list<std::string_view> columns = {
    "code_hash", "methods"
  };

  PopulateTableStream stream(txn, "contract_methods", columns, 1000, false);
  stream.setConflictDoNothing(); // don't update existings
  for (const auto& row : prepared_batch_->contract_methods) {
    stream.insert_row(row);
  }

  stream.finish();
}

//
// InsertManagerPostgres
//

void InsertManagerPostgres::start_up() {
  std::optional<Version> db_version{};
  try {
    db_version = get_current_db_version(credential_.get_connection_string());
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error getting database version: " << e.what();
    std::_Exit(2);
  }

  if (!db_version.has_value()) {
    LOG(ERROR) << "Database version is not set, run `ton-index-postgres-migrate` to prepare the database";
    std::_Exit(2);
  }
  if (*db_version != latest_version) {
    LOG(ERROR) << "Database version mismatch: expected " << latest_version.str() << ", got " << db_version->str();
    LOG(ERROR) << "Run `ton-index-postgres-migrate` to update the database schema";
    std::_Exit(2);
  }

  if (kvrocks_config_.enabled) {
    try {
      KvrocksClient kvrocks(kvrocks_config_);
      kvrocks_ = kvrocks.make_redis();
      auto ping_response = kvrocks_->ping();
      if (ping_response != "PONG") {
        throw std::runtime_error("unexpected Kvrocks PING response: " + ping_response);
      }
      kvrocks_state::load_kvrocks_scripts(*kvrocks_);
      LOG(INFO) << "Kvrocks connection ready: " << kvrocks_config_.describe()
                << "; state and kv tables will be written to Kvrocks only";
    } catch (const std::exception &e) {
      LOG(ERROR) << "Error connecting to Kvrocks: " << e.what();
      std::_Exit(2);
    }
  } else {
    LOG(INFO) << "Kvrocks connection is not configured; Postgres-only state writes remain enabled";
  }

  if (partition_config_.enabled) {
    LOG(INFO) << "Postgres hot partition management enabled: partition_size_mc_seqnos="
              << partition_config_.partition_size_mc_seqnos
              << ", retention_mc_seqnos=" << partition_config_.retention_mc_seqnos
              << ", precreate_count=" << partition_config_.precreate_count;
  }
  if (disable_progress_advance_) {
    LOG(INFO) << "Postgres indexer progress advancement is disabled for this writer";
  }
  
  worker_id_ = get_worker_id();
  if (no_leader_) {
    LOG(WARNING) << "Postgres leader/standby coordination is disabled";
  } else {
    leader_heartbeat_ = td::actor::create_actor<PostgresLeaderHeartbeat>(
        td::actor::ActorOptions().with_name("postgres_leader_heartbeat").with_poll(), credential_.get_connection_string(), worker_id_);
  }
  alarm_timestamp() = td::Timestamp::in(1.0);
}

void InsertManagerPostgres::set_max_data_depth(std::int32_t value) {
  LOG(INFO) << "InsertManagerPostgres max_data_depth set to " << value; 
  max_data_depth_ = value;
}

void InsertManagerPostgres::set_latest_states_prepare_parallelism(std::int32_t value) {
  latest_states_prepare_parallelism_ = std::max<std::int32_t>(1, value);
  LOG(INFO) << "InsertManagerPostgres latest_states_prepare_parallelism set to " << latest_states_prepare_parallelism_;
}

void InsertManagerPostgres::set_latest_states_prepare_chunk_size(std::int32_t value) {
  latest_states_prepare_chunk_size_ = std::max<std::int32_t>(1, value);
  LOG(INFO) << "InsertManagerPostgres latest_states_prepare_chunk_size set to " << latest_states_prepare_chunk_size_;
}

void InsertManagerPostgres::create_insert_actor(std::vector<InsertTaskStruct> insert_tasks,
                                                td::Promise<InsertManagerInterface::InsertResult> promise) {
  td::actor::ActorId<PostgresLeaderHeartbeat> leader_heartbeat;
  if (!no_leader_) {
    if (leader_heartbeat_.empty() || !leader_heartbeat_.is_alive()) {
      leader_heartbeat_ = td::actor::create_actor<PostgresLeaderHeartbeat>(
          td::actor::ActorOptions().with_name("postgres_leader_heartbeat").with_poll(), credential_.get_connection_string(), worker_id_);
    }
    leader_heartbeat = td::actor::actor_dynamic_cast<PostgresLeaderHeartbeat>(leader_heartbeat_.get());
  }
  td::actor::create_actor<InsertBatchPostgres>(
      "insert_batch_postgres",
      credential_,
      kvrocks_,
      leader_heartbeat,
      worker_id_,
      std::move(insert_tasks),
      std::move(promise),
      max_data_depth_,
      latest_states_prepare_parallelism_,
      latest_states_prepare_chunk_size_,
      partition_config_,
      no_leader_,
      disable_progress_advance_).release();
}

void InsertManagerPostgres::get_existing_seqnos(td::Promise<std::vector<std::uint32_t>> promise, std::int32_t from_seqno, std::int32_t to_seqno) {
  LOG(INFO) << "Reading existing seqnos";
  std::vector<std::uint32_t> existing_mc_seqnos;
  try {
    pqxx::connection c(credential_.get_connection_string());
    pqxx::work txn(c);
    td::StringBuilder sb;
    sb << "select seqno from blocks where workchain = -1";
    if (from_seqno > 0) {
      sb << " and seqno >= " << from_seqno;
    }
    if (to_seqno > 0) {
      sb << " and seqno <= " << to_seqno;
    }
    for (auto [seqno]: txn.query<std::uint32_t>(sb.as_cslice().str())) {
      existing_mc_seqnos.push_back(seqno);
    }
    promise.set_result(std::move(existing_mc_seqnos));
  } catch (const std::exception &e) {
    promise.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error selecting from PG: " << e.what()));
  }
}

void InsertManagerPostgres::ensure_resume_state_initialized(td::Promise<bool> promise, std::int32_t from_seqno) {
  LOG(INFO) << "Ensuring Postgres resume state is initialized";
  try {
    pqxx::connection c(credential_.get_connection_string());
    pqxx::work txn(c);
    constexpr std::int32_t first_indexable_mc_seqno = 1;
    const auto initial_from_seqno = from_seqno >= first_indexable_mc_seqno ? from_seqno : first_indexable_mc_seqno;
    const auto initial_finalized_seqno = initial_from_seqno - 1;
    auto inserted = txn.exec(
        "INSERT INTO ton_indexer_progress(id, finalized_mc_seqno, updated_at) "
        "VALUES (1, $1, NOW()) "
        "ON CONFLICT (id) DO NOTHING "
        "RETURNING id",
        pqxx::params{initial_finalized_seqno});
    txn.commit();

    const auto initialized = !inserted.empty();
    if (initialized) {
      LOG(INFO) << "Initialized Postgres resume state with finalized_mc_seqno " << initial_finalized_seqno;
    }
    promise.set_result(initialized);
  } catch (const std::exception &e) {
    promise.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error initializing Postgres resume state: " << e.what()));
  }
}

// Returns the next seqno for normal indexing. The first process that sees an
// empty ton_indexer_progress table seeds it from --from, then fast-forwards it
// through the already present contiguous masterchain blocks. Later restarts
// always resume from persisted finalized_mc_seqno + 1 and do not trust --from.
void InsertManagerPostgres::get_resume_seqno(td::Promise<InsertManagerInterface::ResumeState> promise,
                                             std::int32_t from_seqno, std::int32_t to_seqno) {
  LOG(INFO) << "Reading Postgres resume seqno";
  try {
    pqxx::connection c(credential_.get_connection_string());
    pqxx::work txn(c);
    constexpr std::int32_t first_indexable_mc_seqno = 1;
    const auto initial_from_seqno = from_seqno >= first_indexable_mc_seqno ? from_seqno : first_indexable_mc_seqno;
    const auto initial_finalized_seqno = initial_from_seqno - 1;
    auto inserted = txn.exec(
        "INSERT INTO ton_indexer_progress(id, finalized_mc_seqno, updated_at) "
        "VALUES (1, $1, NOW()) "
        "ON CONFLICT (id) DO NOTHING "
        "RETURNING finalized_mc_seqno",
        pqxx::params{initial_finalized_seqno});

    std::int32_t finalized_seqno = initial_finalized_seqno;
    const auto initialized_from_cli = !inserted.empty();
    if (initialized_from_cli) {
      finalized_seqno = inserted[0][0].as<std::int32_t>();
    } else {
      auto rows = txn.exec("SELECT finalized_mc_seqno FROM ton_indexer_progress WHERE id = 1");
      if (rows.empty()) {
        throw std::runtime_error("ton_indexer_progress row is missing");
      }
      finalized_seqno = rows[0][0].as<std::int32_t>();
    }

    const auto finalized_before_fast_forward = finalized_seqno;
    auto advanced = txn.exec("SELECT advance_ton_indexer_progress()");
    if (advanced.empty() || advanced[0][0].is_null()) {
      throw std::runtime_error("advance_ton_indexer_progress returned no finalized seqno");
    }
    finalized_seqno = advanced[0][0].as<std::int32_t>();
    txn.commit();

    const auto resume_seqno = finalized_seqno < first_indexable_mc_seqno
        ? static_cast<std::uint32_t>(first_indexable_mc_seqno)
        : static_cast<std::uint32_t>(finalized_seqno + 1);
    if (to_seqno > 0 && resume_seqno > static_cast<std::uint32_t>(to_seqno)) {
      LOG(INFO) << "Postgres resume seqno " << resume_seqno << " is above --to " << to_seqno;
    } else if (initialized_from_cli && finalized_seqno > finalized_before_fast_forward) {
      LOG(INFO) << "Initialized Postgres resume seqno from --from " << from_seqno
                << " and fast-forwarded through existing blocks to " << resume_seqno;
    } else if (initialized_from_cli) {
      LOG(INFO) << "Initialized Postgres resume seqno from --from " << from_seqno;
    } else if (finalized_seqno > finalized_before_fast_forward) {
      LOG(INFO) << "Fast-forwarded Postgres resume seqno through existing blocks from "
                << (finalized_before_fast_forward + 1) << " to " << resume_seqno;
    } else if (from_seqno > 0 && resume_seqno != static_cast<std::uint32_t>(from_seqno)) {
      LOG(INFO) << "Using Postgres resume seqno " << resume_seqno << " instead of --from " << from_seqno;
    }
    const auto starts_exactly_from_cli = initialized_from_cli && finalized_seqno == finalized_before_fast_forward;
    promise.set_result(InsertManagerInterface::ResumeState{resume_seqno, starts_exactly_from_cli});
  } catch (const std::exception &e) {
    promise.set_error(td::Status::Error(ErrorCode::DB_ERROR, PSLICE() << "Error reading Postgres resume seqno: " << e.what()));
  }
}
