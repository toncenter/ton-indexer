#pragma once
#include <vector>
#include <variant>
#include <set>
#include "crypto/common/refcnt.hpp"
#include "validator/interfaces/block.h"
#include "validator/interfaces/shard.h"
#include "validator/interfaces/block-handle.h"
#include "crypto/block/block-auto.h"
#include "crypto/block/block-parse.h"
#include "smc-interfaces/InterfacesDetector.h"
#include "AccountAddress.h"

namespace schema {

using raw_bytes = std::string;

struct CurrencyCollection {
  td::RefInt256 grams;
  std::map<uint32_t, td::RefInt256> extra_currencies;
};

enum AccountStatus {
  uninit = block::gen::AccountStatus::acc_state_uninit,
  frozen = block::gen::AccountStatus::acc_state_frozen,
  active = block::gen::AccountStatus::acc_state_active,
  nonexist = block::gen::AccountStatus::acc_state_nonexist
};

enum AccStatusChange {
  acst_unchanged = block::gen::AccStatusChange::acst_unchanged,
  acst_frozen = block::gen::AccStatusChange::acst_frozen,
  acst_deleted = block::gen::AccStatusChange::acst_deleted
};

struct TrStoragePhase {
  td::RefInt256 storage_fees_collected;
  std::optional<td::RefInt256> storage_fees_due;
  AccStatusChange status_change;
};

struct TrCreditPhase {
  std::optional<td::RefInt256> due_fees_collected;
  CurrencyCollection credit;
};

enum ComputeSkipReason {
  cskip_no_state = block::gen::ComputeSkipReason::cskip_no_state,
  cskip_bad_state = block::gen::ComputeSkipReason::cskip_bad_state,
  cskip_no_gas = block::gen::ComputeSkipReason::cskip_no_gas,
  cskip_suspended = block::gen::ComputeSkipReason::cskip_suspended,
};

struct TrComputePhase_skipped {
  ComputeSkipReason reason;
};

struct TrComputePhase_vm {
  bool success;
  bool msg_state_used;
  bool account_activated;
  td::RefInt256 gas_fees;
  uint64_t gas_used;
  uint64_t gas_limit;
  std::optional<uint64_t> gas_credit;
  int8_t mode;
  int32_t exit_code;
  std::optional<int32_t> exit_arg;
  uint32_t vm_steps;
  td::Bits256 vm_init_state_hash;
  td::Bits256 vm_final_state_hash;
};

using TrComputePhase = std::variant<TrComputePhase_skipped, 
                                    TrComputePhase_vm>;

struct StorageUsed {
  uint64_t cells;
  uint64_t bits;
};

struct TrActionPhase {
  bool success;
  bool valid;
  bool no_funds;
  AccStatusChange status_change;
  std::optional<td::RefInt256> total_fwd_fees;
  std::optional<td::RefInt256> total_action_fees;
  int32_t result_code;
  std::optional<int32_t> result_arg;
  uint16_t tot_actions;
  uint16_t spec_actions;
  uint16_t skipped_actions;
  uint16_t msgs_created;
  td::Bits256 action_list_hash;
  StorageUsed tot_msg_size;
};

struct TrBouncePhase_negfunds {
};

struct TrBouncePhase_nofunds {
  StorageUsed msg_size;
  td::RefInt256 req_fwd_fees;
};

struct TrBouncePhase_ok {
  StorageUsed msg_size;
  td::RefInt256 msg_fees;
  td::RefInt256 fwd_fees;
};

using TrBouncePhase = std::variant<TrBouncePhase_negfunds, 
                                   TrBouncePhase_nofunds, 
                                   TrBouncePhase_ok>;

struct SplitMergeInfo {
  uint8_t cur_shard_pfx_len;
  uint8_t acc_split_depth;
  td::Bits256 this_addr;
  td::Bits256 sibling_addr;
};

struct TransactionDescr_ord {
  bool credit_first;
  std::optional<TrStoragePhase> storage_ph;
  std::optional<TrCreditPhase> credit_ph;
  TrComputePhase compute_ph;
  std::optional<TrActionPhase> action;
  bool aborted;
  std::optional<TrBouncePhase> bounce;
  bool destroyed;
};

struct TransactionDescr_storage {
  TrStoragePhase storage_ph;
};

struct TransactionDescr_tick_tock {
  bool is_tock;
  TrStoragePhase storage_ph;
  TrComputePhase compute_ph;
  std::optional<TrActionPhase> action;
  bool aborted;
  bool destroyed;
};

struct TransactionDescr_split_prepare {
  SplitMergeInfo split_info;
  std::optional<TrStoragePhase> storage_ph;
  TrComputePhase compute_ph;
  std::optional<TrActionPhase> action;
  bool aborted;
  bool destroyed;
};

struct TransactionDescr_split_install {
  SplitMergeInfo split_info;
  // Transaction prepare_transaction;
  bool installed;
};

struct TransactionDescr_merge_prepare {
  SplitMergeInfo split_info;
  TrStoragePhase storage_ph;
  bool aborted;
};

struct TransactionDescr_merge_install {
  SplitMergeInfo split_info;
  // Transaction prepare_transaction;
  std::optional<TrStoragePhase> storage_ph;
  std::optional<TrCreditPhase> credit_ph;
  TrComputePhase compute_ph;
  std::optional<TrActionPhase> action;
  bool aborted;
  bool destroyed;
};

using TransactionDescr = std::variant<TransactionDescr_ord, 
                                       TransactionDescr_storage, 
                                       TransactionDescr_tick_tock, 
                                       TransactionDescr_split_prepare, 
                                       TransactionDescr_split_install, 
                                       TransactionDescr_merge_prepare, 
                                       TransactionDescr_merge_install>;

struct Message {
  td::Bits256 hash;
  std::optional<td::Bits256> hash_norm;
  AccountAddress source;
  AccountAddress destination;
  std::optional<CurrencyCollection> value;
  std::optional<td::RefInt256> fwd_fee;
  std::optional<td::RefInt256> ihr_fee;
  std::optional<td::RefInt256> extra_flags;
  std::optional<uint64_t> created_lt;
  std::optional<uint32_t> created_at;
  std::optional<int32_t> opcode;
  std::optional<bool> ihr_disabled;
  std::optional<bool> bounce;
  std::optional<bool> bounced;
  std::optional<td::RefInt256> import_fee;

  td::Ref<vm::Cell> body;
  td::Bits256 body_hash;
  raw_bytes body_boc;

  td::Ref<vm::Cell> init_state;
  std::optional<td::Bits256> init_state_hash;
  std::optional<raw_bytes> init_state_boc;

  td::Bits256 trace_id;
};

struct Transaction {
  td::Bits256 hash;
  AccountAddress account;
  uint64_t lt;
  td::Bits256 prev_trans_hash;
  uint64_t prev_trans_lt;
  uint32_t now;
  uint32_t mc_seqno;

  AccountStatus orig_status;
  AccountStatus end_status;

  std::optional<Message> in_msg;
  std::vector<Message> out_msgs;

  CurrencyCollection total_fees;

  td::Bits256 account_state_hash_before;
  raw_bytes account_state_before;
  td::Bits256 account_state_hash_after;
  raw_bytes account_state_after;

  td::Bits256 trace_id;
  TransactionDescr description;
  raw_bytes description_boc;
};

struct BlockReference {
  int32_t workchain;
  int64_t shard;
  uint32_t seqno;
};

struct Block {
  int32_t workchain;
  int64_t shard;
  uint32_t seqno;
  td::Bits256 root_hash;
  td::Bits256 file_hash;

  std::optional<int32_t> mc_block_workchain;
  std::optional<int64_t> mc_block_shard;
  std::optional<uint32_t> mc_block_seqno;
  
  int32_t global_id;
  int32_t version;
  bool after_merge;
  bool before_split;
  bool after_split;
  bool want_merge;
  bool want_split;
  bool key_block;
  bool vert_seqno_incr;
  int32_t flags;
  int32_t gen_utime;
  uint64_t start_lt;
  uint64_t end_lt;
  int32_t validator_list_hash_short;
  int32_t gen_catchain_seqno;
  int32_t min_ref_mc_seqno;
  int32_t prev_key_block_seqno;
  int32_t vert_seqno;
  std::optional<int32_t> master_ref_seqno;
  td::Bits256 rand_seed;
  td::Bits256 created_by;

  std::vector<Transaction> transactions;
  std::vector<BlockReference> prev_blocks;
};

struct MasterchainBlockShard {
  uint32_t mc_seqno;
  uint64_t mc_block_start_lt;
  int32_t mc_block_gen_utime;
  
  int32_t workchain;
  int64_t shard;
  uint32_t seqno;
};

struct AccountState {
  td::Bits256 hash;           // Note: hash is not unique in case account_status is "nonexist"
  AccountAddress account;
  std::string account_friendly;  // TODO: add account friendly
  uint32_t timestamp;
  CurrencyCollection balance;
  std::string account_status; // "uninit", "frozen", "active", "nonexist"
  std::optional<td::Bits256> frozen_hash;
  td::Ref<vm::Cell> code;
  std::optional<raw_bytes> code_boc;
  std::optional<td::Bits256> code_hash;
  td::Ref<vm::Cell> data;
  std::optional<raw_bytes> data_boc;
  std::optional<td::Bits256> data_hash;
  td::Bits256 last_trans_hash;
  uint64_t last_trans_lt;     // in "nonexist" case it is lt of block, not tx. TODO: fix it

  bool operator==(const AccountState& other) const {
    return hash == other.hash && account == other.account && timestamp == other.timestamp;
  }
};

//
// Traces
//
struct TraceEdge {
  td::Bits256 trace_id;
  td::Bits256 msg_hash;
  std::uint64_t msg_lt;
  std::optional<td::Bits256> left_tx;
  std::optional<td::Bits256> right_tx;
  enum Type { ord = 0, sys = 1, ext = 2, logs = 3 } type;
  bool incomplete;
  bool broken;

  std::string str() const {
    td::StringBuilder sb;
    sb << "TraceEdge("
       << trace_id << ", "
       << msg_hash << ", " 
       << (left_tx.has_value() ? td::base64_encode(left_tx.value().as_slice()) : "null") << ", "
       << (right_tx.has_value() ? td::base64_encode(right_tx.value().as_slice()) : "null") << ", "
       << (incomplete) << ", " << broken << ")";
    return sb.as_cslice().str();
  }
};

struct Trace {
  td::Bits256 trace_id;
  std::optional<td::Bits256> external_hash;
  std::optional<td::Bits256> external_hash_norm;
  std::int32_t mc_seqno_start;
  std::int32_t mc_seqno_end;
  
  // meta
  std::uint64_t start_lt;
  std::uint32_t start_utime;

  std::uint64_t end_lt;
  std::uint32_t end_utime;

  enum State { complete = 0, pending = 1, broken = 2} state;

  std::int64_t pending_edges_;
  std::int64_t edges_;
  std::int64_t nodes_;

  std::vector<TraceEdge> edges;
};

struct TraceAssemblerState {
  std::vector<TraceEdge> pending_edges_;
  std::vector<Trace> pending_traces_;
};

struct JettonMasterData {
  std::string address;
  td::RefInt256 total_supply;
  bool mintable;
  std::optional<std::string> admin_address;
  std::optional<std::map<std::string, std::string>> jetton_content;
  vm::CellHash jetton_wallet_code_hash;
  vm::CellHash data_hash;
  vm::CellHash code_hash;
  uint64_t last_transaction_lt;
  uint32_t last_transaction_now;
  raw_bytes code_boc;
  raw_bytes data_boc;
};

struct JettonMasterDataV2 {
  AccountAddress address;
  td::RefInt256 total_supply;
  bool mintable;
  AccountAddress admin_address;
  std::optional<std::map<std::string, std::string>> jetton_content;
  td::Bits256 jetton_wallet_code_hash;
  td::Bits256 data_hash;
  td::Bits256 code_hash;
  uint64_t last_transaction_lt;
  uint32_t last_transaction_now;
};

struct JettonWalletData {
  td::RefInt256 balance;
  std::string address;
  std::string owner;
  std::string jetton;
  uint64_t last_transaction_lt;
  uint32_t last_transaction_now;
  vm::CellHash code_hash;
  vm::CellHash data_hash;
};

struct JettonWalletDataV2 {
  td::RefInt256 balance;
  AccountAddress address;
  AccountAddress owner;
  AccountAddress jetton;
  std::optional<bool> mintless_is_claimed;
  uint64_t last_transaction_lt;
  uint32_t last_transaction_now;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
};

struct VestingData
{
  AccountAddress address;
  uint32_t vesting_start_time;
  uint32_t vesting_total_duration;
  uint32_t unlock_period;
  uint32_t cliff_duration;
  td::RefInt256 vesting_total_amount;
  AccountAddress vesting_sender_address;
  AccountAddress owner_address;
  std::vector<AccountAddress> whitelist;
  uint64_t last_transaction_lt;
  uint32_t last_transaction_now;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
};

struct JettonTransfer {
  td::Bits256 trace_id;
  td::Bits256 transaction_hash;
  uint64_t transaction_lt;
  uint32_t transaction_now;
  bool transaction_aborted;
  uint32_t mc_seqno;
  uint64_t query_id;
  td::RefInt256 amount;
  AccountAddress source;
  AccountAddress destination;
  AccountAddress jetton_wallet;
  AccountAddress jetton_master;  // ignore
  AccountAddress response_destination;
  td::Ref<vm::Cell> custom_payload;
  std::optional<raw_bytes> custom_payload_boc;
  td::RefInt256 forward_ton_amount;
  td::Ref<vm::Cell> forward_payload;
  std::optional<raw_bytes> forward_payload_boc;
};

struct JettonBurn {
  td::Bits256 trace_id;
  td::Bits256 transaction_hash;
  uint64_t transaction_lt;
  uint32_t transaction_now;
  bool transaction_aborted;
  uint32_t mc_seqno;
  uint64_t query_id;
  AccountAddress owner;
  AccountAddress jetton_wallet;
  AccountAddress jetton_master;  // ignore
  td::RefInt256 amount;
  AccountAddress response_destination;
  td::Ref<vm::Cell> custom_payload;
  std::optional<raw_bytes> custom_payload_boc;
};

struct NominatorPoolIncome {
  td::Bits256 trace_id;
  td::Bits256 transaction_hash;
  uint64_t transaction_lt;
  uint32_t transaction_now;
  uint32_t mc_seqno;

  AccountAddress pool_address;
  AccountAddress nominator_address;
  td::RefInt256 income_amount;
  td::RefInt256 nominator_balance;  // balance at income time
};

struct NFTCollectionData {
  std::string address;
  td::RefInt256 next_item_index;
  std::optional<std::string> owner_address;
  std::optional<std::map<std::string, std::string>> collection_content;
  vm::CellHash data_hash;
  vm::CellHash code_hash;
  uint64_t last_transaction_lt;
  uint32_t last_transaction_now;
  raw_bytes code_boc;
  raw_bytes data_boc;
};

struct NFTCollectionDataV2 {
  AccountAddress address;
  td::RefInt256 next_item_index;
  AccountAddress owner_address;
  std::optional<std::map<std::string, std::string>> collection_content;
  uint64_t last_transaction_lt;
  uint32_t last_transaction_now;
  td::Bits256 data_hash;
  td::Bits256 code_hash;
};

struct NFTItemData {
  std::string address;
  bool init;
  td::RefInt256 index;
  std::string collection_address;
  std::string owner_address;
  std::optional<std::map<std::string, std::string>> content;
  uint64_t last_transaction_lt;
  uint32_t last_transaction_now;
  vm::CellHash code_hash;
  vm::CellHash data_hash;
};

struct NFTItemDataV2 {
  struct DNSEntry {
    std::string domain;
    AccountAddress wallet;
    AccountAddress next_resolver;
    std::optional<td::Bits256> site_adnl;
    std::optional<td::Bits256> storage_bag_id;
  };

  AccountAddress address;
  bool init;
  td::RefInt256 index;
  AccountAddress collection_address;
  AccountAddress owner_address;
  std::optional<std::map<std::string, std::string>> content;
  uint64_t last_transaction_lt;
  uint32_t last_transaction_now;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
  std::optional<DNSEntry> dns_entry;
};

struct NFTTransfer {
  td::Bits256 trace_id;
  td::Bits256 transaction_hash;
  uint64_t transaction_lt;
  uint32_t transaction_now;
  bool transaction_aborted;
  uint32_t mc_seqno;
  uint64_t query_id;
  AccountAddress nft_item;
  td::RefInt256 nft_item_index;  // ignore
  AccountAddress nft_collection;  // ignore
  AccountAddress old_owner;
  AccountAddress new_owner;
  AccountAddress response_destination;
  td::Ref<vm::Cell> custom_payload;
  std::optional<raw_bytes> custom_payload_boc;
  td::RefInt256 forward_amount;
  td::Ref<vm::Cell> forward_payload;
  std::optional<raw_bytes> forward_payload_boc;
};

struct GetGemsNftAuctionData {
  AccountAddress address;
  bool end;
  uint32_t end_time;
  AccountAddress mp_addr;
  AccountAddress nft_addr;
  AccountAddress nft_owner;
  td::RefInt256 last_bid;
  AccountAddress last_member;
  uint32_t min_step;
  AccountAddress mp_fee_addr;
  uint32_t mp_fee_factor, mp_fee_base;
  AccountAddress royalty_fee_addr;
  uint32_t royalty_fee_factor, royalty_fee_base;
  td::RefInt256 max_bid;
  td::RefInt256 min_bid;
  uint32_t created_at;
  uint32_t last_bid_at;
  bool is_canceled;
  uint64_t last_transaction_lt;
  uint32_t last_transaction_now;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
};

struct GetGemsNftFixPriceSaleData {
  AccountAddress address;
  bool is_complete;
  uint32_t created_at;
  AccountAddress marketplace_address;
  AccountAddress nft_address;
  AccountAddress nft_owner_address;
  td::RefInt256 full_price;
  AccountAddress marketplace_fee_address;
  td::RefInt256 marketplace_fee;
  AccountAddress royalty_address;
  td::RefInt256 royalty_amount;
  uint64_t last_transaction_lt;
  uint32_t last_transaction_now;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
};

struct MultisigContractData {
  AccountAddress address;
  td::RefInt256 next_order_seqno;
  uint32_t threshold;
  std::vector<AccountAddress> signers;
  std::vector<AccountAddress> proposers;
  uint64_t last_transaction_lt;
  uint32_t last_transaction_now;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
};

struct MultisigOrderData {
  AccountAddress address;
  AccountAddress multisig_address;
  td::RefInt256 order_seqno;
  uint32_t threshold;
  bool sent_for_execution;
  td::RefInt256 approvals_mask;
  uint32_t approvals_num;
  td::RefInt256 expiration_date;
  td::Ref<vm::Cell> order;
  std::vector<AccountAddress> signers;
  uint64_t last_transaction_lt;
  uint32_t last_transaction_now;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
};

struct DedustPoolData {
  AccountAddress address;
  AccountAddress asset_1;
  AccountAddress asset_2;
  uint64_t last_transaction_lt;
  uint32_t last_transaction_now;
  td::RefInt256 reserve_1;
  td::RefInt256 reserve_2;
  bool is_stable;
  double fee;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
};

struct StonfiPoolV2Data {
  AccountAddress address;
  AccountAddress asset_1;
  AccountAddress asset_2;
  uint64_t last_transaction_lt;
  uint32_t last_transaction_now;
  td::RefInt256 reserve_1;
  td::RefInt256 reserve_2;
  std::string pool_type;
  double fee;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
};


using BlockchainEvent = std::variant<schema::JettonTransfer,
                                     schema::JettonBurn,
                                     schema::NFTTransfer,
                                     schema::NominatorPoolIncome>;

using BlockchainInterface = std::variant<schema::JettonMasterData,
                                         schema::JettonWalletData,
                                         schema::NFTCollectionData,
                                         schema::NFTItemData>;


using BlockchainInterfaceV2 = std::variant<schema::JettonWalletDataV2,
                                           schema::JettonMasterDataV2,
                                           schema::NFTCollectionDataV2,
                                           schema::NFTItemDataV2,
                                           schema::GetGemsNftFixPriceSaleData,
                                           schema::GetGemsNftAuctionData,
                                           schema::MultisigContractData,
                                           schema::MultisigOrderData,
                                           schema::VestingData,
                                           schema::DedustPoolData,
                                           schema::StonfiPoolV2Data>;

//
// Containers
//
struct BlockDataState {
  td::Ref<ton::validator::BlockData> block_data;
  td::Ref<vm::Cell> block_state;
  ton::validator::ConstBlockHandle handle;
};

struct MasterchainBlockDataState {
  std::vector<BlockDataState> shard_blocks_;  // shard state like /shards method
  std::vector<BlockDataState> shard_blocks_diff_;  // blocks corresponding to mc_block.

  std::shared_ptr<block::ConfigInfo> config_;
};
}  // namespace schema

namespace std {
template <>
struct hash<td::Bits256> {
  auto operator()(const td::Bits256 &k) const noexcept -> size_t {
    std::size_t seed = 0;
    for(const auto& el : k.as_array()) {
        seed ^= std::hash<td::uint8>{}(el) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
  }
};

template <>
struct hash<block::StdAddress> {
  auto operator()(const block::StdAddress &addr) const noexcept -> size_t {
    return std::hash<td::uint32>{}(addr.workchain) ^ std::hash<td::Bits256>{}(addr.addr);
  }
};
}  // namespace std

struct DataContainer {
  std::int32_t mc_seqno_;

  schema::MasterchainBlockDataState mc_block_;
  std::shared_ptr<vm::CellDbReader> cell_db_reader_;  // for loading previous states

  std::vector<schema::Block> blocks_;
  std::vector<schema::AccountState> account_states_;
  std::vector<schema::MasterchainBlockShard> shard_state_;

  std::vector<schema::Trace> traces_;

  std::vector<schema::BlockchainEvent> events_;
  std::vector<schema::BlockchainInterface> interfaces_; // deprecated in favour of account_interfaces_

  std::unordered_map<block::StdAddress, std::vector<schema::BlockchainInterfaceV2>> account_interfaces_;

  explicit DataContainer(std::int32_t mc_seqno = 0) : mc_seqno_(mc_seqno) {
    start_time_ = td::Timestamp::now();
    last_time_ = start_time_;
  }

  std::map<std::string, double> timings_;
  td::Timestamp start_time_;
  td::Timestamp last_time_;

  void update_timing(const std::string& key) {
    auto now = td::Timestamp::now();
    if (!start_time_) {
      start_time_ = now;
    }
    timings_[key] = td::Timestamp::now() - start_time_;
    last_time_ = now;
  }

  void print_timings() {
    td::StringBuilder sb;
    sb << "mc_seqno: " << mc_seqno_ << "\n";

    std::vector<std::pair<std::string, double>> sorted_timings(timings_.begin(), timings_.end());
    std::ranges::sort(sorted_timings, {}, &std::pair<std::string,double>::second); // ascending by value

    for (auto const& [key, value] : sorted_timings) {
       sb << key << ": " << value << "\n";
    }
    LOG(INFO) << sb.as_cslice().str();
  }

  template <class T>
  std::vector<T> get_events() {
    std::vector<T> result;
    for (auto& event: events_) {
      if (std::holds_alternative<T>(event)) {
        result.push_back(std::get<T>(event));
      }
    }
    return result;
  }

  // deprecated
  template <class T>
  std::vector<T> get_accounts() {
    std::vector<T> result;
    for (auto& interface: interfaces_) {
      if (std::holds_alternative<T>(interface)) {
        result.push_back(std::get<T>(interface));
      }
    }
    return result;
  }

  template <class T>
  std::vector<T> get_accounts_v2() {
    std::vector<T> result;
    for (const auto& [addr, interfaces]: account_interfaces_) {
      for (const auto& interface: interfaces) {
        if (std::holds_alternative<T>(interface)) {
          result.push_back(std::get<T>(interface));
        }
      }
    }
    return result;
  }

  std::unordered_multimap<td::Bits256, uint64_t> contract_methods_;
};

using DataContainerPtr = std::shared_ptr<DataContainer>;
