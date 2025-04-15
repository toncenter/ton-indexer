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

namespace schema {

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
  std::optional<std::string> source;
  std::optional<std::string> destination;
  std::optional<CurrencyCollection> value;
  std::optional<td::RefInt256> fwd_fee;
  std::optional<td::RefInt256> ihr_fee;
  std::optional<uint64_t> created_lt;
  std::optional<uint32_t> created_at;
  std::optional<int32_t> opcode;
  std::optional<bool> ihr_disabled;
  std::optional<bool> bounce;
  std::optional<bool> bounced;
  std::optional<td::RefInt256> import_fee;

  td::Ref<vm::Cell> body;
  std::string body_boc;

  td::Ref<vm::Cell> init_state;
  std::optional<std::string> init_state_boc;

  td::Bits256 trace_id;
};

struct Transaction {
  td::Bits256 hash;
  block::StdAddress account;
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
  td::Bits256 account_state_hash_after;

  td::Bits256 trace_id;
  TransactionDescr description;
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
  std::string root_hash;
  std::string file_hash;

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
  std::string rand_seed;
  std::string created_by;

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
  block::StdAddress account;
  std::string account_friendly;  // TODO: add account friendly
  uint32_t timestamp;
  CurrencyCollection balance;
  std::string account_status; // "uninit", "frozen", "active", "nonexist"
  std::optional<td::Bits256> frozen_hash;
  td::Ref<vm::Cell> code;
  std::optional<td::Bits256> code_hash;
  td::Ref<vm::Cell> data;
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

}  // namespace schema

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
  std::string code_boc;
  std::string data_boc;
};

struct JettonMasterDataV2 {
  block::StdAddress address;
  td::RefInt256 total_supply;
  bool mintable;
  std::optional<block::StdAddress> admin_address;
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
  block::StdAddress address;
  block::StdAddress owner;
  block::StdAddress jetton;
  std::optional<bool> mintless_is_claimed;
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
  std::string source;
  std::string destination;
  std::string jetton_wallet;
  std::string jetton_master;  // ignore
  std::string response_destination;
  td::Ref<vm::Cell> custom_payload;
  td::RefInt256 forward_ton_amount;
  td::Ref<vm::Cell> forward_payload;
};

struct JettonBurn {
  td::Bits256 trace_id;
  td::Bits256 transaction_hash;
  uint64_t transaction_lt;
  uint32_t transaction_now;
  bool transaction_aborted;
  uint32_t mc_seqno;
  uint64_t query_id;
  std::string owner;
  std::string jetton_wallet;
  std::string jetton_master;  // ignore
  td::RefInt256 amount;
  std::string response_destination;
  td::Ref<vm::Cell> custom_payload;
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
  std::string code_boc;
  std::string data_boc;
};

struct NFTCollectionDataV2 {
  block::StdAddress address;
  td::RefInt256 next_item_index;
  std::optional<block::StdAddress> owner_address;
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
    std::optional<block::StdAddress> wallet;
    std::optional<block::StdAddress> next_resolver;
    std::optional<td::Bits256> site_adnl;
    std::optional<td::Bits256> storage_bag_id;
  };

  block::StdAddress address;
  bool init;
  td::RefInt256 index;
  std::optional<block::StdAddress> collection_address;
  std::optional<block::StdAddress> owner_address;
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
  block::StdAddress nft_item;
  td::RefInt256 nft_item_index;  // ignore
  std::string nft_collection;  // ignore
  std::string old_owner;
  std::string new_owner;
  std::string response_destination;
  td::Ref<vm::Cell> custom_payload;
  td::RefInt256 forward_amount;
  td::Ref<vm::Cell> forward_payload;
};

struct GetGemsNftAuctionData {
  block::StdAddress address;
  bool end;
  uint32_t end_time;
  block::StdAddress mp_addr;
  block::StdAddress nft_addr;
  std::optional<block::StdAddress> nft_owner;
  td::RefInt256 last_bid;
  std::optional<block::StdAddress> last_member;
  uint32_t min_step;
  block::StdAddress mp_fee_addr;
  uint32_t mp_fee_factor, mp_fee_base;
  block::StdAddress royalty_fee_addr;
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
  uint64_t last_transaction_lt;
  uint32_t last_transaction_now;
  td::Bits256 code_hash;
  td::Bits256 data_hash;
};


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

using BlockchainEvent = std::variant<JettonTransfer, 
                                     JettonBurn,
                                     NFTTransfer>;

using BlockchainInterface = std::variant<JettonMasterData, 
                                         JettonWalletData, 
                                         NFTCollectionData, 
                                         NFTItemData>;


using BlockchainInterfaceV2 = std::variant<JettonWalletDataV2, 
                                           JettonMasterDataV2, 
                                           NFTCollectionDataV2, 
                                           NFTItemDataV2,
                                           GetGemsNftFixPriceSaleData,
                                           GetGemsNftAuctionData>;

namespace std {
template <>
struct hash<td::Bits256> {
  auto operator()(const td::Bits256 &k) const -> size_t {
    std::size_t seed = 0;
    for(const auto& el : k.as_array()) {
        seed ^= std::hash<td::uint8>{}(el) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
  }
};

template <>
struct hash<block::StdAddress> {
  auto operator()(const block::StdAddress &addr) const -> size_t {
    return std::hash<td::uint32>{}(addr.workchain) ^ std::hash<td::Bits256>{}(addr.addr);
  }
};
}  // namespace std

struct ParsedBlock {
  MasterchainBlockDataState mc_block_;

  std::vector<schema::Block> blocks_;
  std::vector<schema::AccountState> account_states_;
  std::vector<schema::MasterchainBlockShard> shard_state_;

  std::vector<schema::Trace> traces_;

  std::vector<BlockchainEvent> events_;
  std::vector<BlockchainInterface> interfaces_; // deprecated in favour of account_interfaces_

  std::unordered_map<block::StdAddress, std::vector<BlockchainInterfaceV2>> account_interfaces_;
  
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
};

using ParsedBlockPtr = std::shared_ptr<ParsedBlock>;
