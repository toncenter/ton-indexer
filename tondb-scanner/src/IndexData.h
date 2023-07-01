#pragma once
#include <vector>
#include <variant>
#include <set>
#include "crypto/common/refcnt.hpp"
#include "validator/interfaces/block.h"
#include "validator/interfaces/shard.h"
#include "crypto/block/block-auto.h"
#include "crypto/block/block-parse.h"

namespace schema {

struct Message;

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
  uint64_t storage_fees_collected;
  std::optional<uint64_t> storage_fees_due;
  AccStatusChange status_change;
};

struct TrCreditPhase {
  uint64_t due_fees_collected;
  uint64_t credit;
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
  uint64_t gas_fees;
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

struct StorageUsedShort {
  uint64_t cells;
  uint64_t bits;
};

struct TrActionPhase {
  bool success;
  bool valid;
  bool no_funds;
  AccStatusChange status_change;
  std::optional<uint64_t> total_fwd_fees;
  std::optional<uint64_t> total_action_fees;
  int32_t result_code;
  std::optional<int32_t> result_arg;
  uint16_t tot_actions;
  uint16_t spec_actions;
  uint16_t skipped_actions;
  uint16_t msgs_created;
  td::Bits256 action_list_hash;
  StorageUsedShort tot_msg_size;
};

struct TrBouncePhase_negfunds {
};

struct TrBouncePhase_nofunds {
  StorageUsedShort msg_size;
  uint64_t req_fwd_fees;
};

struct TrBouncePhase_ok {
  StorageUsedShort msg_size;
  uint64_t msg_fees;
  uint64_t fwd_fees;
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
  TrStoragePhase storage_ph;
  TrCreditPhase credit_ph;
  TrComputePhase compute_ph;
  std::optional<TrActionPhase> action;
  bool aborted;
  TrBouncePhase bounce;
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
  td::optional<std::string> source;
  td::optional<std::string> destination;
  td::optional<uint64_t> value;
  td::optional<uint64_t> fwd_fee;
  td::optional<uint64_t> ihr_fee;
  td::optional<uint64_t> created_lt;
  td::optional<uint32_t> created_at;
  td::optional<int32_t> opcode;
  td::optional<bool> ihr_disabled;
  td::optional<bool> bounce;
  td::optional<bool> bounced;
  td::optional<uint64_t> import_fee;

  td::Ref<vm::Cell> body;
  std::string body_boc;

  td::Ref<vm::Cell> init_state;
  td::optional<std::string> init_state_boc;
};

struct Transaction {
  td::Bits256 hash;
  block::StdAddress account;
  uint64_t lt;
  td::Bits256 prev_trans_hash;
  uint64_t prev_trans_lt;
  uint32_t now;

  AccountStatus orig_status;
  AccountStatus end_status;

  std::optional<Message> in_msg;
  std::vector<Message> out_msgs;

  uint64_t total_fees;

  td::Bits256 account_state_hash_before;
  td::Bits256 account_state_hash_after;

  TransactionDescr description;
};

struct Block {
  int32_t workchain;
  int64_t shard;
  int32_t seqno;
  std::string root_hash;
  std::string file_hash;

  td::optional<int32_t> mc_block_workchain;
  td::optional<int64_t> mc_block_shard;
  td::optional<int32_t> mc_block_seqno;
  
  int32_t global_id;
  int32_t version;
  bool after_merge;
  bool before_split;
  bool after_split;
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
  td::optional<int32_t> master_ref_seqno;
  std::string rand_seed;
  std::string created_by;

  std::vector<Transaction> transactions;
};

struct AccountState {
  td::Bits256 hash;
  block::StdAddress account;
  uint64_t balance;
  std::string account_status; // "uninit", "frozen", "active"
  td::optional<std::string> frozen_hash;
  td::Ref<vm::Cell> code;
  td::optional<std::string> code_hash;
  td::Ref<vm::Cell> data;
  td::optional<std::string> data_hash;
  uint64_t last_trans_lt;
};

}

struct JettonMasterData {
  std::string address;
  uint64_t total_supply;
  bool mintable;
  td::optional<std::string> admin_address;
  td::optional<std::map<std::string, std::string>> jetton_content;
  vm::CellHash jetton_wallet_code_hash;
  vm::CellHash data_hash;
  vm::CellHash code_hash;
  uint64_t last_transaction_lt;
  std::string code_boc;
  std::string data_boc;
};

struct JettonWalletData {
  uint64_t balance;
  std::string address;
  std::string owner;
  std::string jetton;
  uint64_t last_transaction_lt;
  vm::CellHash code_hash;
  vm::CellHash data_hash;
};

struct JettonTransfer {
  td::Bits256 transaction_hash;
  uint64_t query_id;
  td::RefInt256 amount;
  std::string source;
  std::string destination;
  std::string response_destination;
  td::Ref<vm::Cell> custom_payload;
  td::RefInt256 forward_ton_amount;
  td::Ref<vm::Cell> forward_payload;
};

struct JettonBurn {
  td::Bits256 transaction_hash;
  uint64_t query_id;
  std::string owner;
  td::RefInt256 amount;
  std::string response_destination;
  td::Ref<vm::Cell> custom_payload;
};

struct NFTCollectionData {
  std::string address;
  td::RefInt256 next_item_index;
  td::optional<std::string> owner_address;
  td::optional<std::map<std::string, std::string>> collection_content;
  vm::CellHash data_hash;
  vm::CellHash code_hash;
  uint64_t last_transaction_lt;
  std::string code_boc;
  std::string data_boc;
};

struct NFTItemData {
  std::string address;
  bool init;
  td::RefInt256 index;
  std::string collection_address;
  std::string owner_address;
  td::optional<std::map<std::string, std::string>> content;
  uint64_t last_transaction_lt;
  vm::CellHash code_hash;
  vm::CellHash data_hash;
};

struct NFTTransfer {
  td::Bits256 transaction_hash;
  uint64_t query_id;
  block::StdAddress nft_item;
  std::string old_owner;
  std::string new_owner;
  std::string response_destination;
  td::Ref<vm::Cell> custom_payload;
  td::RefInt256 forward_amount;
  td::Ref<vm::Cell> forward_payload;
};

struct BlockDataState {
  td::Ref<ton::validator::BlockData> block_data;
  td::Ref<ton::validator::ShardState> block_state;
};

using MasterchainBlockDataState = std::vector<BlockDataState>;
using BlockchainEvent = std::variant<JettonTransfer, 
                                     JettonBurn,
                                     NFTTransfer>;

struct ParsedBlock {
  MasterchainBlockDataState mc_block_;

  std::vector<schema::Block> blocks_;
  std::vector<schema::AccountState> account_states_;

  std::vector<BlockchainEvent> events_;
  
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
};

using ParsedBlockPtr = std::shared_ptr<ParsedBlock>;