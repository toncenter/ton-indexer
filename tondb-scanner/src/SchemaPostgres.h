#pragma once

namespace schema {

struct Transaction {
  // Corresponding block
  int32_t block_workchain;
  int64_t block_shard;
  int32_t block_seqno;

  // Transaction details
  std::string account;
  std::string hash;
  uint64_t lt;
  uint32_t utime;
  std::string transaction_type;

  // Account state change
  std::string account_state_hash_before;
  std::string account_state_hash_after;

  // Fees
  uint64_t fees{0};
  uint64_t storage_fees{0};
  uint64_t in_fwd_fees{0};
  uint64_t computation_fees{0};
  uint64_t action_fees{0};

  // Computation phase
  td::optional<int32_t> compute_exit_code;
  td::optional<uint32_t> compute_gas_used;
  td::optional<uint32_t> compute_gas_limit;
  td::optional<uint32_t> compute_gas_credit;
  td::optional<uint32_t> compute_gas_fees;
  td::optional<uint32_t> compute_vm_steps;
  td::optional<std::string> compute_skip_reason;

  // Action phase
  td::optional<int32_t> action_result_code;
  td::optional<uint64_t> action_total_fwd_fees;
  td::optional<uint64_t> action_total_action_fees;

  // for interface parsing (TODO: refactor)
  td::optional<std::string> in_msg_from;
  td::Ref<vm::Cell> in_msg_body;
};

struct Message {
  std::string hash;
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

  std::string body;
  std::string body_hash;
  td::optional<std::string> init_state;
  td::optional<std::string> init_state_hash;

  // for interface parsing (TODO: refactor)
  td::Ref<vm::Cell> body_cell;
};

struct TransactionMessage {
  std::string transaction_hash;
  std::string message_hash;
  std::string direction; // "in" or "out"
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
};

struct AccountState {
  std::string hash;
  std::string account;
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
