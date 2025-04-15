#pragma once
#include "crypto/block/block-auto.h"
#include "crypto/block/block-parse.h"
#include "msgpack-utils.h"
#include "TraceEmulator.h"


enum AccountStatus {
  uninit = block::gen::AccountStatus::acc_state_uninit,
  frozen = block::gen::AccountStatus::acc_state_frozen,
  active = block::gen::AccountStatus::acc_state_active,
  nonexist = block::gen::AccountStatus::acc_state_nonexist
};
MSGPACK_ADD_ENUM(AccountStatus);

enum AccStatusChange {
  acst_unchanged = block::gen::AccStatusChange::acst_unchanged,
  acst_frozen = block::gen::AccStatusChange::acst_frozen,
  acst_deleted = block::gen::AccStatusChange::acst_deleted
};
MSGPACK_ADD_ENUM(AccStatusChange);

struct TrStoragePhase {
  uint64_t storage_fees_collected;
  std::optional<uint64_t> storage_fees_due;
  AccStatusChange status_change;
  MSGPACK_DEFINE(storage_fees_collected, storage_fees_due, status_change);
};

struct TrCreditPhase {
  std::optional<uint64_t> due_fees_collected;
  uint64_t credit;
  MSGPACK_DEFINE(due_fees_collected, credit);
};

enum ComputeSkipReason {
  cskip_no_state = block::gen::ComputeSkipReason::cskip_no_state,
  cskip_bad_state = block::gen::ComputeSkipReason::cskip_bad_state,
  cskip_no_gas = block::gen::ComputeSkipReason::cskip_no_gas,
  cskip_suspended = block::gen::ComputeSkipReason::cskip_suspended,
};
MSGPACK_ADD_ENUM(ComputeSkipReason);

struct TrComputePhase_skipped {
  ComputeSkipReason reason;

  MSGPACK_DEFINE(reason);
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

  MSGPACK_DEFINE(success, msg_state_used, account_activated, gas_fees, gas_used, gas_limit, gas_credit, mode, exit_code, exit_arg, vm_steps, vm_init_state_hash, vm_final_state_hash);
};

using TrComputePhase = std::variant<TrComputePhase_skipped, 
                                    TrComputePhase_vm>;

struct StorageUsed {
  uint64_t cells;
  uint64_t bits;

  MSGPACK_DEFINE(cells, bits);
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
  StorageUsed tot_msg_size;

  MSGPACK_DEFINE(success, valid, no_funds, status_change, total_fwd_fees, total_action_fees, result_code, result_arg, tot_actions, spec_actions, skipped_actions, msgs_created, action_list_hash, tot_msg_size);
};

struct TrBouncePhase_negfunds {
  bool dummy = true;

  MSGPACK_DEFINE(dummy);
};

struct TrBouncePhase_nofunds {
  StorageUsed msg_size;
  uint64_t req_fwd_fees;

  MSGPACK_DEFINE(msg_size, req_fwd_fees);
};

struct TrBouncePhase_ok {
  StorageUsed msg_size;
  uint64_t msg_fees;
  uint64_t fwd_fees;

  MSGPACK_DEFINE(msg_size, msg_fees, fwd_fees);
};

using TrBouncePhase = std::variant<TrBouncePhase_negfunds, 
                                   TrBouncePhase_nofunds, 
                                   TrBouncePhase_ok>;

struct SplitMergeInfo {
  uint8_t cur_shard_pfx_len;
  uint8_t acc_split_depth;
  td::Bits256 this_addr;
  td::Bits256 sibling_addr;

  MSGPACK_DEFINE(cur_shard_pfx_len, acc_split_depth, this_addr, sibling_addr);
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

  MSGPACK_DEFINE(credit_first, storage_ph, credit_ph, compute_ph, action, aborted, bounce, destroyed);
};

// only ordinary transactions are emulated
using TransactionDescr = TransactionDescr_ord;

struct Message {
  td::Bits256 hash;
  std::optional<std::string> source;
  std::optional<std::string> destination;
  std::optional<uint64_t> value;
  std::optional<uint64_t> fwd_fee;
  std::optional<uint64_t> ihr_fee;
  std::optional<uint64_t> created_lt;
  std::optional<uint32_t> created_at;
  std::optional<int32_t> opcode;
  std::optional<bool> ihr_disabled;
  std::optional<bool> bounce;
  std::optional<bool> bounced;
  std::optional<uint64_t> import_fee;

  td::Ref<vm::Cell> body;
  std::string body_boc;

  td::Ref<vm::Cell> init_state;
  std::optional<std::string> init_state_boc;

  std::optional<td::Bits256> hash_norm;

  MSGPACK_DEFINE(hash, source, destination, value, fwd_fee, ihr_fee, created_lt, created_at, 
                 opcode, ihr_disabled, bounce, bounced, import_fee, body_boc, init_state_boc, hash_norm)
};

struct BlockId {
  int32_t workchain;
  uint64_t shard;
  uint32_t seqno;

  MSGPACK_DEFINE(workchain, shard, seqno);
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

  MSGPACK_DEFINE(hash, account, lt, prev_trans_hash, prev_trans_lt, now, orig_status, end_status, in_msg, out_msgs, 
    total_fees, account_state_hash_before, account_state_hash_after, description);
};

struct RedisTraceNode {
  Transaction transaction;
  bool emulated;
  uint32_t mc_block_seqno;
  BlockId block_id;

  MSGPACK_DEFINE(transaction, emulated, mc_block_seqno, block_id);
};

td::Result<int64_t> to_balance(vm::CellSlice& balance_slice) {
  auto balance = block::tlb::t_Grams.as_integer_skip(balance_slice);
  if (balance.is_null()) {
      return td::Status::Error("Failed to unpack balance");
  }
  auto res = balance->to_long();
  if (res == td::int64(~0ULL << 63)) {
      return td::Status::Error("Failed to unpack balance (2)");
  }
  return res;
}

td::Result<int64_t> to_balance(td::Ref<vm::CellSlice> balance_ref) {
  vm::CellSlice balance_slice = *balance_ref;
  return to_balance(balance_slice);
}

td::Result<Message> parse_message(td::Ref<vm::Cell> msg_cell) {
  Message msg;
  msg.hash = msg_cell->get_hash().bits();

  block::gen::Message::Record message;
  if (!tlb::type_unpack_cell(msg_cell, block::gen::t_Message_Any, message)) {
    return td::Status::Error("Failed to unpack Message");
  }

  td::Ref<vm::CellSlice> body;
  if (message.body->prefetch_long(1) == 0) {
    body = std::move(message.body);
    body.write().advance(1);
  } else {
    body = vm::load_cell_slice_ref(message.body->prefetch_ref());
  }
  msg.body = vm::CellBuilder().append_cellslice(*body).finalize();

  TRY_RESULT(body_boc, convert::to_bytes(msg.body));
  if (!body_boc) {
    return td::Status::Error("Failed to convert message body to bytes");
  }
  msg.body_boc = body_boc.value();

  if (body->prefetch_long(32) != vm::CellSlice::fetch_long_eof) {
    msg.opcode = body->prefetch_long(32);
  }

  td::Ref<vm::Cell> init_state_cell;
  auto& init_state_cs = message.init.write();
  if (init_state_cs.fetch_ulong(1) == 1) {
    if (init_state_cs.fetch_long(1) == 0) {
      msg.init_state = vm::CellBuilder().append_cellslice(init_state_cs).finalize();
    } else {
      msg.init_state = init_state_cs.fetch_ref();
    }
    TRY_RESULT(init_state_boc, convert::to_bytes(msg.init_state));
    if (!init_state_boc) {
      return td::Status::Error("Failed to convert message init state to bytes");
    }
    msg.init_state_boc = init_state_boc.value();
  }
      
  auto tag = block::gen::CommonMsgInfo().get_tag(*message.info);
  if (tag < 0) {
    return td::Status::Error("Failed to read CommonMsgInfo tag");
  }
  switch (tag) {
    case block::gen::CommonMsgInfo::int_msg_info: {
      block::gen::CommonMsgInfo::Record_int_msg_info msg_info;
      if (!tlb::csr_unpack(message.info, msg_info)) {
        return td::Status::Error("Failed to unpack CommonMsgInfo::int_msg_info");
      }

      TRY_RESULT_ASSIGN(msg.value, to_balance(msg_info.value));
      TRY_RESULT_ASSIGN(msg.source, convert::to_raw_address(msg_info.src));
      TRY_RESULT_ASSIGN(msg.destination, convert::to_raw_address(msg_info.dest));
      TRY_RESULT_ASSIGN(msg.fwd_fee, to_balance(msg_info.fwd_fee));
      TRY_RESULT_ASSIGN(msg.ihr_fee, to_balance(msg_info.ihr_fee));
      msg.created_lt = msg_info.created_lt;
      msg.created_at = msg_info.created_at;
      msg.bounce = msg_info.bounce;
      msg.bounced = msg_info.bounced;
      msg.ihr_disabled = msg_info.ihr_disabled;
      return msg;
    }
    case block::gen::CommonMsgInfo::ext_in_msg_info: {
      block::gen::CommonMsgInfo::Record_ext_in_msg_info msg_info;
      if (!tlb::csr_unpack(message.info, msg_info)) {
        return td::Status::Error("Failed to unpack CommonMsgInfo::ext_in_msg_info");
      }
      
      // msg.source = null, because it is external
      TRY_RESULT_ASSIGN(msg.destination, convert::to_raw_address(msg_info.dest))
      TRY_RESULT_ASSIGN(msg.import_fee, to_balance(msg_info.import_fee));
      TRY_RESULT_ASSIGN(msg.hash_norm, ext_in_msg_get_normalized_hash(msg_cell));
      return msg;
    }
    case block::gen::CommonMsgInfo::ext_out_msg_info: {
      block::gen::CommonMsgInfo::Record_ext_out_msg_info msg_info;
      if (!tlb::csr_unpack(message.info, msg_info)) {
        return td::Status::Error("Failed to unpack CommonMsgInfo::ext_out_msg_info");
      }
      TRY_RESULT_ASSIGN(msg.source, convert::to_raw_address(msg_info.src));
      // msg.destination = null, because it is external
      msg.created_lt = static_cast<uint64_t>(msg_info.created_lt);
      msg.created_at = static_cast<uint32_t>(msg_info.created_at);
      return msg;
    }
  }

  return td::Status::Error("Unknown CommonMsgInfo tag");
}


td::Result<TrStoragePhase> parse_tr_storage_phase(vm::CellSlice& cs) {
  block::gen::TrStoragePhase::Record phase_data;
  if (!tlb::unpack(cs, phase_data)) {
    return td::Status::Error("Failed to unpack TrStoragePhase");
  }
  TrStoragePhase phase;
  TRY_RESULT_ASSIGN(phase.storage_fees_collected, to_balance(phase_data.storage_fees_collected));
  auto& storage_fees_due = phase_data.storage_fees_due.write();
  if (storage_fees_due.fetch_ulong(1) == 1) {
    TRY_RESULT_ASSIGN(phase.storage_fees_due, to_balance(storage_fees_due));
  }
  phase.status_change = static_cast<AccStatusChange>(phase_data.status_change);
  return phase;
}

td::Result<TrCreditPhase> parse_tr_credit_phase(vm::CellSlice& cs) {
  block::gen::TrCreditPhase::Record phase_data;
  if (!tlb::unpack(cs, phase_data)) {
    return td::Status::Error("Failed to unpack TrCreditPhase");
  }
  TrCreditPhase phase;
  auto& due_fees_collected = phase_data.due_fees_collected.write();
  if (due_fees_collected.fetch_ulong(1) == 1) {
    TRY_RESULT_ASSIGN(phase.due_fees_collected, to_balance(due_fees_collected));
  }
  TRY_RESULT_ASSIGN(phase.credit, to_balance(phase_data.credit));
  return phase;
}

td::Result<TrComputePhase> parse_tr_compute_phase(vm::CellSlice& cs) {
  int compute_ph_tag = block::gen::t_TrComputePhase.get_tag(cs);
  if (compute_ph_tag == block::gen::TrComputePhase::tr_phase_compute_vm) {
    block::gen::TrComputePhase::Record_tr_phase_compute_vm compute_vm;
    if (!tlb::unpack(cs, compute_vm)) {
      return td::Status::Error("Error unpacking tr_phase_compute_vm");
    }
    TrComputePhase_vm res;
    res.success = compute_vm.success;
    res.msg_state_used = compute_vm.msg_state_used;
    res.account_activated = compute_vm.account_activated;
    TRY_RESULT_ASSIGN(res.gas_fees, to_balance(compute_vm.gas_fees));
    res.gas_used = block::tlb::t_VarUInteger_7.as_uint(*compute_vm.r1.gas_used);
    res.gas_limit = block::tlb::t_VarUInteger_7.as_uint(*compute_vm.r1.gas_limit);
    auto& gas_credit = compute_vm.r1.gas_credit.write();
    if (gas_credit.fetch_ulong(1)) {
      res.gas_credit = block::tlb::t_VarUInteger_3.as_uint(gas_credit);
    }
    res.mode = compute_vm.r1.mode;
    res.exit_code = compute_vm.r1.exit_code;
    auto& exit_arg = compute_vm.r1.exit_arg.write();
    if (exit_arg.fetch_ulong(1)) {
      res.exit_arg = exit_arg.fetch_long(32);
    }
    res.vm_steps = compute_vm.r1.vm_steps;
    res.vm_init_state_hash = compute_vm.r1.vm_init_state_hash;
    res.vm_final_state_hash = compute_vm.r1.vm_final_state_hash;
    return res;
  } else if (compute_ph_tag == block::gen::TrComputePhase::tr_phase_compute_skipped) {
    block::gen::TrComputePhase::Record_tr_phase_compute_skipped skip;
    if (!tlb::unpack(cs, skip)) {
      return td::Status::Error("Error unpacking tr_phase_compute_skipped");
    }
    return TrComputePhase_skipped{static_cast<ComputeSkipReason>(skip.reason)};
  }
  return td::Status::OK();
}

td::Result<StorageUsed> parse_storage_used_short(vm::CellSlice& cs) {
  block::gen::StorageUsed::Record info;
  if (!tlb::unpack(cs, info)) {
    return td::Status::Error("Error unpacking StorageUsed");
  }
  StorageUsed res;
  res.bits = block::tlb::t_VarUInteger_7.as_uint(*info.bits);
  res.cells = block::tlb::t_VarUInteger_7.as_uint(*info.cells);
  return res;
}

td::Result<TrActionPhase> parse_tr_action_phase(vm::CellSlice& cs) {
  block::gen::TrActionPhase::Record info;
  if (!tlb::unpack(cs, info)) {
    return td::Status::Error("Error unpacking TrActionPhase");
  }
  TrActionPhase res;
  res.success = info.success;
  res.valid = info.valid;
  res.no_funds = info.no_funds;
  res.status_change = static_cast<AccStatusChange>(info.status_change);
  auto& total_fwd_fees = info.total_fwd_fees.write();
  if (total_fwd_fees.fetch_ulong(1) == 1) {
    TRY_RESULT_ASSIGN(res.total_fwd_fees, to_balance(info.total_fwd_fees));
  }
  auto& total_action_fees = info.total_action_fees.write();
  if (total_action_fees.fetch_ulong(1) == 1) {
    TRY_RESULT_ASSIGN(res.total_action_fees, to_balance(info.total_action_fees));
  }
  res.result_code = info.result_code;
  auto& result_arg = info.result_arg.write();
  if (result_arg.fetch_ulong(1)) {
    res.result_arg = result_arg.fetch_long(32);
  }
  res.tot_actions = info.tot_actions;
  res.spec_actions = info.spec_actions;
  res.skipped_actions = info.skipped_actions;
  res.msgs_created = info.msgs_created;
  res.action_list_hash = info.action_list_hash;
  TRY_RESULT_ASSIGN(res.tot_msg_size, parse_storage_used_short(info.tot_msg_size.write()));
  return res;
}

td::Result<TrBouncePhase> parse_tr_bounce_phase(vm::CellSlice& cs) {
  int bounce_ph_tag = block::gen::t_TrBouncePhase.get_tag(cs);
  switch (bounce_ph_tag) {
    case block::gen::TrBouncePhase::tr_phase_bounce_negfunds: {
      block::gen::TrBouncePhase::Record_tr_phase_bounce_negfunds negfunds;
      if (!tlb::unpack(cs, negfunds)) {
        return td::Status::Error("Error unpacking tr_phase_bounce_negfunds");
      }
      return TrBouncePhase_negfunds();
    }
    case block::gen::TrBouncePhase::tr_phase_bounce_nofunds: {
      block::gen::TrBouncePhase::Record_tr_phase_bounce_nofunds nofunds;
      if (!tlb::unpack(cs, nofunds)) {
        return td::Status::Error("Error unpacking tr_phase_bounce_nofunds");
      }
      TrBouncePhase_nofunds res;
      TRY_RESULT_ASSIGN(res.msg_size, parse_storage_used_short(nofunds.msg_size.write()));
      TRY_RESULT_ASSIGN(res.req_fwd_fees, to_balance(nofunds.req_fwd_fees));
      return res;
    }
    case block::gen::TrBouncePhase::tr_phase_bounce_ok: {
      block::gen::TrBouncePhase::Record_tr_phase_bounce_ok ok;
      if (!tlb::unpack(cs, ok)) {
        return td::Status::Error("Error unpacking tr_phase_bounce_ok");
      }
      TrBouncePhase_ok res;
      TRY_RESULT_ASSIGN(res.msg_size, parse_storage_used_short(ok.msg_size.write()));
      TRY_RESULT_ASSIGN(res.msg_fees, to_balance(ok.msg_fees));
      TRY_RESULT_ASSIGN(res.fwd_fees, to_balance(ok.fwd_fees));
      return res;
    }
    default:
      return td::Status::Error("Unknown TrBouncePhase tag");
  }
}

td::Result<SplitMergeInfo> parse_split_merge_info(td::Ref<vm::CellSlice>& cs) {
  block::gen::SplitMergeInfo::Record info;
  if (!tlb::csr_unpack(cs, info)) {
    return td::Status::Error("Error unpacking SplitMergeInfo");
  }
  SplitMergeInfo res;
  res.cur_shard_pfx_len = info.cur_shard_pfx_len;
  res.acc_split_depth = info.acc_split_depth;
  res.this_addr = info.this_addr;
  res.sibling_addr = info.sibling_addr;
  return res;
}

td::Result<TransactionDescr> process_transaction_descr(vm::CellSlice& td_cs) {
  auto tag = block::gen::t_TransactionDescr.get_tag(td_cs);
  switch (tag) {
    case block::gen::TransactionDescr::trans_ord: {
      block::gen::TransactionDescr::Record_trans_ord ord;
      if (!tlb::unpack_exact(td_cs, ord)) {
        return td::Status::Error("Error unpacking trans_ord");
      }
      TransactionDescr_ord res;
      res.credit_first = ord.credit_first;
      auto& storage_ph = ord.storage_ph.write();
      if (storage_ph.fetch_ulong(1) == 1) {
        TRY_RESULT_ASSIGN(res.storage_ph, parse_tr_storage_phase(storage_ph));
      }
      auto& credit_ph = ord.credit_ph.write();
      if (credit_ph.fetch_ulong(1) == 1) {
        TRY_RESULT_ASSIGN(res.credit_ph, parse_tr_credit_phase(credit_ph));
      }
      TRY_RESULT_ASSIGN(res.compute_ph, parse_tr_compute_phase(ord.compute_ph.write()));
      auto& action = ord.action.write();
      if (action.fetch_ulong(1) == 1) {
        auto action_cs = vm::load_cell_slice(action.fetch_ref());
        TRY_RESULT_ASSIGN(res.action, parse_tr_action_phase(action_cs));
      }
      res.aborted = ord.aborted;
      auto& bounce = ord.bounce.write();
      if (bounce.fetch_ulong(1)) {
        TRY_RESULT_ASSIGN(res.bounce, parse_tr_bounce_phase(bounce));
      }
      res.destroyed = ord.destroyed;
      return res;
    }
    default:
      return td::Status::Error("Unsupported transaction description type");
  }
}

td::Result<Transaction> parse_tx(td::Ref<vm::Cell> root, ton::WorkchainId workchain) {
  block::gen::Transaction::Record trans;
  if (!tlb::unpack_cell(root, trans)) {
    return td::Status::Error("Failed to unpack Transaction");
  }

  Transaction schema_tx;

  schema_tx.account = block::StdAddress(workchain, trans.account_addr);
  schema_tx.hash = root->get_hash().bits();
  schema_tx.lt = trans.lt;
  schema_tx.prev_trans_hash = trans.prev_trans_hash;
  schema_tx.prev_trans_lt = trans.prev_trans_lt;
  schema_tx.now = trans.now;

  schema_tx.orig_status = static_cast<AccountStatus>(trans.orig_status);
  schema_tx.end_status = static_cast<AccountStatus>(trans.end_status);

  TRY_RESULT_ASSIGN(schema_tx.total_fees, to_balance(trans.total_fees));

  if (trans.r1.in_msg->prefetch_long(1)) {
    auto msg = trans.r1.in_msg->prefetch_ref();
    TRY_RESULT_ASSIGN(schema_tx.in_msg, parse_message(msg));
  }

  if (trans.outmsg_cnt != 0) {
    vm::Dictionary dict{trans.r1.out_msgs, 15};
    for (int x = 0; x < trans.outmsg_cnt; x++) {
      TRY_RESULT(out_msg, parse_message(dict.lookup_ref(td::BitArray<15>{x})));
      schema_tx.out_msgs.push_back(std::move(out_msg));
    }
  }

  block::gen::HASH_UPDATE::Record state_hash_update;
  if (!tlb::type_unpack_cell(std::move(trans.state_update), block::gen::t_HASH_UPDATE_Account, state_hash_update)) {
    return td::Status::Error("Failed to unpack state_update");
  }
  
  schema_tx.account_state_hash_before = state_hash_update.old_hash;
  schema_tx.account_state_hash_after = state_hash_update.new_hash;

  auto descr_cs = vm::load_cell_slice(trans.description);
  TRY_RESULT_ASSIGN(schema_tx.description, process_transaction_descr(descr_cs));
  return schema_tx;
}

td::Result<RedisTraceNode> parse_trace_node(const TraceNode& node) {
  TRY_RESULT(tx, parse_tx(node.transaction_root, node.address.workchain));
  auto redis_blkid = BlockId{.workchain = node.block_id.workchain, 
                             .shard = node.block_id.shard, 
                             .seqno = node.block_id.seqno};
  return RedisTraceNode{.transaction = tx, 
                        .emulated = node.emulated, 
                        .mc_block_seqno = node.mc_block_seqno, 
                        .block_id = redis_blkid};
}

struct JettonWalletSchema {
  td::RefInt256 balance;
  block::StdAddress address;
  block::StdAddress owner;
  block::StdAddress jetton;

  MSGPACK_DEFINE(balance, address, owner, jetton);
};
struct JettonMasterSchema {
  block::StdAddress address;
  td::RefInt256 total_supply;
  bool mintable;
  std::optional<block::StdAddress> admin_address;
  std::optional<std::map<std::string, std::string>> jetton_content;
  td::Bits256 jetton_wallet_code_hash;

  MSGPACK_DEFINE(address, total_supply, mintable, admin_address, jetton_content, jetton_wallet_code_hash);
};

struct NftItemSchema {
  block::StdAddress address;
  bool init;
  td::RefInt256 index;
  std::optional<block::StdAddress> collection_address;
  std::optional<block::StdAddress> owner_address;
  std::optional<std::map<std::string, std::string>> content;

  MSGPACK_DEFINE(address, init, index, collection_address, owner_address, content);
};
struct NftCollectionSchema {
  block::StdAddress address;
  td::RefInt256 next_item_index;
  std::optional<block::StdAddress> owner_address;
  std::optional<std::map<std::string, std::string>> collection_content;

  MSGPACK_DEFINE(address, next_item_index, owner_address, collection_content);
};
struct GetGemsNftFixPriceSaleSchema {
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

  MSGPACK_DEFINE(address, is_complete, created_at, marketplace_address, nft_address, nft_owner_address, full_price, marketplace_fee_address, marketplace_fee, royalty_address, royalty_amount);
};
struct GetGemsNftAuctionSchema {
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

  MSGPACK_DEFINE(address, end, end_time, mp_addr, nft_addr, nft_owner, last_bid, last_member, min_step, mp_fee_addr, mp_fee_factor, mp_fee_base, royalty_fee_addr, royalty_fee_factor, royalty_fee_base, max_bid, min_bid, created_at, last_bid_at, is_canceled);
};

struct AddressInterfaces {
  std::vector<std::variant<JettonWalletSchema, JettonMasterSchema, NftItemSchema, NftCollectionSchema, GetGemsNftFixPriceSaleSchema, GetGemsNftAuctionSchema>> interfaces;

  MSGPACK_DEFINE(interfaces);
};

template <class... T>
constexpr bool always_false = false;

AddressInterfaces parse_interfaces(std::vector<typename Trace::Detector::DetectedInterface> interfaces) {
  AddressInterfaces result;
  for (const auto& interface : interfaces) {
    std::visit([&](auto&& arg) {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, JettonMasterDetectorR::Result>) {
        JettonMasterSchema schema;
        schema.address = arg.address;
        schema.total_supply = arg.total_supply;
        schema.mintable = arg.mintable;
        schema.admin_address = arg.admin_address;
        schema.jetton_content = arg.jetton_content;
        schema.jetton_wallet_code_hash = arg.jetton_wallet_code_hash.bits();
        result.interfaces.push_back(schema);
      } else if constexpr (std::is_same_v<T, JettonWalletDetectorR::Result>) {
        JettonWalletSchema schema;
        schema.balance = arg.balance;
        schema.address = arg.address;
        schema.owner = arg.owner;
        schema.jetton = arg.jetton;
        result.interfaces.push_back(schema);
      } else if constexpr (std::is_same_v<T, NftItemDetectorR::Result>) {
        NftItemSchema schema;
        schema.address = arg.address;
        schema.init = arg.init;
        schema.index = arg.index;
        schema.collection_address = arg.collection_address;
        schema.owner_address = arg.owner_address;
        schema.content = arg.content;
        result.interfaces.push_back(schema);
      } else if constexpr (std::is_same_v<T, NftCollectionDetectorR::Result>) {
        NftCollectionSchema schema;
        schema.address = arg.address;
        schema.next_item_index = arg.next_item_index;
        schema.owner_address = arg.owner_address;
        schema.collection_content = arg.collection_content;
        result.interfaces.push_back(schema);
      } else if constexpr (std::is_same_v<T, GetGemsNftFixPriceSale::Result>) {
        GetGemsNftFixPriceSaleSchema schema;
        schema.address = arg.address;
        schema.is_complete = arg.is_complete;
        schema.created_at = arg.created_at;
        schema.marketplace_address = arg.marketplace_address;
        schema.nft_address = arg.nft_address;
        schema.nft_owner_address = arg.nft_owner_address;
        schema.full_price = arg.full_price;
        schema.marketplace_fee_address = arg.marketplace_fee_address;
        schema.marketplace_fee = arg.marketplace_fee;
        schema.royalty_address = arg.royalty_address;
        schema.royalty_amount = arg.royalty_amount;
        result.interfaces.push_back(schema);
      } else if constexpr (std::is_same_v<T, GetGemsNftAuction::Result>) {
        GetGemsNftAuctionSchema schema;
        schema.address = arg.address;
        schema.end = arg.end;
        schema.end_time = arg.end_time;
        schema.mp_addr = arg.mp_addr;
        schema.nft_addr = arg.nft_addr;
        schema.nft_owner = arg.nft_owner;
        schema.last_bid = arg.last_bid;
        schema.last_member = arg.last_member;
        schema.min_step = arg.min_step;
        schema.mp_fee_addr = arg.mp_fee_addr;
        schema.mp_fee_factor = arg.mp_fee_factor;
        schema.mp_fee_base = arg.mp_fee_base;
        schema.royalty_fee_addr = arg.royalty_fee_addr;
        schema.royalty_fee_factor = arg.royalty_fee_factor;
        schema.royalty_fee_base = arg.royalty_fee_base;
        schema.max_bid = arg.max_bid;
        schema.min_bid = arg.min_bid;
        schema.created_at = arg.created_at;
        schema.last_bid_at = arg.last_bid_at;
        schema.is_canceled = arg.is_canceled;
        result.interfaces.push_back(schema);
      } else {
        static_assert(always_false<T>, "non-exhaustive visitor!");
      }
    }, interface);
  }
  return result;
}

// enum AccountStatusExt {
//   nonexist = block::Account::acc_nonexist,
//   uninit = block::Account::acc_uninit,
//   frozen = block::Account::acc_frozen,
//   active = block::Account::acc_active,
//   deleted = block::Account::acc_deleted
// };
// MSGPACK_ADD_ENUM(AccountStatusExt);


struct AccountState {
  td::Bits256 hash;
  uint64_t balance;
  std::string account_status; // "uninit", "frozen", "active", "nonexist"
  std::optional<td::Bits256> frozen_hash;
  td::Ref<vm::Cell> code_cell;
  std::optional<td::Bits256> code_hash;
  td::Ref<vm::Cell> data_cell;
  std::optional<td::Bits256> data_hash;
  std::optional<td::Bits256> last_trans_hash;
  std::optional<uint64_t> last_trans_lt;
  std::optional<uint32_t> timestamp;

  MSGPACK_DEFINE(hash, timestamp, balance, account_status, frozen_hash, code_hash, data_hash, last_trans_hash, last_trans_lt);
};

td::Result<AccountState> parse_account(const block::Account& account) {
  AccountState result;
  int account_tag = block::gen::t_Account.get_tag(vm::load_cell_slice(account.total_state));
  switch (account_tag) {
  case block::gen::Account::account_none: {
    result.account_status = "nonexist";
    result.balance = 0;
    result.hash = account.total_state->get_hash().bits();
    break;
  }
  case block::gen::Account::account: {
    TRY_RESULT(schema_account, ParseQuery::parse_account(account.total_state, account.now_, account.last_trans_hash_, account.last_trans_lt_));
    result.hash = schema_account.hash;
    result.timestamp = schema_account.timestamp;
    result.balance = schema_account.balance.grams->to_long();
    result.account_status = schema_account.account_status;
    result.frozen_hash = schema_account.frozen_hash;
    result.code_cell = schema_account.code;
    result.code_hash = schema_account.code_hash;
    result.data_cell = schema_account.data;
    result.data_hash = schema_account.data_hash;
    result.last_trans_hash = schema_account.last_trans_hash;
    result.last_trans_lt = schema_account.last_trans_lt;
    break;
  }
  default:
    return td::Status::Error("Unknown account tag");
  }
  
  return result;
}