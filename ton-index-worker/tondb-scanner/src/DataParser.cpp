#include "DataParser.h"
#include "td/utils/common.h"
#include "crypto/common/refcnt.hpp"
#include "crypto/block/block.h"
#include "crypto/block/transaction.h"
#include "crypto/block/block-auto.h"
#include "crypto/block/block-parse.h"
#include "validator/interfaces/block.h"
#include "validator/interfaces/shard.h"
#include "convert-utils.h"
#include "Statistics.h"

using namespace ton::validator; //TODO: remove this

void ParseQuery::start_up() {
  td::Timer timer;
  auto status = parse_impl();
  g_statistics.record_time(PARSE_SEQNO, timer.elapsed() * 1e3);
  
  if(status.is_error()) {
    promise_.set_error(status.move_as_error());
  }
  else {
    promise_.set_result(std::move(result));
  }
  stop();
}

td::Status ParseQuery::parse_impl() {
  td::optional<schema::Block> mc_block;
  for (auto &block_ds : mc_block_.shard_blocks_diff_) {
    td::Timer timer;
    // common block info
    block::gen::Block::Record blk;
    block::gen::BlockInfo::Record info;
    block::gen::BlockExtra::Record extra;
    if (!(tlb::unpack_cell(block_ds.block_data->root_cell(), blk) && tlb::unpack_cell(blk.info, info) && tlb::unpack_cell(blk.extra, extra))) {
        return td::Status::Error("block data info extra unpack failed");
    }

    // block details
    auto schema_block = parse_block(block_ds.block_data->root_cell(), block_ds.block_data->block_id(), blk, info, extra, mc_block);
    if (!mc_block) {
        mc_block = schema_block;
    }

    // transactions and messages
    td::Timer transactions_timer;
    std::map<td::Bits256, AccountStateShort> account_states_to_get;
    TRY_RESULT_ASSIGN(schema_block.transactions, parse_transactions(block_ds.block_data->block_id(), blk, info, extra, account_states_to_get));
    g_statistics.record_time(PARSE_TRANSACTION, transactions_timer.elapsed() * 1e6, schema_block.transactions.size());

    td::Timer acc_states_timer;
    TRY_RESULT(account_states_fast, parse_account_states_new(schema_block.workchain, schema_block.gen_utime, account_states_to_get));
    g_statistics.record_time(PARSE_ACCOUNT_STATE, acc_states_timer.elapsed() * 1e6, account_states_fast.size());
    
    for (auto &acc : account_states_fast) {
      result->account_states_.push_back(std::move(acc));
    }

    // config
    if (block_ds.block_data->block_id().is_masterchain()) {
      td::Timer config_timer;
      TRY_RESULT_ASSIGN(mc_block_.config_, block::ConfigInfo::extract_config(block_ds.block_state, block::ConfigInfo::needCapabilities | block::ConfigInfo::needLibraries));
      g_statistics.record_time(PARSE_CONFIG, config_timer.elapsed() * 1e6);
    }

    result->blocks_.push_back(schema_block);
    g_statistics.record_time(PARSE_BLOCK, timer.elapsed() * 1e3);
  }

  // shard details
  for (const auto &block_ds : mc_block_.shard_blocks_) {
    auto shard_block = parse_shard_state(mc_block.value(), block_ds.block_data->block_id());
    result->shard_state_.push_back(std::move(shard_block));
  }

  result->mc_block_ = mc_block_;
  return td::Status::OK();
}

schema::MasterchainBlockShard ParseQuery::parse_shard_state(const schema::Block& mc_block, const ton::BlockIdExt& shard_blk_id) {
  return {mc_block.seqno, mc_block.start_lt, mc_block.gen_utime, shard_blk_id.id.workchain, static_cast<int64_t>(shard_blk_id.id.shard), shard_blk_id.id.seqno};
}

schema::Block ParseQuery::parse_block(const td::Ref<vm::Cell>& root_cell, const ton::BlockIdExt& blk_id, block::gen::Block::Record& blk, const block::gen::BlockInfo::Record& info, 
                          const block::gen::BlockExtra::Record& extra, const td::optional<schema::Block> &mc_block) {
  schema::Block block;
  block.workchain = blk_id.id.workchain;
  block.shard = static_cast<int64_t>(blk_id.id.shard);
  block.seqno = blk_id.id.seqno;
  block.root_hash = td::base64_encode(blk_id.root_hash.as_slice());
  block.file_hash = td::base64_encode(blk_id.file_hash.as_slice());
  if (mc_block) {
      block.mc_block_workchain = mc_block.value().workchain;
      block.mc_block_shard = mc_block.value().shard;
      block.mc_block_seqno = mc_block.value().seqno;
  }
  else if (block.workchain == -1) {
      block.mc_block_workchain = block.workchain;
      block.mc_block_shard = block.shard;
      block.mc_block_seqno = block.seqno;
  }
  block.global_id = blk.global_id;
  block.version = info.version;
  block.after_merge = info.after_merge;
  block.before_split = info.before_split;
  block.after_split = info.after_split;
  block.want_merge = info.want_merge;
  block.want_split = info.want_split;
  block.key_block = info.key_block;
  block.vert_seqno_incr = info.vert_seqno_incr;
  block.flags = info.flags;
  block.gen_utime = info.gen_utime;
  block.start_lt = info.start_lt;
  block.end_lt = info.end_lt;
  block.validator_list_hash_short = info.gen_validator_list_hash_short;
  block.gen_catchain_seqno = info.gen_catchain_seqno;
  block.min_ref_mc_seqno = info.min_ref_mc_seqno;
  block.key_block = info.key_block;
  block.prev_key_block_seqno = info.prev_key_block_seqno;
  block.vert_seqno = info.vert_seq_no;
  block::gen::ExtBlkRef::Record mcref{};
  if (!info.not_master || tlb::unpack_cell(info.master_ref, mcref)) {
      block.master_ref_seqno = mcref.seq_no;
  }
  block.rand_seed = td::base64_encode(extra.rand_seed.as_slice());
  block.created_by = td::base64_encode(extra.created_by.as_slice());

  // prev blocks
  std::vector<ton::BlockIdExt> prev;
  ton::BlockIdExt mc_blkid;
  bool after_split;
  auto res = block::unpack_block_prev_blk_ext(root_cell, blk_id, prev, mc_blkid, after_split);

  for(auto& p : prev) {
    block.prev_blocks.push_back({p.id.workchain, static_cast<int64_t>(p.id.shard), p.id.seqno});
  }
  return block;
}

td::Result<schema::CurrencyCollection> ParseQuery::parse_currency_collection(td::Ref<vm::CellSlice> csr) {
  td::RefInt256 grams;
  std::map<uint32_t, td::RefInt256> extra_currencies;
  td::Ref<vm::Cell> extra;
  if (!block::unpack_CurrencyCollection(csr, grams, extra)) {
    return td::Status::Error(PSLICE() << "Failed to unpack currency collection");
  }
  vm::Dictionary extra_currencies_dict{extra, 32};
  auto it = extra_currencies_dict.begin();
  while (!it.eof()) {
    auto id = td::BitArray<32>(it.cur_pos()).to_ulong();
    auto value_cs = it.cur_value();
    auto value = block::tlb::t_VarUInteger_32.as_integer(value_cs);
    extra_currencies[id] = value;
    ++it;
  }
  return schema::CurrencyCollection{std::move(grams), std::move(extra_currencies)};
}

td::Result<td::Bits256> ext_in_msg_get_normalized_hash(td::Ref<vm::Cell> ext_in_msg_cell) {
  block::gen::Message::Record message;
  if (!tlb::type_unpack_cell(ext_in_msg_cell, block::gen::t_Message_Any, message)) {
    return td::Status::Error("Failed to unpack Message");
  }
  auto tag = block::gen::CommonMsgInfo().get_tag(*message.info);
  if (tag != block::gen::CommonMsgInfo::ext_in_msg_info) {
    return td::Status::Error("CommonMsgInfo tag is not ext_in_msg_info");
  }
  block::gen::CommonMsgInfo::Record_ext_in_msg_info msg_info;
  if (!tlb::csr_unpack(message.info, msg_info)) {
    return td::Status::Error("Failed to unpack CommonMsgInfo::ext_in_msg_info");
  }

  td::Ref<vm::Cell> body;
  auto body_cs = message.body.write();
  if (body_cs.fetch_ulong(1) == 1) {
    body = body_cs.fetch_ref();
  } else {
    body = vm::CellBuilder().append_cellslice(body_cs).finalize();
  }

  auto cb = vm::CellBuilder();
  bool status = 
    cb.store_long_bool(2, 2) &&                 // message$_ -> info:CommonMsgInfo -> ext_in_msg_info$10
    cb.store_long_bool(0, 2) &&                 // message$_ -> info:CommonMsgInfo -> src:MsgAddressExt -> addr_none$00
    cb.append_cellslice_bool(msg_info.dest) &&  // message$_ -> info:CommonMsgInfo -> dest:MsgAddressInt
    cb.store_long_bool(0, 4) &&                 // message$_ -> info:CommonMsgInfo -> import_fee:Grams -> 0
    cb.store_long_bool(0, 1) &&                 // message$_ -> init:(Maybe (Either StateInit ^StateInit)) -> nothing$0
    cb.store_long_bool(1, 1) &&                 // message$_ -> body:(Either X ^X) -> right$1
    cb.store_ref_bool(body);

  if (!status) {
    return td::Status::Error("Failed to build normalized message");
  }
  return cb.finalize()->get_hash().bits();
}

td::Result<schema::Message> ParseQuery::parse_message(td::Ref<vm::Cell> msg_cell) {
  schema::Message msg;
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

  // TODO: add message decoding

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

      TRY_RESULT_ASSIGN(msg.value, parse_currency_collection(msg_info.value));
      TRY_RESULT_ASSIGN(msg.source, convert::to_raw_address(msg_info.src));
      TRY_RESULT_ASSIGN(msg.destination, convert::to_raw_address(msg_info.dest));
      msg.fwd_fee = block::tlb::t_Grams.as_integer_skip(msg_info.fwd_fee.write());
      msg.ihr_fee = block::tlb::t_Grams.as_integer_skip(msg_info.ihr_fee.write());
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
      msg.import_fee = block::tlb::t_Grams.as_integer_skip(msg_info.import_fee.write());
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

td::Result<schema::TrStoragePhase> ParseQuery::parse_tr_storage_phase(vm::CellSlice& cs) {
  block::gen::TrStoragePhase::Record phase_data;
  if (!tlb::unpack(cs, phase_data)) {
    return td::Status::Error("Failed to unpack TrStoragePhase");
  }
  schema::TrStoragePhase phase;
  phase.storage_fees_collected = block::tlb::t_Grams.as_integer_skip(phase_data.storage_fees_collected.write());
  auto& storage_fees_due = phase_data.storage_fees_due.write();
  if (storage_fees_due.fetch_ulong(1) == 1) {
    phase.storage_fees_due = block::tlb::t_Grams.as_integer_skip(storage_fees_due);
  }
  phase.status_change = static_cast<schema::AccStatusChange>(phase_data.status_change);
  return phase;
}

td::Result<schema::TrCreditPhase> ParseQuery::parse_tr_credit_phase(vm::CellSlice& cs) {
  block::gen::TrCreditPhase::Record phase_data;
  if (!tlb::unpack(cs, phase_data)) {
    return td::Status::Error("Failed to unpack TrCreditPhase");
  }
  schema::TrCreditPhase phase;
  auto& due_fees_collected = phase_data.due_fees_collected.write();
  if (due_fees_collected.fetch_ulong(1) == 1) {
    phase.due_fees_collected = block::tlb::t_Grams.as_integer_skip(due_fees_collected);
  }
  // TRY_RESULT_ASSIGN(phase.credit, convert::to_balance(phase_data.credit));
  TRY_RESULT_ASSIGN(phase.credit, parse_currency_collection(phase_data.credit));
  return phase;
}

td::Result<schema::TrComputePhase> ParseQuery::parse_tr_compute_phase(vm::CellSlice& cs) {
  int compute_ph_tag = block::gen::t_TrComputePhase.get_tag(cs);
  if (compute_ph_tag == block::gen::TrComputePhase::tr_phase_compute_vm) {
    block::gen::TrComputePhase::Record_tr_phase_compute_vm compute_vm;
    if (!tlb::unpack(cs, compute_vm)) {
      return td::Status::Error("Error unpacking tr_phase_compute_vm");
    }
    schema::TrComputePhase_vm res;
    res.success = compute_vm.success;
    res.msg_state_used = compute_vm.msg_state_used;
    res.account_activated = compute_vm.account_activated;
    res.gas_fees = block::tlb::t_Grams.as_integer_skip(compute_vm.gas_fees.write());
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
    return schema::TrComputePhase_skipped{static_cast<schema::ComputeSkipReason>(skip.reason)};
  }
  return td::Status::OK();
}

td::Result<schema::StorageUsed> ParseQuery::parse_storage_used(vm::CellSlice& cs) {
  block::gen::StorageUsed::Record info;
  if (!tlb::unpack(cs, info)) {
    return td::Status::Error("Error unpacking StorageUsed");
  }
  schema::StorageUsed res;
  res.bits = block::tlb::t_VarUInteger_7.as_uint(*info.bits);
  res.cells = block::tlb::t_VarUInteger_7.as_uint(*info.cells);
  return res;
}

td::Result<schema::TrActionPhase> ParseQuery::parse_tr_action_phase(vm::CellSlice& cs) {
  block::gen::TrActionPhase::Record info;
  if (!tlb::unpack(cs, info)) {
    return td::Status::Error("Error unpacking TrActionPhase");
  }
  schema::TrActionPhase res;
  res.success = info.success;
  res.valid = info.valid;
  res.no_funds = info.no_funds;
  res.status_change = static_cast<schema::AccStatusChange>(info.status_change);
  auto& total_fwd_fees = info.total_fwd_fees.write();
  if (total_fwd_fees.fetch_ulong(1) == 1) {
    res.total_fwd_fees = block::tlb::t_Grams.as_integer_skip(info.total_fwd_fees.write());
  }
  auto& total_action_fees = info.total_action_fees.write();
  if (total_action_fees.fetch_ulong(1) == 1) {
    res.total_action_fees = block::tlb::t_Grams.as_integer_skip(info.total_action_fees.write());
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
  TRY_RESULT_ASSIGN(res.tot_msg_size, parse_storage_used(info.tot_msg_size.write()));
  return res;
}

td::Result<schema::TrBouncePhase> ParseQuery::parse_tr_bounce_phase(vm::CellSlice& cs) {
  int bounce_ph_tag = block::gen::t_TrBouncePhase.get_tag(cs);
  switch (bounce_ph_tag) {
    case block::gen::TrBouncePhase::tr_phase_bounce_negfunds: {
      block::gen::TrBouncePhase::Record_tr_phase_bounce_negfunds negfunds;
      if (!tlb::unpack(cs, negfunds)) {
        return td::Status::Error("Error unpacking tr_phase_bounce_negfunds");
      }
      return schema::TrBouncePhase_negfunds();
    }
    case block::gen::TrBouncePhase::tr_phase_bounce_nofunds: {
      block::gen::TrBouncePhase::Record_tr_phase_bounce_nofunds nofunds;
      if (!tlb::unpack(cs, nofunds)) {
        return td::Status::Error("Error unpacking tr_phase_bounce_nofunds");
      }
      schema::TrBouncePhase_nofunds res;
      TRY_RESULT_ASSIGN(res.msg_size, parse_storage_used(nofunds.msg_size.write()));
      res.req_fwd_fees = block::tlb::t_Grams.as_integer_skip(nofunds.req_fwd_fees.write());
      return res;
    }
    case block::gen::TrBouncePhase::tr_phase_bounce_ok: {
      block::gen::TrBouncePhase::Record_tr_phase_bounce_ok ok;
      if (!tlb::unpack(cs, ok)) {
        return td::Status::Error("Error unpacking tr_phase_bounce_ok");
      }
      schema::TrBouncePhase_ok res;
      TRY_RESULT_ASSIGN(res.msg_size, parse_storage_used(ok.msg_size.write()));
      res.msg_fees = block::tlb::t_Grams.as_integer_skip(ok.msg_fees.write());
      res.fwd_fees = block::tlb::t_Grams.as_integer_skip(ok.fwd_fees.write());
      return res;
    }
    default:
      return td::Status::Error("Unknown TrBouncePhase tag");
  }
}

td::Result<schema::SplitMergeInfo> ParseQuery::parse_split_merge_info(td::Ref<vm::CellSlice>& cs) {
  block::gen::SplitMergeInfo::Record info;
  if (!tlb::csr_unpack(cs, info)) {
    return td::Status::Error("Error unpacking SplitMergeInfo");
  }
  schema::SplitMergeInfo res;
  res.cur_shard_pfx_len = info.cur_shard_pfx_len;
  res.acc_split_depth = info.acc_split_depth;
  res.this_addr = info.this_addr;
  res.sibling_addr = info.sibling_addr;
  return res;
}

td::Result<schema::TransactionDescr> ParseQuery::process_transaction_descr(vm::CellSlice& td_cs) {
  auto tag = block::gen::t_TransactionDescr.get_tag(td_cs);
  switch (tag) {
    case block::gen::TransactionDescr::trans_ord: {
      block::gen::TransactionDescr::Record_trans_ord ord;
      if (!tlb::unpack_exact(td_cs, ord)) {
        return td::Status::Error("Error unpacking trans_ord");
      }
      schema::TransactionDescr_ord res;
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
    case block::gen::TransactionDescr::trans_storage: {
      block::gen::TransactionDescr::Record_trans_storage storage;
      if (!tlb::unpack_exact(td_cs, storage)) {
        return td::Status::Error("Error unpacking trans_storage");
      }
      schema::TransactionDescr_storage res;
      TRY_RESULT_ASSIGN(res.storage_ph, parse_tr_storage_phase(storage.storage_ph.write()));
      return res;
    }
    case block::gen::TransactionDescr::trans_tick_tock: {
      block::gen::TransactionDescr::Record_trans_tick_tock tick_tock;
      if (!tlb::unpack_exact(td_cs, tick_tock)) {
        return td::Status::Error("Error unpacking trans_tick_tock");
      }
      schema::TransactionDescr_tick_tock res;
      res.is_tock = tick_tock.is_tock;
      TRY_RESULT_ASSIGN(res.storage_ph, parse_tr_storage_phase(tick_tock.storage_ph.write()));
      TRY_RESULT_ASSIGN(res.compute_ph, parse_tr_compute_phase(tick_tock.compute_ph.write()));
      auto& action = tick_tock.action.write();
      if (action.fetch_ulong(1) == 1) {
        auto action_cs = vm::load_cell_slice(action.fetch_ref());
        TRY_RESULT_ASSIGN(res.action, parse_tr_action_phase(action_cs));
      }
      res.aborted = tick_tock.aborted;
      res.destroyed = tick_tock.destroyed;
      return res;
    }
    case block::gen::TransactionDescr::trans_split_prepare: {
      block::gen::TransactionDescr::Record_trans_split_prepare split_prepare;
      if (!tlb::unpack_exact(td_cs, split_prepare)) {
        return td::Status::Error("Error unpacking trans_split_prepare");
      }
      schema::TransactionDescr_split_prepare res;
      TRY_RESULT_ASSIGN(res.split_info, parse_split_merge_info(split_prepare.split_info));
      auto& storage_ph = split_prepare.storage_ph.write();
      if (storage_ph.fetch_ulong(1)) {
        TRY_RESULT_ASSIGN(res.storage_ph, parse_tr_storage_phase(storage_ph));
      }
      TRY_RESULT_ASSIGN(res.compute_ph, parse_tr_compute_phase(split_prepare.compute_ph.write()));
      auto& action = split_prepare.action.write();
      if (action.fetch_ulong(1)) {
        auto action_cs = vm::load_cell_slice(action.fetch_ref());
        TRY_RESULT_ASSIGN(res.action, parse_tr_action_phase(action_cs));
      }
      res.aborted = split_prepare.aborted;
      res.destroyed = split_prepare.destroyed;
      return res;
    }
    case block::gen::TransactionDescr::trans_split_install: {
      block::gen::TransactionDescr::Record_trans_split_install split_install;
      if (!tlb::unpack_exact(td_cs, split_install)) {
        return td::Status::Error("Error unpacking trans_split_install");
      }
      schema::TransactionDescr_split_install res;
      TRY_RESULT_ASSIGN(res.split_info, parse_split_merge_info(split_install.split_info));
      res.installed = split_install.installed;
      return res;
    }
    case block::gen::TransactionDescr::trans_merge_prepare: {
      block::gen::TransactionDescr::Record_trans_merge_prepare merge_prepare;
      if (!tlb::unpack_exact(td_cs, merge_prepare)) {
        return td::Status::Error("Error unpacking trans_merge_prepare");
      }
      schema::TransactionDescr_merge_prepare res;
      TRY_RESULT_ASSIGN(res.split_info, parse_split_merge_info(merge_prepare.split_info));
      TRY_RESULT_ASSIGN(res.storage_ph, parse_tr_storage_phase(merge_prepare.storage_ph.write()));
      res.aborted = merge_prepare.aborted;
      return res;
    }
    case block::gen::TransactionDescr::trans_merge_install: {
      block::gen::TransactionDescr::Record_trans_merge_install merge_install;
      if (!tlb::unpack_exact(td_cs, merge_install)) {
        return td::Status::Error("Error unpacking trans_merge_install");
      }
      schema::TransactionDescr_merge_install res;
      TRY_RESULT_ASSIGN(res.split_info, parse_split_merge_info(merge_install.split_info));
      auto& storage_ph = merge_install.storage_ph.write();
      if (storage_ph.fetch_ulong(1)) {
        TRY_RESULT_ASSIGN(res.storage_ph, parse_tr_storage_phase(storage_ph));
      }
      auto& credit_ph = merge_install.credit_ph.write();
      if (credit_ph.fetch_ulong(1)) {
        TRY_RESULT_ASSIGN(res.credit_ph, parse_tr_credit_phase(credit_ph));
      }
      TRY_RESULT_ASSIGN(res.compute_ph, parse_tr_compute_phase(merge_install.compute_ph.write()));
      auto& action = merge_install.action.write();
      if (action.fetch_ulong(1)) {
        auto action_cs = vm::load_cell_slice(action.fetch_ref());
        TRY_RESULT_ASSIGN(res.action, parse_tr_action_phase(action_cs));
      }
      res.aborted = merge_install.aborted;
      res.destroyed = merge_install.destroyed;
      return res;
    }
    default:
      return td::Status::Error("Unknown transaction description type");
  }
}

td::Result<std::vector<schema::Transaction>> ParseQuery::parse_transactions(const ton::BlockIdExt& blk_id, const block::gen::Block::Record &block, 
                              const block::gen::BlockInfo::Record &info, const block::gen::BlockExtra::Record &extra,
                              std::map<td::Bits256, AccountStateShort> &account_states) {
  std::vector<schema::Transaction> res;
  try {
    vm::AugmentedDictionary acc_dict{vm::load_cell_slice_ref(extra.account_blocks), 256, block::tlb::aug_ShardAccountBlocks};

    td::Bits256 cur_addr = td::Bits256::zero();
    bool eof = false;
    bool allow_same = true;
    while (!eof) {
      td::Ref<vm::CellSlice> value;
      try {
        value = acc_dict.extract_value(
            acc_dict.vm::DictionaryFixed::lookup_nearest_key(cur_addr.bits(), 256, true, allow_same));
      } catch (vm::VmError err) {
        return td::Status::Error(PSLICE() << "error while traversing account block dictionary: " << err.get_msg());
      }
      if (value.is_null()) {
        eof = true;
        break;
      }
      allow_same = false;
      block::gen::AccountBlock::Record acc_blk;
      if (!(tlb::csr_unpack(std::move(value), acc_blk) && acc_blk.account_addr == cur_addr)) {
        return td::Status::Error("invalid AccountBlock for account " + cur_addr.to_hex());
      }
      vm::AugmentedDictionary trans_dict{vm::DictNonEmpty(), std::move(acc_blk.transactions), 64,
                                          block::tlb::aug_AccountTransactions};
      td::BitArray<64> cur_trans{(long long)0};
      while (true) {
        td::Ref<vm::Cell> tvalue;
        try {
          tvalue = trans_dict.extract_value_ref(
              trans_dict.vm::DictionaryFixed::lookup_nearest_key(cur_trans.bits(), 64, true));
        } catch (vm::VmError err) {
          return td::Status::Error(PSLICE() << "error while traversing transaction dictionary of an AccountBlock: " << err.get_msg());
        }
        if (tvalue.is_null()) {
          break;
        }
        block::gen::Transaction::Record trans;
        if (!tlb::unpack_cell(tvalue, trans)) {
          return td::Status::Error("Failed to unpack Transaction");
        }

        schema::Transaction schema_tx;

        schema_tx.account = block::StdAddress(blk_id.id.workchain, cur_addr);
        schema_tx.hash = tvalue->get_hash().bits();
        schema_tx.lt = trans.lt;
        schema_tx.prev_trans_hash = trans.prev_trans_hash;
        schema_tx.prev_trans_lt = trans.prev_trans_lt;
        schema_tx.now = trans.now;
        schema_tx.mc_seqno = mc_seqno_;

        schema_tx.orig_status = static_cast<schema::AccountStatus>(trans.orig_status);
        schema_tx.end_status = static_cast<schema::AccountStatus>(trans.end_status);

        TRY_RESULT_ASSIGN(schema_tx.total_fees, parse_currency_collection(trans.total_fees));

        if (trans.r1.in_msg->prefetch_long(1)) {
          auto msg = trans.r1.in_msg->prefetch_ref();
          TRY_RESULT_ASSIGN(schema_tx.in_msg, parse_message(trans.r1.in_msg->prefetch_ref()));
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

        res.push_back(schema_tx);
        
        account_states[cur_addr] = {schema_tx.account_state_hash_after, schema_tx.lt, schema_tx.hash};
      }
    }
  } catch (vm::VmError err) {
      return td::Status::Error(PSLICE() << "error while parsing AccountBlocks : " << err.get_msg());
  }
  return res;
}

td::Result<std::vector<schema::AccountState>> ParseQuery::parse_account_states_new(ton::WorkchainId workchain_id, uint32_t gen_utime, std::map<td::Bits256, AccountStateShort> &account_states) {
  std::vector<schema::AccountState> res;
  res.reserve(account_states.size());
  for (auto &[addr, state_short] : account_states) {
    auto account_cell_r = cell_db_reader_->load_cell(state_short.account_cell_hash.as_slice());
    if (account_cell_r.is_error()) {
      LOG(ERROR) << "Failed to load account state cell " << state_short.account_cell_hash.to_hex();
      continue;
    }
    auto account_cell = account_cell_r.move_as_ok();
    int account_tag = block::gen::t_Account.get_tag(vm::load_cell_slice(account_cell));
    switch (account_tag) {
    case block::gen::Account::account_none: {
      auto address = block::StdAddress(workchain_id, addr);
      TRY_RESULT(account, parse_none_account(std::move(account_cell), address, gen_utime, state_short.last_transaction_hash, state_short.last_transaction_lt));
      res.push_back(account);
      break;
    }
    case block::gen::Account::account: {
      TRY_RESULT(account, parse_account(std::move(account_cell), gen_utime, state_short.last_transaction_hash, state_short.last_transaction_lt));
      res.push_back(account);
      break;
    }
    default:
      return td::Status::Error("Unknown account tag");
    }
  }
  return res;
}


td::Result<schema::AccountState> ParseQuery::parse_none_account(td::Ref<vm::Cell> account_root, block::StdAddress address, uint32_t gen_utime, td::Bits256 last_trans_hash, uint64_t last_trans_lt) {
  block::gen::Account::Record_account_none account_none;
  if (!tlb::unpack_cell(account_root, account_none)) {
    return td::Status::Error("Failed to unpack Account none");
  }
  schema::AccountState schema_account;
  schema_account.account = address;
  schema_account.hash = account_root->get_hash().bits();
  schema_account.timestamp = gen_utime;
  schema_account.account_status = "nonexist";
  schema_account.balance = schema::CurrencyCollection{td::RefInt256{true, 0}, {}};
  schema_account.last_trans_hash = last_trans_hash;
  schema_account.last_trans_lt = last_trans_lt;
  return schema_account;
}

td::Result<schema::AccountState> ParseQuery::parse_account(td::Ref<vm::Cell> account_root, uint32_t gen_utime, td::Bits256 last_trans_hash, uint64_t last_trans_lt) {
  block::gen::Account::Record_account account;
  if (!tlb::unpack_cell(account_root, account)) {
    return td::Status::Error("Failed to unpack Account");
  }
  block::gen::AccountStorage::Record storage;
  if (!tlb::csr_unpack(account.storage, storage)) {
    return td::Status::Error("Failed to unpack AccountStorage");
  }

  schema::AccountState schema_account;
  schema_account.hash = account_root->get_hash().bits();
  TRY_RESULT(account_addr, convert::to_raw_address(account.addr));
  TRY_RESULT_ASSIGN(schema_account.account, block::StdAddress::parse(account_addr));
  TRY_RESULT_ASSIGN(schema_account.balance, parse_currency_collection(storage.balance));
  schema_account.timestamp = gen_utime;
  schema_account.last_trans_hash = last_trans_hash;
  schema_account.last_trans_lt = last_trans_lt;
  
  int account_state_tag = block::gen::t_AccountState.get_tag(storage.state.write());
  switch (account_state_tag) {
    case block::gen::AccountState::account_uninit:
      schema_account.account_status = "uninit";
      break;
    case block::gen::AccountState::account_frozen: {
      schema_account.account_status = "frozen";
      block::gen::AccountState::Record_account_frozen frozen;
      if (!tlb::csr_unpack(storage.state, frozen)) {
        return td::Status::Error("Failed to unpack AccountState frozen");
      }
      schema_account.frozen_hash = frozen.state_hash;
      break;
    }
    case block::gen::AccountState::account_active: {
      schema_account.account_status = "active";
      block::gen::AccountState::Record_account_active active;
      if (!tlb::csr_unpack(storage.state, active)) {
        return td::Status::Error("Failed to unpack AccountState active");
      }
      block::gen::StateInit::Record state_init;
      if (!tlb::csr_unpack(active.x, state_init)) {
        return td::Status::Error("Failed to unpack StateInit");
      }
      auto& code_cs = state_init.code.write();
      if (code_cs.fetch_long(1) != 0) {
        schema_account.code = code_cs.prefetch_ref();
        schema_account.code_hash = schema_account.code->get_hash().bits();
      }
      auto& data_cs = state_init.data.write();
      if (data_cs.fetch_long(1) != 0) {
        schema_account.data = data_cs.prefetch_ref();
        schema_account.data_hash = schema_account.data->get_hash().bits();
      }
      break;
    }
    default:
      return td::Status::Error("Unknown account state tag");
  }
  return schema_account;
}
