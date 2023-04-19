#include "DataParser.h"
#include "td/utils/common.h"
#include "crypto/common/refcnt.hpp"
#include "crypto/block/block.h"
#include "crypto/block/transaction.h"
#include "crypto/block/block-auto.h"
#include "crypto/block/block-parse.h"
#include "validator/interfaces/block.h"
#include "validator/interfaces/shard.h"

using namespace ton::validator; //TODO: remove this

class ParseQuery: public td::actor::Actor {
private:
  const int mc_seqno_;
  MasterchainBlockDataState mc_block_;
  ParsedBlock result;
  td::Promise<ParsedBlock> promise_;
public:
  ParseQuery(int mc_seqno, MasterchainBlockDataState mc_block, td::Promise<ParsedBlock> promise)
    : mc_seqno_(mc_seqno), mc_block_(std::move(mc_block)), promise_(std::move(promise)) {}

  void start_up() override {
    auto status = parse_impl();
    if(status.is_error()) {
      promise_.set_error(status.move_as_error());
    }
    else {
      promise_.set_result(std::move(result));
    }
    stop();
  }

private:
  td::Status parse_impl() {
    td::optional<schema::Block> mc_block;
    for (auto &block_ds : mc_block_) {
      // common block info
      block::gen::Block::Record blk;
      block::gen::BlockInfo::Record info;
      block::gen::BlockExtra::Record extra;
      if (!(tlb::unpack_cell(block_ds.block_data->root_cell(), blk) && tlb::unpack_cell(blk.info, info) && tlb::unpack_cell(blk.extra, extra))) {
          return td::Status::Error("block data info extra unpack failed");
      }

      // block details
      auto schema_block = parse_block(block_ds.block_data->block_id(), blk, info, extra, mc_block);
      if (!mc_block) {
          mc_block = schema_block;
      }
      result.blocks_.push_back(schema_block);

      // transactions and messages
      std::set<td::Bits256> addresses;
      TRY_STATUS(parse_transactions(block_ds.block_data->block_id(), blk, info, extra, addresses));

      // account states
      TRY_STATUS(parse_account_states(block_ds.block_state, addresses));
    }
    return td::Status::OK();
  }

  schema::Block parse_block(const ton::BlockIdExt& blk_id, block::gen::Block::Record& blk, const block::gen::BlockInfo::Record& info, 
                            const block::gen::BlockExtra::Record& extra, td::optional<schema::Block> &mc_block) {
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
    block.global_id = blk.global_id;
    block.version = info.version;
    block.after_merge = info.after_merge;
    block.before_split = info.before_split;
    block.after_split = info.after_split;
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
    return block;
  }

  td::Result<schema::Message> parse_message(td::Ref<vm::Cell> msg_cell) {
    schema::Message msg;
    msg.hash = td::base64_encode(msg_cell->get_hash().as_slice());

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
    auto body_cell = vm::CellBuilder().append_cellslice(*body).finalize();
    TRY_RESULT(body_boc, to_bytes(body_cell));
    msg.body = body_boc.value();
    msg.body_hash = td::base64_encode(body_cell->get_hash().as_slice());

    if (body->prefetch_long(32) != vm::CellSlice::fetch_long_eof) {
      msg.opcode = body->prefetch_long(32);
    }

    td::Ref<vm::Cell> init_state_cell;
    auto& init_state_cs = message.init.write();
    if (init_state_cs.fetch_ulong(1) == 1) {
      if (init_state_cs.fetch_long(1) == 0) {
        init_state_cell = vm::CellBuilder().append_cellslice(init_state_cs).finalize();
      } else {
        init_state_cell = init_state_cs.fetch_ref();
      }
    }
    TRY_RESULT_ASSIGN(msg.init_state, to_bytes(init_state_cell));
    if (init_state_cell.not_null()) {
      msg.init_state_hash = td::base64_encode(init_state_cell->get_hash().as_slice());
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
        TRY_RESULT_ASSIGN(msg.source, to_std_address(msg_info.src));
        TRY_RESULT_ASSIGN(msg.destination, to_std_address(msg_info.dest));
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
        TRY_RESULT_ASSIGN(msg.destination, to_std_address(msg_info.dest))
        TRY_RESULT_ASSIGN(msg.import_fee, to_balance(msg_info.import_fee));
        return msg;
      }
      case block::gen::CommonMsgInfo::ext_out_msg_info: {
        block::gen::CommonMsgInfo::Record_ext_out_msg_info msg_info;
        if (!tlb::csr_unpack(message.info, msg_info)) {
          return td::Status::Error("Failed to unpack CommonMsgInfo::ext_out_msg_info");
        }
        TRY_RESULT_ASSIGN(msg.source, to_std_address(msg_info.src));
        // msg.destination = null, because it is external
        msg.created_lt = static_cast<uint64_t>(msg_info.created_lt);
        msg.created_at = static_cast<uint32_t>(msg_info.created_at);
        return msg;
      }
    }

    return td::Status::Error("Unknown CommonMsgInfo tag");
  }

  td::Result<td::int64> to_balance(vm::CellSlice& balance_slice) {
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

  td::Result<td::int64> to_balance(td::Ref<vm::CellSlice> balance_ref) {
    vm::CellSlice balance_slice = *balance_ref;
    return to_balance(balance_slice);
  }

  td::Result<td::optional<std::string>> to_bytes(td::Ref<vm::Cell> cell) {
    if (cell.is_null()) {
      return td::optional<std::string>();
    }
    TRY_RESULT(boc, vm::std_boc_serialize(cell, vm::BagOfCells::Mode::WithCRC32C));
    return td::base64_encode(boc.as_slice().str());
  }

  td::Result<std::string> to_std_address(td::Ref<vm::CellSlice> cs) {
    auto tag = block::gen::MsgAddressInt().get_tag(*cs);
    if (tag < 0) {
      return td::Status::Error("Failed to read MsgAddressInt tag");
    }
    if (tag != block::gen::MsgAddressInt::addr_std) {
      return "";
    }
    block::gen::MsgAddressInt::Record_addr_std addr;
    if (!tlb::csr_unpack(cs, addr)) {
      return td::Status::Error("Failed to unpack MsgAddressInt");
    }
    return std::to_string(addr.workchain_id) + ":" + addr.address.to_hex();
  }

  td::Status process_transaction_descr(schema::Transaction& schema_tx, vm::CellSlice& td_cs, int tag, td::Ref<vm::CellSlice>& compute_ph, td::Ref<vm::CellSlice>& action_ph) {
    switch (tag) {
      case block::gen::TransactionDescr::trans_ord: {
        schema_tx.transaction_type = "trans_ord";
        block::gen::TransactionDescr::Record_trans_ord ord;
        if (!tlb::unpack_exact(td_cs, ord)) {
          return td::Status::Error("Error unpacking trans_ord");
        }
        compute_ph = ord.compute_ph;
        action_ph = ord.action;
        break;
      }
      case block::gen::TransactionDescr::trans_storage: {
        schema_tx.transaction_type = "trans_storage";
        break;
      }
      case block::gen::TransactionDescr::trans_tick_tock: {
        schema_tx.transaction_type = "trans_tick_tock";
        block::gen::TransactionDescr::Record_trans_tick_tock tick_tock;
        if (!tlb::unpack_exact(td_cs, tick_tock)) {
          return td::Status::Error("Error unpacking trans_tick_tock");
        }
        compute_ph = tick_tock.compute_ph;
        action_ph = tick_tock.action;
        break;
      }
      case block::gen::TransactionDescr::trans_split_prepare: {
        schema_tx.transaction_type = "trans_split_prepare";
        block::gen::TransactionDescr::Record_trans_split_prepare split_prepare;
        if (!tlb::unpack_exact(td_cs, split_prepare)) {
          return td::Status::Error("Error unpacking trans_split_prepare");
        }
        compute_ph = split_prepare.compute_ph;
        action_ph = split_prepare.action;
        break;
      }
      case block::gen::TransactionDescr::trans_split_install: {
        schema_tx.transaction_type = "trans_split_install";
        break;
      }
      case block::gen::TransactionDescr::trans_merge_prepare: {
        schema_tx.transaction_type = "trans_merge_prepare";
        break;
      }
      case block::gen::TransactionDescr::trans_merge_install: {
        schema_tx.transaction_type = "trans_merge_install";
        block::gen::TransactionDescr::Record_trans_merge_install merge_install;
        if (!tlb::unpack_exact(td_cs, merge_install)) {
          return td::Status::Error("Error unpacking trans_merge_install");
        }
        compute_ph = merge_install.compute_ph;
        action_ph = merge_install.action;
        break;
      }
    }
    return td::Status::OK();
  }

  td::Status process_compute_phase(schema::Transaction& schema_tx, td::Ref<vm::CellSlice>& compute_ph) {
    int compute_ph_tag = block::gen::t_TrComputePhase.get_tag(*compute_ph);
    if (compute_ph_tag == block::gen::TrComputePhase::tr_phase_compute_vm) {
      block::gen::TrComputePhase::Record_tr_phase_compute_vm compute_vm;
      if (!tlb::csr_unpack(compute_ph, compute_vm)) {
        return td::Status::Error("Error unpacking tr_phase_compute_vm");
      }
      schema_tx.compute_exit_code = compute_vm.r1.exit_code;
      schema_tx.compute_gas_used = block::tlb::t_VarUInteger_7.as_uint(*compute_vm.r1.gas_used);
      schema_tx.compute_gas_limit = block::tlb::t_VarUInteger_7.as_uint(*compute_vm.r1.gas_limit);
      schema_tx.compute_gas_credit = block::tlb::t_VarUInteger_3.as_uint(*compute_vm.r1.gas_credit);
      schema_tx.compute_gas_fees = block::tlb::t_Grams.as_uint(*compute_vm.gas_fees);
      schema_tx.compute_vm_steps = compute_vm.r1.vm_steps;
    } else if (compute_ph_tag == block::gen::TrComputePhase::tr_phase_compute_skipped) {
      block::gen::TrComputePhase::Record_tr_phase_compute_skipped skip;
      if (!tlb::csr_unpack(compute_ph, skip)) {
        return td::Status::Error("Error unpacking tr_phase_compute_skipped");
      }
      switch (skip.reason) {
        case block::gen::ComputeSkipReason::cskip_no_state:
          schema_tx.compute_skip_reason = "cskip_no_state";
          break;
        case block::gen::ComputeSkipReason::cskip_bad_state:
          schema_tx.compute_skip_reason = "cskip_bad_state";
          break;
        case block::gen::ComputeSkipReason::cskip_no_gas:
          schema_tx.compute_skip_reason = "cskip_no_gas";
          break;
        // case block::gen::ComputeSkipReason::cskip_suspended:
        //   schema_tx.compute_skip_reason = "cskip_suspended";
        //   break;
      }
    }
    return td::Status::OK();
  }

  td::Status process_action_phase(schema::Transaction& schema_tx, td::Ref<vm::CellSlice>& action_ph) {
    auto& action_cs = action_ph.write();
    if (action_cs.fetch_ulong(1) == 1) {
      block::gen::TrActionPhase::Record action;
      if (!tlb::unpack_cell(action_cs.fetch_ref(), action)) {
        return td::Status::Error("Error unpacking TrActionPhase");
      }
      schema_tx.action_result_code = action.result_code;
      auto& total_fwd_fees_cs = action.total_fwd_fees.write();
      if (total_fwd_fees_cs.fetch_ulong(1) == 1) {
        TRY_RESULT_ASSIGN(schema_tx.action_total_fwd_fees, to_balance(total_fwd_fees_cs));
      }
      auto& total_action_fees_cs = action.total_action_fees.write();
      if (total_action_fees_cs.fetch_ulong(1) == 1) {
        TRY_RESULT_ASSIGN(schema_tx.action_total_action_fees, to_balance(total_action_fees_cs));
      }
    }
    return td::Status::OK();
  }

  td::Status parse_transactions(const ton::BlockIdExt& blk_id, const block::gen::Block::Record &block, 
                                const block::gen::BlockInfo::Record &info, const block::gen::BlockExtra::Record &extra,
                                std::set<td::Bits256> &addresses) {
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
          schema_tx.block_workchain = blk_id.id.workchain;
          schema_tx.block_shard = static_cast<int64_t>(blk_id.id.shard);
          schema_tx.block_seqno = blk_id.id.seqno;

          schema_tx.account = std::to_string(blk_id.id.workchain) + ":" + cur_addr.to_hex();
          schema_tx.hash = td::base64_encode(tvalue->get_hash().as_slice());
          schema_tx.lt = trans.lt;
          schema_tx.utime = trans.now;

          TRY_RESULT_ASSIGN(schema_tx.fees, to_balance(trans.total_fees));

          td::RefInt256 storage_fees;
          if (!block::tlb::t_TransactionDescr.get_storage_fees(trans.description, storage_fees)) {
            return td::Status::Error("Failed to fetch storage fee from transaction");
          }
          schema_tx.storage_fees = storage_fees->to_long();

          auto td_cs = vm::load_cell_slice(trans.description);
          int tag = block::gen::t_TransactionDescr.get_tag(td_cs);
          td::Ref<vm::CellSlice> compute_ph;
          td::Ref<vm::CellSlice> action_ph;
          
          TRY_STATUS(process_transaction_descr(schema_tx, td_cs, tag, compute_ph, action_ph));

          if (compute_ph.not_null()) {
            TRY_STATUS(process_compute_phase(schema_tx, compute_ph));
          }
          if (action_ph.not_null()) {
            TRY_STATUS(process_action_phase(schema_tx, action_ph));
          }

          auto is_just = trans.r1.in_msg->prefetch_long(1);
          if (is_just == trans.r1.in_msg->fetch_long_eof) {
            return td::Status::Error("Failed to parse long");
          }
          if (is_just == -1) {
            auto msg = trans.r1.in_msg->prefetch_ref();
            TRY_RESULT(in_msg, parse_message(trans.r1.in_msg->prefetch_ref()));

            result.messages_.push_back(in_msg);

            schema::TransactionMessage tx_msg;
            tx_msg.transaction_hash = schema_tx.hash;
            tx_msg.message_hash = in_msg.hash;
            tx_msg.direction = "in";
            result.transaction_messages_.push_back(tx_msg);
          }

          if (trans.outmsg_cnt != 0) {
            vm::Dictionary dict{trans.r1.out_msgs, 15};
            for (int x = 0; x < trans.outmsg_cnt; x++) {
              TRY_RESULT(out_msg, parse_message(dict.lookup_ref(td::BitArray<15>{x})));

              result.messages_.push_back(out_msg);

              schema::TransactionMessage tx_msg;
              tx_msg.transaction_hash = schema_tx.hash;
              tx_msg.message_hash = out_msg.hash;
              tx_msg.direction = "out";
              result.transaction_messages_.push_back(tx_msg);
            }
          }

          block::gen::HASH_UPDATE::Record state_hash_update;
          if (!tlb::type_unpack_cell(std::move(trans.state_update), block::gen::t_HASH_UPDATE_Account, state_hash_update)) {
            return td::Status::Error("Failed to unpack state_update");
          }
          
          schema_tx.account_state_hash_before = td::base64_encode(state_hash_update.old_hash.as_slice());
          schema_tx.account_state_hash_after = td::base64_encode(state_hash_update.new_hash.as_slice());

          result.transactions_.push_back(schema_tx);

          addresses.insert(cur_addr);
        }
      }
    } catch (vm::VmError err) {
        return td::Status::Error(PSLICE() << "error while parsing AccountBlocks : " << err.get_msg());
    }
    return td::Status::OK();
  }

  td::Status parse_account_states(const td::Ref<ShardState>& block_state, std::set<td::Bits256> &addresses) {
    auto root = block_state->root_cell();
    block::gen::ShardStateUnsplit::Record sstate;
    if (!tlb::unpack_cell(root, sstate)) {
      return td::Status::Error("Failed to unpack ShardStateUnsplit");
    }
    vm::AugmentedDictionary accounts_dict{vm::load_cell_slice_ref(sstate.accounts), 256, block::tlb::aug_ShardAccounts};
    for (auto &addr : addresses) {
      auto shard_account_csr = accounts_dict.lookup(addr);
      if (shard_account_csr.is_null()) {
        if (addr.to_hex() != "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEF"){
          LOG(WARNING) << "Could not find account " << addr.to_hex() << " in shard state";
        }
        continue;
      } 
      td::Ref<vm::Cell> account_root = shard_account_csr->prefetch_ref();

      int account_tag = block::gen::t_Account.get_tag(vm::load_cell_slice(account_root));
      switch (account_tag) {
      case block::gen::Account::account_none:
        continue;
      case block::gen::Account::account: {
        TRY_RESULT(account, parse_account(account_root));
        result.account_states_.push_back(account);
        break;
      }
      default:
        return td::Status::Error("Unknown account tag");
      }
    }
    LOG(DEBUG) << "Parsed " << result.account_states_.size() << " account states";
    return td::Status::OK();
  }

  td::Result<schema::AccountState> parse_account(td::Ref<vm::Cell> account_root) {
    auto hash = td::base64_encode(account_root->get_hash().as_slice());
    block::gen::Account::Record_account account;
    if (!tlb::unpack_cell(account_root, account)) {
      return td::Status::Error("Failed to unpack Account");
    }
    block::gen::AccountStorage::Record storage;
    if (!tlb::csr_unpack(account.storage, storage)) {
      return td::Status::Error("Failed to unpack AccountStorage");
    }

    schema::AccountState schema_account;
    schema_account.hash = hash;
    TRY_RESULT_ASSIGN(schema_account.account, to_std_address(account.addr));
    TRY_RESULT_ASSIGN(schema_account.balance, to_balance(storage.balance));

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
        schema_account.frozen_hash = td::base64_encode(frozen.state_hash.as_slice());
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
          schema_account.code_hash = td::base64_encode(code_cs.prefetch_ref()->get_hash().as_slice());
        }
        auto& data_cs = state_init.data.write();
        if (data_cs.fetch_long(1) != 0) {
          schema_account.data_hash = td::base64_encode(data_cs.prefetch_ref()->get_hash().as_slice());
        }
        break;
      }
      default:
        return td::Status::Error("Unknown account state tag");
    }
    return schema_account;
  }
};


ParseManager::ParseManager() {
    
}

void ParseManager::parse(int mc_seqno, MasterchainBlockDataState mc_block, td::Promise<ParsedBlock> promise) {
    td::actor::create_actor<ParseQuery>("parsequery", mc_seqno, std::move(mc_block), std::move(promise)).release();
}

void ParseManager::start_up() {

}
