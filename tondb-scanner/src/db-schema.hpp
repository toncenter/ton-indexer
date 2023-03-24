
#include "td/utils/common.h"
#include "crypto/common/refcnt.hpp"
#include "crypto/block/block.h"
#include "crypto/block/transaction.h"
#include "crypto/block/block-auto.h"
#include "crypto/block/block-parse.h"
#include "validator/interfaces/block.h"
#include "validator/interfaces/shard.h"


using namespace ton::validator;

struct BlockDataState {
  td::Ref<BlockData> block_data;
  td::Ref<ShardState> block_state;
};

namespace schema {

struct Transaction {
    // Block-related fields
    int32_t block_workchain;
    uint64_t block_shard;
    int32_t block_seqno;

    // Transaction-related fields
    std::string account;
    std::string hash;
    uint64_t lt;
    uint32_t utime;
    std::string transaction_type;

    // Account state-related fields
    std::string account_state_hash_before;
    std::string account_state_hash_after;

    // Fee-related fields
    uint64_t fees;
    uint64_t storage_fees;
    uint64_t in_fwd_fees;
    uint64_t computation_fees;
    uint64_t action_fees;

    // Computation-related fields
    int32_t compute_exit_code;
    uint32_t compute_gas_used;
    uint32_t compute_gas_limit;
    uint32_t compute_gas_credit;
    uint32_t compute_gas_fees;
    uint32_t compute_vm_steps;
    std::string compute_skip_reason;

    // Action-related fields
    int32_t action_result_code;
    uint64_t action_total_fwd_fees;
    uint64_t action_total_action_fees;
};

struct Message {
    std::string hash;
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

struct McBlockEntry {
    std::vector<schema::Block> blocks;
    std::vector<schema::Transaction> transactions;
};

class BlockToSchema {
private:
  std::vector<BlockDataState> mc_block_;

  std::vector<schema::Block> blocks_;
  std::vector<schema::Transaction> transactions_;
  std::vector<schema::Message> messages_;
  std::vector<schema::TransactionMessage> transaction_messages_;
public:
  BlockToSchema(std::vector<BlockDataState> mc_block) : mc_block_(mc_block) {}

  td::Status parse() {
      TRY_STATUS(parse_blocks());
      TRY_STATUS(parse_transactions_and_messages());
      return td::Status::OK();
  }

  std::vector<schema::Block> get_blocks() {
      return blocks_;
  }

  std::vector<schema::Transaction> get_transactions() {
      return transactions_;
  }

  std::vector<schema::Message> get_messages() {
      return messages_;
  }

  td::Status parse_blocks() {
      td::optional<int32_t> mc_block_workchain;
      td::optional<int64_t> mc_block_shard;
      td::optional<int32_t> mc_block_seqno;

      for (auto &block_ds : mc_block_) {
          block::gen::Block::Record blk;
          block::gen::BlockInfo::Record info;
          block::gen::BlockExtra::Record extra;
          if (!(tlb::unpack_cell(block_ds.block_data->root_cell(), blk) && tlb::unpack_cell(blk.info, info) && tlb::unpack_cell(blk.extra, extra))) {
              return td::Status::Error("block data info extra unpack failed");
          }
          schema::Block block;
          block.workchain = block_ds.block_data->block_id().id.workchain;
          block.shard = static_cast<int64_t>(block_ds.block_data->block_id().id.shard);
          block.seqno = block_ds.block_data->block_id().id.seqno;
          block.root_hash = td::base64_encode(block_ds.block_data->block_id().root_hash.as_slice());
          block.file_hash = td::base64_encode(block_ds.block_data->block_id().file_hash.as_slice());
          block.mc_block_workchain = mc_block_workchain;
          block.mc_block_shard = mc_block_shard;
          block.mc_block_seqno = mc_block_seqno;
          if (!mc_block_workchain) {
              mc_block_workchain = block.workchain;
              mc_block_shard = block.shard;
              mc_block_seqno = block.seqno;
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
          blocks_.push_back(block);
      }
      return td::Status::OK();
  }

  td::Status parse_transactions_and_messages() {
      for (auto& block_ds: mc_block_) {
          TRY_STATUS(fetch_transactions(block_ds.block_data->root_cell()));
      }
      return td::Status::OK();
  }

  td::Result<schema::Message> parse_message(td::Ref<vm::Cell> msg_cell) {
      schema::Message msg;
      msg.hash = td::base64_encode(msg_cell->get_hash().as_slice());
      // TODO: add rest of fields
      return msg;
  }

  td::Result<td::int64> to_balance_or_throw(vm::CellSlice& balance_slice) {
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
    return TRY_VM(to_balance_or_throw(balance_slice));
  }

  td::Result<td::int64> to_balance(vm::CellSlice& balance_slice) {
    return TRY_VM(to_balance_or_throw(balance_slice));
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

  td::Status fetch_transactions(td::Ref<vm::Cell> block_root) {
    try {
      if (!block_root.not_null()) {
          return td::Status::Error("Block root cell is null");
      }

      block::gen::Block::Record block;
      block::gen::BlockInfo::Record info;
      block::gen::BlockExtra::Record extra;
      block::ShardId shard;
      if (!(tlb::unpack_cell(block_root, block) && tlb::unpack_cell(block.info, info) && tlb::unpack_cell(block.extra, extra) && shard.deserialize(info.shard.write()))) {
          return td::Status::Error("Cannot unpack block");
      }

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
          schema_tx.block_workchain = shard.workchain_id;
          schema_tx.block_shard = shard.shard_pfx;
          schema_tx.block_seqno = info.seq_no;

          schema_tx.account = std::to_string(shard.workchain_id) + ":" + cur_addr.to_hex();
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

            messages_.push_back(std::move(in_msg));

            schema::TransactionMessage tx_msg;
            tx_msg.transaction_hash = schema_tx.hash;
            tx_msg.message_hash = in_msg.hash;
            tx_msg.direction = "in";
            transaction_messages_.push_back(tx_msg);
          }

          if (trans.outmsg_cnt != 0) {
            vm::Dictionary dict{trans.r1.out_msgs, 15};
            for (int x = 0; x < trans.outmsg_cnt; x++) {
              TRY_RESULT(out_msg, parse_message(dict.lookup_ref(td::BitArray<15>{x})));
              schema::TransactionMessage tx_msg;
              tx_msg.transaction_hash = schema_tx.hash;
              tx_msg.message_hash = out_msg.hash;
              tx_msg.direction = "out";
              transaction_messages_.push_back(tx_msg);
            }
          }

          transactions_.push_back(schema_tx);
        }
        block::gen::HASH_UPDATE::Record state_hash_update;
        if (!tlb::type_unpack_cell(std::move(acc_blk.state_update), block::gen::t_HASH_UPDATE_Account, state_hash_update)) {
          return td::Status::Error("Failed to unpack state_update");
        }
        
        auto state_hash_before_b64 = td::base64_encode(state_hash_update.old_hash.as_slice());
        auto state_hash_after_b64 = td::base64_encode(state_hash_update.new_hash.as_slice());
      }
    } catch (vm::VmError err) {
        return td::Status::Error(PSLICE() << "error while parsing AccountBlocks : " << err.get_msg());
    }
    return td::Status::OK();
  }
};
}
