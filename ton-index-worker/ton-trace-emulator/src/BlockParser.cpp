#include "BlockParser.h"

#include "TraceEmulator.h"

void BlockParser::start_up() {
  std::vector<TransactionInfo> res;

  block::gen::Block::Record blk;
  block::gen::BlockInfo::Record info;
  block::gen::BlockExtra::Record extra;
  if (!(tlb::unpack_cell(block_data_->root_cell(), blk) && tlb::unpack_cell(blk.info, info) &&
        tlb::unpack_cell(blk.extra, extra))) {
    promise_.set_error(td::Status::Error("block data info extra unpack failed"));
    stop();
    return;
  }
  try {
    vm::AugmentedDictionary acc_dict{vm::load_cell_slice_ref(extra.account_blocks), 256,
                                     block::tlb::aug_ShardAccountBlocks};

    td::Bits256 cur_addr = td::Bits256::zero();
    bool eof = false;
    bool allow_same = true;
    while (!eof) {
      auto value = acc_dict.extract_value(
          acc_dict.vm::DictionaryFixed::lookup_nearest_key(cur_addr.bits(), 256, true, allow_same));
      if (value.is_null()) {
        eof = true;
        break;
      }
      allow_same = false;
      block::gen::AccountBlock::Record acc_blk;
      if (!(tlb::csr_unpack(std::move(value), acc_blk) && acc_blk.account_addr == cur_addr)) {
        promise_.set_error(td::Status::Error("invalid AccountBlock for account " + cur_addr.to_hex()));
        stop();
        return;
      }
      vm::AugmentedDictionary trans_dict{vm::DictNonEmpty(), std::move(acc_blk.transactions), 64,
                                         block::tlb::aug_AccountTransactions};
      td::BitArray<64> cur_trans{(long long)0};
      while (true) {
        auto tvalue = trans_dict.extract_value_ref(
            trans_dict.vm::DictionaryFixed::lookup_nearest_key(cur_trans.bits(), 64, true));
        if (tvalue.is_null()) {
          break;
        }
        block::gen::Transaction::Record trans;
        if (!tlb::unpack_cell(tvalue, trans)) {
          promise_.set_error(td::Status::Error("Failed to unpack Transaction"));
          stop();
          return;
        }
        block::gen::TransactionDescr::Record_trans_ord descr;
        if (!tlb::unpack_cell(trans.description, descr)) {
          continue;
        }

        TransactionInfo tx_info;

        tx_info.account = block::StdAddress(block_data_->block_id().id.workchain, cur_addr);
        tx_info.hash = tvalue->get_hash().bits();
        tx_info.root = tvalue;
        tx_info.lt = trans.lt;
        tx_info.block_id = block_data_->block_id().id;
        tx_info.mc_block_seqno = mc_block_seqno_;

        if (trans.r1.in_msg->prefetch_long(1)) {
          auto msg = trans.r1.in_msg->prefetch_ref();
          tx_info.in_msg_hash = msg->get_hash().bits();
          auto message_cs = vm::load_cell_slice(trans.r1.in_msg->prefetch_ref());
          auto msg_tag = block::gen::t_CommonMsgInfo.get_tag(message_cs);
          if (msg_tag == block::gen::CommonMsgInfo::ext_in_msg_info) {
            tx_info.trace_ids = TraceIds{.root_tx_hash = tx_info.hash,
                                         .ext_in_msg_hash = msg->get_hash().bits(),
                                         .ext_in_msg_hash_norm = ext_in_msg_get_normalized_hash(msg).move_as_ok()};
          } else if (msg_tag == block::gen::CommonMsgInfo::int_msg_info) {
            block::gen::CommonMsgInfo::Record_int_msg_info msg_info;
            block::StdAddress source;
            if (tlb::unpack(message_cs, msg_info) &&
                block::tlb::t_MsgAddressInt.extract_std_address(msg_info.src, source) &&
                source.workchain == ton::masterchainId && source.addr.is_zero()) {
              // Protocol-generated masterchain messages have no external-message trace root.
              continue;
            }
          }
        } else {
          LOG(ERROR) << "Ordinary transaction without in_msg, skipping";
          continue;
        }

        // LOG(INFO) << "TX hash: " << tx_info.hash.to_hex();

        if (trans.outmsg_cnt != 0) {
          vm::Dictionary dict{trans.r1.out_msgs, 15};
          for (int x = 0; x < trans.outmsg_cnt; x++) {
            auto value = dict.lookup_ref(td::BitArray<15>{x});
            OutMsgInfo out_msg_info;
            out_msg_info.hash = value->get_hash().bits();
            out_msg_info.root = value;
            tx_info.out_msgs.push_back(std::move(out_msg_info));

            // LOG(INFO) << "  out msg: " << out_msg_info.hash.to_hex();
          }
        }

        res.push_back(tx_info);
      }
    }
  } catch (const vm::VmError& err) {
    promise_.set_error(td::Status::Error(PSLICE() << "error while parsing AccountBlocks : " << err.get_msg()));
    stop();
    return;
  }
  promise_.set_value(std::move(res));
  stop();
}
