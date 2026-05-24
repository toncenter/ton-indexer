#include "ActionDetector.h"
#include "IndexData.h"
#include "common/refcnt.hpp"
#include "convert-utils.h"
#include "smc-envelope/SmartContract.h"
#include "td/actor/actor.h"
#include "tokens-tlb.h"
#include "Statistics.h"


void ActionDetector::start_up() {
  timer_.resume();
  for (const auto &block : block_->blocks_) {
    for (const auto &transaction : block.transactions) {
      process_tx(transaction);
    }
  }
  g_statistics.record_time(DETECT_ACTIONS_SEQNO, timer_.elapsed() * 1e6);
  promise_.set_value(std::move(block_));
  stop();
}
void ActionDetector::process_tx(const schema::Transaction &transaction) {
  auto interfaces_it = block_->account_interfaces_.find(transaction.account);

  if (interfaces_it == block_->account_interfaces_.end()) {
    return;
  }
  auto interfaces = interfaces_it->second;

  if (!transaction.in_msg) {
    return;
  }

  auto in_msg_body_cs =
      vm::load_cell_slice_ref(transaction.in_msg.value().body);

  for (auto &v : interfaces) {
    if (auto jetton_wallet_ptr = std::get_if<schema::JettonWalletDataV2>(&v)) {
      if (tokens::gen::t_InternalMsgBody.check_tag(*in_msg_body_cs) ==
          tokens::gen::InternalMsgBody::transfer_jetton) {
        auto transfer = parse_jetton_transfer(*jetton_wallet_ptr, transaction,
                                              in_msg_body_cs);
        if (transfer.is_error()) {
          LOG(DEBUG) << "Failed to parse jetton transfer: "
                     << transfer.move_as_error();
        } else {
          block_->events_.push_back(transfer.move_as_ok());
        }
      } else if (tokens::gen::t_InternalMsgBody.check_tag(*in_msg_body_cs) ==
                 tokens::gen::InternalMsgBody::burn) {
        auto burn =
            parse_jetton_burn(*jetton_wallet_ptr, transaction, in_msg_body_cs);
        if (burn.is_error()) {
          LOG(DEBUG) << "Failed to parse jetton burn: " << burn.move_as_error();
        } else {
          block_->events_.push_back(burn.move_as_ok());
        }
      }
    }

    if (auto nft_item_ptr = std::get_if<schema::NFTItemDataV2>(&v)) {
      if (tokens::gen::t_InternalMsgBody.check_tag(*in_msg_body_cs) ==
          tokens::gen::InternalMsgBody::transfer_nft) {
        auto transfer =
            parse_nft_transfer(*nft_item_ptr, transaction, in_msg_body_cs);
        if (transfer.is_error()) {
          LOG(DEBUG) << "Failed to parse nft transfer: "
                     << transfer.move_as_error();
        } else {
          block_->events_.push_back(transfer.move_as_ok());
        }
      }
    }
  }
}
td::Result<schema::JettonTransfer> ActionDetector::parse_jetton_transfer(
    const schema::JettonWalletDataV2 &jetton_wallet,
    const schema::Transaction &transaction,
    td::Ref<vm::CellSlice> in_msg_body_cs) {
  tokens::gen::InternalMsgBody::Record_transfer_jetton transfer_record;
  if (!tlb::csr_unpack_inexact(in_msg_body_cs, transfer_record)) {
    return td::Status::Error("Failed to unpack transfer");
  }

  schema::JettonTransfer transfer;
  transfer.trace_id = transaction.trace_id;
  transfer.transaction_hash = transaction.hash;
  transfer.transaction_lt = transaction.lt;
  transfer.transaction_now = transaction.now;
  if (auto *v =
          std::get_if<schema::TransactionDescr_ord>(&transaction.description)) {
    transfer.transaction_aborted = v->aborted;
  } else {
    return td::Status::Error("Unexpected transaction description");
  }
  transfer.mc_seqno = transaction.mc_seqno;

  transfer.query_id = transfer_record.query_id;
  transfer.amount =
      block::tlb::t_VarUInteger_16.as_integer(transfer_record.amount);
  if (transfer.amount.is_null()) {
    return td::Status::Error("Failed to unpack transfer amount");
  }
  if (!transaction.in_msg || !transaction.in_msg->source) {
    return td::Status::Error("Failed to unpack transfer source");
  }
  transfer.source = transaction.in_msg->source.value();
  transfer.jetton_wallet = transaction.account;
  transfer.jetton_master = jetton_wallet.jetton;
  auto destination = convert::to_std_address(transfer_record.destination);
  if (destination.is_error()) {
    return destination.move_as_error_prefix(
        "Failed to unpack transfer destination: ");
  }
  transfer.destination = destination.move_as_ok();
  auto response_destination =
      convert::to_std_address(transfer_record.response_destination);
  if (response_destination.is_error()) {
    return response_destination.move_as_error_prefix(
        "Failed to unpack transfer response destination: ");
  }
  transfer.response_destination = response_destination.move_as_ok();
  if (!transfer_record.custom_payload.write().fetch_maybe_ref(
          transfer.custom_payload)) {
    return td::Status::Error("Failed to fetch custom payload");
  }
  transfer.forward_ton_amount = block::tlb::t_VarUInteger_16.as_integer(
      transfer_record.forward_ton_amount);
  if (!transfer_record.forward_payload.write().fetch_maybe_ref(
          transfer.forward_payload)) {
    return td::Status::Error("Failed to fetch forward payload");
  }

  return transfer;
}
td::Result<schema::JettonBurn> ActionDetector::parse_jetton_burn(
    const schema::JettonWalletDataV2 &jetton_wallet,
    const schema::Transaction &transaction,
    td::Ref<vm::CellSlice> in_msg_body_cs) {
  tokens::gen::InternalMsgBody::Record_burn burn_record;
  if (!tlb::csr_unpack_inexact(in_msg_body_cs, burn_record)) {
    return td::Status::Error("Failed to unpack burn");
  }

  schema::JettonBurn burn;
  burn.trace_id = transaction.trace_id;
  burn.transaction_hash = transaction.hash;
  burn.transaction_lt = transaction.lt;
  burn.transaction_now = transaction.now;
  if (auto *v =
          std::get_if<schema::TransactionDescr_ord>(&transaction.description)) {
    burn.transaction_aborted = v->aborted;
  } else {
    return td::Status::Error("Unexpected transaction description");
  }
  burn.mc_seqno = transaction.mc_seqno;

  burn.query_id = burn_record.query_id;
  if (!transaction.in_msg || !transaction.in_msg->source) {
    return td::Status::Error("Failed to unpack burn source");
  }
  burn.owner = transaction.in_msg->source.value();
  burn.jetton_wallet = transaction.account;
  burn.jetton_master = jetton_wallet.jetton;
  burn.amount = block::tlb::t_VarUInteger_16.as_integer(burn_record.amount);
  if (burn.amount.is_null()) {
    return td::Status::Error("Failed to unpack burn amount");
  }
  auto response_destination =
      convert::to_std_address(burn_record.response_destination);
  if (response_destination.is_error()) {
    return response_destination.move_as_error_prefix(
        "Failed to unpack burn response destination: ");
  }
  burn.response_destination = response_destination.move_as_ok();
  if (!burn_record.custom_payload.write().fetch_maybe_ref(
          burn.custom_payload)) {
    return td::Status::Error("Failed to fetch custom payload");
  }

  return burn;
}
td::Result<schema::NFTTransfer>
ActionDetector::parse_nft_transfer(const schema::NFTItemDataV2 &nft_item,
                                   const schema::Transaction &transaction,
                                   td::Ref<vm::CellSlice> in_msg_body_cs) {
  tokens::gen::InternalMsgBody::Record_transfer_nft transfer_record;
  if (!tlb::csr_unpack_inexact(in_msg_body_cs, transfer_record)) {
    return td::Status::Error("Failed to unpack transfer");
  }

  schema::NFTTransfer transfer;
  transfer.trace_id = transaction.trace_id;
  transfer.transaction_hash = transaction.hash;
  transfer.transaction_lt = transaction.lt;
  transfer.transaction_now = transaction.now;
  if (auto *v =
          std::get_if<schema::TransactionDescr_ord>(&transaction.description)) {
    transfer.transaction_aborted = v->aborted;
  } else {
    transfer.transaction_aborted = 0;
  }
  transfer.mc_seqno = transaction.mc_seqno;

  transfer.query_id = transfer_record.query_id;
  transfer.nft_item = transaction.account;
  transfer.nft_item_index = nft_item.index;
  transfer.nft_collection = nft_item.collection_address;
  if (!transaction.in_msg.has_value() || !transaction.in_msg.value().source) {
    return td::Status::Error("Failed to fetch NFT old owner address");
  }
  transfer.old_owner = transaction.in_msg.value().source;
  auto new_owner = convert::to_std_address(transfer_record.new_owner);
  if (new_owner.is_error()) {
    return new_owner.move_as_error_prefix(
        "Failed to unpack new owner address: ");
  }
  transfer.new_owner = new_owner.move_as_ok();
  auto response_destination =
      convert::to_std_address(transfer_record.response_destination);
  if (response_destination.is_error()) {
    return response_destination.move_as_error_prefix(
        "Failed to unpack response destination: ");
  }
  transfer.response_destination = response_destination.move_as_ok();
  if (!transfer_record.custom_payload.write().fetch_maybe_ref(
          transfer.custom_payload)) {
    return td::Status::Error("Failed to fetch custom payload");
  }
  transfer.forward_amount =
      block::tlb::t_VarUInteger_16.as_integer(transfer_record.forward_amount);
  if (!transfer_record.forward_payload.write().fetch_maybe_ref(
          transfer.forward_payload)) {
    return td::Status::Error("Failed to fetch forward payload");
  }

  return transfer;
}
