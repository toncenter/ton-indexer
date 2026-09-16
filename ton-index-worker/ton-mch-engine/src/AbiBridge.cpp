// ton-abi bridge: AbiValue to mch::Value adapter and production
// ABI parser rows. Registry keys are stable bare spec-facing names and may be
// decoupled from internal generated declaration names.
#include "AbiBridge.h"
#include "AbiProjection.h"
#include "AbiTryFirst.h"
#include "parse/Parsers.h"

#include "common/refint.h"
#include "vm/cells/CellBuilder.h"

#include "cocoon_gen.h"
#include "coffee_gen.h"
#include "coffee_staking_withdraw3_gen.h"
#include "dedust_gen.h"
#include "dedust_v2_gen.h"
#include "dns_gen.h"
#include "evaa_gen.h"
#include "evaa_supply_forward_gen.h"
#include "jetton_gen.h"
#include "jvault_gen.h"
#include "jvault_payload_gen.h"
#include "layerzero_gen.h"
#include "multisig_gen.h"
#include "nft_sale_gen.h"
#include "pton_gen.h"
#include "stonfi_gen.h"
#include "subscriptions_gen.h"
#include "teleitem_gen.h"
#include "tonco_gen.h"
#include "tonstakers_gen.h"
#include "vesting_gen.h"

#include <string>
#include <variant>

namespace mch {

using ton_abi::AbiValue;
using ton_abi::AbiValueKind;
using ton_abi::AbiAddressKind;

namespace {

namespace evaa = ton_abi::gen::evaa;
namespace layerzero = ton_abi::gen::layerzero;

// Copy a bit slice into a fresh, root-hash-comparable cell. Use the non-throwing
// append operation to preserve adapter totality.
Value cell_from_slice(const td::Ref<vm::CellSlice> &slice) {
  vm::CellBuilder cb;
  if (slice.not_null() && !cb.append_cellslice_bool(*slice)) {
    return Value::make_cell(vm::CellBuilder().finalize());
  }
  return Value::make_cell(cb.finalize());
}

Value adapt_address(const ton_abi::AbiAddress &a) {
  switch (a.kind) {
    case AbiAddressKind::Std: {
      // "wc:hex" -> canonical "wc:HEX" (uppercase raw form, identical to AccountId.as_str).
      std::string raw = std::to_string(a.workchain) + ":" + a.hash.to_hex();
      return Value::make_account_raw(raw);
    }
    case AbiAddressKind::None:
      // Present-but-empty address (addr_none from any_address). This is not null;
      // faithful to AccountId(None) vs a null Value.
      return Value::make_account_none();
    case AbiAddressKind::Extern: {
      // There is no native external-address VType. Mirror
      // AbiValue's {"extern":{bits,value}} as an Obj.
      Value::Fields f;
      f.emplace_back("bits", Value::make_int(td::make_refint(a.ext_bits)));
      f.emplace_back("value", cell_from_slice(a.ext_value));
      return Value::make_obj(std::move(f));
    }
  }
  return Value::null();  // unreachable
}

template <class T>
td::Result<T> decode_abi_body(const td::Ref<vm::Cell> &body) {
  if (body.is_null()) {
    return td::Status::Error("abi bridge: null body cell");
  }
  bool special = false;
  vm::CellSlice cs = vm::load_cell_slice_special(body, special);
  if (special) {
    return td::Status::Error("abi bridge: exotic cell not supported");
  }
  return T::from_slice(cs);
}

td::Result<Value> remaining_slice_cell(const vm::CellSlice &slice) {
  vm::CellBuilder cb;
  if (!cb.append_cellslice_bool(slice)) {
    return td::Status::Error("abi bridge: cannot materialize remaining slice");
  }
  return Value::make_cell(cb.finalize());
}

// Re-decode from a fresh slice to find the decoder's stopping point; the
// unconsumed remainder becomes the classifier's residual "s" field.
template <class T>
td::Result<Value> residual_after_decode(const td::Ref<vm::Cell> &cell) {
  if (cell.is_null()) {
    return td::Status::Error("abi bridge: null nested cell");
  }
  vm::CellSlice cs = vm::load_cell_slice(cell);
  TRY_RESULT(discarded, T::from_slice(cs));
  (void)discarded;
  return remaining_slice_cell(cs);
}

template <class T>
Value abi_int_value(const T &value) {
  return abi_value_to_mch(ton_abi::gen::abi_v_int(value));
}

template <class T>
td::Status require_cell_of(const ton_abi::gen::CellOf<T> &value, const char *field) {
  if (value.cell.is_null() || !value.ref) {
    return td::Status::Error(std::string("abi bridge: missing decoded Cell<T> field ") + field);
  }
  return td::Status::OK();
}

td::Result<Value> adapt_lz_path(const ton_abi::gen::CellOf<layerzero::LzPath> &wrapped) {
  TRY_STATUS(require_cell_of(wrapped, "LzPath"));
  const auto &path = *wrapped.ref;
  TRY_RESULT(residual, residual_after_decode<layerzero::LzPath>(wrapped.cell));
  return Value::make_obj({
      {"b", Value::make_cell(wrapped.cell)},
      {"s", std::move(residual)},
      {"header_info", abi_int_value(path.header_info)},
      {"header_filler", abi_int_value(path.header_filler)},
      {"src_eid", abi_int_value(path.src_eid)},
      {"src_oapp", minimal_hex(path.src_oapp)},
      {"dst_eid", abi_int_value(path.dst_eid)},
      {"dst_oapp", minimal_hex(path.dst_oapp)},
  });
}

td::Result<Value> adapt_lz_packet(const ton_abi::gen::CellOf<layerzero::LzPacket> &wrapped) {
  TRY_STATUS(require_cell_of(wrapped, "LzPacket"));
  const auto &packet = *wrapped.ref;
  TRY_RESULT(path, adapt_lz_path(packet.path));
  if (packet.message.is_null()) {
    return td::Status::Error("abi bridge: null LzPacket.message cell");
  }
  bool message_special = false;
  vm::CellSlice message_slice = vm::load_cell_slice_special(packet.message, message_special);
  TRY_RESULT(residual, residual_after_decode<layerzero::LzPacket>(wrapped.cell));
  return Value::make_obj({
      {"b", Value::make_cell(wrapped.cell)},
      {"s", std::move(residual)},
      {"header_info", abi_int_value(packet.header_info)},
      {"header_filler", abi_int_value(packet.header_filler)},
      {"path", std::move(path)},
      {"message", root_bits_hex(message_slice)},
      {"nonce", abi_int_value(packet.nonce)},
      {"guid", minimal_hex(packet.guid)},
  });
}

td::Result<Value> adapt_lz_send(const ton_abi::gen::CellOf<layerzero::LzMdLzSend> &wrapped) {
  TRY_STATUS(require_cell_of(wrapped, "LzMdLzSend"));
  const auto &send = *wrapped.ref;
  TRY_RESULT(packet, adapt_lz_packet(send.packet));
  TRY_STATUS(require_cell_of(send.ref, "LzMdLzSend.ref"));
  const auto &fee = *send.ref.ref;
  TRY_RESULT(residual, residual_after_decode<layerzero::LzMdLzSend>(wrapped.cell));
  return Value::make_obj({
      {"b", Value::make_cell(wrapped.cell)},
      {"s", std::move(residual)},
      {"name", abi_int_value(send.name)},
      {"header_info", abi_int_value(send.header_info)},
      {"header_filler", abi_int_value(send.header_filler)},
      {"send_request_id", abi_int_value(send.send_request_id)},
      {"send_msglib_manager", minimal_hex(send.send_msglib_manager)},
      {"send_msglib", minimal_hex(send.send_msglib)},
      {"packet", std::move(packet)},
      {"extra_options", Value::make_cell(send.extra_options)},
      {"ref", Value::make_cell(send.ref.cell)},
      {"send_msglib_connection", abi_int_value(fee.send_msglib_connection)},
      {"native_fee", abi_int_value(fee.native_fee)},
      {"zro_fee", abi_int_value(fee.zro_fee)},
      {"enforced_options", Value::make_cell(fee.enforced_options)},
      {"callback_data", Value::make_cell(fee.callback_data)},
  });
}

}  // namespace

Value abi_value_to_mch(const AbiValue &v) {
  switch (v.kind) {
    case AbiValueKind::Int:
      return Value::make_int(v.int_v);
    case AbiValueKind::Bool:
      return Value::make_bool(v.bool_v);
    case AbiValueKind::Address:
      return adapt_address(v.address_v);
    case AbiValueKind::Cell:
      return Value::make_cell(v.cell_v);
    case AbiValueKind::CellOf: {
      // "ref" and "cell" mirror the decoded inner value and raw cell; defensive null
      // guards keep this conversion total but are not expected for decoded values.
      Value::Fields f;
      f.emplace_back("ref", v.inner ? abi_value_to_mch(*v.inner) : Value::null());
      f.emplace_back("cell", v.cell_v.not_null() ? Value::make_cell(v.cell_v) : Value::null());
      return Value::make_obj(std::move(f));
    }
    case AbiValueKind::Bits:
      return cell_from_slice(v.bits_v);
    case AbiValueKind::String:
      // Raw bytes into Str (byte-preserving). The lossy-UTF8 decode is a
      // dump-side concern (AbiValue::to_json), not re-imposed here.
      return Value::make_str(v.string_v);
    case AbiValueKind::List: {
      std::vector<Value> items;
      items.reserve(v.list_v.size());
      for (const auto &e : v.list_v) {
        items.push_back(abi_value_to_mch(e));
      }
      return Value::make_list(std::move(items));
    }
    case AbiValueKind::Struct: {
      // Obj (attribute access), "$" = struct name FIRST, then fields in decl
      // order. This mirrors the AbiValue dump so a consumer can switch on ".$".
      Value::Fields f;
      f.emplace_back("$", Value::make_str(v.struct_name));
      for (const auto &kv : v.struct_fields) {
        f.emplace_back(kv.first, abi_value_to_mch(kv.second));
      }
      return Value::make_obj(std::move(f));
    }
    case AbiValueKind::Union: {
      // has_value_field variants only reach here: {"$":label,"value":inner}.
      // Struct-labeled variants are Struct-kind (carry their own "$"). Same
      // totality guard on `inner` as the CellOf branch.
      Value::Fields f;
      f.emplace_back("$", Value::make_str(v.union_label));
      f.emplace_back("value", v.inner ? abi_value_to_mch(*v.inner) : Value::null());
      return Value::make_obj(std::move(f));
    }
    case AbiValueKind::Void: {
      Value::Fields f;
      f.emplace_back("$", Value::make_str("void"));
      return Value::make_obj(std::move(f));
    }
    case AbiValueKind::Null:
      return Value::null();
    case AbiValueKind::Map: {
      // Mirror dump [[k,v],...] in wire order; List-of-pairs preserves non-string
      // key types (mch Dict keys are std::string only).
      std::vector<Value> pairs;
      pairs.reserve(v.map_entries.size());
      for (const auto &kv : v.map_entries) {
        std::vector<Value> pair;
        pair.push_back(abi_value_to_mch(kv.first));
        pair.push_back(abi_value_to_mch(kv.second));
        pairs.push_back(Value::make_list(std::move(pair)));
      }
      return Value::make_list(std::move(pairs));
    }
  }
  return Value::null();  // unreachable
}

namespace {

td::Result<Value> abi_parse_evaa_withdraw_fail_excess(const td::Ref<vm::Cell> &body) {
  if (body.is_null()) {
    return td::Status::Error("abi bridge: null body cell");
  }
  bool special = false;
  [[maybe_unused]] vm::CellSlice checked_slice = vm::load_cell_slice_special(body, special);
  if (special) {
    return td::Status::Error("abi bridge: exotic cell not supported");
  }

  auto match = try_parse_first<evaa::EvaaWithdrawLockedExcess,
                               evaa::EvaaWithdrawNotCollateralizedExcess,
                               evaa::EvaaWithdrawMissingPricesExcess,
                               evaa::EvaaWithdrawExecutionCrashed>(body);
  if (match && std::holds_alternative<evaa::EvaaWithdrawLockedExcess>(*match)) {
    return Value::make_obj({{"opcode", Value::make_int64(0x21e6)},
                            {"reason", Value::make_str("withdraw_locked_excess")}});
  }
  if (match &&
      std::holds_alternative<evaa::EvaaWithdrawNotCollateralizedExcess>(*match)) {
    return Value::make_obj(
        {{"opcode", Value::make_int64(0x21e7)},
         {"reason", Value::make_str("withdraw_not_collateralized_excess")}});
  }
  if (match && std::holds_alternative<evaa::EvaaWithdrawMissingPricesExcess>(*match)) {
    return Value::make_obj(
        {{"opcode", Value::make_int64(0x21e8)},
         {"reason", Value::make_str("withdraw_missing_prices_excess")}});
  }
  if (match && std::holds_alternative<evaa::EvaaWithdrawExecutionCrashed>(*match)) {
    return Value::make_obj({{"opcode", Value::make_int64(0x21ec)},
                            {"reason", Value::make_str("withdraw_execution_crashed")}});
  }
  return td::Status::Error("evaa fail excess: unknown opcode");
}

td::Result<Value> abi_parse_layerzero_oapp_execute_callback(
    const td::Ref<vm::Cell> &body) {
  TRY_RESULT(parsed, decode_abi_body<layerzero::LayerZeroOappExecuteCallbackAbi>(body));
  TRY_STATUS(require_cell_of(parsed.packet_ref, "LayerZeroOappExecuteCallback.packet_ref"));
  TRY_RESULT(packet, adapt_lz_packet(parsed.packet_ref.ref->packet));
  return Value::make_obj({
      {"cell", Value::make_cell(body)},
      {"packet", std::move(packet)},
  });
}

td::Result<Value> abi_parse_layerzero_channel_send_callback(
    const td::Ref<vm::Cell> &body) {
  TRY_RESULT(parsed, decode_abi_body<layerzero::LayerzeroChannelSendCallbackAbi>(body));
  TRY_STATUS(require_cell_of(parsed.next, "LayerzeroChannelSendCallback.next"));
  TRY_STATUS(require_cell_of(parsed.next.ref->next, "LayerzeroChannelSendCallback.next.next"));
  TRY_RESULT(lz_send, adapt_lz_send(parsed.next.ref->next.ref->lz_send));

  vm::CellSlice residual_slice = vm::load_cell_slice(body);
  if (!residual_slice.advance(32)) {
    return td::Status::Error("lz send callback: opcode underflow");
  }
  TRY_RESULT(residual, remaining_slice_cell(residual_slice));
  return Value::make_obj({
      {"cell", Value::make_cell(body)},
      {"s", std::move(residual)},
      {"opcode", abi_int_value(static_cast<td::uint64>(0xa2b5fbaeULL))},
      {"lz_send", std::move(lz_send)},
  });
}

td::Result<Value> abi_parse_layerzero_channel_commit_packet(
    const td::Ref<vm::Cell> &body) {
  TRY_RESULT(parsed, decode_abi_body<layerzero::ChannelCommitPacketAbi>(body));
  TRY_STATUS(require_cell_of(parsed.extended_md, "ChannelCommitPacket.extended_md"));
  const auto &packet_cell = parsed.extended_md.ref->packet;
  TRY_RESULT(packet, adapt_lz_packet(packet_cell));
  return Value::make_obj({
      {"extended_md_cell", Value::make_cell(parsed.extended_md.cell)},
      {"packet_cell", Value::make_cell(packet_cell.cell)},
      {"packet", std::move(packet)},
  });
}

td::Result<Value> abi_parse_uln_connection_verify_callback(
    const td::Ref<vm::Cell> &body) {
  TRY_RESULT(parsed, decode_abi_body<layerzero::UlnConnectionVerifyCallbackAbi>(body));
  TRY_STATUS(require_cell_of(parsed.md_obj, "UlnConnectionVerifyCallback.md_obj"));
  const auto &status_cell = parsed.md_obj.ref->verification_status;
  TRY_STATUS(require_cell_of(status_cell, "UlnConnectionVerifyCallback.verification_status"));
  const auto &verification = *status_cell.ref;

  Value status = enum_name(layerzero::UlnVerificationStatusCode_name_table(),
                           verification.status_code);
  if (status.is_null()) {
    status = Value::make_str("unknown_" + td::dec_string(verification.status_code));
  }
  return Value::make_obj({
      {"md_obj_cell", Value::make_cell(parsed.md_obj.cell)},
      {"verification_status_cell", Value::make_cell(status_cell.cell)},
      {"nonce", abi_int_value(verification.nonce)},
      {"status_code", abi_int_value(verification.status_code)},
      {"status", std::move(status)},
  });
}

// One public logical parser, two honest wire prefixes. The generated API is
// per-struct, so retry the Ethena arm with a fresh slice when the standard
// arm's from_slice rejects its prefix.
td::Result<Value> abi_parse_jetton_internal_transfer(const td::Ref<vm::Cell> &body) {
  auto standard = abi_parse_body<ton_abi::gen::jetton::JettonInternalTransfer>(body);
  if (standard.is_ok()) {
    return standard;
  }
  auto error = standard.move_as_error();
  if (error.message().str().find("JettonInternalTransfer: prefix:") == std::string::npos) {
    return std::move(error);
  }
  return abi_parse_body<ton_abi::gen::jetton::JettonInternalTransferEthena>(body);
}

// Flatten the generated nested mint value into the classifier's flat public
// field shape.
td::Result<Value> abi_parse_minter_mint_legacy_shape(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(parsed,
             abi_parse_body<ton_abi::gen::jetton::MinterJettonMint>(body));
  const Value *query_id = parsed.field("query_id");
  const Value *to_address = parsed.field("to_address");
  const Value *ton_amount = parsed.field("ton_amount");
  const Value *master_msg = parsed.field("master_msg");
  const Value *master_msg_ref = master_msg ? master_msg->field("ref") : nullptr;
  const Value *master_msg_cell = master_msg ? master_msg->field("cell") : nullptr;
  const Value *master_msg_query_id =
      master_msg_ref ? master_msg_ref->field("query_id") : nullptr;
  const Value *master_msg_jetton_amount =
      master_msg_ref ? master_msg_ref->field("jetton_amount") : nullptr;
  if (!query_id || !to_address || !ton_amount || !master_msg_cell ||
      !master_msg_query_id || !master_msg_jetton_amount) {
    return td::Status::Error("abi bridge: unexpected MinterJettonMint value shape");
  }
  return Value::make_obj({
      {"query_id", *query_id},
      {"to_address", *to_address},
      {"ton_amount", *ton_amount},
      {"master_msg", *master_msg_cell},
      {"master_msg_query_id", *master_msg_query_id},
      {"master_msg_jetton_amount", *master_msg_jetton_amount},
  });
}

// Flatten the Cell<Node>|void whitelist linked list into a plain address list.
td::Status flatten_vesting_whitelist_node(const Value &node,
                                          std::vector<Value> &addresses) {
  const Value *addr = node.field("addr");
  const Value *next = node.field("next");
  const Value *tag = next ? next->field("$") : nullptr;
  if (!addr || !next || !tag || tag->t != VType::Str) {
    return td::Status::Error("abi bridge: unexpected VestingAddWhiteList node shape");
  }
  addresses.push_back(*addr);
  if (tag->str == "void") {
    return td::Status::OK();
  }
  if (tag->str != "Cell") {
    return td::Status::Error("abi bridge: unexpected VestingAddWhiteList next tag");
  }
  const Value *value = next->field("value");
  const Value *ref = value ? value->field("ref") : nullptr;
  if (!ref) {
    return td::Status::Error("abi bridge: unexpected VestingAddWhiteList Cell shape");
  }
  return flatten_vesting_whitelist_node(*ref, addresses);
}

td::Result<Value> abi_parse_vesting_add_whitelist(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(parsed,
             abi_parse_body<ton_abi::gen::vesting::VestingAddWhiteList>(body));
  const Value *query_id = parsed.field("query_id");
  const Value *head = parsed.field("head");
  if (!query_id || !head) {
    return td::Status::Error("abi bridge: unexpected VestingAddWhiteList value shape");
  }
  std::vector<Value> addresses;
  TRY_STATUS(flatten_vesting_whitelist_node(*head, addresses));
  return Value::make_obj({
      {"query_id", *query_id},
      {"addresses", Value::make_list(std::move(addresses))},
  });
}

// This opcode has with-query_id and without-query_id shapes; try the with-query
// form first, then fall back to the prefix-only form.
template <class WithQuery, class NoQuery>
td::Result<Value> abi_parse_conditional_query_id(const td::Ref<vm::Cell> &body) {
  if (body.is_null()) {
    return td::Status::Error("abi bridge: null body cell");
  }
  bool special = false;
  auto with_query_cs = vm::load_cell_slice_special(body, special);
  if (special) {
    return td::Status::Error("abi bridge: exotic cell not supported");
  }
  auto with_query = WithQuery::from_slice(with_query_cs);
  if (with_query.is_ok()) {
    auto parsed = with_query.move_as_ok();
    return Value::make_obj(
        {{"query_id", abi_value_to_mch(ton_abi::gen::abi_v_int(parsed.query_id))}});
  }
  auto no_query_cs = vm::load_cell_slice(body);
  auto no_query = NoQuery::from_slice(no_query_cs);
  if (no_query.is_error()) {
    return no_query.move_as_error();
  }
  return Value::make_obj({{"query_id", Value::null()}});
}

}  // namespace

const std::vector<std::pair<std::string, MsgParserFn>> &abi_message_parsers() {
  namespace cocoon = ton_abi::gen::cocoon;
  namespace coffee = ton_abi::gen::coffee;
  namespace coffee_w3 = ton_abi::gen::coffee_staking_withdraw3;
  namespace dedust = ton_abi::gen::dedust;
  namespace dedust_v2 = ton_abi::gen::dedust_v2;
  namespace dns = ton_abi::gen::dns;
  namespace evaa = ton_abi::gen::evaa;
  namespace evaa_forward = ton_abi::gen::evaa_supply_forward;
  namespace jetton = ton_abi::gen::jetton;
  namespace jvault = ton_abi::gen::jvault;
  namespace jvault_payload = ton_abi::gen::jvault_payload;
  namespace layerzero = ton_abi::gen::layerzero;
  namespace multisig = ton_abi::gen::multisig;
  namespace nft_sale = ton_abi::gen::nft_sale;
  namespace pton = ton_abi::gen::pton;
  namespace stonfi = ton_abi::gen::stonfi;
  namespace subscriptions = ton_abi::gen::subscriptions;
  namespace teleitem = ton_abi::gen::teleitem;
  namespace tonco = ton_abi::gen::tonco;
  namespace tonstakers = ton_abi::gen::tonstakers;

  static const std::vector<std::pair<std::string, MsgParserFn>> rows = {
      {"ClaimRewardMessage", &abi_parse_body<dedust_v2::ClaimRewardMessage>},
      {"PayoutRewardMessage", &abi_parse_body<dedust_v2::PayoutRewardMessage>},
      {"PayoutMessage", &abi_parse_body<dedust_v2::PayoutMessage>},
      {"JettonTransfer", &parse_jetton_transfer},
      {"JettonInternalTransfer", &abi_parse_jetton_internal_transfer},
      {"JettonBurn", &abi_parse_body<jetton::JettonBurn>},
      {"JettonNotify", &abi_parse_body<jetton::JettonNotify>},
      {"JettonMint", &abi_parse_body<jetton::JettonMint>},
      {"MinterJettonMint", &abi_parse_minter_mint_legacy_shape},
      {"MultisigNewOrder", &abi_parse_body<multisig::MultisigNewOrder>},
      {"MultisigInitOrder", &abi_parse_body<multisig::MultisigInitOrder>},
      {"MultisigApprove", &abi_parse_body<multisig::MultisigApprove>},
      {"MultisigApproveRejected", &abi_parse_body<multisig::MultisigApproveRejected>},
      {"MultisigExecute", &abi_parse_body<multisig::MultisigExecute>},
      {"PTonTransfer", &abi_parse_body<pton::PTonTransfer>},
      {"AuctionFillUpMessage",
       &abi_parse_conditional_query_id<dns::AuctionFillUpWithQuery,
                                       dns::AuctionFillUpNoQuery>},
      {"DnsReleaseBalanceMessage",
       &abi_parse_conditional_query_id<dns::DnsReleaseBalanceWithQuery,
                                       dns::DnsReleaseBalanceNoQuery>},
      {"StonfiV2ProvideLiquidity", &abi_parse_body<stonfi::StonfiV2ProvideLiquidity>},
      {"StonfiPaymentRequest", &abi_parse_body<stonfi::StonfiPaymentRequest>},
      {"StonfiSwapMessage", &abi_parse_body<stonfi::StonfiSwapMessage>},
      {"StonfiV2PayTo", &abi_parse_body<stonfi::StonfiV2PayTo>},
      {"DedustPayoutFromPool", &abi_parse_body<dedust::DedustPayoutFromPool>},
      {"DedustSwapNotification", &abi_parse_body<dedust::DedustSwapNotification>},
      {"DedustDepositLiquidityToPool", &abi_parse_body<dedust::DedustDepositLiquidityToPool>},
      {"DedustV2PayNative", &abi_parse_body<dedust_v2::DedustV2PayNative>},
      {"DedustV2PayJetton", &abi_parse_body<dedust_v2::DedustV2PayJetton>},
      {"DedustV2SwapEvent", &abi_parse_body<dedust_v2::DedustV2SwapEvent>},
      {"DedustV2SwapPayload", &abi_parse_body<dedust_v2::DedustV2SwapPayload>},
      {"DedustV2PayoutPositionFees", &abi_parse_body<dedust_v2::DedustV2PayoutPositionFees>},
      {"DedustV2Withdraw", &abi_parse_body<dedust_v2::DedustV2Withdraw>},
      {"DedustV2WithdrawalEvent", &abi_parse_body<dedust_v2::DedustV2WithdrawalEvent>},
      {"DedustV2CreditAsset", &abi_parse_body<dedust_v2::DedustV2CreditAsset>},
      {"DedustV2JoinLiquidity", &abi_parse_body<dedust_v2::DedustV2JoinLiquidity>},
      {"DedustV2DepositEvent", &abi_parse_body<dedust_v2::DedustV2DepositEvent>},
      {"DedustV2DepositPayload", &abi_parse_body<dedust_v2::DedustV2DepositPayload>},
      {"CoffeeCreateVault", &abi_parse_body<coffee::CoffeeCreateVault>},
      {"CoffeeSwapEvent", &abi_parse_body<coffee::CoffeeSwapEvent>},
      {"CoffeeCreateLiquidityDepositoryRequest",
       &abi_parse_body<coffee::CoffeeCreateLiquidityDepositoryRequest>},
      {"CoffeeDepositLiquiditySuccessfulEvent",
       &abi_parse_body<coffee::CoffeeDepositLiquiditySuccessfulEvent>},
      {"CoffeeLiquidityWithdrawalEvent", &abi_parse_body<coffee::CoffeeLiquidityWithdrawalEvent>},
      {"CoffeeCreatePoolCreatorRequest", &abi_parse_body<coffee::CoffeeCreatePoolCreatorRequest>},
      {"CoffeeCreatePoolRequest", &abi_parse_body<coffee::CoffeeCreatePoolRequest>},
      {"CoffeeMevProtectFailedSwap", &abi_parse_body<coffee::CoffeeMevProtectFailedSwap>},
      {"CoffeeStakingClaimRewards", &abi_parse_body<coffee::CoffeeStakingClaimRewards>},
      {"CoffeeStakingPositionWithdraw2",
       &abi_parse_body<coffee::CoffeeStakingPositionWithdraw2>},
      {"CoffeeStakingPositionWithdraw3",
       &abi_parse_body<coffee_w3::CoffeeStakingPositionWithdraw3>},
      {"EvaaSupplyMaster", &abi_parse_body<evaa::EvaaSupplyMaster>},
      {"EvaaSupplySuccess", &abi_parse_body<evaa::EvaaSupplySuccess>},
      {"EvaaWithdrawMaster", &abi_parse_body<evaa::EvaaWithdrawMaster>},
      {"EvaaWithdrawCollateralized", &abi_parse_body<evaa::EvaaWithdrawCollateralized>},
      {"EvaaWithdrawFailExcess", &abi_parse_evaa_withdraw_fail_excess},
      {"EvaaSupplyJettonForward", &abi_parse_body<evaa_forward::EvaaSupplyJettonForward>},
      {"LayerZeroOappExecuteCallback", &abi_parse_layerzero_oapp_execute_callback},
      {"LayerzeroChannelSendCallback", &abi_parse_layerzero_channel_send_callback},
      {"ChannelCommitPacket", &abi_parse_layerzero_channel_commit_packet},
      {"UlnConnectionVerifyCallbackParser", &abi_parse_uln_connection_verify_callback},
      {"JVaultUnstakeJettons", &abi_parse_body<jvault::JVaultUnstakeJettons>},
      {"JVaultUnstakeRequest", &abi_parse_body<jvault::JVaultUnstakeRequest>},
      {"JVaultClaim", &abi_parse_body<jvault::JVaultClaim>},
      {"JVaultStakePeriodPayload",
       &abi_parse_body<jvault_payload::JVaultStakePeriodPayload>},
      {"SubscriptionPaymentRequest",
       &abi_parse_body<subscriptions::SubscriptionPaymentRequest>},
      {"ToncoRouterV3SwapSourceWallet",
       &abi_parse_body<tonco::ToncoRouterV3SwapSourceWallet>},
      {"ToncoPoolV3StartBurn", &abi_parse_body<tonco::ToncoPoolV3StartBurn>},
      {"ToncoPositionNftV3PositionBurn",
       &abi_parse_body<tonco::ToncoPositionNftV3PositionBurn>},
      {"ToncoPoolV3Burn", &abi_parse_body<tonco::ToncoPoolV3Burn>},
      {"ToncoRouterV3CreatePool", &abi_parse_body<tonco::ToncoRouterV3CreatePool>},
      {"ToncoPoolV3Init", &abi_parse_body<tonco::ToncoPoolV3Init>},
      {"TeleitemStartAuction", &abi_parse_body<teleitem::TeleitemStartAuction>},
      {"NftOwnershipAssignedPrevOwner",
       &abi_parse_body<nft_sale::NftOwnershipAssignedPrevOwner>},
      {"SaleUpdateMessage", &abi_parse_body<nft_sale::SaleUpdateMessage>},
      {"NftReportStaticData", &abi_parse_body<nft_sale::NftReportStaticData>},
      {"TONStakersNftBurnNotification",
       &abi_parse_body<tonstakers::TONStakersNftBurnNotification>},
      {"VestingAddWhiteList", &abi_parse_vesting_add_whitelist},
      {"CocoonPayoutPayload", &abi_parse_body<cocoon::CocoonPayoutPayload>},
      {"CocoonLastPayoutPayload", &abi_parse_body<cocoon::CocoonLastPayoutPayload>},
      {"CocoonWorkerProxyRequest", &abi_parse_body<cocoon::CocoonWorkerProxyRequest>},
      {"CocoonClientProxyRequest", &abi_parse_body<cocoon::CocoonClientProxyRequest>},
      {"CocoonClientProxyRefundGranted",
       &abi_parse_body<cocoon::CocoonClientProxyRefundGranted>},
      {"CocoonExtProxyPayoutRequest", &abi_parse_body<cocoon::CocoonExtProxyPayoutRequest>},
      {"CocoonChargePayload", &abi_parse_body<cocoon::CocoonChargePayload>},
      {"CocoonGrantRefundPayload", &abi_parse_body<cocoon::CocoonGrantRefundPayload>},
      {"CocoonExtClientTopUp", &abi_parse_body<cocoon::CocoonExtClientTopUp>},
      {"CocoonRegisterProxy", &abi_parse_body<cocoon::CocoonRegisterProxy>},
      {"CocoonUnregisterProxy", &abi_parse_body<cocoon::CocoonUnregisterProxy>},
      {"CocoonOwnerClientRegister", &abi_parse_body<cocoon::CocoonOwnerClientRegister>},
      {"CocoonOwnerClientChangeSecretHash",
       &abi_parse_body<cocoon::CocoonOwnerClientChangeSecretHash>},
      {"CocoonOwnerClientRequestRefund",
       &abi_parse_body<cocoon::CocoonOwnerClientRequestRefund>},
      {"CocoonOwnerClientIncreaseStake",
       &abi_parse_body<cocoon::CocoonOwnerClientIncreaseStake>},
      {"CocoonOwnerClientWithdraw", &abi_parse_body<cocoon::CocoonOwnerClientWithdraw>},
  };
  return rows;
}

}  // namespace mch
