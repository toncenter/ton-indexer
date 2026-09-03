// Jetton-family parsers. Shared machinery is in parse/PSlice.h.
#include "parse/Parsers.h"

#include "AbiTryFirst.h"
#include "parse/PSlice.h"

#include "jetton_gen.h"
#include "jetton_payloads_gen.h"

#include "vm/cells/CellBuilder.h"
#include "vm/cellslice.h"

#include <string>
#include <utility>
#include <variant>

namespace mch {

namespace {

// Native BOC writer: container bytes differ on multi-ref trees, but payload
// fields are compared by cell root hash, so the difference is invisible.
td::Result<std::string> boc_bytes(const td::Ref<vm::Cell> &c) {
  return td_boc_serialize_crc(c);
}

td::Result<Value> legacy_address_value(const ton_abi::AbiAddress &address) {
  vm::CellBuilder cb;
  TRY_STATUS(ton_abi::store_address_any(cb, address));
  vm::CellSlice cs = vm::load_cell_slice(cb.finalize());
  return load_address_py(cs);
}

td::Result<td::Ref<vm::Cell>> remaining_slice_to_cell(
    const td::Ref<vm::CellSlice> &slice) {
  if (slice.is_null()) {
    return td::Status::Error("transfer: null remaining slice");
  }
  vm::CellBuilder cb;
  if (!cb.append_cellslice_bool(*slice)) {
    return td::Status::Error("transfer: cannot materialize remaining slice");
  }
  return cb.finalize();
}

td::Result<Value> decode_comment(const td::Ref<vm::CellSlice> &comment) {
  vm::CellSlice comment_slice = *comment;
  TRY_RESULT(bytes, load_snake_bytes(comment_slice));
  return Value::make_bytes(std::move(bytes));
}

td::Result<Value> adapt_stonfi_swap(
    const ton_abi::gen::jetton_payloads::StonfiSwapPayload &swap) {
  TRY_RESULT(jetton_wallet, legacy_address_value(swap.jetton_wallet));
  TRY_RESULT(user_address, legacy_address_value(swap.user_address));
  Value::Fields f;
  f.emplace_back("jetton_wallet", std::move(jetton_wallet));
  f.emplace_back("min_amount", Value::make_int(swap.min_amount));
  f.emplace_back("user_address", std::move(user_address));
  return Value::make_dict(std::move(f));
}

}  // namespace

td::Result<Value> parse_jetton_transfer(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(ctx, open_body(body));
  auto &cs = ctx.cs;
  TRY_RESULT(rec, ton_abi::gen::jetton::JettonTransfer::from_slice(cs));
  TRY_RESULT(destination, legacy_address_value(rec.destination));
  TRY_RESULT(response, legacy_address_value(rec.response_destination));
  Value custom_payload = Value::null();
  if (rec.custom_payload && rec.custom_payload->not_null()) {
    TRY_RESULT(cp_boc, boc_bytes(*rec.custom_payload));
    custom_payload = Value::make_bytes(std::move(cp_boc));
  }

  Value forward_payload = Value::null();
  Value comment = Value::null();
  Value payload_sum_type = Value::null();
  Value stonfi_swap_body = Value::null();
  bool encrypted_comment = false;
  bool has_sum_type = false;
  std::string sum_type;

  if (cs.size() > 0) {
    TRY_RESULT(tail, ton_abi::gen::jetton::JettonForwardTail_from_slice(cs));
    td::Ref<vm::Cell> payload_cell;
    if (const auto *in_ref =
            std::get_if<ton_abi::gen::jetton::JettonForwardPayloadRef>(&tail)) {
      payload_cell = in_ref->value;
    } else {
      const auto &inline_payload =
          std::get<ton_abi::gen::jetton::JettonForwardPayloadInline>(tail);
      TRY_RESULT(materialized, remaining_slice_to_cell(inline_payload.value));
      payload_cell = std::move(materialized);
    }

    PSlice ps = pslice_from_cell(payload_cell);
    if (ps.cs.size() > 0) {
      TRY_RESULT(fp_boc, boc_bytes(payload_cell));
      forward_payload = Value::make_bytes(std::move(fp_boc));
      if (ps.cs.size() < 32) {
        has_sum_type = true;
        sum_type = "Unknown";
      } else {
        auto st = static_cast<td::uint32>(ps.cs.fetch_ulong(32));
        payload_sum_type = Value::make_int(refint_u64(st));
        if (st == 0 || st == 0x2167da4b || st == 0x25938561) {
          namespace payloads = ton_abi::gen::jetton_payloads;
          if (st == 0) {
            auto match = try_parse_first<payloads::TextCommentPayload>(payload_cell);
            has_sum_type = true;
            sum_type = "TextComment";
            const auto *parsed =
                match ? std::get_if<payloads::TextCommentPayload>(&*match) : nullptr;
            auto decoded = parsed ? decode_comment(parsed->comment)
                                  : td::Result<Value>(
                                        td::Status::Error("text comment payload did not match"));
            if (decoded.is_ok()) {
              comment = decoded.move_as_ok();
            } else {
              sum_type = "Unknown";
            }
          } else if (st == 0x2167da4b) {
            auto match = try_parse_first<payloads::EncryptedCommentPayload>(payload_cell);
            has_sum_type = true;
            sum_type = "EncryptedTextComment";
            const auto *parsed =
                match ? std::get_if<payloads::EncryptedCommentPayload>(&*match) : nullptr;
            auto decoded = parsed ? decode_comment(parsed->comment)
                                  : td::Result<Value>(td::Status::Error(
                                        "encrypted comment payload did not match"));
            if (decoded.is_ok()) {
              comment = decoded.move_as_ok();
              encrypted_comment = true;
            } else {
              sum_type = "Unknown";
            }
          } else {
            auto match = try_parse_first<payloads::StonfiSwapPayload>(payload_cell);
            const auto *parsed =
                match ? std::get_if<payloads::StonfiSwapPayload>(&*match) : nullptr;
            auto decoded = parsed ? adapt_stonfi_swap(*parsed)
                                  : td::Result<Value>(
                                        td::Status::Error("stonfi payload did not match"));
            if (decoded.is_ok()) {
              stonfi_swap_body = decoded.move_as_ok();
              // No sum_type on Ston.fi success. Intentional.
            } else {
              has_sum_type = true;
              sum_type = "Unknown";
            }
          }
        } else {
          has_sum_type = true;
          sum_type = "Unknown";
        }
      }
    }
  }

  Value::Fields f;
  f.emplace_back("query_id", Value::make_int(refint_u64(rec.query_id)));
  f.emplace_back("amount", Value::make_int(std::move(rec.amount)));
  f.emplace_back("destination", std::move(destination));
  f.emplace_back("response", std::move(response));
  f.emplace_back("custom_payload", std::move(custom_payload));
  f.emplace_back("forward_amount", Value::make_int(std::move(rec.forward_ton_amount)));
  f.emplace_back("comment", std::move(comment));
  f.emplace_back("encrypted_comment", Value::make_bool(encrypted_comment));
  f.emplace_back("payload_sum_type", std::move(payload_sum_type));
  f.emplace_back("stonfi_swap_body", std::move(stonfi_swap_body));
  f.emplace_back("forward_payload", std::move(forward_payload));
  if (has_sum_type) {
    f.emplace_back("sum_type", Value::make_str(std::move(sum_type)));
  }
  return Value::make_obj(std::move(f));
}

}  // namespace mch
