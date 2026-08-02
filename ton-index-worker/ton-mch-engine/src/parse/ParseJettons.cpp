// Jetton-family message-body parsers (jettons.py). See parse/PSlice.h for the
// shared machinery and MsgParse.cpp's header for the pytoniq-parity catalogue.
#include "parse/Parsers.h"

#include "parse/PSlice.h"

#include "common/refint.h"
#include "vm/cellslice.h"

#include "generated/mch-msgs-tlb.h"

#include <cstdio>
#include <string>
#include <utility>
#include <vector>

namespace mch {

namespace {

// Serialized BOC bytes use the native writer. The container
// bytes differ from pytoniq's on multi-ref trees, but payload fields are
// compared/rendered by cell root hash, so the difference is invisible.
td::Result<std::string> boc_bytes(const td::Ref<vm::Cell> &c) {
  return td_boc_serialize(c);
}

td::Result<Value> addr_value(td::Ref<vm::CellSlice> csr) {
  vm::CellSlice cs{*csr};
  return load_address_py(cs);
}

// stonfi swap body inside JettonTransfer.forward_payload (sum type 0x25938561).
td::Result<Value> parse_stonfi_swap(vm::CellSlice &cs) {
  TRY_RESULT(jw, load_address_py(cs));
  TRY_RESULT(min_amount, load_coins_py(cs));
  TRY_RESULT(user, load_address_py(cs));
  Value::Fields f;
  f.emplace_back("jetton_wallet", std::move(jw));
  f.emplace_back("min_amount", Value::make_int(std::move(min_amount)));
  f.emplace_back("user_address", std::move(user));
  return Value::make_dict(std::move(f));
}

}  // namespace

// Per-family adapters

td::Result<Value> parse_jetton_transfer(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(ctx, open_body(body));
  auto &cs = ctx.cs;
  mchmsg::gen::MchJettonTransfer::Record rec;
  if (!mchmsg::gen::t_MchJettonTransfer.unpack(cs, rec)) {
    return td::Status::Error("tlb: transfer unpack failed");
  }
  TRY_RESULT(amount, var_uint16(rec.amount));
  TRY_RESULT(destination, addr_value(rec.destination));
  TRY_RESULT(response, addr_value(rec.response));
  td::Ref<vm::Cell> cp;
  if (!rec.custom_payload.write().fetch_maybe_ref(cp)) {
    return td::Status::Error("transfer: bad custom_payload");
  }
  Value custom_payload = Value::null();
  if (cp.not_null()) {
    TRY_RESULT(cp_boc, boc_bytes(cp));
    custom_payload = Value::make_bytes(std::move(cp_boc));
  }
  TRY_RESULT(forward_amount, var_uint16(rec.forward_amount));

  Value forward_payload = Value::null();
  Value comment = Value::null();
  Value payload_sum_type = Value::null();
  Value stonfi_swap_body = Value::null();
  bool encrypted_comment = false;
  bool has_sum_type = false;
  std::string sum_type;

  if (cs.size() > 0) {
    bool in_ref = cs.fetch_ulong(1) != 0;
    PSlice ps;
    if (in_ref) {
      if (cs.size_refs() == 0) {
        return td::Status::Error("transfer: forward_payload ref missing");
      }
      ps = pslice_from_cell(cs.fetch_ref());
    } else {
      // pytoniq boc.copy(): remaining bits, but refs reset to the FULL body
      // ref list with offset 0 (Slice.copy() does not preserve ref_offset).
      ps.cs = cs;
      ps.refs = ctx.all_refs;
      ps.off = 0;
    }
    // _load_forward_payload
    if (ps.cs.size() > 0) {
      TRY_RESULT(fp_cell, pslice_to_cell(ps));
      TRY_RESULT(fp_boc, boc_bytes(fp_cell));
      forward_payload = Value::make_bytes(std::move(fp_boc));
      if (ps.cs.size() < 32) {
        has_sum_type = true;
        sum_type = "Unknown";
      } else {
        auto st = static_cast<td::uint32>(ps.cs.fetch_ulong(32));
        char buf[16];
        std::snprintf(buf, sizeof(buf), "0x%x", st);  // Python hex()
        payload_sum_type = Value::make_str(buf);
        if (st == 0) {
          has_sum_type = true;
          sum_type = "TextComment";
          auto r = load_snake_bytes(ps);
          if (r.is_ok()) {
            comment = Value::make_bytes(r.move_as_ok());
          } else {
            sum_type = "Unknown";
          }
        } else if (st == 0x2167da4b) {
          has_sum_type = true;
          sum_type = "EncryptedTextComment";
          auto r = load_snake_bytes(ps);
          if (r.is_ok()) {
            comment = Value::make_bytes(r.move_as_ok());
            encrypted_comment = true;
          } else {
            sum_type = "Unknown";
          }
        } else if (st == 0x25938561) {
          auto r = parse_stonfi_swap(ps.cs);
          if (r.is_ok()) {
            stonfi_swap_body = r.move_as_ok();
            // Python does NOT set sum_type on this path.
          } else {
            has_sum_type = true;
            sum_type = "Unknown";
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
  f.emplace_back("amount", Value::make_int(std::move(amount)));
  f.emplace_back("destination", std::move(destination));
  f.emplace_back("response", std::move(response));
  f.emplace_back("custom_payload", std::move(custom_payload));
  f.emplace_back("forward_amount", Value::make_int(std::move(forward_amount)));
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

// JettonInternalTransfer, JettonBurn, JettonNotify, and JettonMint use protocol
// ABI rows. The forward-payload-tail transfer and nested-capture minter mint
// require the hand-written parsers below.

td::Result<Value> parse_minter_jetton_mint(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(ctx, open_body(body));
  mchmsg::gen::MchMinterJettonMint::Record rec;
  if (!mchmsg::gen::t_MchMinterJettonMint.unpack(ctx.cs, rec)) {
    return td::Status::Error("tlb: minter mint unpack failed");
  }
  TRY_RESULT(to_address, addr_value(rec.to_address));
  TRY_RESULT(ton_amount, var_uint16(rec.ton_amount));
  bool special = false;
  vm::CellSlice ms;
  try {
    ms = vm::load_cell_slice_special(rec.master_msg, special);
  } catch (...) {
    return td::Status::Error("minter mint: bad master_msg");
  }
  if (!ms.have(32) || !ms.advance(32) || !ms.have(64)) {
    return td::Status::Error("minter mint: master_msg underflow");
  }
  auto master_query_id = ms.fetch_ulong(64);
  TRY_RESULT(master_amount, load_coins_py(ms));
  Value::Fields f;
  f.emplace_back("query_id", Value::make_int(refint_u64(rec.query_id)));
  f.emplace_back("to_address", std::move(to_address));
  f.emplace_back("ton_amount", Value::make_int(std::move(ton_amount)));
  f.emplace_back("master_msg", Value::make_cell(rec.master_msg));
  f.emplace_back("master_msg_query_id", Value::make_int(refint_u64(master_query_id)));
  f.emplace_back("master_msg_jetton_amount", Value::make_int(std::move(master_amount)));
  return Value::make_obj(std::move(f));
}

}  // namespace mch
