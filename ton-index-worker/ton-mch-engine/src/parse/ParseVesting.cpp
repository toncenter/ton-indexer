// Vesting message parser (messages/vesting.py). See parse/PSlice.h for shared
// machinery and MsgParse.cpp's header for the pytoniq-parity catalogue.
#include "parse/Parsers.h"

#include "parse/PSlice.h"

#include "common/refint.h"
#include "vm/cellslice.h"

#include <utility>
#include <vector>

namespace mch {

// VestingSendMessage (messages/vesting.py): the inner message cell is read as a
// full MessageAny (header + init + body), yielding message_body_hash alongside
// destination/value; when only the int_msg_info header parses, Python falls back
// to InternalMsgInfo and leaves message_body_hash None. The header prefix is
// identical in both paths, so the fallback here is just a null hash.
// Deviation (documented): a present extra-currency dict ref is consumed but
// not deep-validated (Python's HashMap.parse would validate; vesting inner
// messages never carry extra currencies in practice).
td::Result<Value> parse_vesting_send_message(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(ctx, open_body(body));
  auto &cs = ctx.cs;
  if (!cs.have(32 + 64 + 8) || !cs.advance(32)) {
    return td::Status::Error("vesting: header underflow");
  }
  auto query_id = cs.fetch_ulong(64);
  auto send_mode = cs.fetch_ulong(8);
  if (cs.size_refs() == 0) {
    return td::Status::Error("vesting: message_cell ref missing");
  }
  td::Ref<vm::Cell> message_cell = cs.fetch_ref();

  vm::CellSlice ms;
  bool special = false;
  try {
    ms = vm::load_cell_slice_special(message_cell, special);
  } catch (...) {
    return td::Status::Error("vesting: bad message cell");
  }
  // InternalMsgInfo.deserialize: tag bit 0, ihr_disabled/bounce/bounced,
  // src, dest, value (grams + extra dict), ihr_fee, fwd_fee, lt, at.
  if (!ms.have(4)) {
    return td::Status::Error("vesting: msg info underflow");
  }
  if (ms.fetch_ulong(1) != 0) {
    return td::Status::Error("vesting: not int_msg_info");
  }
  ms.advance(3);
  TRY_RESULT(src, load_address_py(ms));
  (void)src;
  TRY_RESULT(dest, load_address_py(ms));
  TRY_RESULT(grams, load_coins_py(ms));
  if (!ms.have(1)) {
    return td::Status::Error("vesting: extra dict underflow");
  }
  if (ms.fetch_ulong(1)) {
    if (ms.size_refs() == 0) {
      return td::Status::Error("vesting: extra dict ref missing");
    }
    ms.fetch_ref();
  }
  TRY_RESULT(ihr_fee, load_coins_py(ms));
  (void)ihr_fee;
  TRY_RESULT(fwd_fee, load_coins_py(ms));
  (void)fwd_fee;
  if (!ms.have(64 + 32)) {
    return td::Status::Error("vesting: lt/at underflow");
  }
  ms.advance(64 + 32);

  // MessageAny tail; any failure = Python's `except` branch (hash stays null).
  Value body_hash = Value::null();
  auto r_body = message_any_body(ms);
  if (r_body.is_ok()) {
    auto h = r_body.ok()->get_hash();
    td::Slice hs = h.as_slice();
    body_hash = Value::make_bytes(std::string(hs.data(), hs.size()));
  }

  Value::Fields f;
  f.emplace_back("query_id", Value::make_int(refint_u64(query_id)));
  f.emplace_back("send_mode", Value::make_int64(static_cast<std::int64_t>(send_mode)));
  f.emplace_back("message_cell", Value::make_cell(std::move(message_cell)));
  f.emplace_back("message_destination", std::move(dest));
  f.emplace_back("message_value", Value::make_int(std::move(grams)));
  f.emplace_back("message_body_hash", std::move(body_hash));
  return Value::make_obj(std::move(f));
}

// VestingAddWhiteList (messages/vesting.py): query_id followed by an UNBOUNDED
// chain of one address per cell, each level stores an address and, when it has
// a ref, continues in it; the last level stores the trailing address only. The
// schema's `ref { }` is fixed-depth, which is the whole reason this one stays
// hand-written. Loop order is the Python one exactly: test refs BEFORE reading
// the address, so a level with no ref contributes its address through the tail
// read instead. Termination is the cell tree's own depth bound.
td::Result<Value> parse_vesting_add_whitelist(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(ctx, open_body(body));
  auto &cs = ctx.cs;
  if (!cs.have(32 + 64) || !cs.advance(32)) {
    return td::Status::Error("vesting_whitelist: header underflow");
  }
  auto query_id = cs.fetch_ulong(64);

  std::vector<Value> addresses;
  vm::CellSlice cur = cs;
  while (cur.size_refs() > 0) {
    TRY_RESULT(addr, load_address_py(cur));
    addresses.push_back(std::move(addr));
    bool special = false;
    vm::CellSlice next;
    try {
      next = vm::load_cell_slice_special(cur.fetch_ref(), special);
    } catch (...) {
      return td::Status::Error("vesting_whitelist: bad chain cell");
    }
    cur = std::move(next);
  }
  TRY_RESULT(last, load_address_py(cur));
  addresses.push_back(std::move(last));

  Value::Fields f;
  f.emplace_back("query_id", Value::make_int(refint_u64(query_id)));
  f.emplace_back("addresses", Value::make_list(std::move(addresses)));
  return Value::make_obj(std::move(f));
}

}  // namespace mch
