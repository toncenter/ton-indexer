// Shared low-level helpers for the host binding TUs (see host/HostCommon.h).
#include "host/HostCommon.h"

#include "BlockTree.h"
#include "TraceLoader.h"

#include "td/utils/base64.h"
#include "vm/boc.h"

namespace mch {

// pytoniq Boc(str) decode order: bytes.fromhex first, base64 fallback (real
// BOCs start "te6…" -> fromhex fails -> base64). Then one_from_boc.
td::Result<td::Ref<vm::Cell>> cell_from_pystr(const std::string &s) {
  auto is_hex = [](const std::string &x) {
    if (x.empty() || x.size() % 2 != 0) return false;
    for (char c : x) {
      if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
        return false;
      }
    }
    return true;
  };
  std::string raw;
  if (is_hex(s)) {
    TRY_RESULT_ASSIGN(raw, td::hex_decode(td::Slice(s)));
  } else {
    TRY_RESULT_ASSIGN(raw, td::base64_decode(td::Slice(s)));
  }
  return vm::std_boc_deserialize(raw);
}

td::Result<td::Ref<vm::Cell>> block_body(const Block *b) {
  if (b->event_nodes.empty() || b->event_nodes.front()->msg == nullptr ||
      !b->event_nodes.front()->msg->content) {
    return td::Status::Error("no body");
  }
  TRY_RESULT(raw, td::base64_decode(td::Slice(b->event_nodes.front()->msg->content->body)));
  return vm::std_boc_deserialize(raw);
}

const Block *as_block(const Value &v) { return v.t == VType::Block ? v.block : nullptr; }

bool is_call_op(const Block *b, std::uint32_t op) {
  return b != nullptr && b->btype == "call_contract" && b->opcode && *b->opcode == op;
}

// AccountId(envelope-address-str).as_str(): canonical "wc:HEX" upper; a null or
// unparseable address -> addr_none (AccountId(None)).
Value account_from_opt(const std::optional<std::string> &s) {
  if (!s) {
    return Value::make_account_none();
  }
  auto norm = normalize_raw_address(*s);
  return norm ? Value::make_account_raw(*norm) : Value::make_account_none();
}

const Message *block_msg(const Block *b) {
  return b->event_nodes.empty() ? nullptr : b->event_nodes.front()->msg;
}

// Amount(x): a numeric-carrying Value (Int/Amount) -> Amount with that value.
Value to_amount(const Value &v) {
  return (v.t == VType::Int || v.t == VType::Amount) ? Value::make_amount(v.num)
                                                     : Value::make_amount_none();
}

// Amount(x or 0): coins are never None, and `0 or 0 == 0`, so this is just
// Amount(x), but keep the null-guard for parse-failure paths that reach here.
Value amount_or_zero(td::RefInt256 v) {
  return v.is_null() ? Value::make_amount(td::make_refint(0)) : Value::make_amount(std::move(v));
}

// Copy a field out of a Block's data Dict (Null when absent).
Value data_field(const Block *b, const char *name) {
  const Value *f = b->data.field(name);
  return f != nullptr ? *f : Value::null();
}

// Two Account values are equal iff same addr_none state and canonical string
// (mirrors AccountId.__eq__ over the canonical "wc:HEX" form).
bool same_account(const Value &a, const Value &b) {
  return a.t == VType::Account && b.t == VType::Account && a.addr_none == b.addr_none &&
         a.str == b.str;
}

}  // namespace mch
