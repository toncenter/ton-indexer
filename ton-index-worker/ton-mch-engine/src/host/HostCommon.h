// Shared low-level helpers for the per-protocol host binding TUs (src/host/*).
// Extracted verbatim from HostRegistry.cpp so the split predicate/fn files can
// share them; see HostRegistry.h for the public registry surface.
#pragma once

#include "Value.h"

#include "td/utils/Status.h"
#include "vm/cells/Cell.h"

#include <cstdint>
#include <optional>
#include <string>

namespace mch {

struct Block;     // BlockTree.h
struct Message;   // TraceLoader.h

td::Result<td::Ref<vm::Cell>> block_body(const Block *b);

// Decode a Python-side BOC string field (jetton forward_payload, custom_payload,
// ...) into a cell, in pytoniq's Boc(str) order: hex first, base64 fallback.
td::Result<td::Ref<vm::Cell>> cell_from_pystr(const std::string &s);

const Block *as_block(const Value &v);

bool is_call_op(const Block *b, std::uint32_t op);

template <typename Vec>
const Block *find_call(const Vec &blocks, std::uint32_t op) {
  for (const Block *b : blocks) {
    if (is_call_op(b, op)) {
      return b;
    }
  }
  return nullptr;
}

// AccountId(envelope-address-str).as_str(): canonical "wc:HEX" upper; a null or
// unparseable address -> addr_none (AccountId(None)).
Value account_from_opt(const std::optional<std::string> &s);

const Message *block_msg(const Block *b);

// Amount(x): a numeric-carrying Value (Int/Amount) -> Amount with that value.
Value to_amount(const Value &v);

// Amount(x or 0): coins are never None, and `0 or 0 == 0`, so this is just
// Amount(x), but keep the null-guard for parse-failure paths that reach here.
Value amount_or_zero(td::RefInt256 v);

// Copy a field out of a Block's data Dict (Null when absent).
Value data_field(const Block *b, const char *name);

// Two Account values are equal iff same addr_none state and canonical string
// (mirrors AccountId.__eq__ over the canonical "wc:HEX" form).
bool same_account(const Value &a, const Value &b);

}  // namespace mch
