// Shared low-level helpers for per-protocol host bindings.
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

// True when uri is a Str containing the Fragment NFT host.
bool fragment_uri(const Value *uri);

// Decode a base64-encoded BOC string field into a cell.
td::Result<td::Ref<vm::Cell>> cell_from_pystr(const std::string &s);

const Block *as_block(const Value &v);

bool is_call_op(const Block *b, std::uint32_t op);

// Canonical "wc:HEX" upper; a null or unparseable address is addr_none.
Value account_from_opt(const std::optional<std::string> &s);

const Message *block_msg(const Block *b);

// Int/Amount becomes Amount; anything else becomes Amount-none.
Value to_amount(const Value &v);

// Message value as Amount, or Amount-none when the message or value is missing.
Value msg_value_amount(const Message *m);

// Null RefInt256 values coalesce to zero for callers that continue computing.
td::RefInt256 or_zero(td::RefInt256 v);

// Coins are never none; keep the null-guard for parse-failure paths.
Value amount_or_zero(td::RefInt256 v);

// Parse a pTON body as PTonTransfer and return its ton_amount field.
td::Result<td::RefInt256> pton_ton_amount(const Block *pton);

// Copy a field out of a Block's data Dict (Null when absent).
Value data_field(const Block *b, const char *name);

// Equal iff same addr_none state and canonical "wc:HEX" string.
bool same_account(const Value &a, const Value &b);

}  // namespace mch
