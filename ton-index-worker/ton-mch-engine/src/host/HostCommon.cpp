#include "host/HostCommon.h"

#include "BlockTree.h"
#include "MsgParse.h"
#include "TraceLoader.h"
#include "btypes_gen.h"

#include "td/utils/base64.h"
#include "vm/boc.h"

namespace mch {

// Cell payload strings are base64-encoded BOCs.
td::Result<td::Ref<vm::Cell>> cell_from_pystr(const std::string &s) {
  std::string raw;
  TRY_RESULT_ASSIGN(raw, td::base64_decode(td::Slice(s)));
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

bool fragment_uri(const Value *uri) {
  return uri != nullptr && uri->t == VType::Str &&
         uri->str.find("https://nft.fragment.com") != std::string::npos;
}

const Block *as_block(const Value &v) { return v.t == VType::Block ? v.block : nullptr; }

bool is_call_op(const Block *b, std::uint32_t op) {
  return b != nullptr && b->btype == mch::btype::kCallContract && b->opcode &&
         *b->opcode == op;
}

// Canonical "wc:HEX" upper; a null or unparseable address is addr_none.
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

// Int/Amount becomes Amount; anything else becomes Amount-none.
Value to_amount(const Value &v) {
  return (v.t == VType::Int || v.t == VType::Amount) ? Value::make_amount(v.num)
                                                     : Value::make_amount_none();
}

Value msg_value_amount(const Message *m) {
  return m != nullptr && m->value ? Value::make_amount(td::make_refint(*m->value))
                                  : Value::make_amount_none();
}

td::RefInt256 or_zero(td::RefInt256 v) {
  return v.is_null() ? td::make_refint(0) : std::move(v);
}

// Coins are never none; keep the null-guard for parse-failure paths.
Value amount_or_zero(td::RefInt256 v) {
  return Value::make_amount(or_zero(std::move(v)));
}

td::Result<td::RefInt256> pton_ton_amount(const Block *pton) {
  TRY_RESULT(body, block_body(pton));
  TRY_RESULT(parsed, parse_message_body("PTonTransfer", body));
  const Value *amount = parsed.field("ton_amount");
  if (amount == nullptr || (amount->t != VType::Int && amount->t != VType::Amount)) {
    return td::Status::Error("PTonTransfer has no ton_amount");
  }
  return amount->num;
}

// Copy a field out of a Block's data Dict (Null when absent).
Value data_field(const Block *b, const char *name) {
  const Value *f = b->data.field(name);
  return f != nullptr ? *f : Value::null();
}

// Equal iff same addr_none state and canonical "wc:HEX" string.
bool same_account(const Value &a, const Value &b) {
  return a.t == VType::Account && b.t == VType::Account && a.addr_none == b.addr_none &&
         a.str == b.str;
}

}  // namespace mch
