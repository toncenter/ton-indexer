#include "host/HostCommon.h"

#include "BlockTree.h"
#include "MsgParse.h"
#include "TraceLoader.h"
#include "btypes_gen.h"

#include "parse/PSlice.h"

#include "block/block.h"

#include "td/utils/base64.h"
#include "td/utils/crypto.h"
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

td::Result<vm::CellSlice> ordinary_cell_slice(const td::Ref<vm::Cell> &cell) {
  if (cell.is_null()) {
    return td::Status::Error("no cell");
  }
  bool special = false;
  vm::CellSlice cs = vm::load_cell_slice_special(cell, special);
  if (special) {
    return td::Status::Error("exotic cell");
  }
  return cs;
}

td::Result<Value> load_std_account(vm::CellSlice &cs) {
  TRY_RESULT(v, load_address_py(cs));
  if (v.t != VType::Account || v.addr_none) {
    return td::Status::Error("not a std address");
  }
  return v;
}

bool is_state_init_address(const td::Bits256 &address, td::Slice code_hash_b64, int code_depth,
                           const td::Ref<vm::Cell> &data) {
  auto code_hash = td::base64_decode(code_hash_b64);
  if (code_hash.is_error()) {
    return false;
  }
  int data_depth = data->get_depth();
  std::string repr = {2, 1, 0b00110100, static_cast<char>(code_depth >> 8), static_cast<char>(code_depth & 0xff),
                      static_cast<char>(data_depth >> 8), static_cast<char>(data_depth & 0xff)};
  repr += code_hash.move_as_ok();
  repr += data->get_hash().as_slice().str();
  td::Bits256 state_init_hash;
  td::sha256(repr, state_init_hash.as_slice());
  return state_init_hash == address;
}

td::Result<td::Ref<vm::Cell>> deploy_data(const Message *msg, td::Bits256 &address) {
  if (msg == nullptr || !msg->init_state || !msg->destination) {
    return td::Status::Error("no StateInit");
  }
  TRY_RESULT(dest, block::StdAddress::parse(td::Slice(*msg->destination)));
  TRY_RESULT(root, cell_from_pystr(msg->init_state->body));
  if (td::Bits256(root->get_hash().bits()) != dest.addr) {
    return td::Status::Error("StateInit hash is not the destination");
  }
  TRY_RESULT(cs, open_ref_cell(root));
  address = dest.addr;
  return state_init_data_cell(cs);
}

}  // namespace mch
