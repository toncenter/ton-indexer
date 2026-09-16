#include "host/HostImpls.h"

#include "vm/cellslice.h"

#include <string>
#include <vector>

namespace mch {

namespace {

// MsgParse addr_extern "extern:<len>:<hex>" -> "len;hex". Both halves are
// already in the Value; only the separator changes (colon -> semicolon).
Value extern_address_string(const std::string &s) {
  static const std::string kPrefix = "extern:";
  if (s.compare(0, kPrefix.size(), kPrefix) != 0) {
    return Value::null();
  }
  auto sep = s.find(':', kPrefix.size());
  if (sep == std::string::npos) {
    return Value::null();
  }
  std::string out = s.substr(kPrefix.size());
  out[sep - kPrefix.size()] = ';';
  return Value::make_str(std::move(out));
}

// The ABI bridge represents addr_extern as
//   obj{bits: Int, value: Cell(bits)}
// rather than MsgParse's reference "extern:<len>:<hex>" sentinel. Render the
// value directly from the cell so widths above RefInt256's limit stay exact.
Value extern_address_object(const Value &v) {
  const Value *bits = v.field("bits");
  const Value *value = v.field("value");
  if (v.t != VType::Obj || bits == nullptr || bits->t != VType::Int || bits->num.is_null() ||
      bits->num->sgn() < 0 || td::cmp(bits->num, td::make_refint(511)) > 0 || value == nullptr ||
      value->t != VType::Cell || value->cell.is_null()) {
    return Value::null();
  }

  int len = static_cast<int>(bits->num->to_long());
  vm::CellSlice cs = vm::load_cell_slice(value->cell);
  if (cs.size() != static_cast<unsigned>(len) || cs.size_refs() != 0) {
    return Value::null();
  }

  static const char kHexDigits[] = "0123456789abcdef";
  std::string hex;
  hex.reserve(static_cast<std::size_t>(len) / 4 + 1);
  int head = len % 4;
  if (head != 0) {
    hex.push_back(kHexDigits[cs.fetch_ulong(head)]);
  }
  for (int rem = len - head; rem > 0; rem -= 4) {
    hex.push_back(kHexDigits[cs.fetch_ulong(4)]);
  }
  std::size_t first = hex.find_first_not_of('0');
  hex = first == std::string::npos ? std::string("0") : hex.substr(first);
  return Value::make_str(std::to_string(len) + ";" + hex);
}

}  // namespace

// expected_address for worker payout / proxy charge / grant refund:
// extern -> "len;hex", account -> canonical "wc:HEX", addr_none stays null.
EvalResult cocoon_expected_address(BuildEnv &, const std::vector<Value> &args) {
  const Value &v = args[0];
  if (v.t == VType::Account) {
    return rt_ok(v.addr_none ? Value::null() : Value::make_str(v.str));
  }
  if (v.t == VType::Str) {
    return rt_ok(extern_address_string(v.str));
  }
  if (v.t == VType::Obj) {
    return rt_ok(extern_address_object(v));
  }
  return rt_ok(Value::null());
}

}  // namespace mch
