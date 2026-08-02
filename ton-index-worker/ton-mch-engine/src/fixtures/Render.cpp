// Dump renderers + vector comparator (see fixtures/Render.h for why they are here
// and not in the product lib). Split out of BuildDriver.cpp and Value.cpp.
#include "fixtures/Render.h"

#include "BlockTree.h"
#include "MsgParse.h"
#include "parse/PSlice.h"

#include "td/utils/base64.h"
#include "vm/boc.h"

#include <algorithm>
#include <cstdio>
#include <vector>

namespace mch {

namespace {

// Canonical float text shared with the Python twin ("%.17g" both sides). Chosen
// over shortest-round-trip (repr / to_chars) because those disagree on round
// numbers (repr keeps a trailing ".0", to_chars drops it); %.17g is one agreed
// format, byte-verified on the corpus values.
std::string fmt_g17(double v) {
  char buf[64];
  std::snprintf(buf, sizeof(buf), "%.17g", v);
  return std::string(buf);
}

}  // namespace

std::string block_key(const Block *b) {
  std::string key = b->btype;
  if (b->opcode) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "#%08x", *b->opcode);
    key += buf;
  }
  key += "@" + std::to_string(b->min_lt);
  return key;
}

std::string render_value(const Value &v) {
  switch (v.t) {
    case VType::Null:
      return "null";
    case VType::Bool:
      return v.boolean ? "true" : "false";
    case VType::Int:
      return v.num.is_null() ? "nan" : v.num->to_dec_string();
    case VType::Float:
      // Bare Python float: "float:" plus shortest-safe %.17g. This keeps float
      // fields usable by the NFT purchase and shaper paths.
      return "float:" + fmt_g17(v.dnum);
    case VType::Amount:
      if (v.amount_float) {
        return "amount:" + fmt_g17(v.dnum);  // Python Amount(float): str(float)
      }
      return "amount:" + (v.num.is_null() ? std::string("nan") : v.num->to_dec_string());
    case VType::Str:
      return "str:" + v.str;
    case VType::Bytes:
      return "b64:" + td::base64_encode(td::Slice(v.str));
    case VType::Account:
      return v.addr_none ? "addr_none" : v.str;
    case VType::Asset:
      return v.is_ton ? "asset:TON" : "asset:" + v.str;
    case VType::Cell: {
      if (v.cell.is_null()) {
        return "cell:null";
      }
      auto h = v.cell->get_hash();
      return "cell:" + hex_upper(h.as_slice().ubegin(), h.as_slice().size());
    }
    case VType::List: {
      std::string out = "[";
      for (std::size_t i = 0; i < v.items->size(); i++) {
        if (i) out += ",";
        out += render_value((*v.items)[i]);
      }
      return out + "]";
    }
    case VType::Dict:
    case VType::Obj: {
      std::vector<std::string> keys;
      for (const auto &kv : *v.fields) {
        keys.push_back(kv.first);
      }
      std::sort(keys.begin(), keys.end());
      std::string out = v.t == VType::Obj ? "obj{" : "{";
      bool first = true;
      for (const auto &k : keys) {
        if (!first) out += ",";
        first = false;
        // Cell-derived BOC fields render as a cell hash.
        std::string rendered;
        if (is_boc_field(k)) rendered = boc_field_cellhash(*v.field(k));
        if (rendered.empty()) rendered = render_value(*v.field(k));
        out += k + "=" + rendered;
      }
      return out + "}";
    }
    case VType::Block:
      return "block:" + (v.block != nullptr ? block_key(v.block) : std::string("null"));
  }
  return "?";
}

std::string render_parse_fields_sorted(const Value::Fields &fields) {
  std::vector<std::pair<std::string, std::string>> kv;
  kv.reserve(fields.size());
  for (const auto &f : fields) {
    // Cell-derived BOC fields render as a cell hash.
    std::string rendered;
    if (is_boc_field(f.first)) rendered = boc_field_cellhash(f.second);
    if (rendered.empty()) rendered = render_parse_value(f.second);
    kv.emplace_back(f.first, rendered);
  }
  std::sort(kv.begin(), kv.end());
  std::string out = "{";
  for (size_t i = 0; i < kv.size(); i++) {
    if (i) {
      out += ",";
    }
    out += kv[i].first + "=" + kv[i].second;
  }
  out += "}";
  return out;
}

std::string render_parse_value(const Value &v) {
  switch (v.t) {
    case VType::Null:
      return "null";
    case VType::Bool:
      return v.boolean ? "true" : "false";
    case VType::Int:
    case VType::Amount:
      return v.num->to_dec_string();
    case VType::Str:
      return "str:" + v.str;
    case VType::Bytes:
      return "b64:" + td::base64_encode(td::Slice(v.str));
    case VType::Account:
      return v.addr_none ? "addr_none" : v.str;
    case VType::Cell: {
      if (v.cell.is_null()) {
        return "cell:null";
      }
      auto h = v.cell->get_hash();
      return "cell:" + hex_upper(h.as_slice().ubegin(), h.as_slice().size());
    }
    case VType::Asset:
      return v.is_ton ? "asset:TON" : "asset:" + v.str;
    case VType::Dict:
    case VType::Obj:
      return render_parse_fields_sorted(*v.fields);
    default:
      return v.describe();
  }
}

const std::set<std::string> &boc_field_names() {
  // Cell-derived BOC container fields. `comment`, `bitcoin_txid`, and
  // `pubkey` are raw non-BOC bytes and are DELIBERATELY excluded.
  static const std::set<std::string> kBocFields = {
      "forward_payload", "custom_payload", "message_boc", "message_boc_str",
      "order_boc",       "order_boc_str",  "stonfi_swap_body",
  };
  return kBocFields;
}

bool is_boc_field(const std::string &key) { return boc_field_names().count(key) != 0; }

std::string boc_field_cellhash(const Value &v) {
  auto hash_of = [](const td::Ref<vm::Cell> &cell) -> std::string {
    auto h = cell->get_hash();
    return "cellhash:" + hex_upper(h.as_slice().ubegin(), h.as_slice().size());
  };
  // Decode-fail marker (never throws, the dump must not abort/trace-fail on a
  // bad BOC field). A short deterministic prefix aids diagnosis; both engines
  // compute it identically from the same value type (str → the b64 string;
  // bytes → base64 of the raw), so a fail-case would still be A/B-comparable.
  // Never fires over the corpus (all BOC fields decode), defensive only.
  auto decode_fail = [](const std::string &enc_prefix_src) {
    return "cellhash:DECODE_FAIL:" + enc_prefix_src.substr(0, 12);
  };
  if (v.t == VType::Cell) {
    return v.cell.is_null() ? std::string() : hash_of(v.cell);  // null cell → fall back
  }
  std::string raw;
  if (v.t == VType::Bytes) {
    raw = v.str;  // raw BOC bytes (MsgParse payload)
    auto r_cell = vm::std_boc_deserialize(td::Slice(raw));
    if (r_cell.is_error() || r_cell.ok().is_null()) {
      return decode_fail(td::base64_encode(td::Slice(raw)));
    }
    return hash_of(r_cell.move_as_ok());
  }
  if (v.t == VType::Str) {
    auto r = td::base64_decode(v.str);  // b64() builtin output
    if (r.is_error()) {
      return decode_fail(v.str);
    }
    auto r_cell = vm::std_boc_deserialize(td::Slice(r.ok()));
    if (r_cell.is_error() || r_cell.ok().is_null()) {
      return decode_fail(v.str);
    }
    return hash_of(r_cell.move_as_ok());
  }
  return std::string();  // null / other type → fall back to normal render
}

bool structural_equal(const Value &a, const Value &b) {
  // Mirrors the Python vector runner's _equal: expected type drives the check,
  // actual must match that type and be deeply equal.
  switch (b.t) {
    case VType::Null:
      return a.t == VType::Null;
    case VType::Bool:
      return a.t == VType::Bool && a.boolean == b.boolean;
    case VType::Int:
      return a.t == VType::Int && cmp(a.num, b.num) == 0;
    case VType::Amount:
      if (a.t != VType::Amount) {
        return false;
      }
      if (a.num.is_null() || b.num.is_null()) {
        return a.num.is_null() && b.num.is_null();  // Amount(None) == Amount(None)
      }
      return cmp(a.num, b.num) == 0;
    case VType::Str:
      return a.t == VType::Str && a.str == b.str;
    case VType::Bytes:
      return a.t == VType::Bytes && a.str == b.str;
    case VType::Account:
      if (a.t != VType::Account) {
        return false;
      }
      if (a.addr_none || b.addr_none) {
        return a.addr_none && b.addr_none;
      }
      return a.str == b.str;
    case VType::Asset:
      if (a.t != VType::Asset || a.is_ton != b.is_ton || a.has_jetton != b.has_jetton) {
        return false;
      }
      return !a.has_jetton || a.str == b.str;
    case VType::Cell: {
      if (a.t != VType::Cell) {
        return false;
      }
      if (a.cell.is_null() || b.cell.is_null()) {
        return a.cell.is_null() && b.cell.is_null();
      }
      return a.cell->get_hash() == b.cell->get_hash();
    }
    case VType::List: {
      if (a.t != VType::List) {
        return false;
      }
      const auto &xs = *a.items;
      const auto &ys = *b.items;
      if (xs.size() != ys.size()) {
        return false;
      }
      for (std::size_t i = 0; i < xs.size(); i++) {
        if (!structural_equal(xs[i], ys[i])) {
          return false;
        }
      }
      return true;
    }
    case VType::Block:
      // Python Block.__eq__ is object identity.
      return a.t == VType::Block && a.block == b.block;
    case VType::Dict:
    case VType::Obj: {
      if (a.t != b.t || !a.fields || !b.fields) {
        return false;
      }
      const auto &af = *a.fields;
      const auto &bf = *b.fields;
      if (af.size() != bf.size()) {
        return false;
      }
      // Set-of-keys comparison (order-independent), each key deep-equal.
      for (const auto &kv : bf) {
        const Value *av = a.field(kv.first);
        if (av == nullptr || !structural_equal(*av, kv.second)) {
          return false;
        }
      }
      return true;
    }
  }
  return false;
}

void warn_missing_artifact_parsers(const std::vector<CompiledMatcher> &matchers) {
  const auto &parsers = message_parsers();
  std::set<std::string> missing;
  for (const CompiledMatcher &m : matchers) {
    for (const std::string &t : m.ref_msgtypes) {
      if (parsers.find(t) == parsers.end()) {
        missing.insert(t);
      }
    }
  }
  if (missing.empty()) {
    return;
  }
  std::string joined;
  for (const std::string &t : missing) {
    if (!joined.empty()) joined += ", ";
    joined += t;
  }
  std::fprintf(stderr,
               "WARNING: the matcher table references %zu message type(s) with no registered C++ "
               "parser "
               "(matchers that parse them will be build-skipped): %s\n",
               missing.size(), joined.c_str());
}

}  // namespace mch
