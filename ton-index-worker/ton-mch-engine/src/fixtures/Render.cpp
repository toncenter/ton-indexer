// Dump renderers + vector comparator. See fixtures/Render.h.
#include "fixtures/Render.h"

#include "BlockTree.h"
#include "MsgParse.h"
#include "parse/PSlice.h"

#include "td/utils/base64.h"
#include "vm/boc.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <vector>

namespace mch {

namespace {
std::string boc_field_cellhash(const Value &v);
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

std::string render_value(const Value &v, bool omit_null_fields) {
  switch (v.t) {
    case VType::Null:
      return "null";
    case VType::Bool:
      return v.boolean ? "true" : "false";
    case VType::Int:
      return v.num.is_null() ? "nan" : v.num->to_dec_string();
    case VType::Amount:
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
        out += render_value((*v.items)[i], omit_null_fields);
      }
      return out + "]";
    }
    case VType::Dict:
    case VType::Obj: {
      std::vector<std::string> keys;
      for (const auto &kv : *v.fields) {
        if (!omit_null_fields || !kv.second.is_null()) keys.push_back(kv.first);
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
        if (rendered.empty()) rendered = render_value(*v.field(k), omit_null_fields);
        out += k + "=" + rendered;
      }
      return out + "}";
    }
    case VType::Block:
      return "block:" + (v.block != nullptr ? block_key(v.block) : std::string("null"));
  }
  return "?";
}

std::string yaml_str(const std::string &s) {
  auto is_number = [](const std::string &x) {
    std::size_t i = (x[0] == '-' || x[0] == '+') ? 1 : 0;
    if (i >= x.size()) return false;
    if (x.compare(i, 2, "0x") == 0 || x.compare(i, 2, "0o") == 0) {
      return x.size() > i + 2 && std::all_of(x.begin() + i + 2, x.end(), ::isxdigit);
    }
    bool digit = false;
    for (; i < x.size(); i++) {
      char c = x[i];
      if (std::isdigit(static_cast<unsigned char>(c))) digit = true;
      else if (c != '.' && c != 'e' && c != 'E' && c != '+' && c != '-' && c != '_') return false;
    }
    return digit;
  };
  bool plain = !s.empty();
  if (plain) {
    static const char *const words[] = {"true", "false", "null", "~", "yes", "no", "on", "off",
                                        "True", "False", "Null", "Yes", "No", "On", "Off",
                                        ".inf", ".nan", "-.inf"};
    for (const char *w : words) if (s == w) plain = false;
    if (is_number(s)) plain = false;
    unsigned char first = s.front(), last = s.back();
    if (std::strchr("-?:,[]{}#&*!|>'\"%@`", first) || std::isspace(first) || std::isspace(last) ||
        last == ':') {
      plain = false;
    }
    if (s.find(": ") != std::string::npos || s.find(" #") != std::string::npos) plain = false;
    for (unsigned char c : s) if (c < 0x20 || c == 0x7f) plain = false;
  }
  if (plain) return s;
  std::string out = "\"";
  for (unsigned char c : s) {
    if (c == '"' || c == '\\') { out += '\\'; out += static_cast<char>(c); }
    else if (c == '\n') out += "\\n";
    else if (c == '\t') out += "\\t";
    else if (c < 0x20 || c == 0x7f) {
      char buf[8];
      std::snprintf(buf, sizeof(buf), "\\x%02x", c);
      out += buf;
    } else out += static_cast<char>(c);
  }
  return out + "\"";
}

namespace {
bool yaml_block(const Value &v) {
  return v.t == VType::Dict || v.t == VType::Obj || (v.t == VType::List && !v.items->empty());
}

std::string yaml_scalar(const Value &v) {
  switch (v.t) {
    case VType::Null: return "null";
    case VType::Bool: return v.boolean ? "true" : "false";
    case VType::Int:
    case VType::Amount: return v.num.is_null() ? "null" : v.num->to_dec_string();
    case VType::Str: return yaml_str(v.str);
    case VType::Bytes: return yaml_str(td::base64_encode(td::Slice(v.str)));
    case VType::Account: return v.addr_none ? "addr_none" : yaml_str(v.str);
    case VType::Asset: return v.is_ton ? "TON" : yaml_str(v.str);
    case VType::Cell: {
      if (v.cell.is_null()) return "null";
      auto h = v.cell->get_hash();
      return hex_upper(h.as_slice().ubegin(), h.as_slice().size());
    }
    case VType::List: return "[]";
    case VType::Dict:
    case VType::Obj: return "{}";
    case VType::Block: return v.block != nullptr ? block_key(v.block) : "null";
  }
  return "null";
}

void yaml_body(const Value &v, int indent, std::string &out);

void yaml_dict_body(const Value &v, int indent, std::string &out) {
  std::vector<std::string> keys;
  for (const auto &kv : *v.fields) keys.push_back(kv.first);
  std::sort(keys.begin(), keys.end());
  for (const std::string &k : keys) {
    const Value &f = *v.field(k);
    std::string hash = is_boc_field(k) ? boc_field_cellhash(f) : std::string();
    if (!hash.empty()) {
      out += std::string(indent, ' ') + k + ": " + hash + "\n";
    } else {
      render_yaml_field(k, f, indent, out);
    }
  }
}

void yaml_body(const Value &v, int indent, std::string &out) {
  if (v.t == VType::List) {
    for (const Value &item : *v.items) {
      if (!yaml_block(item)) {
        out += std::string(indent, ' ') + "- " + yaml_scalar(item) + "\n";
        continue;
      }
      std::string sub;
      yaml_body(item, indent + 2, sub);
      sub.replace(indent, 2, "- ");  // first line rides the dash
      out += sub;
    }
    return;
  }
  yaml_dict_body(v, indent, out);
}
}  // namespace

void render_yaml_field(const std::string &key, const Value &v, int indent, std::string &out) {
  out += std::string(indent, ' ') + key + ":";
  if (!yaml_block(v)) {
    out += " " + yaml_scalar(v) + "\n";
    return;
  }
  out += "\n";
  yaml_body(v, indent + 2, out);
}

namespace {
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
}  // namespace


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
    case VType::List:
    case VType::Block:
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

namespace {
// Cell-derived BOC containers render as the cell's root hash ("cellhash:HEX-UPPER")
// so serialization-order / CRC is invisible. Returns "" if not a decodable BOC.
std::string boc_field_cellhash(const Value &v) {
  auto hash_of = [](const td::Ref<vm::Cell> &cell) -> std::string {
    auto h = cell->get_hash();
    return "cellhash:" + hex_upper(h.as_slice().ubegin(), h.as_slice().size());
  };
  // Decode-fail marker (never throws). Prefix is deterministic: str uses the
  // b64 string, bytes use base64 of the raw, so fail-cases stay A/B-comparable.
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
}  // namespace


bool structural_equal(const Value &a, const Value &b) {
  // Expected type drives the check; actual must match that type and be deeply
  // equal. Intentional.
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
      // Blocks compare by object identity. Intentional.
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
