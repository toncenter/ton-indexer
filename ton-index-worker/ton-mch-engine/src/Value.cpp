#include "Value.h"

#include "td/utils/base64.h"
#include "td/utils/crypto.h"
#include "vm/boc.h"
#include "vm/cells/DataCell.h"

#include <cctype>
#include <list>
#include <map>
#include <set>

namespace mch {

Value Value::make_bool(bool b) {
  Value v;
  v.t = VType::Bool;
  v.boolean = b;
  return v;
}

Value Value::make_int(td::RefInt256 n) {
  Value v;
  v.t = VType::Int;
  v.num = std::move(n);
  return v;
}

Value Value::make_int64(std::int64_t n) {
  return make_int(td::make_refint(n));
}

Value Value::make_float(double n) {
  Value v;
  v.t = VType::Float;
  v.dnum = n;
  return v;
}

Value Value::make_amount_float(double n) {
  Value v;
  v.t = VType::Amount;
  v.amount_float = true;
  v.dnum = n;
  return v;
}

Value Value::make_str(std::string s) {
  Value v;
  v.t = VType::Str;
  v.str = std::move(s);
  return v;
}

Value Value::make_bytes(std::string raw) {
  Value v;
  v.t = VType::Bytes;
  v.str = std::move(raw);
  return v;
}

Value Value::make_amount(td::RefInt256 n) {
  Value v;
  v.t = VType::Amount;
  v.num = std::move(n);
  return v;
}

Value Value::make_amount_none() {
  Value v;
  v.t = VType::Amount;  // num stays null == Python Amount(None)
  return v;
}

Value Value::make_block(const Block *b) {
  Value v;
  v.t = VType::Block;
  v.block = b;
  return v;
}

Value Value::make_account_raw(std::string canonical) {
  Value v;
  v.t = VType::Account;
  v.str = std::move(canonical);
  return v;
}

Value Value::make_account_none() {
  Value v;
  v.t = VType::Account;
  v.addr_none = true;
  return v;
}

Value Value::make_asset_ton() {
  Value v;
  v.t = VType::Asset;
  v.is_ton = true;
  return v;
}

Value Value::make_asset_jetton(std::string canonical_master) {
  Value v;
  v.t = VType::Asset;
  v.is_ton = false;
  v.has_jetton = true;
  v.str = std::move(canonical_master);
  return v;
}

Value Value::make_cell(td::Ref<vm::Cell> c) {
  Value v;
  v.t = VType::Cell;
  v.cell = std::move(c);
  return v;
}

Value Value::make_list(std::vector<Value> xs) {
  Value v;
  v.t = VType::List;
  v.items = std::make_shared<std::vector<Value>>(std::move(xs));
  return v;
}

Value Value::make_dict(Fields fs) {
  Value v;
  v.t = VType::Dict;
  v.fields = std::make_shared<Fields>(std::move(fs));
  return v;
}

Value Value::make_obj(Fields fs) {
  Value v;
  v.t = VType::Obj;
  v.fields = std::make_shared<Fields>(std::move(fs));
  return v;
}

const Value *Value::field(const std::string &name) const {
  if (!fields) {
    return nullptr;
  }
  for (const auto &kv : *fields) {
    if (kv.first == name) {
      return &kv.second;
    }
  }
  return nullptr;
}

td::Result<std::string> td_boc_serialize(const td::Ref<vm::Cell> &root) {
  // Dumps render cell-derived fields by root hash, so use the deterministic
  // native writer; BOC container byte order is not observable here.
  if (root.is_null()) {
    return td::Status::Error("td_boc_serialize: null cell");
  }
  auto r = vm::std_boc_serialize(root, 0);
  if (r.is_error()) {
    return r.move_as_error();
  }
  return r.move_as_ok().as_slice().str();
}

std::optional<std::string> normalize_raw_address(const std::string &s) {
  auto colon = s.find(':');
  if (colon == std::string::npos || colon == 0) {
    return std::nullopt;
  }
  const std::string wc_part = s.substr(0, colon);
  const std::string hex_part = s.substr(colon + 1);

  // Workchain: optional leading '-', then digits.
  std::size_t p = 0;
  if (wc_part[p] == '-') {
    p++;
  }
  if (p >= wc_part.size()) {
    return std::nullopt;
  }
  for (; p < wc_part.size(); p++) {
    if (!std::isdigit(static_cast<unsigned char>(wc_part[p]))) {
      return std::nullopt;
    }
  }
  // 256-bit account id == exactly 64 hex digits.
  if (hex_part.size() != 64) {
    return std::nullopt;
  }
  std::string up;
  up.reserve(64);
  for (char c : hex_part) {
    if (!std::isxdigit(static_cast<unsigned char>(c))) {
      return std::nullopt;
    }
    up.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
  }
  return wc_part + ":" + up;
}

std::string Value::describe() const {
  switch (t) {
    case VType::Null:
      return "null";
    case VType::Bool:
      return boolean ? "true" : "false";
    case VType::Int:
      return "int(" + (num.is_null() ? std::string("nan") : dec_string(num)) + ")";
    case VType::Amount:
      return "amount(" + (num.is_null() ? std::string("nan") : dec_string(num)) + ")";
    case VType::Str:
      return "str(" + str + ")";
    case VType::Bytes:
      return "bytes[" + std::to_string(str.size()) + "]";
    case VType::Account:
      return addr_none ? "addr_none" : ("account(" + str + ")");
    case VType::Asset:
      return is_ton ? "asset(TON)" : ("asset(" + str + ")");
    case VType::Cell:
      return "cell";
    case VType::Block:
      return "Block";
    case VType::List: {
      std::string out = "[";
      for (std::size_t i = 0; i < items->size(); i++) {
        if (i) {
          out += ", ";
        }
        out += (*items)[i].describe();
      }
      return out + "]";
    }
    case VType::Dict:
    case VType::Obj: {
      std::string out = (t == VType::Obj ? "obj{" : "{");
      bool first = true;
      for (const auto &kv : *fields) {
        if (!first) {
          out += ", ";
        }
        first = false;
        out += kv.first + ": " + kv.second.describe();
      }
      return out + "}";
    }
  }
  return "?";
}

}  // namespace mch
