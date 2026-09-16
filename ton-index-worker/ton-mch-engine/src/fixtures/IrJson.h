#pragma once

#include "td/utils/JsonBuilder.h"
#include "td/utils/crypto.h"

#include <cstdint>
#include <string>
#include <unordered_set>

namespace mch {

inline const td::JsonValue *jfield(const td::JsonValue &e, td::Slice name) {
  if (e.type() != td::JsonValue::Type::Object) {
    return nullptr;
  }
  for (const auto &kv : e.get_object().field_values_) {
    if (kv.first == name) {
      return &kv.second;
    }
  }
  return nullptr;
}

inline bool has(const td::JsonValue &e, td::Slice name) { return jfield(e, name) != nullptr; }

inline std::string jstr(const td::JsonValue &e, td::Slice name, const std::string &dflt = {}) {
  const td::JsonValue *f = jfield(e, name);
  return (f != nullptr && f->type() == td::JsonValue::Type::String) ? f->get_string().str() : dflt;
}

inline std::int64_t jint(const td::JsonValue &e, td::Slice name, std::int64_t dflt = 0) {
  const td::JsonValue *f = jfield(e, name);
  if (f == nullptr || f->type() != td::JsonValue::Type::Number) {
    return dflt;
  }
  return std::stoll(f->get_number().str());
}

inline bool jbool(const td::JsonValue &e, td::Slice name, bool dflt = false) {
  const td::JsonValue *f = jfield(e, name);
  return (f != nullptr && f->type() == td::JsonValue::Type::Boolean) ? f->get_boolean() : dflt;
}

// Expression builtins (rt_call_builtin arity table). Other `call` names are host fns.
inline bool is_builtin_name(const std::string &name) {
  static const std::unordered_set<std::string> kBuiltins = {
      "account", "amount", "asset", "ton_asset", "addr_none", "b64",
      "asset_of", "tail_unwrap", "bytes_of", "first", "last", "len", "sum",
      "zip", "map", "concat", "contains"};
  return kBuiltins.count(name) != 0;
}

inline std::string sha256_hex(td::Slice buf) {
  unsigned char digest[32];
  td::sha256(buf, td::MutableSlice(digest, 32));
  static const char *hex = "0123456789abcdef";
  std::string out;
  for (int i = 0; i < 32; i++) {
    out += hex[digest[i] >> 4];
    out += hex[digest[i] & 0xF];
  }
  return out;
}

}  // namespace mch
