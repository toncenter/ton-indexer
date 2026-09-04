// DNS-record message parser. Shared machinery is in parse/PSlice.h.
#include "parse/Parsers.h"

#include "parse/PSlice.h"

#include "common/refint.h"
#include "vm/cellslice.h"

#include <string>
#include <utility>

namespace mch {

namespace {

// Strict UTF-8: invalid bytes fail the whole parse. Intentional.
bool valid_utf8(const std::string &s) {
  size_t i = 0;
  while (i < s.size()) {
    unsigned char c = static_cast<unsigned char>(s[i]);
    int n;
    if (c < 0x80) {
      n = 0;
    } else if ((c & 0xE0) == 0xC0 && c >= 0xC2) {
      n = 1;
    } else if ((c & 0xF0) == 0xE0) {
      n = 2;
    } else if ((c & 0xF8) == 0xF0 && c <= 0xF4) {
      n = 3;
    } else {
      return false;
    }
    if (i + n >= s.size()) {
      return false;
    }
    for (int k = 1; k <= n; k++) {
      if ((static_cast<unsigned char>(s[i + k]) & 0xC0) != 0x80) {
        return false;
      }
    }
    // Reject overlongs, surrogates, and code points above U+10FFFF.
    if (n == 2) {
      unsigned char c1 = static_cast<unsigned char>(s[i + 1]);
      if ((c == 0xE0 && c1 < 0xA0) || (c == 0xED && c1 >= 0xA0)) {
        return false;
      }
    }
    if (n == 3) {
      unsigned char c1 = static_cast<unsigned char>(s[i + 1]);
      if ((c == 0xF0 && c1 < 0x90) || (c == 0xF4 && c1 >= 0x90)) {
        return false;
      }
    }
    i += n + 1;
  }
  return true;
}

td::Result<std::string> fetch_bytes_str(vm::CellSlice &cs, int n) {
  if (!cs.have(n * 8)) {
    return td::Status::Error("bytes: underflow");
  }
  std::string out(static_cast<size_t>(n), '\0');
  if (!cs.fetch_bytes(reinterpret_cast<unsigned char *>(out.data()), n)) {
    return td::Status::Error("bytes: fetch failed");
  }
  return out;
}

// DNS value dicts wrap addresses: addr_none becomes an Account none (not null);
// an extern address string fails the whole parse. Intentional.
td::Result<Value> account_id_of(Value addr) {
  if (addr.is_null()) {
    return Value::make_account_none();
  }
  if (addr.t == VType::Str) {
    return td::Status::Error("AccountId(extern) raises");
  }
  return addr;
}

}  // namespace

// DNSText chunk walk (intentional): every chunk's length+text
// is read from the SAME value slice; the first extra chunk consumes one of
// ITS refs into a dead variable; a third chunk always fails the parse.
td::Result<Value> parse_change_dns(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(ctx, open_body(body));
  auto &cs = ctx.cs;
  if (!cs.have(32 + 64) || !cs.advance(32)) {
    return td::Status::Error("dns: header underflow");
  }
  auto query_id = cs.fetch_ulong(64);
  TRY_RESULT(key, fetch_bytes_str(cs, 32));
  bool has_value = cs.size_refs() > 0;
  Value value = Value::null();
  if (has_value) {
    vm::CellSlice vs;
    bool special = false;
    try {
      vs = vm::load_cell_slice_special(cs.fetch_ref(), special);
    } catch (...) {
      return td::Status::Error("dns: bad value cell");
    }
    if (!vs.have(16)) {
      return td::Status::Error("dns: schema underflow");
    }
    auto schema = static_cast<unsigned>(vs.fetch_ulong(16));
    Value::Fields vf;
    if (schema == 0xba93) {
      TRY_RESULT(addr, load_address_py(vs));
      TRY_RESULT(acc, account_id_of(std::move(addr)));
      vf.emplace_back("schema", Value::make_str("DNSNextResolver"));
      vf.emplace_back("address", std::move(acc));
    } else if (schema == 0xad01) {
      TRY_RESULT(adnl, fetch_bytes_str(vs, 32));
      if (!vs.have(8)) {
        return td::Status::Error("dns: adnl flags underflow");
      }
      vf.emplace_back("schema", Value::make_str("DNSAdnlAddress"));
      vf.emplace_back("address", Value::make_bytes(std::move(adnl)));
      vf.emplace_back("flags", Value::make_int64(static_cast<std::int64_t>(vs.fetch_ulong(8))));
    } else if (schema == 0x9fd3) {
      TRY_RESULT(addr, load_address_py(vs));
      TRY_RESULT(acc, account_id_of(std::move(addr)));
      if (!vs.have(8)) {
        return td::Status::Error("dns: smc flags underflow");
      }
      vf.emplace_back("schema", Value::make_str("DNSSmcAddress"));
      vf.emplace_back("address", std::move(acc));
      vf.emplace_back("flags", Value::make_int64(static_cast<std::int64_t>(vs.fetch_ulong(8))));
    } else if (schema == 0x7473) {
      TRY_RESULT(bag, fetch_bytes_str(vs, 32));
      vf.emplace_back("schema", Value::make_str("DNSStorageAddress"));
      vf.emplace_back("address", Value::make_bytes(std::move(bag)));
    } else if (schema == 0x1eda) {
      if (!vs.have(8)) {
        return td::Status::Error("dns: chunk count underflow");
      }
      int chunks = static_cast<int>(vs.fetch_ulong(8));
      std::string dns_text;
      bool value_slice_is_cell = false;
      while (chunks > 0) {
        if (!vs.have(8)) {
          return td::Status::Error("dns: chunk len underflow");
        }
        int len = static_cast<int>(vs.fetch_ulong(8));
        TRY_RESULT(chunk, fetch_bytes_str(vs, len));
        if (!valid_utf8(chunk)) {
          return td::Status::Error("dns: chunk not utf-8");
        }
        dns_text += chunk;
        chunks--;
        if (chunks > 0) {
          if (value_slice_is_cell) {
            // A Cell has no load_ref; this path always fails.
            return td::Status::Error("dns: load_ref on a cell");
          }
          if (vs.size_refs() == 0) {
            return td::Status::Error("dns: chunk ref missing");
          }
          vs.fetch_ref();  // consumed into the dead value_slice variable
          value_slice_is_cell = true;
        }
      }
      vf.emplace_back("schema", Value::make_str("DNSText"));
      vf.emplace_back("dns_text", Value::make_str(std::move(dns_text)));
    } else {
      vf.emplace_back("schema", Value::make_str("Unknown"));
    }
    value = Value::make_dict(std::move(vf));
  }
  Value::Fields f;
  f.emplace_back("query_id", Value::make_int(refint_u64(query_id)));
  f.emplace_back("key", Value::make_bytes(std::move(key)));
  f.emplace_back("has_value", Value::make_bool(has_value));
  f.emplace_back("value", std::move(value));
  return Value::make_obj(std::move(f));
}

}  // namespace mch
