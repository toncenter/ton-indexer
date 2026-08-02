// Shared pytoniq-Slice stand-in machinery (see parse/PSlice.h). Every quirk of
// the Python reference parsers reproduced here is documented at its definition;
// see MsgParse.cpp's header for the file-level catalogue.
#include "parse/PSlice.h"

#include "MsgParse.h"

#include "crypto/block/block-parse.h"
#include "common/refint.h"
#include "td/utils/base64.h"
#include "vm/cellslice.h"
#include "vm/cells/CellBuilder.h"

#include <optional>
#include <string>
#include <vector>

namespace mch {

std::string hex_upper(const unsigned char *p, size_t n) {
  static const char *digits = "0123456789ABCDEF";
  std::string out(n * 2, '0');
  for (size_t i = 0; i < n; i++) {
    out[2 * i] = digits[p[i] >> 4];
    out[2 * i + 1] = digits[p[i] & 0xF];
  }
  return out;
}

td::RefInt256 refint_u64(unsigned long long v) {
  auto hi = td::make_refint(static_cast<long long>(v >> 32));
  return (hi << 32) + td::make_refint(static_cast<long long>(v & 0xFFFFFFFFULL));
}

// pytoniq Slice.load_address(). Advances `cs`. Error == the Python parser
// raising (whole message parse fails, or the enclosing try downgrades it).
td::Result<Value> load_address_py(vm::CellSlice &cs) {
  if (!cs.have(2)) {
    return td::Status::Error("address: underflow");
  }
  auto tag = cs.fetch_ulong(2);
  if (tag == 0) {
    return Value::null();  // pytoniq returns None for addr_none
  }
  if (tag == 1) {
    if (!cs.have(9)) {
      return td::Status::Error("address: extern underflow");
    }
    int len = static_cast<int>(cs.fetch_ulong(9));
    if (len == 0) {
      return td::Status::Error("address: extern len 0");  // ba2int('') raises
    }
    if (!cs.have(len)) {
      return td::Status::Error("address: extern underflow");
    }
    // pytoniq addr_extern: ba2int(bits, signed=False), an unsigned big-endian
    // integer of exactly `len` bits (1..511). NO td::RefInt256 CAN HOLD IT: that
    // type is a signed 257-bit BigInt, so anything past 256 bits is an
    // invalid-but-non-null value that renders "NaN" (which is what a 260-bit
    // cocoon expectedMyAddress, both in --parse-dump and
    // through to the cocoon_expected_address host fn). The Str therefore carries
    // the value in HEX, assembled nibble by nibble straight off the wire, exact
    // for every length up to 511 and the direct twin of Python's `f"{int:x}"`:
    // lowercase, no leading zeros, "0" for zero.
    static const char kHexDigits[] = "0123456789abcdef";
    std::string hex;
    hex.reserve(static_cast<size_t>(len) / 4 + 1);
    int head = len % 4;  // the top group is short when len is not a multiple of 4
    if (head != 0) {
      hex.push_back(kHexDigits[cs.fetch_ulong(head)]);
    }
    for (int rem = len - head; rem > 0; rem -= 4) {
      hex.push_back(kHexDigits[cs.fetch_ulong(4)]);
    }
    size_t first = hex.find_first_not_of('0');
    hex = first == std::string::npos ? std::string("0") : hex.substr(first);
    return Value::make_str("extern:" + std::to_string(len) + ":" + hex);
  }
  if (tag == 3) {
    return td::Status::Error("address: addr_var unsupported");  // pytoniq raises
  }
  if (!cs.have(1)) {
    return td::Status::Error("address: underflow");
  }
  if (cs.fetch_ulong(1)) {  // anycast: consumed, never applied (pytoniq)
    if (!cs.have(5)) {
      return td::Status::Error("address: anycast underflow");
    }
    int depth = static_cast<int>(cs.fetch_ulong(5));
    if (depth < 1) {
      return td::Status::Error("address: anycast depth 0");  // pytoniq raises
    }
    if (!cs.have(depth) || !cs.advance(depth)) {
      return td::Status::Error("address: anycast underflow");
    }
  }
  if (!cs.have(8 + 256)) {
    return td::Status::Error("address: std underflow");
  }
  int wc = static_cast<int>(cs.fetch_long(8));
  unsigned char buf[32];
  if (!cs.fetch_bytes(buf, 32)) {
    return td::Status::Error("address: std fetch failed");
  }
  return Value::make_account_raw(std::to_string(wc) + ":" + hex_upper(buf, 32));
}

// pytoniq Slice.load_coins(): len nibble 0 -> 0. Advances `cs`.
td::Result<td::RefInt256> load_coins_py(vm::CellSlice &cs) {
  if (!cs.have(4)) {
    return td::Status::Error("coins: underflow");
  }
  int len = static_cast<int>(cs.fetch_ulong(4));
  if (len == 0) {
    return td::make_refint(0);
  }
  if (!cs.have(len * 8)) {
    return td::Status::Error("coins: underflow");
  }
  auto v = cs.fetch_int256(len * 8, false);
  if (v.is_null()) {
    return td::Status::Error("coins: fetch failed");
  }
  return v;
}

td::Result<td::RefInt256> var_uint16(const td::Ref<vm::CellSlice> &csr) {
  auto v = block::tlb::t_VarUInteger_16.as_integer(csr);
  if (v.is_null()) {
    return td::Status::Error("VarUInteger16: bad slice");
  }
  return v;
}

PSlice pslice_from_cell(const td::Ref<vm::Cell> &c) {
  PSlice ps;
  bool special = false;
  ps.cs = vm::load_cell_slice_special(c, special);
  for (unsigned i = 0; i < ps.cs.size_refs(); i++) {
    ps.refs.push_back(ps.cs.prefetch_ref(i));
  }
  return ps;
}

// pytoniq Slice.to_cell(): Cell(remaining bits, refs[off:]).
td::Result<td::Ref<vm::Cell>> pslice_to_cell(const PSlice &ps) {
  try {
    vm::CellBuilder cb;
    cb.store_bits(ps.cs.data_bits(), ps.cs.size());
    for (size_t i = ps.off; i < ps.refs.size(); i++) {
      cb.store_ref(ps.refs[i]);
    }
    return cb.finalize();
  } catch (...) {
    return td::Status::Error("to_cell failed");
  }
}

// pytoniq Slice.load_snake_bytes(): byte-aligned, <=1 ref per link.
td::Result<std::string> load_snake_bytes(PSlice ps) {
  std::string out;
  for (;;) {
    if (ps.cs.size() % 8 != 0) {
      return td::Status::Error("snake: not byte-aligned");
    }
    size_t nrefs = ps.refs.size() - ps.off;
    if (nrefs > 1) {
      return td::Status::Error("snake: >1 ref");
    }
    size_t n = ps.cs.size() / 8;
    if (n > 0) {
      std::string chunk(n, '\0');
      if (!ps.cs.fetch_bytes(reinterpret_cast<unsigned char *>(chunk.data()), static_cast<int>(n))) {
        return td::Status::Error("snake: fetch failed");
      }
      out += chunk;
    }
    if (nrefs == 0) {
      return out;
    }
    ps = pslice_from_cell(ps.refs[ps.off]);
  }
}

td::Result<BodyCtx> open_body(const td::Ref<vm::Cell> &body) {
  if (body.is_null()) {
    return td::Status::Error("null body");
  }
  BodyCtx ctx;
  bool special = false;
  try {
    ctx.cs = vm::load_cell_slice_special(body, special);
  } catch (...) {
    return td::Status::Error("bad body cell");
  }
  for (unsigned i = 0; i < ctx.cs.size_refs(); i++) {
    ctx.all_refs.push_back(ctx.cs.prefetch_ref(i));
  }
  return ctx;
}

td::Result<td::RefInt256> pyslice_load_coins(vm::CellSlice &cs) {
  return load_coins_py(cs);
}

td::Result<vm::CellSlice> open_ref_cell(const td::Ref<vm::Cell> &c) {
  bool special = false;
  try {
    return vm::load_cell_slice_special(c, special);
  } catch (...) {
    return td::Status::Error("ref slice");
  }
}

td::Status skip_state_init_py(vm::CellSlice &cs) {
  if (!cs.have(1)) return td::Status::Error("state_init: split_depth underflow");
  if (cs.fetch_ulong(1)) {
    if (!cs.have(5)) return td::Status::Error("state_init: split_depth bits underflow");
    cs.advance(5);
  }
  if (!cs.have(1)) return td::Status::Error("state_init: special underflow");
  if (cs.fetch_ulong(1)) {
    if (!cs.have(2)) return td::Status::Error("state_init: tick_tock underflow");
    cs.advance(2);  // TickTock: tick:Bool tock:Bool
  }
  for (int i = 0; i < 3; i++) {  // code, data, library
    if (!cs.have(1)) return td::Status::Error("state_init: maybe-ref underflow");
    if (cs.fetch_ulong(1)) {
      if (cs.size_refs() == 0) return td::Status::Error("state_init: ref missing");
      cs.fetch_ref();
    }
  }
  return td::Status::OK();
}

td::Result<td::Ref<vm::Cell>> slice_to_cell(const vm::CellSlice &cs) {
  try {
    vm::CellBuilder cb;
    cb.store_bits(cs.data_bits(), cs.size());
    for (unsigned i = 0; i < cs.size_refs(); i++) {
      cb.store_ref(cs.prefetch_ref(i));
    }
    return cb.finalize();
  } catch (...) {
    return td::Status::Error("to_cell failed");
  }
}

td::Result<td::Ref<vm::Cell>> message_any_body(vm::CellSlice &cs) {
  if (!cs.have(1)) return td::Status::Error("message: init maybe underflow");
  if (cs.fetch_ulong(1)) {
    if (!cs.have(1)) return td::Status::Error("message: init either underflow");
    if (cs.fetch_ulong(1)) {  // right: ^StateInit
      if (cs.size_refs() == 0) return td::Status::Error("message: init ref missing");
      TRY_RESULT(inner, open_ref_cell(cs.fetch_ref()));
      TRY_STATUS(skip_state_init_py(inner));
    } else {  // left: inline
      TRY_STATUS(skip_state_init_py(cs));
    }
  }
  if (!cs.have(1)) return td::Status::Error("message: body either underflow");
  if (cs.fetch_ulong(1)) {  // right: ^X
    if (cs.size_refs() == 0) return td::Status::Error("message: body ref missing");
    return cs.fetch_ref();
  }
  return slice_to_cell(cs);  // left: the rest of the message cell
}

namespace {

// Python bytes.decode("utf-8", errors="backslashreplace") + .replace("\u0000", "").
std::string decode_backslashreplace_strip_nul(const std::string &raw) {
  std::string out;
  size_t i = 0;
  auto bad = [&out](unsigned char c) {
    static const char *hex = "0123456789abcdef";
    out += "\\x";
    out += hex[c >> 4];
    out += hex[c & 0xF];
  };
  while (i < raw.size()) {
    unsigned char c = static_cast<unsigned char>(raw[i]);
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
      bad(c);
      i++;
      continue;
    }
    bool ok = i + n < raw.size();
    for (int k = 1; ok && k <= n; k++) {
      if ((static_cast<unsigned char>(raw[i + k]) & 0xC0) != 0x80) {
        ok = false;
      }
    }
    if (ok && n == 2) {
      unsigned char c1 = static_cast<unsigned char>(raw[i + 1]);
      if ((c == 0xE0 && c1 < 0xA0) || (c == 0xED && c1 >= 0xA0)) {
        ok = false;
      }
    }
    if (ok && n == 3) {
      unsigned char c1 = static_cast<unsigned char>(raw[i + 1]);
      if ((c == 0xF0 && c1 < 0x90) || (c == 0xF4 && c1 >= 0x90)) {
        ok = false;
      }
    }
    if (!ok) {
      bad(c);
      i++;
      continue;
    }
    if (c == 0) {
      i++;  // U+0000 stripped
      continue;
    }
    out.append(raw, i, static_cast<size_t>(n) + 1);
    i += static_cast<size_t>(n) + 1;
  }
  return out;
}

}  // namespace

std::optional<std::string> ton_transfer_comment(const td::Ref<vm::Cell> &body) {
  auto r_ctx = open_body(body);
  if (r_ctx.is_error()) {
    return std::nullopt;
  }
  auto ctx = r_ctx.move_as_ok();
  auto &cs = ctx.cs;
  if (cs.size() < 32) {
    return std::nullopt;  // TonTransferMessage: comment None
  }
  auto op = static_cast<td::uint32>(cs.fetch_ulong(32));
  bool encrypted = op == 0x2167da4b;
  if (!(cs.size() >= 8 && cs.size() % 8 == 0 &&
        (cs.size_refs() == 0 || cs.size_refs() == 1))) {
    return std::nullopt;
  }
  PSlice ps;
  ps.cs = cs;
  for (unsigned i = 0; i < cs.size_refs(); i++) {
    ps.refs.push_back(cs.prefetch_ref(i));
  }
  auto r = load_snake_bytes(ps);
  if (r.is_error()) {
    return std::nullopt;  // Python's inner try swallows
  }
  std::string bytes = r.move_as_ok();
  if (encrypted) {
    return td::base64_encode(td::Slice(bytes));
  }
  return decode_backslashreplace_strip_nul(bytes);
}

// Public wrapper used when jetton-transfer and NFT comment fields carry
// RAW payload bytes that the fill decodes with the same codec as ton_transfer).
std::string decode_comment_bytes(const std::string &raw) {
  return decode_backslashreplace_strip_nul(raw);
}

}  // namespace mch
