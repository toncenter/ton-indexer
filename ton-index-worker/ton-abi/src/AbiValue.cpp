#include "AbiValue.h"

#include "common/bitstring.h"

#include "td/utils/base64.h"

#include "vm/boc.h"

#include <cctype>

namespace ton_abi {

namespace {

std::string boc_base64(const td::Ref<vm::Cell> &root) {
  auto r = vm::std_boc_serialize(root, 2);  // Mode::WithCRC32C, matches @ton/core toBoc() default
  if (r.is_error()) {
    return {};  // unreachable for a well-formed cell tree; dump never fails loudly
  }
  return td::base64_encode(r.move_as_ok().as_slice());
}

std::string lower(std::string s) {
  for (char &c : s) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return s;
}

// Lossy UTF-8 decode: invalid first continuation bytes are reprocessed;
// after a valid continuation, a later failure replaces the valid prefix
// and leaves the failing byte for the next iteration.
std::string decode_utf8_lossy(const std::string &bytes) {
  std::string out;
  const std::size_t n = bytes.size();
  std::size_t i = 0;
  auto byte_at = [&](std::size_t idx) { return static_cast<unsigned char>(bytes[idx]); };
  auto is_cont = [&](std::size_t idx) { return idx < n && (byte_at(idx) & 0xC0) == 0x80; };
  auto replacement = [&]() { out += "\xEF\xBF\xBD"; };  // U+FFFD in UTF-8

  while (i < n) {
    unsigned char b0 = byte_at(i);
    if (b0 < 0x80) {
      out += static_cast<char>(b0);
      i += 1;
    } else if (b0 < 0xC2) {
      // continuation byte with no lead, or an always-invalid overlong lead (C0/C1)
      replacement();
      i += 1;
    } else if (b0 < 0xE0) {
      if (is_cont(i + 1)) {
        out.append(bytes, i, 2);
        i += 2;
      } else {
        replacement();
        i += 1;
      }
    } else if (b0 < 0xF0) {
      unsigned char lo = 0x80, hi = 0xBF;
      if (b0 == 0xE0) lo = 0xA0;        // exclude overlong 3-byte encodings
      else if (b0 == 0xED) hi = 0x9F;   // exclude UTF-16 surrogate range D800-DFFF
      unsigned char b1 = (i + 1 < n) ? byte_at(i + 1) : 0;
      if (i + 1 < n && b1 >= lo && b1 <= hi) {
        if (is_cont(i + 2)) {
          out.append(bytes, i, 3);
          i += 3;
        } else {
          replacement();
          i += 2;  // b0,b1 validated as a unit; the failing byte is reprocessed next
        }
      } else {
        replacement();
        i += 1;  // b1 invalid (or absent) for this lead; reprocess it on its own
      }
    } else if (b0 < 0xF5) {
      unsigned char lo = 0x80, hi = 0xBF;
      if (b0 == 0xF0) lo = 0x90;        // exclude overlong 4-byte encodings
      else if (b0 == 0xF4) hi = 0x8F;   // cap at U+10FFFF
      unsigned char b1 = (i + 1 < n) ? byte_at(i + 1) : 0;
      if (i + 1 < n && b1 >= lo && b1 <= hi) {
        if (is_cont(i + 2)) {
          if (is_cont(i + 3)) {
            out.append(bytes, i, 4);
            i += 4;
          } else {
            replacement();
            i += 3;
          }
        } else {
          replacement();
          i += 2;
        }
      } else {
        replacement();
        i += 1;
      }
    } else {
      // 0xF5-0xFF: always-invalid lead (beyond Unicode's 4-byte range)
      replacement();
      i += 1;
    }
  }
  return out;
}

void dump_json_string(const std::string &s, std::string &out) {
  out += '"';
  for (unsigned char c : s) {
    switch (c) {
      case '"':
        out += "\\\"";
        break;
      case '\\':
        out += "\\\\";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        if (c < 0x20) {
          static const char *hex = "0123456789abcdef";
          out += "\\u00";
          out += hex[(c >> 4) & 0xF];
          out += hex[c & 0xF];
        } else {
          out += static_cast<char>(c);
        }
    }
  }
  out += '"';
}

}  // namespace

AbiValue AbiValue::make_int(td::RefInt256 v) {
  AbiValue r;
  r.kind = AbiValueKind::Int;
  r.int_v = std::move(v);
  return r;
}

AbiValue AbiValue::make_bool(bool v) {
  AbiValue r;
  r.kind = AbiValueKind::Bool;
  r.bool_v = v;
  return r;
}

AbiValue AbiValue::make_address(AbiAddress a) {
  AbiValue r;
  r.kind = AbiValueKind::Address;
  r.address_v = std::move(a);
  return r;
}

AbiValue AbiValue::make_cell(td::Ref<vm::Cell> c) {
  AbiValue r;
  r.kind = AbiValueKind::Cell;
  r.cell_v = std::move(c);
  return r;
}

AbiValue AbiValue::make_cell_of(AbiValue inner_value, td::Ref<vm::Cell> raw_cell) {
  AbiValue r;
  r.kind = AbiValueKind::CellOf;
  r.inner = std::make_unique<AbiValue>(std::move(inner_value));
  r.cell_v = std::move(raw_cell);
  return r;
}

AbiValue AbiValue::make_bits(td::Ref<vm::CellSlice> s) {
  AbiValue r;
  r.kind = AbiValueKind::Bits;
  r.bits_v = std::move(s);
  return r;
}

AbiValue AbiValue::make_string(std::string s) {
  AbiValue r;
  r.kind = AbiValueKind::String;
  r.string_v = std::move(s);
  return r;
}

AbiValue AbiValue::make_list(std::vector<AbiValue> items) {
  AbiValue r;
  r.kind = AbiValueKind::List;
  r.list_v = std::move(items);
  return r;
}

AbiValue AbiValue::make_struct(std::string name) {
  AbiValue r;
  r.kind = AbiValueKind::Struct;
  r.struct_name = std::move(name);
  return r;
}

AbiValue AbiValue::make_union(std::string label, AbiValue inner_value) {
  AbiValue r;
  r.kind = AbiValueKind::Union;
  r.union_label = std::move(label);
  r.inner = std::make_unique<AbiValue>(std::move(inner_value));
  return r;
}

AbiValue AbiValue::make_void() {
  AbiValue r;
  r.kind = AbiValueKind::Void;
  return r;
}

AbiValue AbiValue::make_null() {
  AbiValue r;
  r.kind = AbiValueKind::Null;
  return r;
}

AbiValue AbiValue::make_map(std::vector<std::pair<AbiValue, AbiValue>> entries) {
  AbiValue r;
  r.kind = AbiValueKind::Map;
  r.map_entries = std::move(entries);
  return r;
}

void AbiValue::add_field(std::string name, AbiValue v) {
  struct_fields.emplace_back(std::move(name), std::move(v));
}

std::string AbiValue::to_json() const {
  std::string out;
  dump_to(out);
  return out;
}

void AbiValue::dump_to(std::string &out) const {
  switch (kind) {
    case AbiValueKind::Int:
      dump_json_string(int_v->to_dec_string(), out);
      break;

    case AbiValueKind::Bool:
      out += bool_v ? "true" : "false";
      break;

    case AbiValueKind::Null:
      out += "null";
      break;

    case AbiValueKind::Void:
      out += R"({"$":"void"})";
      break;

    case AbiValueKind::Address:
      switch (address_v.kind) {
        case AbiAddressKind::None:
          out += "\"none\"";
          break;
        case AbiAddressKind::Std:
          out += '"';
          out += std::to_string(address_v.workchain);
          out += ':';
          out += lower(address_v.hash.to_hex());
          out += '"';
          break;
        case AbiAddressKind::Extern: {
          std::string hex = address_v.ext_bits > 0
                                 ? td::bitstring::bits_to_hex(address_v.ext_value->data_bits(), address_v.ext_bits)
                                 : std::string();
          out += R"({"extern":{"bits":)";
          out += std::to_string(address_v.ext_bits);
          out += R"(,"value":)";
          dump_json_string(lower(hex), out);
          out += "}}";
          break;
        }
      }
      break;

    case AbiValueKind::Cell:
      dump_json_string(boc_base64(cell_v), out);
      break;

    case AbiValueKind::CellOf:
      out += R"({"ref":)";
      inner->dump_to(out);
      out += '}';
      break;

    case AbiValueKind::Bits: {
      std::string bits = td::bitstring::bits_to_binary(bits_v->data_bits(), bits_v->size());
      out += R"({"bits":)";
      dump_json_string(bits, out);
      out += R"(,"refs":[)";
      for (unsigned i = 0; i < bits_v->size_refs(); ++i) {
        if (i) {
          out += ',';
        }
        dump_json_string(boc_base64(bits_v->prefetch_ref(i)), out);
      }
      out += "]}";
      break;
    }

    case AbiValueKind::String:
      // string_v holds RAW bytes (pack round-trips them byte-for-byte via
      // store_string); lossy UTF-8 decoding is a dump-only transform.
      dump_json_string(decode_utf8_lossy(string_v), out);
      break;

    case AbiValueKind::List:
      out += '[';
      for (std::size_t i = 0; i < list_v.size(); ++i) {
        if (i) {
          out += ',';
        }
        list_v[i].dump_to(out);
      }
      out += ']';
      break;

    case AbiValueKind::Struct:
      out += R"({"$":)";
      dump_json_string(struct_name, out);
      for (const auto &f : struct_fields) {
        out += ',';
        dump_json_string(f.first, out);
        out += ':';
        f.second.dump_to(out);
      }
      out += '}';
      break;

    case AbiValueKind::Union:
      out += R"({"$":)";
      dump_json_string(union_label, out);
      out += R"(,"value":)";
      inner->dump_to(out);
      out += '}';
      break;

    case AbiValueKind::Map:
      out += '[';
      for (std::size_t i = 0; i < map_entries.size(); ++i) {
        if (i) {
          out += ',';
        }
        out += '[';
        map_entries[i].first.dump_to(out);
        out += ',';
        map_entries[i].second.dump_to(out);
        out += ']';
      }
      out += ']';
      break;
  }
}

}  // namespace ton_abi
