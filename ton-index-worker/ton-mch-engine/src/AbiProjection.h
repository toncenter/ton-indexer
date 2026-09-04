// Project generated ABI values into the mch::Value shapes used by classifiers.
#pragma once

#include "Value.h"

#include "AbiGenSupport.h"

#include "common/bitstring.h"
#include "common/refint.h"
#include "td/utils/int_types.h"
#include "vm/cells/CellSlice.h"

#include <string>
#include <utility>

namespace mch {

inline Value enum_name(const ton_abi::gen::EnumNameTable &table, const td::RefInt256 &value) {
  auto name = ton_abi::gen::enum_name_lookup(table, value);
  return name ? Value::make_str(std::move(*name)) : Value::null();
}

inline Value minimal_hex(const td::RefInt256 &value) {
  if (value.is_null()) {
    return Value::null();
  }
  return Value::make_str("0x" + td::hex_string(value, /*upcase=*/false, /*zero_pad=*/0));
}

inline Value minimal_hex(td::uint64 value) {
  return minimal_hex(td::make_refint(value));
}

// Hex-encode only the root cell's bits, zero-padding the final partial byte.
inline Value root_bits_hex(const vm::CellSlice &slice) {
  const unsigned bit_count = slice.size();
  const std::size_t byte_count = (bit_count + 7) / 8;
  std::string bytes(byte_count, '\0');
  if (bit_count != 0) {
    td::BitPtr(reinterpret_cast<unsigned char *>(bytes.data())).copy_from(slice.data_bits(), bit_count);
  }

  static constexpr char kHexDigits[] = "0123456789abcdef";
  std::string result = "0x";
  result.reserve(2 + byte_count * 2);
  for (unsigned char byte : bytes) {
    result += kHexDigits[byte >> 4];
    result += kHexDigits[byte & 0x0f];
  }
  return Value::make_str(std::move(result));
}

}  // namespace mch
