#include "AbiLeavesScalar.h"

namespace ton_abi {

namespace {

// Shared var* reader: read a `prefix_bits`-wide byte count, then that many
// bytes as a signed/unsigned integer.
td::Result<td::RefInt256> load_var_raw(vm::CellSlice &cs, int prefix_bits, bool value_signed) {
  unsigned long long size = 0;
  if (prefix_bits > 0) {
    if (!cs.fetch_ulong_bool(static_cast<unsigned>(prefix_bits), size)) {
      return td::Status::Error("var-int: not enough bits for size prefix");
    }
  }
  unsigned bits = static_cast<unsigned>(size) * 8u;
  if (bits == 0) {
    return td::zero_refint();
  }
  if (!cs.have(bits)) {
    return td::Status::Error(PSLICE() << "var-int: not enough bits for " << size << "-byte value");
  }
  td::RefInt256 v = cs.fetch_int256(bits, value_signed);
  if (v.is_null()) {
    return td::Status::Error("var-int: failed to fetch value");
  }
  return v;
}

// Shared var* writer. Size prefix measures BYTES; unsigned uses ceil(bitlen/8),
// signed uses ceil((bitlen(|v|)+1)/8) (the +1 is the sign bit, per @ton/core).
td::Status store_var_raw(vm::CellBuilder &cb, const td::RefInt256 &v, int prefix_bits, bool value_signed) {
  if (v.is_null()) {
    return td::Status::Error("var-int: null value");
  }
  int sign = v->sgn();
  if (!value_signed && sign < 0) {
    return td::Status::Error("varuint: value is negative");
  }
  if (sign == 0) {
    // zero -> zero-size prefix
    if (!cb.store_long_bool(0, static_cast<unsigned>(prefix_bits))) {
      return td::Status::Error("var-int: cannot store zero size prefix");
    }
    return td::Status::OK();
  }
  int magnitude_bits;
  if (value_signed) {
    td::RefInt256 absv = sign < 0 ? -v : v;
    magnitude_bits = absv->bit_size(false) + 1;  // +1 sign bit, per @ton/core
  } else {
    magnitude_bits = v->bit_size(false);
  }
  int size_bytes = (magnitude_bits + 7) / 8;
  // The byte count must be representable in the prefix width.
  if (prefix_bits < 31 && static_cast<long long>(size_bytes) >= (1LL << prefix_bits)) {
    return td::Status::Error(PSLICE() << "var-int: " << size_bytes << " bytes does not fit in a "
                                       << prefix_bits << "-bit size prefix");
  }
  if (!cb.store_long_bool(static_cast<long long>(size_bytes), static_cast<unsigned>(prefix_bits))) {
    return td::Status::Error("var-int: cannot store size prefix");
  }
  if (!cb.store_int256_bool(v, static_cast<unsigned>(size_bytes) * 8u, value_signed)) {
    return td::Status::Error("var-int: cannot store value");
  }
  return td::Status::OK();
}

}  // namespace

td::Result<int> var_prefix_bits(int n) {
  if (n <= 0) {
    return td::Status::Error(PSLICE() << "varintN width must be positive, got " << n);
  }
  for (int k = 0; k < 31; ++k) {
    if ((1 << k) == n) {
      return k;
    }
  }
  return td::Status::Error(PSLICE() << "varintN width must be a power of two, got " << n);
}


td::Result<td::RefInt256> load_int(vm::CellSlice &cs, int n) {
  if (n < 1 || n > 257) {
    return td::Status::Error(PSLICE() << "intN: width " << n << " out of [1,257]");
  }
  if (!cs.have(static_cast<unsigned>(n))) {
    return td::Status::Error(PSLICE() << "intN: not enough bits for int" << n);
  }
  td::RefInt256 v = cs.fetch_int256(static_cast<unsigned>(n), true);
  if (v.is_null()) {
    return td::Status::Error(PSLICE() << "intN: failed to fetch int" << n);
  }
  return v;
}

td::Status store_int(vm::CellBuilder &cb, const td::RefInt256 &v, int n) {
  if (n < 1 || n > 257) {
    return td::Status::Error(PSLICE() << "intN: width " << n << " out of [1,257]");
  }
  if (v.is_null()) {
    return td::Status::Error("intN: null value");
  }
  if (!cb.store_int256_bool(v, static_cast<unsigned>(n), true)) {
    return td::Status::Error(PSLICE() << "intN: value out of range for int" << n << " (or cell overflow)");
  }
  return td::Status::OK();
}

td::Result<td::RefInt256> load_uint(vm::CellSlice &cs, int n) {
  if (n < 1 || n > 256) {
    return td::Status::Error(PSLICE() << "uintN: width " << n << " out of [1,256]");
  }
  if (!cs.have(static_cast<unsigned>(n))) {
    return td::Status::Error(PSLICE() << "uintN: not enough bits for uint" << n);
  }
  td::RefInt256 v = cs.fetch_int256(static_cast<unsigned>(n), false);
  if (v.is_null()) {
    return td::Status::Error(PSLICE() << "uintN: failed to fetch uint" << n);
  }
  return v;
}

td::Status store_uint(vm::CellBuilder &cb, const td::RefInt256 &v, int n) {
  if (n < 1 || n > 256) {
    return td::Status::Error(PSLICE() << "uintN: width " << n << " out of [1,256]");
  }
  if (v.is_null()) {
    return td::Status::Error("uintN: null value");
  }
  if (!cb.store_int256_bool(v, static_cast<unsigned>(n), false)) {
    return td::Status::Error(PSLICE() << "uintN: value out of range for uint" << n << " (or cell overflow)");
  }
  return td::Status::OK();
}

// Bit-for-bit identical to load_int/store_int/load_uint/store_uint above, but
// over long long / unsigned long long via vm's native fetch_long/store_long --
// no RefInt256 allocation. Range + error-text semantics mirror the RefInt256
// path (store_long_rchk_bool / store_ulong_rchk_bool are the signed/unsigned
// range-checked stores; bits==1 signed accepts only {-1,0}).

td::Result<td::int64> load_int64(vm::CellSlice &cs, int n) {
  if (n < 1 || n > 64) {
    return td::Status::Error(PSLICE() << "intN: width " << n << " out of [1,64]");
  }
  if (!cs.have(static_cast<unsigned>(n))) {
    return td::Status::Error(PSLICE() << "intN: not enough bits for int" << n);
  }
  long long v = 0;
  if (!cs.fetch_long_bool(static_cast<unsigned>(n), v)) {
    return td::Status::Error(PSLICE() << "intN: failed to fetch int" << n);
  }
  return static_cast<td::int64>(v);
}

td::Status store_int64(vm::CellBuilder &cb, td::int64 v, int n) {
  if (n < 1 || n > 64) {
    return td::Status::Error(PSLICE() << "intN: width " << n << " out of [1,64]");
  }
  if (!cb.store_long_rchk_bool(static_cast<long long>(v), static_cast<unsigned>(n))) {
    return td::Status::Error(PSLICE() << "intN: value out of range for int" << n << " (or cell overflow)");
  }
  return td::Status::OK();
}

td::Result<td::uint64> load_uint64(vm::CellSlice &cs, int n) {
  if (n < 1 || n > 64) {
    return td::Status::Error(PSLICE() << "uintN: width " << n << " out of [1,64]");
  }
  if (!cs.have(static_cast<unsigned>(n))) {
    return td::Status::Error(PSLICE() << "uintN: not enough bits for uint" << n);
  }
  unsigned long long v = 0;
  if (!cs.fetch_ulong_bool(static_cast<unsigned>(n), v)) {
    return td::Status::Error(PSLICE() << "uintN: failed to fetch uint" << n);
  }
  return static_cast<td::uint64>(v);
}

td::Status store_uint64(vm::CellBuilder &cb, td::uint64 v, int n) {
  if (n < 1 || n > 64) {
    return td::Status::Error(PSLICE() << "uintN: width " << n << " out of [1,64]");
  }
  if (!cb.store_ulong_rchk_bool(static_cast<unsigned long long>(v), static_cast<unsigned>(n))) {
    return td::Status::Error(PSLICE() << "uintN: value out of range for uint" << n << " (or cell overflow)");
  }
  return td::Status::OK();
}


td::Result<bool> load_bool(vm::CellSlice &cs) {
  if (!cs.have(1)) {
    return td::Status::Error("bool: not enough bits");
  }
  unsigned long long b = 0;
  if (!cs.fetch_ulong_bool(1, b)) {
    return td::Status::Error("bool: failed to fetch bit");
  }
  return b != 0;
}

td::Status store_bool(vm::CellBuilder &cb, bool v) {
  if (!cb.store_long_bool(v ? 1 : 0, 1)) {
    return td::Status::Error("bool: cannot store bit");
  }
  return td::Status::OK();
}


td::Result<td::RefInt256> load_coins(vm::CellSlice &cs) {
  return load_var_raw(cs, 4, false);
}

td::Status store_coins(vm::CellBuilder &cb, const td::RefInt256 &v) {
  return store_var_raw(cb, v, 4, false);
}

td::Result<td::RefInt256> load_varint(vm::CellSlice &cs, int n) {
  TRY_RESULT(prefix, var_prefix_bits(n));
  return load_var_raw(cs, prefix, true);
}

td::Status store_varint(vm::CellBuilder &cb, const td::RefInt256 &v, int n) {
  TRY_RESULT(prefix, var_prefix_bits(n));
  return store_var_raw(cb, v, prefix, true);
}

td::Result<td::RefInt256> load_varuint(vm::CellSlice &cs, int n) {
  TRY_RESULT(prefix, var_prefix_bits(n));
  return load_var_raw(cs, prefix, false);
}

td::Status store_varuint(vm::CellBuilder &cb, const td::RefInt256 &v, int n) {
  TRY_RESULT(prefix, var_prefix_bits(n));
  return store_var_raw(cb, v, prefix, false);
}


td::Result<td::Ref<vm::CellSlice>> load_bits(vm::CellSlice &cs, int n) {
  if (n < 0 || n > 1023) {
    return td::Status::Error(PSLICE() << "bitsN: width " << n << " out of [0,1023]");
  }
  if (!cs.have(static_cast<unsigned>(n))) {
    return td::Status::Error(PSLICE() << "bitsN: not enough bits for bits" << n);
  }
  td::Ref<vm::CellSlice> r = cs.fetch_subslice(static_cast<unsigned>(n), 0);
  if (r.is_null()) {
    return td::Status::Error(PSLICE() << "bitsN: failed to fetch bits" << n);
  }
  return r;
}

td::Status store_bits(vm::CellBuilder &cb, const vm::CellSlice &v, int n) {
  if (n < 0 || n > 1023) {
    return td::Status::Error(PSLICE() << "bitsN: width " << n << " out of [0,1023]");
  }
  if (v.size() != static_cast<unsigned>(n) || v.size_refs() != 0) {
    return td::Status::Error(PSLICE() << "bitsN: expected exactly " << n << " bits and 0 refs, got "
                                       << v.size() << " bits and " << v.size_refs() << " refs");
  }
  if (!cb.store_bits_bool(v.data_bits(), static_cast<unsigned>(n))) {
    return td::Status::Error(PSLICE() << "bitsN: cannot store bits" << n);
  }
  return td::Status::OK();
}

}  // namespace ton_abi
