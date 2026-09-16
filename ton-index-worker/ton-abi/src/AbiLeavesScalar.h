#pragma once

// Fail-closed scalar load/store primitives matching @ton/core wire semantics.
// Integer values use RefInt256 except for the explicit native-width overloads;
// bitsN returns exactly n bits and no refs.

#include "common/refint.h"
#include "td/utils/Status.h"

#include "vm/cellslice.h"
#include "vm/cells/CellBuilder.h"

namespace ton_abi {

// Size-prefix bit width for varintN/varuintN: log2(n). Fails closed if n is not
// a positive power of two.
td::Result<int> var_prefix_bits(int n);

// intN (n in [1,257]): two's-complement signed, n bits.
td::Result<td::RefInt256> load_int(vm::CellSlice &cs, int n);
td::Status store_int(vm::CellBuilder &cb, const td::RefInt256 &v, int n);

// uintN (n in [1,256]): unsigned, n bits.
td::Result<td::RefInt256> load_uint(vm::CellSlice &cs, int n);
td::Status store_uint(vm::CellBuilder &cb, const td::RefInt256 &v, int n);

// Native fast-path overloads for intN/uintN with n in [1,64]: read/write via
// vm's native fetch_long/store_long (no bignum allocation), bit-for-bit
// identical to the RefInt256 path above (same wire, same range semantics, same
// error-text shape). The emitter (ton-abi-gen) maps intN/uintN with n<=64 to
// td::int64 / td::uint64 struct members and calls these; n>64, varintN,
// varuintN and coins keep the RefInt256 path.
td::Result<td::int64> load_int64(vm::CellSlice &cs, int n);
td::Status store_int64(vm::CellBuilder &cb, td::int64 v, int n);
td::Result<td::uint64> load_uint64(vm::CellSlice &cs, int n);
td::Status store_uint64(vm::CellBuilder &cb, td::uint64 v, int n);

// bool: a single bit.
td::Result<bool> load_bool(vm::CellSlice &cs);
td::Status store_bool(vm::CellBuilder &cb, bool v);

// coins: varuint with a 4-bit size prefix (@ton/core loadCoins/storeCoins).
td::Result<td::RefInt256> load_coins(vm::CellSlice &cs);
td::Status store_coins(vm::CellBuilder &cb, const td::RefInt256 &v);

// varintN / varuintN: log2(n)-bit byte-count prefix, then that many bytes,
// signed / unsigned respectively.
td::Result<td::RefInt256> load_varint(vm::CellSlice &cs, int n);
td::Status store_varint(vm::CellBuilder &cb, const td::RefInt256 &v, int n);
td::Result<td::RefInt256> load_varuint(vm::CellSlice &cs, int n);
td::Status store_varuint(vm::CellBuilder &cb, const td::RefInt256 &v, int n);

// bitsN (n in [0,1023]): exactly n bits, 0 refs.
td::Result<td::Ref<vm::CellSlice>> load_bits(vm::CellSlice &cs, int n);
td::Status store_bits(vm::CellBuilder &cb, const vm::CellSlice &v, int n);

}  // namespace ton_abi
