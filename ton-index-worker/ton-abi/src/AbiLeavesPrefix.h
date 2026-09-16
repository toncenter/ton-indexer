#pragma once

// Fail-closed struct-prefix checks and the non-consuming probe used by generated
// union dispatch. Dispatch itself is baked into generated code. Prefix numbers
// use uint64; widths above 64 require zero high bits.

#include "td/utils/Status.h"

#include <cstdint>

#include "vm/cellslice.h"
#include "vm/cells/CellBuilder.h"

namespace ton_abi {

// Read `len` bits and require they equal `num` (the struct-opcode check). len==0
// always matches. Fail-closed on truncation or mismatch.
td::Status load_and_check_prefix(vm::CellSlice &cs, std::uint64_t num, int len);

// Write `len` bits of `num`.
td::Status store_prefix(vm::CellBuilder &cb, std::uint64_t num, int len);

// PEEK (no consume): true iff `len` bits are available and equal `num`.
bool lookup_prefix(const vm::CellSlice &cs, std::uint64_t num, int len);

}  // namespace ton_abi
