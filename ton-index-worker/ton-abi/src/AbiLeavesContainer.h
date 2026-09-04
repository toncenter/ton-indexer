#pragma once

// Structural walkers for the @ton/core arrayOf and lispListOf wire formats.
// Element recursion belongs to the supplied callback.

#include "td/utils/Status.h"

#include "vm/cellslice.h"
#include "vm/cells/CellBuilder.h"

#include <cstddef>
#include <functional>

namespace ton_abi {

// arrayOf. `unpack_one(elem_slice)` must consume exactly one element from the
// slice (advancing it). Returns error on count mismatch or malformed chain.
td::Status load_array(vm::CellSlice &cs,
                      const std::function<td::Status(vm::CellSlice &)> &unpack_one);

// `pack_one(builder, idx)` packs element `idx` into the given (fresh chunk)
// builder. STORE writes 1 element per ref cell.
td::Status store_array(vm::CellBuilder &cb, std::size_t count,
                       const std::function<td::Status(vm::CellBuilder &, std::size_t)> &pack_one);

td::Status load_lisp_list(vm::CellSlice &cs,
                          const std::function<td::Status(vm::CellSlice &)> &unpack_one);
td::Status store_lisp_list(vm::CellBuilder &cb, std::size_t count,
                           const std::function<td::Status(vm::CellBuilder &, std::size_t)> &pack_one);

}  // namespace ton_abi
