#pragma once

// HashmapE load/store adapter for fixed-width intN, uintN, and address keys.
// Key and value recursion belongs to the callbacks; iteration preserves raw
// key-bit wire order.

#include "td/utils/Status.h"

#include "vm/cellslice.h"
#include "vm/cells/CellBuilder.h"

#include <cstddef>
#include <functional>

namespace ton_abi {

// mapKV load. Parses the HashmapE from `cs` (advancing past the maybe-bit +
// optional ref), then calls `on_entry(key_slice, value_slice)` per entry in
// ascending key order. An absent dict (maybe bit 0) yields no calls.
td::Status load_dict(vm::CellSlice &cs, int key_bits,
                     const std::function<td::Status(vm::CellSlice &, vm::CellSlice &)> &on_entry);

// mapKV store. `emit(idx, key_out, val_out)` must write EXACTLY key_bits into
// key_out and the value body into val_out for entry idx. Serializes as HashmapE
// (maybe bit + ref). Duplicate keys are rejected.
td::Status store_dict(vm::CellBuilder &cb, int key_bits, std::size_t count,
                      const std::function<td::Status(std::size_t, vm::CellBuilder &, vm::CellBuilder &)> &emit);

}  // namespace ton_abi
