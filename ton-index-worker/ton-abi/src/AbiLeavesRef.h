#pragma once

// Fail-closed primitives for refs, nullable prefixes, snake strings, and
// remaining slices. Inner-type recursion belongs to the caller.

#include "td/utils/Status.h"

#include "vm/cellslice.h"
#include "vm/cells/CellBuilder.h"

#include <string>
#include <utility>

namespace ton_abi {

// Open a ref as an ordinary slice, fail-closed on special/exotic cells (the
// reference assumes none; the top-level body open also rejects exotic cells).
// `who` prefixes the error messages ("ref", "container", ...).
inline td::Result<vm::CellSlice> open_ordinary(const td::Ref<vm::Cell> &cell, const char *who) {
  if (cell.is_null()) {
    return td::Status::Error(PSLICE() << who << ": null cell");
  }
  bool is_special = false;
  vm::CellSlice cs = vm::load_cell_slice_special(cell, is_special);
  if (is_special) {
    return td::Status::Error(PSLICE() << who << ": special/exotic cell not supported");
  }
  return cs;
}

// cell: an opaque whole cell, stored/loaded as a ref (no inner parse).
td::Result<td::Ref<vm::Cell>> load_cell(vm::CellSlice &cs);
td::Status store_cell(vm::CellBuilder &cb, td::Ref<vm::Cell> cell);  // also used for cellOf store

// Load a Cell<T> as both its raw child cell and an ordinary slice for decoding.
// Exotic children fail closed; storing uses store_cell().
td::Result<std::pair<td::Ref<vm::Cell>, td::Ref<vm::CellSlice>>> load_ref_cell_and_slice(vm::CellSlice &cs);

// maybe_ref: 1-bit presence, then an optional ref. Absent -> null Ref.
td::Result<td::Ref<vm::Cell>> load_maybe_ref(vm::CellSlice &cs);
td::Status store_maybe_ref(vm::CellBuilder &cb, td::Ref<vm::Cell> maybe_cell);

// nullable presence prefix: the single 1-bit maybe flag (inner recursion is the
// walker's). true = value present.
td::Result<bool> load_maybe_prefix(vm::CellSlice &cs);
td::Status store_maybe_prefix(vm::CellBuilder &cb, bool present);

// string: snake ref-tail. Returns raw bytes; JSON dumping performs UTF-8 decode.
td::Result<std::string> load_string(vm::CellSlice &cs);
td::Status store_string(vm::CellBuilder &cb, const std::string &s);

// remaining (RemainingBitsAndRefs): snapshot the rest of the slice -- ALL
// remaining bits and refs -- draining the source. Store appends bits + refs.
td::Result<td::Ref<vm::CellSlice>> load_remaining(vm::CellSlice &cs);
td::Status store_remaining(vm::CellBuilder &cb, const vm::CellSlice &v);

}  // namespace ton_abi
