#include "AbiLeavesRef.h"

#include "vm/cells/CellSlice.h"

#include <algorithm>

namespace ton_abi {

namespace {

// Open a ref as an ordinary slice, fail-closed on special/exotic cells (the
// reference assumes none; the top-level body open also rejects exotic cells).
td::Result<vm::CellSlice> open_ordinary(const td::Ref<vm::Cell> &cell) {
  if (cell.is_null()) {
    return td::Status::Error("ref: null cell");
  }
  bool is_special = false;
  vm::CellSlice cs = vm::load_cell_slice_special(cell, is_special);
  if (is_special) {
    return td::Status::Error("ref: special/exotic cell not supported");
  }
  return cs;
}

// Snake reader: byte-aligned bytes per cell + at most one continuation ref
// (@ton/core strings.js readBuffer).
td::Result<std::string> read_snake(td::Ref<vm::Cell> cell) {
  std::string out;
  for (;;) {
    TRY_RESULT(cs, open_ordinary(cell));
    if (cs.size() % 8 != 0) {
      return td::Status::Error(PSLICE() << "string: invalid length, " << cs.size() << " bits not byte-aligned");
    }
    if (cs.size_refs() > 1) {
      return td::Status::Error(PSLICE() << "string: invalid number of refs: " << cs.size_refs());
    }
    unsigned nbytes = cs.size() / 8;
    if (nbytes > 0) {
      std::string chunk(nbytes, '\0');
      if (!cs.fetch_bits_to(td::BitPtr(reinterpret_cast<unsigned char *>(&chunk[0])), nbytes * 8)) {
        return td::Status::Error("string: failed to read bytes");
      }
      out += chunk;
    }
    if (cs.size_refs() == 1) {
      cell = cs.fetch_ref();
      if (cell.is_null()) {
        return td::Status::Error("string: failed to follow snake tail ref");
      }
      continue;
    }
    return out;
  }
}

// Snake writer: chunk at 127 bytes/cell, tail in a continuation ref
// (@ton/core strings.js writeBuffer over a fresh 1023-bit builder).
td::Result<td::Ref<vm::Cell>> string_to_cell(td::Slice bytes) {
  vm::CellBuilder b;
  std::size_t n = bytes.size();
  std::size_t head = std::min<std::size_t>(n, 127);
  if (head > 0) {
    if (!b.store_bytes_bool(reinterpret_cast<const unsigned char *>(bytes.data()), head)) {
      return td::Status::Error("string: cannot store snake head bytes");
    }
  }
  if (n > head) {
    TRY_RESULT(tail, string_to_cell(bytes.substr(head)));
    if (!b.store_ref_bool(tail)) {
      return td::Status::Error("string: cannot store snake tail ref");
    }
  }
  return td::Ref<vm::Cell>(b.finalize());
}

}  // namespace

// ---- cell ----

td::Result<td::Ref<vm::Cell>> load_cell(vm::CellSlice &cs) {
  if (!cs.have_refs(1)) {
    return td::Status::Error("cell: missing ref");
  }
  td::Ref<vm::Cell> ref = cs.fetch_ref();
  if (ref.is_null()) {
    return td::Status::Error("cell: failed to fetch ref");
  }
  return ref;
}

td::Status store_cell(vm::CellBuilder &cb, td::Ref<vm::Cell> cell) {
  if (cell.is_null()) {
    return td::Status::Error("cell: null cell");
  }
  if (!cb.store_ref_bool(std::move(cell))) {
    return td::Status::Error("cell: cannot store ref (too many refs?)");
  }
  return td::Status::OK();
}

// ---- cellOf ----

td::Result<td::Ref<vm::CellSlice>> load_ref_slice(vm::CellSlice &cs) {
  TRY_RESULT(cell, load_cell(cs));
  TRY_RESULT(inner, open_ordinary(cell));
  return td::make_ref<vm::CellSlice>(std::move(inner));
}

// ---- maybe_ref ----

td::Result<td::Ref<vm::Cell>> load_maybe_ref(vm::CellSlice &cs) {
  unsigned long long present = 0;
  if (!cs.fetch_ulong_bool(1, present)) {
    return td::Status::Error("maybe_ref: truncated at presence bit");
  }
  if (present == 0) {
    return td::Ref<vm::Cell>{};  // absent
  }
  if (!cs.have_refs(1)) {
    return td::Status::Error("maybe_ref: presence bit set but no ref");
  }
  td::Ref<vm::Cell> ref = cs.fetch_ref();
  if (ref.is_null()) {
    return td::Status::Error("maybe_ref: failed to fetch ref");
  }
  return ref;
}

td::Status store_maybe_ref(vm::CellBuilder &cb, td::Ref<vm::Cell> maybe_cell) {
  if (maybe_cell.is_null()) {
    if (!cb.store_long_bool(0, 1)) {
      return td::Status::Error("maybe_ref: cannot store absence bit");
    }
    return td::Status::OK();
  }
  if (!cb.store_long_bool(1, 1) || !cb.store_ref_bool(std::move(maybe_cell))) {
    return td::Status::Error("maybe_ref: cannot store presence bit + ref");
  }
  return td::Status::OK();
}

// ---- nullable presence prefix ----

td::Result<bool> load_maybe_prefix(vm::CellSlice &cs) {
  unsigned long long b = 0;
  if (!cs.fetch_ulong_bool(1, b)) {
    return td::Status::Error("nullable: truncated at presence bit");
  }
  return b != 0;
}

td::Status store_maybe_prefix(vm::CellBuilder &cb, bool present) {
  if (!cb.store_long_bool(present ? 1 : 0, 1)) {
    return td::Status::Error("nullable: cannot store presence bit");
  }
  return td::Status::OK();
}

// ---- string (snake ref-tail) ----

td::Result<std::string> load_string(vm::CellSlice &cs) {
  if (!cs.have_refs(1)) {
    return td::Status::Error("string: missing ref");
  }
  td::Ref<vm::Cell> ref = cs.fetch_ref();
  if (ref.is_null()) {
    return td::Status::Error("string: failed to fetch ref");
  }
  return read_snake(std::move(ref));
}

td::Status store_string(vm::CellBuilder &cb, const std::string &s) {
  TRY_RESULT(cell, string_to_cell(td::Slice(s)));
  if (!cb.store_ref_bool(std::move(cell))) {
    return td::Status::Error("string: cannot store snake ref");
  }
  return td::Status::OK();
}

// ---- remaining ----

td::Result<td::Ref<vm::CellSlice>> load_remaining(vm::CellSlice &cs) {
  td::Ref<vm::CellSlice> snap = cs.fetch_subslice_ext(cs.size_ext());
  if (snap.is_null()) {
    return td::Status::Error("remaining: failed to snapshot rest of slice");
  }
  return snap;
}

td::Status store_remaining(vm::CellBuilder &cb, const vm::CellSlice &v) {
  if (v.size() > 0) {
    if (!cb.store_bits_bool(v.data_bits(), v.size())) {
      return td::Status::Error("remaining: cannot store bits");
    }
  }
  for (unsigned i = 0; i < v.size_refs(); ++i) {
    if (!cb.store_ref_bool(v.prefetch_ref(i))) {
      return td::Status::Error("remaining: cannot store ref");
    }
  }
  return td::Status::OK();
}

}  // namespace ton_abi
