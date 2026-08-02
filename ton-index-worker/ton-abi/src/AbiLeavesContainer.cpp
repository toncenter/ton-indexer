#include "AbiLeavesContainer.h"

#include "AbiLeavesRef.h"

#include "vm/cells/CellSlice.h"

#include <vector>

namespace ton_abi {

namespace {

td::Result<vm::CellSlice> open_ordinary(const td::Ref<vm::Cell> &cell) {
  if (cell.is_null()) {
    return td::Status::Error("container: null cell");
  }
  bool is_special = false;
  vm::CellSlice cs = vm::load_cell_slice_special(cell, is_special);
  if (is_special) {
    return td::Status::Error("container: special/exotic cell not supported");
  }
  return cs;
}

}  // namespace

// ---- arrayOf ----

td::Status load_array(vm::CellSlice &cs, const std::function<td::Status(vm::CellSlice &)> &unpack_one) {
  unsigned long long len = 0;
  if (!cs.fetch_ulong_bool(8, len)) {
    return td::Status::Error("array: truncated at length prefix");
  }
  TRY_RESULT(head, load_maybe_ref(cs));

  std::size_t count = 0;
  td::Ref<vm::Cell> cur = std::move(head);
  while (cur.not_null()) {
    TRY_RESULT(hs, open_ordinary(cur));
    // FIRST the next-chunk ref, THEN drain this cell's elements (accepts both
    // the 1-elem/ref and the compiler-chunked forms).
    TRY_RESULT(next, load_maybe_ref(hs));
    while (hs.size() > 0 || hs.size_refs() > 0) {
      unsigned before_bits = hs.size();
      unsigned before_refs = hs.size_refs();
      TRY_STATUS(unpack_one(hs));
      if (hs.size() == before_bits && hs.size_refs() == before_refs) {
        return td::Status::Error("array: element consumed no bits/refs (malformed inner)");
      }
      ++count;
    }
    cur = std::move(next);
  }
  if (count != len) {
    return td::Status::Error(PSLICE() << "array: length mismatch, prefix says " << len << " but found " << count);
  }
  return td::Status::OK();
}

td::Status store_array(vm::CellBuilder &cb, std::size_t count,
                       const std::function<td::Status(vm::CellBuilder &, std::size_t)> &pack_one) {
  if (count > 255) {
    return td::Status::Error(PSLICE() << "array: length " << count << " exceeds the 8-bit prefix");
  }
  // Build the chain in REVERSE so the head chunk holds element 0.
  td::Ref<vm::Cell> tail;  // null == no continuation
  for (std::size_t i = 0; i < count; ++i) {
    vm::CellBuilder chunk;
    TRY_STATUS(store_maybe_ref(chunk, tail));
    TRY_STATUS(pack_one(chunk, count - 1 - i));
    tail = chunk.finalize();
  }
  if (!cb.store_long_bool(static_cast<long long>(count), 8)) {
    return td::Status::Error("array: cannot store length prefix");
  }
  return store_maybe_ref(cb, std::move(tail));
}

// ---- lispListOf ----

td::Status load_lisp_list(vm::CellSlice &cs, const std::function<td::Status(vm::CellSlice &)> &unpack_one) {
  TRY_RESULT(head_ref, load_cell(cs));
  // The wire chain runs from the LAST logical element down to the first
  // (pack folds forward, storing a ref to the previous node), and the reference
  // unshifts on read. So collect the per-node element slices head->tail, then
  // deliver them to the caller in REVERSE (logical) order.
  std::vector<vm::CellSlice> nodes;
  td::Ref<vm::Cell> cur = std::move(head_ref);
  for (;;) {
    TRY_RESULT(hs, open_ordinary(cur));
    if (hs.size_refs() == 0) {
      break;  // terminator node (empty cell)
    }
    // cons node: read the tail ref FIRST (reference reads ref[0]); what remains
    // is the element.
    td::Ref<vm::Cell> tail = hs.fetch_ref();
    if (tail.is_null()) {
      return td::Status::Error("lisp_list: failed to follow tail ref");
    }
    nodes.push_back(hs);  // element bits + refs, positioned at the element start
    cur = std::move(tail);
  }
  for (std::size_t i = nodes.size(); i-- > 0;) {
    vm::CellSlice es = nodes[i];
    TRY_STATUS(unpack_one(es));
    if (es.size() != 0 || es.size_refs() != 0) {
      return td::Status::Error("lisp_list: cons node not fully consumed by element");
    }
  }
  return td::Status::OK();
}

td::Status store_lisp_list(vm::CellBuilder &cb, std::size_t count,
                           const std::function<td::Status(vm::CellBuilder &, std::size_t)> &pack_one) {
  vm::CellBuilder empty;
  td::Ref<vm::Cell> tail = empty.finalize();  // nil terminator
  for (std::size_t i = 0; i < count; ++i) {
    vm::CellBuilder item;
    TRY_STATUS(pack_one(item, i));
    if (!item.store_ref_bool(tail)) {
      return td::Status::Error("lisp_list: cannot store tail ref");
    }
    tail = item.finalize();
  }
  if (!cb.store_ref_bool(std::move(tail))) {
    return td::Status::Error("lisp_list: cannot store head ref");
  }
  return td::Status::OK();
}

}  // namespace ton_abi
