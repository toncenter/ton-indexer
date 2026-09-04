// Shared Slice stand-in used by every per-family message parser. Helpers
// keep the Slice quirks the family adapters rely on (see MsgParse.h).
#pragma once

#include "Value.h"

#include "td/utils/Status.h"
#include "vm/cells/Cell.h"
#include "vm/cellslice.h"

#include <cstddef>
#include <string>
#include <vector>

namespace mch {

std::string hex_upper(const unsigned char *p, size_t n);

td::RefInt256 refint_u64(unsigned long long v);

// Load an address; advances `cs`. Error fails the whole message parse, or
// the enclosing try downgrades it. Intentional.
td::Result<Value> load_address_py(vm::CellSlice &cs);

// Load coins; len nibble 0 yields 0. Advances `cs`.
td::Result<td::RefInt256> load_coins_py(vm::CellSlice &cs);

// `cs` is the bit cursor; `refs`/`off` hold the ref list separately because
// copy()/to_cell() do not always agree with CellSlice's own ref cursor.
struct PSlice {
  vm::CellSlice cs;
  std::vector<td::Ref<vm::Cell>> refs;
  size_t off{0};
};

PSlice pslice_from_cell(const td::Ref<vm::Cell> &c);

// Remaining bits plus refs[off:].
td::Result<td::Ref<vm::Cell>> pslice_to_cell(const PSlice &ps);

// Byte-aligned snake; at most one ref per link.
td::Result<std::string> load_snake_bytes(vm::CellSlice &cs);

struct BodyCtx {
  vm::CellSlice cs;
  std::vector<td::Ref<vm::Cell>> all_refs;  // body refs from index 0; copy() starts there
};

td::Result<BodyCtx> open_body(const td::Ref<vm::Cell> &body);

// Open a ref cell into a CellSlice. A throwing load becomes an Error, never abort().
td::Result<vm::CellSlice> open_ref_cell(const td::Ref<vm::Cell> &c);

// StateInit: split_depth:(Maybe (## 5)) special:(Maybe TickTock)
// code/data/library:(Maybe ^Cell). Only the cursor matters, but every field
// is walked because any of them can fail the parse. Intentional.
td::Status skip_state_init_py(vm::CellSlice &cs);

// After the StateInit prefix, skip the code maybe-ref and return the required
// data cell.
td::Result<td::Ref<vm::Cell>> state_init_data_cell(vm::CellSlice &cs);

// Remaining bits plus remaining refs.
td::Result<td::Ref<vm::Cell>> slice_to_cell(const vm::CellSlice &cs);

// The Message/MessageRelaxed tail, resumed after CommonMsgInfo:
//   init:(Maybe (Either StateInit ^StateInit)) body:(Either X ^X)
// Returns the body cell (the ref, or the rest of the message cell inline).
td::Result<td::Ref<vm::Cell>> message_any_body(vm::CellSlice &cs);

}  // namespace mch
