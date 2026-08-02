// Shared pytoniq-Slice stand-in machinery used by every per-family message
// parser (see MsgParse.h for the semantics contract). These helpers reproduce
// the pytoniq Slice quirks the family adapters rely on; they were internal to
// MsgParse.cpp and are declared here so the split parser TUs can share them.
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

// pytoniq Slice.load_address(). Advances `cs`. Error == the Python parser
// raising (whole message parse fails, or the enclosing try downgrades it).
td::Result<Value> load_address_py(vm::CellSlice &cs);

// pytoniq Slice.load_coins(): len nibble 0 -> 0. Advances `cs`.
td::Result<td::RefInt256> load_coins_py(vm::CellSlice &cs);

td::Result<td::RefInt256> var_uint16(const td::Ref<vm::CellSlice> &csr);

// pytoniq-Slice stand-in: `cs` carries the bit cursor, `refs`/`off` carry the
// ref list SEPARATELY, because pytoniq's copy()/to_cell()/snake semantics do
// not always agree with the CellSlice's own ref cursor (see MsgParse.cpp header).
struct PSlice {
  vm::CellSlice cs;
  std::vector<td::Ref<vm::Cell>> refs;
  size_t off{0};
};

PSlice pslice_from_cell(const td::Ref<vm::Cell> &c);

// pytoniq Slice.to_cell(): Cell(remaining bits, refs[off:]).
td::Result<td::Ref<vm::Cell>> pslice_to_cell(const PSlice &ps);

// pytoniq Slice.load_snake_bytes(): byte-aligned, <=1 ref per link.
td::Result<std::string> load_snake_bytes(PSlice ps);

struct BodyCtx {
  vm::CellSlice cs;
  std::vector<td::Ref<vm::Cell>> all_refs;  // body refs from index 0 (copy() quirk)
};

td::Result<BodyCtx> open_body(const td::Ref<vm::Cell> &body);

// pytoniq ref.begin_parse(): open a ref cell into a CellSlice, abort-safe (a
// throwing load becomes an Error, never abort()). The shared "open a ref"
// helper the stonfi + tonco cores and the dedust step walk each duplicated.
td::Result<vm::CellSlice> open_ref_cell(const td::Ref<vm::Cell> &c);

// pytoniq StateInit.deserialize: split_depth:(Maybe (## 5)) special:(Maybe
// TickTock) code/data/library:(Maybe ^Cell). Only the cursor matters, but every
// field is walked because Python parses (and can raise on) them all.
td::Status skip_state_init_py(vm::CellSlice &cs);

// pytoniq Slice.to_cell(): Cell(remaining bits, remaining refs).
td::Result<td::Ref<vm::Cell>> slice_to_cell(const vm::CellSlice &cs);

// The Message/MessageRelaxed tail, resumed after CommonMsgInfo:
//   init:(Maybe (Either StateInit ^StateInit)) body:(Either X ^X)
// Returns the body cell (the ref, or the rest of the message cell inline).
td::Result<td::Ref<vm::Cell>> message_any_body(vm::CellSlice &cs);

}  // namespace mch
