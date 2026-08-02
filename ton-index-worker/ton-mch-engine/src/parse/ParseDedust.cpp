// DeDust message parsers (messages/swaps.py + blocks/swaps.py). See
// parse/PSlice.h for shared machinery and MsgParse.cpp's header for the
// pytoniq-parity catalogue.
#include "parse/Parsers.h"

#include "MsgParse.h"
#include "parse/PSlice.h"

#include "common/refint.h"
#include "vm/cellslice.h"

#include <string>
#include <utility>
#include <vector>

namespace mch {

// DedustPayoutFromPool and DedustSwapNotification use protocol ABI rows.
// parse_dedust_steps remains a host-function chain walk rather than a
// registered message parser.

// blocks/swaps.py _parse_dedust_steps: walk the SwapStep chain (already
// positioned past the sum-type/header). Each link: pool addr, 1 flag bit,
// coins, maybe-ref to the next step. Returns the pool addresses in walk order
// (AccountId(load_address()).as_str(); addr_none -> "addr_none").
td::Result<std::vector<std::string>> parse_dedust_steps(vm::CellSlice cs) {
  std::vector<std::string> steps;
  for (;;) {
    TRY_RESULT(addr, load_address_py(cs));  // pytoniq load_address
    std::string pool = (addr.t == VType::Account && !addr.addr_none) ? addr.str : "addr_none";
    if (!cs.have(1)) {
      return td::Status::Error("dedust steps: flag bit underflow");
    }
    cs.advance(1);                    // load_bit
    TRY_RESULT(_coins, load_coins_py(cs));
    (void)_coins;
    if (!cs.have(1)) {                // load_maybe_ref: the Maybe bit
      return td::Status::Error("dedust steps: maybe-ref bit underflow");
    }
    bool has_next = cs.fetch_ulong(1) != 0;
    steps.push_back(std::move(pool));
    if (!has_next) {
      return steps;
    }
    if (cs.size_refs() == 0) {
      return td::Status::Error("dedust steps: next-step ref missing");
    }
    TRY_RESULT_ASSIGN(cs, open_ref_cell(cs.fetch_ref()));  // next_step.to_slice()
  }
}

}  // namespace mch
