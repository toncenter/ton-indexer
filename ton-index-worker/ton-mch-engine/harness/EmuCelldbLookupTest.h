// --celldb-tier2-test: self-contained tests for the cell-db tier-2 hook.
// Covers the load-bearing design property (a hook-produced V2 renders through
// the SAME code path tier 1 uses, so the two tiers cannot drift), the jvault
// data-cell chain over synthetic cells, and the memo / fetch budget / wall-clock
// budget. Same flag-driven pattern as --shape-test / --actions-msgpack-test.
//
// The account reads are injected (EmuCelldbTier2::set_account_source), so no
// celldb, no shard state and no node are needed. What that CANNOT cover is any
// path through a real get-method, see the header comment in the .cpp for the
// exact list of legs that still need a live run.
#pragma once

namespace mch {

// Returns 0 on all-pass, 1 on any failure.
int run_celldb_tier2_test();

}  // namespace mch
