// --actions-msgpack-test: self-contained unit tests for the write-back writer.
// Covers the presence rule (absent vs null), the natural-types rule (which
// fields are decimal strings and which naturalize), the []string element
// discipline, the Value->msgpack table, the bucket-B fills, the synthetic
// `unknown` row, and the two insert-side decisions (view_finality and the
// actions staleness guard). Same flag-driven pattern as --shape-test /
// --dexrecords-test.
#pragma once

#include <string>

namespace mch {

// Source of truth also consumed by the local dev-engine's
// --actions-msgpack-out fixture regeneration path.
std::string wire_fixture_bytes();

// Returns 0 on all-pass, 1 on any failure.
int run_action_msgpack_test();

}  // namespace mch
