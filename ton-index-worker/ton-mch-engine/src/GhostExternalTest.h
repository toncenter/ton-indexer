// --ghost-external-test: hermetic coverage for wallet-external ghost synthesis.
#pragma once

namespace mch {

// Returns 0 on all-pass, 1 on any failure.
int run_ghost_external_test();

}  // namespace mch
