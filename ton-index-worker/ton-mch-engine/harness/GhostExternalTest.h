// --ghost-external-test: wallet-request opcode recovery, gasless markers and
// ghost synthesis over synthetic tg-wallet / v5 bodies. Needs no fixtures.
#pragma once

namespace mch {

// Returns 0 when every check passes, 1 otherwise.
int run_ghost_external_test();

}  // namespace mch
