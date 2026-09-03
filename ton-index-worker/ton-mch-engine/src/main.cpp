// ton-mch-engine exposes the upstream product-boundary MCH CLI modes.

#include "td/utils/OptionParser.h"
#include "td/utils/check.h"
#include "td/utils/logging.h"
#include "td/utils/port/signals.h"
#include "crypto/vm/cp0.h"

#include "AbiBridgeTest.h"
#include "Classify.h"
#include "EmuActionSerializeTest.h"
#include "EmuCelldbLookupTest.h"
#include "GhostExternalTest.h"
#include "MsgParse.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

int main(int argc, char *argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  td::set_default_failure_signal_handler().ensure();

  CHECK(vm::init_op_cp0());

  bool actions_mode = false;
  bool abi_bridge_test_mode = false;
  bool actions_msgpack_test_mode = false;
  bool celldb_tier2_test_mode = false;
  bool ghost_external_test_mode = false;
  std::vector<std::string> trace_paths;
  std::string actions_output_dir;
  std::string fixtures_manifest;

  td::OptionParser p;
  p.set_description("MCH matcher IR engine");
  p.add_option('\0', "actions",
               "Serialized-Action-row dump: -T <fixture|dir> (repeatable)",
               [&]() { actions_mode = true; });
  p.add_option('\0', "abi-bridge-test",
               "ton-abi bridge self-test (trampoline + AbiValue->Value adapter + registry policy)",
               [&]() { abi_bridge_test_mode = true; });
  p.add_option('\0', "actions-msgpack-test",
               "Write-back writer self-test (mch::Action -> the msgpack `actions` blob)",
               [&]() { actions_msgpack_test_mode = true; });
  p.add_option('\0', "celldb-tier2-test",
               "Celldb tier-2 lookup self-test (tier shape equality, jvault chain, memo/budget)",
               [&]() { celldb_tier2_test_mode = true; });
  p.add_option('\0', "ghost-external-test",
               "Wallet request/ghost self-test (tg-wallet single/bulk/opcode/gasless)",
               [&]() { ghost_external_test_mode = true; });
  p.add_option('\0', "help", "prints help", [&]() {
    char b[10240];
    td::StringBuilder sb(td::MutableSlice{b, 10000});
    sb << p;
    std::cout << sb.as_cslice().c_str();
    std::exit(2);
  });
  p.add_option('T', "trace", "Path to an .lz4 trace fixture (repeatable)", [&](td::Slice value) {
    trace_paths.push_back(value.str());
  });
  p.add_option('O', "output-dir", "Write --actions output per fixture", [&](td::Slice value) {
    actions_output_dir = value.str();
  });
  p.add_option('\0', "fixtures", "Fixture manifest (goldens/fixtures.json): names + layout for -O",
               [&](td::Slice value) { fixtures_manifest = value.str(); });

  auto status = p.run(argc, argv);
  if (status.is_error()) {
    LOG(ERROR) << "failed to parse options: " << status.move_as_error();
    std::_Exit(2);
  }

  // Check the merged parser registry before every dispatch. A duplicate key
  // across the hand and ABI sources must stop startup rather than selecting an
  // arbitrary first-source parser.
  if (auto reg_st = mch::validate_registries(); reg_st.is_error()) {
    LOG(ERROR) << "message parser registry invalid: " << reg_st.message();
    return 1;
  }

  if (actions_mode) {
    return mch::run_actions(trace_paths, actions_output_dir, fixtures_manifest);
  }
  if (abi_bridge_test_mode) {
    return mch::run_abi_bridge_test();
  }
  if (actions_msgpack_test_mode) {
    return mch::run_action_msgpack_test();
  }
  if (celldb_tier2_test_mode) {
    return mch::run_celldb_tier2_test();
  }
  if (ghost_external_test_mode) {
    return mch::run_ghost_external_test();
  }
  return 0;
}
