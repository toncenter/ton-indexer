#pragma once

// Shared prelude for the ton-abi doctest TUs. Include it FIRST.
// td/utils/Status.h -> logging.h -> check.h #defines CHECK as an abort-style
// macro that shadows doctest's soft CHECK. The push_macro/#undef/pop_macro
// dance happens once here around every td/vm/ton-abi header; include guards
// make later per-TU includes no-ops so CHECK cannot be redefined.
// Also carries the fixture helpers the ABI-loading TUs share.

#include "doctest.h"

#pragma push_macro("CHECK")
#undef CHECK
#include "AbiEmit.h"
#include "AbiGenSupport.h"
#include "AbiKernel.h"
#include "AbiLeavesAddress.h"
#include "AbiLeavesContainer.h"
#include "AbiLeavesDict.h"
#include "AbiLeavesPrefix.h"
#include "AbiLeavesRef.h"
#include "AbiLeavesScalar.h"
#include "AbiLoader.h"
#include "AbiModel.h"
#include "AbiValue.h"

#include "common/bitstring.h"
#include "common/refint.h"
#include "td/utils/JsonBuilder.h"
#include "td/utils/base64.h"
#include "vm/boc.h"
#include "vm/cells/Cell.h"
#include "vm/cells/CellBuilder.h"
#include "vm/cells/CellSlice.h"
#pragma pop_macro("CHECK")

#include <fstream>
#include <sstream>
#include <string>

#ifndef TON_ABI_FIXTURES_DIR
#error "TON_ABI_FIXTURES_DIR must be defined by CMake"
#endif

namespace ton_abi_test {

// Whole-file read; REQUIRE-fails the current case if the path cannot be opened.
inline std::string read_file(const std::string &path) {
  std::ifstream in(path, std::ios::binary);
  REQUIRE_MESSAGE(static_cast<bool>(in), "cannot open: " << path);
  std::stringstream ss;
  ss << in.rdbuf();
  return ss.str();
}

inline std::string fixture_path(const std::string &stem) {
  return std::string(TON_ABI_FIXTURES_DIR) + "/" + stem + ".abi.json";
}

// Load testdata/fixtures/<stem>.abi.json; REQUIRE-fails on a loader error.
inline ton_abi::ContractABI load_fixture_abi(const std::string &stem) {
  auto r = ton_abi::load_abi_from_json(read_file(fixture_path(stem)));
  REQUIRE_MESSAGE(r.is_ok(), stem << ": " << (r.is_error() ? r.error().message().str() : ""));
  return r.move_as_ok();
}

// All 18 fixtures that compile to a valid ABI. imports/ are not independently
// compilable; small.tolk is a Tolk compiler error (see gen_vectors.mjs).
inline constexpr const char *kLoadableFixtures[] = {
    "client-type-anno", "debug-print-demos", "err-cont-on-stack-1", "err-cont-on-stack-2",
    "err-invalid-map-key-1", "err-invalid-map-key-2", "generic-union-labels", "has-not-init-storage",
    "jetton-minter-contract", "jetton-wallet-contract", "lots-of-annotations", "lots-of-getters",
    "lots-of-messages", "lots-of-storage", "lots-of-throws", "lots-of-wrappers", "only-header",
    "tolk_counter",
};

}  // namespace ton_abi_test
