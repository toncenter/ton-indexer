#pragma once

// Shared prelude for the ton-abi doctest TUs. Include it FIRST in a test TU;
// after it, include anything (generated/*_gen.h too) with no ceremony.
//
// WHY IT EXISTS: td/utils/Status.h -> logging.h -> check.h #defines CHECK as an
// abort-style macro that shadows doctest's soft CHECK for the rest of the TU.
// Every test TU used to hand-roll the same push_macro/#undef/pop_macro dance
// around its own includes. Here that dance happens ONCE, around every td / vm /
// ton-abi header the suite uses: check.h has an include guard, so the later
// per-TU includes are no-ops and can no longer redefine CHECK.
//
// It also carries the two fixture helpers the ABI-loading TUs share.

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

}  // namespace ton_abi_test
