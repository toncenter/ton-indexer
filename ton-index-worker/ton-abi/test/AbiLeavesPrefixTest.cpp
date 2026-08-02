// Prefix ops over hand-built cells cover store/check round-trip, the wide
// (48-bit) struct opcode, zero length, truncation reject, and the peek
// primitive. The union prefix-tree cases that used to live here drove the
// interpreter-era runtime dispatch_union helper; with pure codegen that helper
// is gone and the dispatch FORMS are gated on the generated path instead
// (test/AbiABGateTest.cpp, "union dispatch forms").

#include "AbiTestSupport.h"

namespace {

using namespace ton_abi;

vm::CellSlice cs_of(vm::CellBuilder &cb) { return vm::load_cell_slice(cb.finalize()); }

}  // namespace

// prefix ops

TEST_CASE("prefix: store + check round-trip, mismatch rejects") {
  vm::CellBuilder cb;
  REQUIRE(store_prefix(cb, 0x12345678u, 32).is_ok());
  auto ok = cs_of(cb);
  CHECK(load_and_check_prefix(ok, 0x12345678u, 32).is_ok());

  auto bad = cs_of(cb);
  CHECK(load_and_check_prefix(bad, 0x12345679u, 32).is_error());
}

TEST_CASE("prefix: 48-bit struct opcode round-trip (wider than the 32-bit union bound)") {
  vm::CellBuilder cb;
  REQUIRE(store_prefix(cb, 0x123456789ABCull, 48).is_ok());
  auto cs = cs_of(cb);
  CHECK(cs.size() == 48);
  CHECK(load_and_check_prefix(cs, 0x123456789ABCull, 48).is_ok());
}

TEST_CASE("prefix: length 0 always matches and consumes nothing") {
  vm::CellBuilder cb;
  REQUIRE(cb.store_long_bool(0xF, 4));
  auto cs = cs_of(cb);
  CHECK(load_and_check_prefix(cs, 0, 0).is_ok());
  CHECK(cs.size() == 4);  // untouched
}

TEST_CASE("prefix: check truncation rejects") {
  vm::CellBuilder cb;
  REQUIRE(cb.store_long_bool(0x1, 4));  // only 4 bits
  auto cs = cs_of(cb);
  CHECK(load_and_check_prefix(cs, 0, 8).is_error());
}

TEST_CASE("prefix: lookup peeks without consuming") {
  vm::CellBuilder cb;
  REQUIRE(cb.store_long_bool(0xAB, 8));
  auto cs = cs_of(cb);
  CHECK(lookup_prefix(cs, 0xAB, 8));
  CHECK_FALSE(lookup_prefix(cs, 0xAC, 8));
  CHECK(cs.size() == 8);  // not consumed
}
