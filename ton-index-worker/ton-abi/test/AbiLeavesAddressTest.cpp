// Address leaf load+store units use hand-built cells via vm::CellBuilder;
// accepted inputs round-trip byte-identically (compared via cell hash).

#include "AbiTestSupport.h"

namespace {

using namespace ton_abi;

vm::CellSlice cs_of(vm::CellBuilder &cb) { return vm::load_cell_slice(cb.finalize()); }

// Append a 256-bit test hash (4 x 64-bit chunks, all < 2^63).
void store_test_hash(vm::CellBuilder &cb) {
  REQUIRE(cb.store_long_bool(0x0123456789ABCDEFLL, 64));
  REQUIRE(cb.store_long_bool(0x1122334455667788LL, 64));
  REQUIRE(cb.store_long_bool(0x2233445566778899LL, 64));
  REQUIRE(cb.store_long_bool(0x3344556677889900LL, 64));
}

}  // namespace


TEST_CASE("address: addr_std round-trips byte-identically (masterchain wc=-1)") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(2, 2));  // tag addr_std
  REQUIRE(src.store_long_bool(0, 1));  // anycast = 0
  REQUIRE(src.store_long_bool(-1, 8));
  store_test_hash(src);
  auto src_cell = src.finalize();

  auto cs = vm::load_cell_slice(src_cell);
  auto r = load_address(cs);
  REQUIRE(r.is_ok());
  CHECK(r.ok().kind == AbiAddressKind::Std);
  CHECK(r.ok().workchain == -1);
  CHECK(cs.size() == 0);  // fully consumed (267 bits)

  vm::CellBuilder out;
  REQUIRE(store_address(out, r.ok()).is_ok());
  CHECK(src_cell->get_hash() == out.finalize()->get_hash());
}

TEST_CASE("address: workchain 0 round-trips") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(2, 2));
  REQUIRE(src.store_long_bool(0, 1));
  REQUIRE(src.store_long_bool(0, 8));
  store_test_hash(src);
  auto src_cell = src.finalize();
  auto cs = vm::load_cell_slice(src_cell);
  auto r = load_address(cs);
  REQUIRE(r.is_ok());
  CHECK(r.ok().workchain == 0);
  vm::CellBuilder out;
  REQUIRE(store_address(out, r.ok()).is_ok());
  CHECK(src_cell->get_hash() == out.finalize()->get_hash());
}

TEST_CASE("address REJECT: anycast bit set") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(2, 2));
  REQUIRE(src.store_long_bool(1, 1));  // anycast = 1
  REQUIRE(src.store_long_bool(0, 8));
  store_test_hash(src);
  auto cs = cs_of(src);
  auto r = load_address(cs);
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("anycast") != std::string::npos);
}

TEST_CASE("address REJECT: addr_none tag (0b00)") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(0, 2));
  auto cs = cs_of(src);
  CHECK(load_address(cs).is_error());
}

TEST_CASE("address REJECT: addr_var tag (0b11)") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(3, 2));
  auto cs = cs_of(src);
  CHECK(load_address(cs).is_error());
}

TEST_CASE("address REJECT: truncated std body (no hash)") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(2, 2));
  REQUIRE(src.store_long_bool(0, 1));
  REQUIRE(src.store_long_bool(0, 8));  // wc present but hash missing
  auto cs = cs_of(src);
  CHECK(load_address(cs).is_error());
}

TEST_CASE("address store REJECT: non-std kind") {
  vm::CellBuilder cb;
  CHECK(store_address(cb, AbiAddress{}).is_error());  // None
}

TEST_CASE("address store REJECT: workchain out of int8 range") {
  AbiAddress a;
  a.kind = AbiAddressKind::Std;
  a.hash.set_zero();
  a.workchain = 200;
  vm::CellBuilder cb;
  CHECK(store_address(cb, a).is_error());
}


TEST_CASE("address?: none round-trips") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(0, 2));
  auto src_cell = src.finalize();
  auto cs = vm::load_cell_slice(src_cell);
  auto r = load_maybe_address(cs);
  REQUIRE(r.is_ok());
  CHECK(r.ok().kind == AbiAddressKind::None);
  vm::CellBuilder out;
  REQUIRE(store_maybe_address(out, r.ok()).is_ok());
  CHECK(src_cell->get_hash() == out.finalize()->get_hash());
}

TEST_CASE("address?: std round-trips") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(2, 2));
  REQUIRE(src.store_long_bool(0, 1));
  REQUIRE(src.store_long_bool(0, 8));
  store_test_hash(src);
  auto cs = cs_of(src);
  auto r = load_maybe_address(cs);
  REQUIRE(r.is_ok());
  CHECK(r.ok().kind == AbiAddressKind::Std);
}

TEST_CASE("address? REJECT: addr_extern tag") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(1, 2));
  auto cs = cs_of(src);
  CHECK(load_maybe_address(cs).is_error());
}

TEST_CASE("address? store REJECT: extern kind not valid") {
  AbiAddress a;
  a.kind = AbiAddressKind::Extern;
  a.ext_bits = 0;
  vm::CellBuilder zero;  // build an empty 0-bit slice for ext_value
  a.ext_value = cs_of(zero).fetch_subslice(0, 0);
  vm::CellBuilder cb;
  CHECK(store_maybe_address(cb, a).is_error());
}


TEST_CASE("ext_address: round-trips byte-identically (10-bit value)") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(1, 2));      // tag addr_extern
  REQUIRE(src.store_long_bool(10, 9));     // length = 10 bits
  REQUIRE(src.store_long_bool(0x2AB, 10)); // value
  auto src_cell = src.finalize();

  auto cs = vm::load_cell_slice(src_cell);
  auto r = load_external_address(cs);
  REQUIRE(r.is_ok());
  CHECK(r.ok().kind == AbiAddressKind::Extern);
  CHECK(r.ok().ext_bits == 10);

  vm::CellBuilder out;
  REQUIRE(store_external_address(out, r.ok()).is_ok());
  CHECK(src_cell->get_hash() == out.finalize()->get_hash());
}

TEST_CASE("ext_address: zero-length value round-trips") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(1, 2));
  REQUIRE(src.store_long_bool(0, 9));  // length 0
  auto src_cell = src.finalize();
  auto cs = vm::load_cell_slice(src_cell);
  auto r = load_external_address(cs);
  REQUIRE(r.is_ok());
  CHECK(r.ok().ext_bits == 0);
  vm::CellBuilder out;
  REQUIRE(store_external_address(out, r.ok()).is_ok());
  CHECK(src_cell->get_hash() == out.finalize()->get_hash());
}

TEST_CASE("ext_address REJECT: wrong tag (addr_std)") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(2, 2));
  auto cs = cs_of(src);
  CHECK(load_external_address(cs).is_error());
}

TEST_CASE("ext_address REJECT: truncated value (length claims more than present)") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(1, 2));
  REQUIRE(src.store_long_bool(16, 9));  // claims 16 bits
  REQUIRE(src.store_long_bool(0x1, 4)); // only 4 present
  auto cs = cs_of(src);
  CHECK(load_external_address(cs).is_error());
}

TEST_CASE("ext_address store REJECT: value slice size mismatch") {
  vm::CellBuilder val;
  REQUIRE(val.store_long_bool(0x7, 3));  // 3 bits
  AbiAddress a;
  a.kind = AbiAddressKind::Extern;
  a.ext_bits = 5;  // claims 5 but slice has 3
  a.ext_value = cs_of(val).fetch_subslice(3, 0);
  vm::CellBuilder cb;
  CHECK(store_external_address(cb, a).is_error());
}


TEST_CASE("any_address: none/std/extern all round-trip") {
  SUBCASE("none") {
    vm::CellBuilder src;
    REQUIRE(src.store_long_bool(0, 2));
    auto src_cell = src.finalize();
    auto cs = vm::load_cell_slice(src_cell);
    auto r = load_address_any(cs);
    REQUIRE(r.is_ok());
    CHECK(r.ok().kind == AbiAddressKind::None);
    vm::CellBuilder out;
    REQUIRE(store_address_any(out, r.ok()).is_ok());
    CHECK(src_cell->get_hash() == out.finalize()->get_hash());
  }
  SUBCASE("std") {
    vm::CellBuilder src;
    REQUIRE(src.store_long_bool(2, 2));
    REQUIRE(src.store_long_bool(0, 1));
    REQUIRE(src.store_long_bool(0, 8));
    store_test_hash(src);
    auto src_cell = src.finalize();
    auto cs = vm::load_cell_slice(src_cell);
    auto r = load_address_any(cs);
    REQUIRE(r.is_ok());
    CHECK(r.ok().kind == AbiAddressKind::Std);
    vm::CellBuilder out;
    REQUIRE(store_address_any(out, r.ok()).is_ok());
    CHECK(src_cell->get_hash() == out.finalize()->get_hash());
  }
  SUBCASE("extern") {
    vm::CellBuilder src;
    REQUIRE(src.store_long_bool(1, 2));
    REQUIRE(src.store_long_bool(12, 9));
    REQUIRE(src.store_long_bool(0xABC, 12));
    auto src_cell = src.finalize();
    auto cs = vm::load_cell_slice(src_cell);
    auto r = load_address_any(cs);
    REQUIRE(r.is_ok());
    CHECK(r.ok().kind == AbiAddressKind::Extern);
    vm::CellBuilder out;
    REQUIRE(store_address_any(out, r.ok()).is_ok());
    CHECK(src_cell->get_hash() == out.finalize()->get_hash());
  }
}

TEST_CASE("any_address REJECT: addr_var (tag 0b11)") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(3, 2));
  auto cs = cs_of(src);
  auto r = load_address_any(cs);
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("addr_var") != std::string::npos);
}
