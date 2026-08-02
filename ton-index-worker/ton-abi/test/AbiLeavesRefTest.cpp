// Ref / string / remaining / maybe leaf units. Round-trips are compared via
// cell hash; string PACK byte-identity checked against Node-generated @ton/core
// reference BOCs; the snake REJECT space enumerated from strings.js. Also pins
// the BOC serialization mode (vm::std_boc_serialize(_, 2) == @ton/core
// toBoc() default). See AbiLeavesRef.h for citations.

#include "AbiTestSupport.h"

namespace {

using namespace ton_abi;

std::string boc_b64(const td::Ref<vm::Cell> &root) {
  auto r = vm::std_boc_serialize(root, 2);  // Mode::WithCRC32C == @ton/core toBoc() default
  REQUIRE(r.is_ok());
  return td::base64_encode(r.move_as_ok().as_slice());
}

td::Ref<vm::Cell> cell_u(unsigned long long val, int bits) {
  vm::CellBuilder b;
  REQUIRE(b.store_long_bool(static_cast<long long>(val), bits));
  return b.finalize();
}

}  // namespace

// BOC mode pin

TEST_CASE("BOC mode: std_boc_serialize(_,2) matches @ton/core toBoc() default") {
  vm::CellBuilder root;
  REQUIRE(root.store_long_bool(0x1234, 16));
  REQUIRE(root.store_ref_bool(cell_u(0xAB, 8)));
  // Reference produced by @ton/core: root.toBoc().toString('base64').
  CHECK(boc_b64(root.finalize()) == "te6cckEBAgEACAABBBI0AQACqyFjJBU=");
}

// cell

TEST_CASE("cell: store/load whole ref, identity preserved") {
  auto child = cell_u(0xDEAD, 16);
  vm::CellBuilder cb;
  REQUIRE(store_cell(cb, child).is_ok());
  auto cs = vm::load_cell_slice(cb.finalize());
  auto r = load_cell(cs);
  REQUIRE(r.is_ok());
  CHECK(r.ok()->get_hash() == child->get_hash());
}

TEST_CASE("cell: load with no ref present -> error") {
  vm::CellBuilder empty;
  auto cs = vm::load_cell_slice(empty.finalize());
  CHECK(load_cell(cs).is_error());
}

// cellOf

TEST_CASE("cellOf: load_ref_slice opens the inner cell for the walker") {
  vm::CellBuilder inner;
  REQUIRE(inner.store_long_bool(0x1234, 16));
  vm::CellBuilder cb;
  REQUIRE(store_cell(cb, inner.finalize()).is_ok());
  auto cs = vm::load_cell_slice(cb.finalize());
  auto r = load_ref_slice(cs);
  REQUIRE(r.is_ok());
  CHECK(r.ok()->size() == 16);
  CHECK(r.ok()->prefetch_ulong(16) == 0x1234);
}

// maybe_ref

TEST_CASE("maybe_ref: present round-trips") {
  auto child = cell_u(0x55, 8);
  vm::CellBuilder cb;
  REQUIRE(store_maybe_ref(cb, child).is_ok());
  auto cs = vm::load_cell_slice(cb.finalize());
  auto r = load_maybe_ref(cs);
  REQUIRE(r.is_ok());
  REQUIRE(r.ok().not_null());
  CHECK(r.ok()->get_hash() == child->get_hash());
}

TEST_CASE("maybe_ref: absent round-trips (bit 0, no ref)") {
  vm::CellBuilder cb;
  REQUIRE(store_maybe_ref(cb, td::Ref<vm::Cell>{}).is_ok());
  auto cs = vm::load_cell_slice(cb.finalize());
  CHECK(cs.size() == 1);       // just the presence bit
  CHECK(cs.size_refs() == 0);
  auto r = load_maybe_ref(cs);
  REQUIRE(r.is_ok());
  CHECK(r.ok().is_null());
}

// nullable presence prefix

TEST_CASE("nullable prefix: 1-bit presence round-trips") {
  for (bool present : {false, true}) {
    vm::CellBuilder cb;
    REQUIRE(store_maybe_prefix(cb, present).is_ok());
    auto cs = vm::load_cell_slice(cb.finalize());
    auto r = load_maybe_prefix(cs);
    REQUIRE(r.is_ok());
    CHECK(r.ok() == present);
  }
}

// string (snake ref-tail)

TEST_CASE("string: short round-trips and packs byte-identically to @ton/core") {
  vm::CellBuilder cb;
  REQUIRE(store_string(cb, "hello").is_ok());
  auto root = cb.finalize();
  CHECK(boc_b64(root) == "te6cckEBAgEACgABAAEACmhlbGxvjW7ihA==");
  auto cs = vm::load_cell_slice(root);
  auto r = load_string(cs);
  REQUIRE(r.is_ok());
  CHECK(r.ok() == "hello");
}

TEST_CASE("string: empty round-trips and packs byte-identically") {
  vm::CellBuilder cb;
  REQUIRE(store_string(cb, "").is_ok());
  auto root = cb.finalize();
  CHECK(boc_b64(root) == "te6cckEBAgEABQABAAEAAG4cXEQ=");
  auto cs = vm::load_cell_slice(root);
  auto r = load_string(cs);
  REQUIRE(r.is_ok());
  CHECK(r.ok() == "");
}

TEST_CASE("string: 200 bytes chunks across a multi-cell snake, byte-identical") {
  std::string big(200, 'a');
  vm::CellBuilder cb;
  REQUIRE(store_string(cb, big).is_ok());
  auto root = cb.finalize();
  CHECK(boc_b64(root) ==
        "te6cckEBAwEA0AABAAEB/mFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWECAJJhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhRleZqg==");
  auto cs = vm::load_cell_slice(root);
  auto r = load_string(cs);
  REQUIRE(r.is_ok());
  CHECK(r.ok() == big);
}

TEST_CASE("string REJECT: non-byte-aligned snake cell") {
  vm::CellBuilder snake;
  REQUIRE(snake.store_long_bool(0x5, 4));  // 4 bits -> not byte-aligned
  vm::CellBuilder outer;
  REQUIRE(outer.store_ref_bool(snake.finalize()));
  auto cs = vm::load_cell_slice(outer.finalize());
  auto r = load_string(cs);
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("byte-aligned") != std::string::npos);
}

TEST_CASE("string REJECT: snake cell with two refs") {
  vm::CellBuilder snake;
  REQUIRE(snake.store_long_bool(0x41, 8));
  REQUIRE(snake.store_ref_bool(cell_u(0x42, 8)));
  REQUIRE(snake.store_ref_bool(cell_u(0x43, 8)));  // 2 refs -> invalid snake
  vm::CellBuilder outer;
  REQUIRE(outer.store_ref_bool(snake.finalize()));
  auto cs = vm::load_cell_slice(outer.finalize());
  auto r = load_string(cs);
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("refs") != std::string::npos);
}

TEST_CASE("string REJECT: missing ref") {
  vm::CellBuilder empty;
  auto cs = vm::load_cell_slice(empty.finalize());
  CHECK(load_string(cs).is_error());
}

// remaining

TEST_CASE("remaining: snapshots all bits + refs, drains source, re-stores identically") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(0xABC, 12));
  REQUIRE(src.store_ref_bool(cell_u(0xAB, 8)));
  auto src_cell = src.finalize();

  auto cs = vm::load_cell_slice(src_cell);
  auto snap = load_remaining(cs);
  REQUIRE(snap.is_ok());
  CHECK(cs.size() == 0);
  CHECK(cs.size_refs() == 0);
  CHECK(snap.ok()->size() == 12);
  CHECK(snap.ok()->size_refs() == 1);

  vm::CellBuilder out;
  REQUIRE(store_remaining(out, *snap.ok()).is_ok());
  CHECK(src_cell->get_hash() == out.finalize()->get_hash());
}

TEST_CASE("remaining: empty slice snapshots to empty") {
  vm::CellBuilder empty;
  auto cs = vm::load_cell_slice(empty.finalize());
  auto snap = load_remaining(cs);
  REQUIRE(snap.is_ok());
  CHECK(snap.ok()->size() == 0);
  CHECK(snap.ok()->size_refs() == 0);
}
