// Scalar leaf load+store units use hand-built cells via vm::CellBuilder;
// boundary/min/max/truncation per type. Semantics pinned to @ton/core
// (BitReader.js / BitBuilder.js) -- see AbiLeavesScalar.h for the citations.

#include "AbiTestSupport.h"

namespace {

using namespace ton_abi;

// Finalize a builder and open it as a fresh mutable slice.
vm::CellSlice cs_of(vm::CellBuilder &cb) {
  return vm::load_cell_slice(cb.finalize());
}

td::RefInt256 I(long long x) { return td::make_refint(x); }
td::RefInt256 Idec(const std::string &s) { return td::dec_string_to_int256(s); }

}  // namespace

// var_prefix_bits

TEST_CASE("var_prefix_bits: log2 of powers of two, reject otherwise") {
  CHECK(var_prefix_bits(16).ok() == 4);
  CHECK(var_prefix_bits(32).ok() == 5);
  CHECK(var_prefix_bits(1).ok() == 0);
  CHECK(var_prefix_bits(24).is_error());
  CHECK(var_prefix_bits(0).is_error());
  CHECK(var_prefix_bits(-4).is_error());
}

// intN

TEST_CASE("intN: round-trip signed values incl boundaries") {
  for (long long x : {0LL, 1LL, -1LL, 127LL, -128LL}) {
    vm::CellBuilder cb;
    REQUIRE(store_int(cb, I(x), 8).is_ok());
    auto cs = cs_of(cb);
    auto r = load_int(cs, 8);
    REQUIRE(r.is_ok());
    CHECK(r.ok()->to_dec_string() == std::to_string(x));
  }
}

TEST_CASE("intN: store rejects out-of-range (two's-complement window)") {
  vm::CellBuilder cb;
  CHECK(store_int(cb, I(128), 8).is_error());   // 2^7 too big
  vm::CellBuilder cb2;
  CHECK(store_int(cb2, I(-129), 8).is_error());  // below -2^7
}

TEST_CASE("intN: bits==1 accepts only {-1,0} (matches @ton/core writeInt)") {
  vm::CellBuilder a; CHECK(store_int(a, I(0), 1).is_ok());
  vm::CellBuilder b; CHECK(store_int(b, I(-1), 1).is_ok());
  vm::CellBuilder c; CHECK(store_int(c, I(1), 1).is_error());
}

TEST_CASE("intN: load truncation -> error") {
  vm::CellBuilder cb;
  REQUIRE(cb.store_long_bool(0, 4));  // only 4 bits present
  auto cs = cs_of(cb);
  CHECK(load_int(cs, 8).is_error());
}

TEST_CASE("intN: wide 257-bit round-trip") {
  auto big = Idec("-115792089237316195423570985008687907853269984665640564039457584007913129639936"); // -2^256
  vm::CellBuilder cb;
  REQUIRE(store_int(cb, big, 257).is_ok());
  auto cs = cs_of(cb);
  auto r = load_int(cs, 257);
  REQUIRE(r.is_ok());
  CHECK(r.ok()->to_dec_string() == big->to_dec_string());
}

// uintN

TEST_CASE("uintN: round-trip incl max, reject negative and overflow") {
  vm::CellBuilder cb;
  REQUIRE(store_uint(cb, I(255), 8).is_ok());  // max uint8
  auto cs = cs_of(cb);
  auto r = load_uint(cs, 8);
  REQUIRE(r.is_ok());
  CHECK(r.ok()->to_dec_string() == "255");

  vm::CellBuilder o; CHECK(store_uint(o, I(256), 8).is_error());
  vm::CellBuilder n; CHECK(store_uint(n, I(-1), 8).is_error());
}

TEST_CASE("uintN: 256-bit max round-trip") {
  auto max256 = Idec("115792089237316195423570985008687907853269984665640564039457584007913129639935"); // 2^256-1
  vm::CellBuilder cb;
  REQUIRE(store_uint(cb, max256, 256).is_ok());
  auto cs = cs_of(cb);
  auto r = load_uint(cs, 256);
  REQUIRE(r.is_ok());
  CHECK(r.ok()->to_dec_string() == max256->to_dec_string());
}

// native int64 / uint64 fast paths must be bit-for-bit identical to
// the RefInt256 path (same wire) at every boundary width the emitter uses.

TEST_CASE("load_int64/store_int64: boundaries at n=1,32,63,64, wire == RefInt256 path") {
  struct Case { int n; long long v; };
  for (Case c : {Case{1, 0}, Case{1, -1}, Case{8, 127}, Case{8, -128}, Case{32, 2147483647LL},
                 Case{32, -2147483648LL}, Case{63, (1LL << 62) - 1}, Case{63, -(1LL << 62)},
                 Case{64, 9223372036854775807LL}, Case{64, -9223372036854775807LL - 1}}) {
    vm::CellBuilder cb;
    REQUIRE(store_int64(cb, c.v, c.n).is_ok());
    auto root = cb.finalize();
    // wire-identical to the RefInt256 store
    vm::CellBuilder cb_ref;
    REQUIRE(store_int(cb_ref, I(c.v), c.n).is_ok());
    CHECK(root->get_hash() == cb_ref.finalize()->get_hash());
    auto cs = vm::load_cell_slice(root);
    auto r = load_int64(cs, c.n);
    REQUIRE(r.is_ok());
    CHECK(r.ok() == c.v);
    CHECK(cs.size() == 0);  // consumed exactly n bits
  }
}

TEST_CASE("store_int64: out-of-range rejected, n=1 accepts only {-1,0}") {
  vm::CellBuilder a; CHECK(store_int64(a, 128, 8).is_error());
  vm::CellBuilder b; CHECK(store_int64(b, -129, 8).is_error());
  vm::CellBuilder c; CHECK(store_int64(c, 1, 1).is_error());
  vm::CellBuilder d; CHECK(store_int64(d, 0, 1).is_ok());
  vm::CellBuilder e; CHECK(store_int64(e, -1, 1).is_ok());
}

TEST_CASE("load_int64: truncation and bad width -> error") {
  vm::CellBuilder cb;
  REQUIRE(cb.store_long_bool(0, 4));
  auto cs = vm::load_cell_slice(cb.finalize());
  CHECK(load_int64(cs, 8).is_error());
  vm::CellBuilder empty;
  auto cs2 = vm::load_cell_slice(empty.finalize());
  CHECK(load_int64(cs2, 65).is_error());  // width out of [1,64]
}

TEST_CASE("load_uint64/store_uint64: boundaries incl uint64 max, wire == RefInt256 path") {
  struct Case { int n; unsigned long long v; };
  for (Case c : {Case{1, 1}, Case{8, 255}, Case{32, 4294967295ULL}, Case{63, (1ULL << 63) - 1},
                 Case{64, 0xFFFFFFFFFFFFFFFFULL}, Case{64, 0}}) {
    vm::CellBuilder cb;
    REQUIRE(store_uint64(cb, c.v, c.n).is_ok());
    auto root = cb.finalize();
    vm::CellBuilder cb_ref;
    REQUIRE(store_uint(cb_ref, Idec(std::to_string(c.v)), c.n).is_ok());
    CHECK(root->get_hash() == cb_ref.finalize()->get_hash());
    auto cs = vm::load_cell_slice(root);
    auto r = load_uint64(cs, c.n);
    REQUIRE(r.is_ok());
    CHECK(r.ok() == c.v);
    CHECK(cs.size() == 0);
  }
}

TEST_CASE("store_uint64: overflow rejected") {
  vm::CellBuilder o; CHECK(store_uint64(o, 256, 8).is_error());
  vm::CellBuilder p; CHECK(store_uint64(p, (1ULL << 63), 63).is_error());
}

// bool

TEST_CASE("bool: round-trip both values, truncation error") {
  for (bool v : {false, true}) {
    vm::CellBuilder cb;
    REQUIRE(store_bool(cb, v).is_ok());
    auto cs = cs_of(cb);
    auto r = load_bool(cs);
    REQUIRE(r.is_ok());
    CHECK(r.ok() == v);
  }
  vm::CellBuilder empty;
  auto cs = cs_of(empty);
  CHECK(load_bool(cs).is_error());
}

// coins

TEST_CASE("coins: round-trip zero and large, reject negative") {
  for (const std::string &s : {std::string("0"), std::string("1"), std::string("1000000000000")}) {
    vm::CellBuilder cb;
    REQUIRE(store_coins(cb, Idec(s)).is_ok());
    auto cs = cs_of(cb);
    auto r = load_coins(cs);
    REQUIRE(r.is_ok());
    CHECK(r.ok()->to_dec_string() == s);
  }
  vm::CellBuilder neg; CHECK(store_coins(neg, I(-5)).is_error());
}

TEST_CASE("coins: value needing 16 bytes overflows the 4-bit size prefix") {
  auto big = Idec("1329227995784915872903807060280344576");  // 2^120 -> 16 bytes
  vm::CellBuilder cb;
  CHECK(store_coins(cb, big).is_error());
}

// varintN / varuintN

TEST_CASE("varuintN: round-trip zero/large, reject negative") {
  for (const std::string &s : {std::string("0"), std::string("255"), std::string("1267650600228229401496703205376")}) { // 2^100
    vm::CellBuilder cb;
    REQUIRE(store_varuint(cb, Idec(s), 16).is_ok());
    auto cs = cs_of(cb);
    auto r = load_varuint(cs, 16);
    REQUIRE(r.is_ok());
    CHECK(r.ok()->to_dec_string() == s);
  }
  vm::CellBuilder neg; CHECK(store_varuint(neg, I(-1), 16).is_error());
}

TEST_CASE("varintN: round-trip signed incl the -128 power-of-two edge") {
  // @ton/core writeVarInt uses ceil((bitlen(|v|)+1)/8): -128 needs 2 bytes,
  // NOT the 1 byte a two's-complement bit_size would suggest. Round-trip must
  // still recover it exactly.
  for (long long x : {0LL, 1LL, -1LL, 127LL, -128LL, 32767LL, -32768LL}) {
    vm::CellBuilder cb;
    REQUIRE(store_varint(cb, I(x), 32).is_ok());
    auto cs = cs_of(cb);
    auto r = load_varint(cs, 32);
    REQUIRE(r.is_ok());
    CHECK(r.ok()->to_dec_string() == std::to_string(x));
  }
}

TEST_CASE("varuintN: non-power-of-two width rejected at the leaf") {
  vm::CellBuilder cb;
  CHECK(store_varuint(cb, I(1), 24).is_error());
  auto cs = cs_of(cb);
  CHECK(load_varuint(cs, 24).is_error());
}

// bitsN

TEST_CASE("bitsN: round-trip exact n bits") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(0xB, 4));  // 1011
  auto cs = cs_of(src);
  auto r = load_bits(cs, 4);
  REQUIRE(r.is_ok());
  CHECK(r.ok()->size() == 4);
  CHECK(cs.size() == 0);  // consumed

  vm::CellBuilder out;
  REQUIRE(store_bits(out, *r.ok(), 4).is_ok());
  auto cs2 = cs_of(out);
  REQUIRE(cs2.size() == 4);
  CHECK(cs2.fetch_ulong(4) == 0xB);
}

TEST_CASE("bitsN: store rejects wrong-sized slice") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(0x7, 3));  // 3 bits
  auto cs = cs_of(src);
  auto r = load_bits(cs, 3);
  REQUIRE(r.is_ok());
  vm::CellBuilder out;
  CHECK(store_bits(out, *r.ok(), 4).is_error());  // claims 4, has 3
}

TEST_CASE("bitsN: load truncation -> error") {
  vm::CellBuilder src;
  REQUIRE(src.store_long_bool(0x1, 2));
  auto cs = cs_of(src);
  CHECK(load_bits(cs, 8).is_error());
}
