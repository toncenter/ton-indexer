// mapKV dict adapter. STORE is proven byte-identical to @ton/core
// storeDict (dict built both sides -> same BOC); LOAD yields entries in
// ascending key order (matching @ton/core's HashmapE order). uint8->uint8
// key/value callbacks stand in for the generated walker.

#include "AbiTestSupport.h"

#include <utility>
#include <vector>

namespace {

using namespace ton_abi;

// Root CELL HASH is the true test of label-encoding identity: it is
// order-independent of BOC cell framing (a BOC can list the same cell tree in
// different orders and still be valid), so two byte-different BOCs with the same
// root hash are the SAME cell tree -- same hashmap labels. Golden-hex / fift
// (the pack-parity medium) is likewise a per-cell dump, so hash identity is what
// makes pack parity hold.
std::string hash_hex(const td::Ref<vm::Cell> &root) { return root->get_hash().to_hex(); }

}  // namespace

TEST_CASE("mapKV: store label-encoding-identical to @ton/core storeDict (uint8->uint8)") {
  std::vector<std::pair<int, int>> entries{{1, 0xAA}, {5, 0xBB}, {3, 0xCC}};
  auto emit = [&](std::size_t idx, vm::CellBuilder &kb, vm::CellBuilder &vb) -> td::Status {
    TRY_STATUS(store_uint(kb, td::make_refint(entries[idx].first), 8));
    return store_uint(vb, td::make_refint(entries[idx].second), 8);
  };
  vm::CellBuilder cb;
  REQUIRE(store_dict(cb, 8, entries.size(), emit).is_ok());
  // Reference: beginCell().storeDict(Dictionary uint8->uint8 {1:0xAA,5:0xBB,3:0xCC}).hash().
  CHECK(hash_hex(cb.finalize()) == "6B9026AAF2F6347BFAEBCBDE1818D98F1B7FBC165934DB1455324B21CC546567");
}

TEST_CASE("mapKV: empty dict label-encoding-identical (maybe bit 0)") {
  auto emit = [](std::size_t, vm::CellBuilder &, vm::CellBuilder &) { return td::Status::OK(); };
  vm::CellBuilder cb;
  REQUIRE(store_dict(cb, 8, 0, emit).is_ok());
  CHECK(hash_hex(cb.finalize()) == "90AEC8965AFABB16EBC3CB9B408EBAE71B618D78788BC80D09843593CAC98DA4");
}

TEST_CASE("mapKV: load yields entries in ascending key order") {
  // Build via store_dict (insertion order 1,5,3), then load -> must come back
  // ascending: 1, 3, 5.
  std::vector<std::pair<int, int>> entries{{1, 0xAA}, {5, 0xBB}, {3, 0xCC}};
  auto emit = [&](std::size_t idx, vm::CellBuilder &kb, vm::CellBuilder &vb) -> td::Status {
    TRY_STATUS(store_uint(kb, td::make_refint(entries[idx].first), 8));
    return store_uint(vb, td::make_refint(entries[idx].second), 8);
  };
  vm::CellBuilder cb;
  REQUIRE(store_dict(cb, 8, entries.size(), emit).is_ok());
  auto cs = vm::load_cell_slice(cb.finalize());

  std::vector<std::pair<std::string, std::string>> out;
  auto on_entry = [&](vm::CellSlice &k, vm::CellSlice &v) -> td::Status {
    TRY_RESULT(kv, load_uint(k, 8));
    TRY_RESULT(vv, load_uint(v, 8));
    out.emplace_back(kv->to_dec_string(), vv->to_dec_string());
    return td::Status::OK();
  };
  REQUIRE(load_dict(cs, 8, on_entry).is_ok());
  std::vector<std::pair<std::string, std::string>> expect{{"1", "170"}, {"3", "204"}, {"5", "187"}};
  CHECK(out == expect);
}

TEST_CASE("mapKV: empty dict loads to zero entries") {
  auto emit = [](std::size_t, vm::CellBuilder &, vm::CellBuilder &) { return td::Status::OK(); };
  vm::CellBuilder cb;
  REQUIRE(store_dict(cb, 8, 0, emit).is_ok());
  auto cs = vm::load_cell_slice(cb.finalize());
  int calls = 0;
  auto on_entry = [&](vm::CellSlice &, vm::CellSlice &) -> td::Status {
    ++calls;
    return td::Status::OK();
  };
  REQUIRE(load_dict(cs, 8, on_entry).is_ok());
  CHECK(calls == 0);
}

TEST_CASE("mapKV: store rejects duplicate key") {
  std::vector<std::pair<int, int>> entries{{1, 0xAA}, {1, 0xBB}};
  auto emit = [&](std::size_t idx, vm::CellBuilder &kb, vm::CellBuilder &vb) -> td::Status {
    TRY_STATUS(store_uint(kb, td::make_refint(entries[idx].first), 8));
    return store_uint(vb, td::make_refint(entries[idx].second), 8);
  };
  vm::CellBuilder cb;
  CHECK(store_dict(cb, 8, entries.size(), emit).is_error());
}

TEST_CASE("mapKV: load propagates a walker error") {
  std::vector<std::pair<int, int>> entries{{1, 0xAA}};
  auto emit = [&](std::size_t idx, vm::CellBuilder &kb, vm::CellBuilder &vb) -> td::Status {
    TRY_STATUS(store_uint(kb, td::make_refint(entries[idx].first), 8));
    return store_uint(vb, td::make_refint(entries[idx].second), 8);
  };
  vm::CellBuilder cb;
  REQUIRE(store_dict(cb, 8, entries.size(), emit).is_ok());
  auto cs = vm::load_cell_slice(cb.finalize());
  auto on_entry = [&](vm::CellSlice &, vm::CellSlice &) -> td::Status {
    return td::Status::Error("walker boom");
  };
  auto r = load_dict(cs, 8, on_entry);
  REQUIRE(r.is_error());
  CHECK(r.message().str().find("boom") != std::string::npos);
}
