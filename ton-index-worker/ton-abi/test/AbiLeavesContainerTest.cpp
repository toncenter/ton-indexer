// arrayOf / lispListOf structural leaves. Round-trips + PACK
// byte-identity vs Node @ton/core references; arrayOf LOAD accepts both the
// 1-elem/ref form and the compiler-chunked form. Element (un)pack is a uint8
// callback standing in for the generated walker.

#include "AbiTestSupport.h"

#include <vector>

namespace {

using namespace ton_abi;

std::string boc_b64(const td::Ref<vm::Cell> &root) {
  auto r = vm::std_boc_serialize(root, 2);
  REQUIRE(r.is_ok());
  return td::base64_encode(r.move_as_ok().as_slice());
}

// uint8 element callbacks standing in for the walker.
std::function<td::Status(vm::CellBuilder &, std::size_t)> packer(const std::vector<int> &vals) {
  return [&vals](vm::CellBuilder &b, std::size_t idx) { return store_uint(b, td::make_refint(vals[idx]), 8); };
}
std::function<td::Status(vm::CellSlice &)> collector(std::vector<std::string> &out) {
  return [&out](vm::CellSlice &s) -> td::Status {
    TRY_RESULT(v, load_uint(s, 8));
    out.push_back(v->to_dec_string());
    return td::Status::OK();
  };
}

}  // namespace

// arrayOf

TEST_CASE("arrayOf: pack byte-identical to @ton/core, load round-trips") {
  std::vector<int> vals{10, 20, 30};
  vm::CellBuilder cb;
  REQUIRE(store_array(cb, vals.size(), packer(vals)).is_ok());
  auto root = cb.finalize();
  CHECK(boc_b64(root) == "te6cckEBBAEAEwABAwPAAQEDhUACAQOKQAMAAw9AAaHIQg==");

  auto cs = vm::load_cell_slice(root);
  std::vector<std::string> out;
  REQUIRE(load_array(cs, collector(out)).is_ok());
  CHECK(out == std::vector<std::string>{"10", "20", "30"});
}

TEST_CASE("arrayOf: empty array pack byte-identical + load") {
  std::vector<int> vals;
  vm::CellBuilder cb;
  REQUIRE(store_array(cb, 0, packer(vals)).is_ok());
  auto root = cb.finalize();
  CHECK(boc_b64(root) == "te6cckEBAQEABAAAAwBABu7M8A==");

  auto cs = vm::load_cell_slice(root);
  std::vector<std::string> out;
  REQUIRE(load_array(cs, collector(out)).is_ok());
  CHECK(out.empty());
}

TEST_CASE("arrayOf: LOAD accepts the compiler-chunked form (many elements per cell)") {
  // Hand-build the chunked form: length 3, one chunk cell holding all three
  // uint8 elements (after its own maybeRef=0), 0 continuation refs.
  vm::CellBuilder chunk;
  REQUIRE(store_maybe_ref(chunk, td::Ref<vm::Cell>{}).is_ok());  // next-chunk = none
  REQUIRE(chunk.store_long_bool(10, 8));
  REQUIRE(chunk.store_long_bool(20, 8));
  REQUIRE(chunk.store_long_bool(30, 8));
  vm::CellBuilder outer;
  REQUIRE(outer.store_long_bool(3, 8));
  REQUIRE(store_maybe_ref(outer, chunk.finalize()).is_ok());

  auto cs = vm::load_cell_slice(outer.finalize());
  std::vector<std::string> out;
  REQUIRE(load_array(cs, collector(out)).is_ok());
  CHECK(out == std::vector<std::string>{"10", "20", "30"});
}

TEST_CASE("arrayOf: length-prefix mismatch -> error") {
  // Prefix claims 3, but the chunk holds only 2 elements.
  vm::CellBuilder chunk;
  REQUIRE(store_maybe_ref(chunk, td::Ref<vm::Cell>{}).is_ok());
  REQUIRE(chunk.store_long_bool(7, 8));
  REQUIRE(chunk.store_long_bool(8, 8));
  vm::CellBuilder outer;
  REQUIRE(outer.store_long_bool(3, 8));  // claims 3
  REQUIRE(store_maybe_ref(outer, chunk.finalize()).is_ok());
  auto cs = vm::load_cell_slice(outer.finalize());
  std::vector<std::string> out;
  CHECK(load_array(cs, collector(out)).is_error());
}

TEST_CASE("arrayOf: store rejects length > 255") {
  auto never = [](vm::CellBuilder &, std::size_t) { return td::Status::OK(); };
  vm::CellBuilder cb;
  CHECK(store_array(cb, 256, never).is_error());
}

// lispListOf

TEST_CASE("lispListOf: pack byte-identical to @ton/core, load round-trips") {
  std::vector<int> vals{1, 2, 3};
  vm::CellBuilder cb;
  REQUIRE(store_lisp_list(cb, vals.size(), packer(vals)).is_ok());
  auto root = cb.finalize();
  CHECK(boc_b64(root) == "te6cckEBBQEAEQABAAEBAgMCAQICAwECAQQAAOoxPXg=");

  auto cs = vm::load_cell_slice(root);
  std::vector<std::string> out;
  REQUIRE(load_lisp_list(cs, collector(out)).is_ok());
  CHECK(out == std::vector<std::string>{"1", "2", "3"});
}

TEST_CASE("lispListOf: empty list pack byte-identical + load") {
  std::vector<int> vals;
  vm::CellBuilder cb;
  REQUIRE(store_lisp_list(cb, 0, packer(vals)).is_ok());
  auto root = cb.finalize();
  CHECK(boc_b64(root) == "te6cckEBAgEABQABAAEAAG4cXEQ=");

  auto cs = vm::load_cell_slice(root);
  std::vector<std::string> out;
  REQUIRE(load_lisp_list(cs, collector(out)).is_ok());
  CHECK(out.empty());
}

TEST_CASE("lispListOf: cons node not fully consumed by element -> error") {
  // Build a cons node with 16 bits but read only 8 -> 8 bits left -> reject.
  vm::CellBuilder empty;
  auto nil = empty.finalize();
  vm::CellBuilder node;
  REQUIRE(node.store_long_bool(0xABCD, 16));
  REQUIRE(node.store_ref_bool(nil));
  vm::CellBuilder outer;
  REQUIRE(outer.store_ref_bool(node.finalize()));
  auto cs = vm::load_cell_slice(outer.finalize());
  std::vector<std::string> out;
  CHECK(load_lisp_list(cs, collector(out)).is_error());  // collector reads 8, leaves 8
}
