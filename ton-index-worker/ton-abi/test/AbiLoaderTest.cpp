// AbiLoader fail-closed unit tests. Each REJECT case hand-builds a
// minimal ABI JSON that violates EXACTLY one invariant; each ACCEPT case
// either uses the minimal valid skeleton or one of the 3 committed fixture
// ABIs from testdata/fixtures/. Together these mirror the loader's invariant
// list.

#include "AbiTestSupport.h"

namespace {

using ton_abi_test::read_file;

// Minimal ABI skeleton that parses successfully on its own (used as the base
// for every REJECT test -- exactly one field is broken per test).
const char *kMinimalValid = R"JSON({
  "contract_name": "Minimal",
  "abi_schema_version": "1.0",
  "unique_types": [ {"kind": "int"} ],
  "struct_instantiations": [],
  "alias_instantiations": [],
  "declarations": [],
  "storage": {},
  "incoming_messages": [],
  "incoming_external": [],
  "outgoing_messages": [],
  "emitted_events": [],
  "get_methods": [],
  "thrown_errors": [],
  "compiler_name": "tolk",
  "compiler_version": "1.4.1"
})JSON";

}  // namespace

TEST_CASE("AbiLoader accept: minimal valid skeleton") {
  auto r = ton_abi::load_abi_from_json(kMinimalValid);
  REQUIRE(r.is_ok());
  auto abi = r.move_as_ok();
  CHECK(abi.contract_name == "Minimal");
  CHECK(abi.unique_types.size() == 1);
  CHECK(abi.declarations.empty());
}

TEST_CASE("AbiLoader accept: all 18 committed fixture ABIs load") {
  for (const std::string &name : ton_abi_test::kLoadableFixtures) {
    CAPTURE(name);
    std::string path = std::string(TON_ABI_FIXTURES_DIR) + "/" + name + ".abi.json";
    auto r = ton_abi::load_abi_from_json(read_file(path));
    REQUIRE_MESSAGE(r.is_ok(), name << ": " << (r.is_error() ? r.error().message().str() : ""));
    auto abi = r.move_as_ok();
    CHECK(abi.abi_schema_version == "1.0");
    CHECK_FALSE(abi.unique_types.empty());
    // NOT declarations.empty(): err-cont-on-stack-2 legitimately has zero
    // struct/alias/enum declarations (a getter-only fixture -- just the
    // stack-level `cont` type, no ABI declarations at all).
  }
}

TEST_CASE("AbiLoader reject: unknown Ty kind") {
  auto r = ton_abi::load_abi_from_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [ {"kind": "totally_bogus_kind"} ],
    "struct_instantiations": [], "alias_instantiations": [], "declarations": [],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("unknown Ty kind") != std::string::npos);
}

TEST_CASE("AbiLoader reject: ty_idx out of bounds") {
  auto r = ton_abi::load_abi_from_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [ {"kind": "int"} ],
    "struct_instantiations": [], "alias_instantiations": [],
    "declarations": [
      {"kind": "struct", "name": "S", "ty_idx": 0, "fields": [
        {"name": "f", "ty_idx": 999}
      ]}
    ],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("out of bounds") != std::string::npos);
}

TEST_CASE("AbiLoader reject: missing required field") {
  // struct declaration missing 'fields'
  auto r = ton_abi::load_abi_from_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [ {"kind": "int"} ],
    "struct_instantiations": [], "alias_instantiations": [],
    "declarations": [
      {"kind": "struct", "name": "S", "ty_idx": 0}
    ],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("fields") != std::string::npos);
}

TEST_CASE("AbiLoader reject: abi_schema_version != 1.0") {
  auto r = ton_abi::load_abi_from_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "2.0",
    "unique_types": [ {"kind": "int"} ],
    "struct_instantiations": [], "alias_instantiations": [], "declarations": [],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("abi_schema_version") != std::string::npos);
}

TEST_CASE("AbiLoader reject: malformed bigint") {
  auto r = ton_abi::load_abi_from_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [ {"kind": "int"} ],
    "struct_instantiations": [], "alias_instantiations": [],
    "declarations": [
      {"kind": "enum", "name": "E", "ty_idx": 0, "encoded_as_ty_idx": 0,
       "members": [ {"name": "A", "value": "not_a_number"} ]}
    ],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("malformed bigint") != std::string::npos);
}

TEST_CASE("AbiLoader reject: duplicate decl names") {
  auto r = ton_abi::load_abi_from_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [ {"kind": "int"} ],
    "struct_instantiations": [], "alias_instantiations": [],
    "declarations": [
      {"kind": "struct", "name": "Dup", "ty_idx": 0, "fields": []},
      {"kind": "struct", "name": "Dup", "ty_idx": 0, "fields": []}
    ],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("duplicate") != std::string::npos);
}

TEST_CASE("AbiLoader reject: struct_instantiation arity mismatch") {
  auto r = ton_abi::load_abi_from_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [ {"kind": "int"}, {"kind": "genericT", "name_t": "T"} ],
    "struct_instantiations": [
      {"ty_idx": 0, "struct_name": "Wrapper", "monomorphic_fields_ty_idx": []}
    ],
    "alias_instantiations": [],
    "declarations": [
      {"kind": "struct", "name": "Wrapper", "ty_idx": 0, "type_params": ["T"], "fields": [
        {"name": "item", "ty_idx": 1}
      ]}
    ],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("arity mismatch") != std::string::npos);
}

TEST_CASE("AbiLoader reject: StructRef type_args arity mismatch") {
  auto r = ton_abi::load_abi_from_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [
      {"kind": "int"},
      {"kind": "StructRef", "struct_name": "Wrapper", "type_args_ty_idx": [0, 0]}
    ],
    "struct_instantiations": [], "alias_instantiations": [],
    "declarations": [
      {"kind": "struct", "name": "Wrapper", "ty_idx": 0, "type_params": ["T"], "fields": [
        {"name": "item", "ty_idx": 0}
      ]}
    ],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("arity mismatch") != std::string::npos);
}

TEST_CASE("AbiLoader reject: intN width out of range") {
  auto r = ton_abi::load_abi_from_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [ {"kind": "intN", "n": 999} ],
    "struct_instantiations": [], "alias_instantiations": [], "declarations": [],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("out of allowed range") != std::string::npos);
}

TEST_CASE("AbiLoader reject: uintN width out of range (negative)") {
  auto r = ton_abi::load_abi_from_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [ {"kind": "uintN", "n": 0} ],
    "struct_instantiations": [], "alias_instantiations": [], "declarations": [],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("out of allowed range") != std::string::npos);
}

TEST_CASE("AbiLoader reject: bitsN width out of range") {
  auto r = ton_abi::load_abi_from_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [ {"kind": "bitsN", "n": 5000} ],
    "struct_instantiations": [], "alias_instantiations": [], "declarations": [],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("out of allowed range") != std::string::npos);
}

TEST_CASE("AbiLoader reject: alias cycle") {
  auto r = ton_abi::load_abi_from_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [
      {"kind": "AliasRef", "alias_name": "B"},
      {"kind": "AliasRef", "alias_name": "A"}
    ],
    "struct_instantiations": [], "alias_instantiations": [],
    "declarations": [
      {"kind": "alias", "name": "A", "ty_idx": 0, "target_ty_idx": 0},
      {"kind": "alias", "name": "B", "ty_idx": 1, "target_ty_idx": 1}
    ],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("alias cycle") != std::string::npos);
}

TEST_CASE("AbiLoader reject: union prefix-freedom violation") {
  auto r = ton_abi::load_abi_from_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [
      {"kind": "int"},
      {"kind": "union", "variants": [
        {"variant_ty_idx": 0, "prefix_num": 0, "prefix_len": 1},
        {"variant_ty_idx": 0, "prefix_num": 0, "prefix_len": 2}
      ]}
    ],
    "struct_instantiations": [], "alias_instantiations": [], "declarations": [],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("prefix-freedom") != std::string::npos);
}

TEST_CASE("AbiLoader reject: union variant prefix_len > 32") {
  auto r = ton_abi::load_abi_from_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [
      {"kind": "int"},
      {"kind": "union", "variants": [
        {"variant_ty_idx": 0, "prefix_num": 0, "prefix_len": 40}
      ]}
    ],
    "struct_instantiations": [], "alias_instantiations": [], "declarations": [],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("prefix_len") != std::string::npos);
}

TEST_CASE("AbiLoader accept: struct prefix_len > 32 (real Tolk contracts do this)") {
  // MsgSinglePrefix48 in the committed lots-of-wrappers fixture has a real
  // 48-bit struct prefix -- unlike union-variant dispatch prefixes, struct
  // opcode prefixes are NOT bound to 32 bits (only to the cell bit limit).
  auto r = ton_abi::load_abi_from_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [ {"kind": "int"} ],
    "struct_instantiations": [], "alias_instantiations": [],
    "declarations": [
      {"kind": "struct", "name": "S", "ty_idx": 0,
       "prefix": {"prefix_num": 1, "prefix_len": 48}, "fields": []}
    ],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  REQUIRE(r.is_ok());
}

TEST_CASE("AbiLoader reject: struct prefix_len > 1023 (cell bit-width limit)") {
  auto r = ton_abi::load_abi_from_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [ {"kind": "int"} ],
    "struct_instantiations": [], "alias_instantiations": [],
    "declarations": [
      {"kind": "struct", "name": "S", "ty_idx": 0,
       "prefix": {"prefix_num": 1, "prefix_len": 2000}, "fields": []}
    ],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  REQUIRE(r.is_error());
  CHECK(r.error().message().str().find("prefix_len") != std::string::npos);
}
