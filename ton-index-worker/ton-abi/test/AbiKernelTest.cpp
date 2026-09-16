// AbiKernel resolution-layer units: struct fields, alias targets, union
// labels, plus the generic fixtures (ResetTo<int64>, GenericPair T1|T2)
// and the three named invariants exercised below.

#include "AbiTestSupport.h"

namespace {

using ton_abi::AbiKernel;
using ton_abi::ContractABI;
using ton_abi_test::load_fixture_abi;

ContractABI load_json(const std::string &json) {
  auto r = ton_abi::load_abi_from_json(json);
  REQUIRE_MESSAGE(r.is_ok(), (r.is_error() ? r.error().message().str() : ""));
  return r.move_as_ok();
}

const ton_abi::TyUnion &union_at(const AbiKernel &k, int ty_idx) {
  auto r = k.ty_by_idx(ty_idx);
  REQUIRE(r.is_ok());
  return std::get<ton_abi::TyUnion>(r.ok()->data);
}

}  // namespace


TEST_CASE("AbiKernel create: all committed fixture ABIs build a kernel") {
  for (const std::string &name : ton_abi_test::kLoadableFixtures) {
    CAPTURE(name);
    ContractABI abi = load_fixture_abi(name);
    auto kr = AbiKernel::create(abi);
    REQUIRE_MESSAGE(kr.is_ok(), name << ": " << (kr.is_error() ? kr.error().message().str() : ""));
  }
}


TEST_CASE("AbiKernel SymTable: struct/alias/enum lookup + not-found errors") {
  ContractABI abi = load_fixture_abi("lots-of-messages");
  auto k = AbiKernel::create(abi).move_as_ok();

  auto s = k.get_struct("IncreaseBy");
  REQUIRE(s.is_ok());
  CHECK(s.ok()->name == "IncreaseBy");

  CHECK(k.get_struct("NoSuchStruct").is_error());
  CHECK(k.get_alias("NoSuchAlias").is_error());
  CHECK(k.get_enum("NoSuchEnum").is_error());
}

TEST_CASE("AbiKernel ty_by_idx: bounds") {
  ContractABI abi = load_fixture_abi("lots-of-messages");
  auto k = AbiKernel::create(abi).move_as_ok();
  CHECK(k.ty_by_idx(0).is_ok());
  CHECK(k.ty_by_idx(-1).is_error());
  CHECK(k.ty_by_idx(100000).is_error());
}


TEST_CASE("AbiKernel structFieldsOf: non-generic struct -> fields verbatim, no uLabel") {
  ContractABI abi = load_fixture_abi("lots-of-messages");
  auto k = AbiKernel::create(abi).move_as_ok();
  // ty_idx 12 = StructRef IncreaseBy (non-generic): counter_id:int32, inc_by:int32.
  auto fr = k.struct_fields_of(12);
  REQUIRE(fr.is_ok());
  const auto &fields = *fr.ok();
  REQUIRE(fields.size() == 2);
  CHECK(fields[0].name() == "counter_id");
  CHECK(fields[0].ty_idx == 8);
  CHECK_FALSE(fields[0].u_label_ty_idx.has_value());
  CHECK(fields[1].name() == "inc_by");
  CHECK(fields[1].ty_idx == 8);
  CHECK_FALSE(fields[1].u_label_ty_idx.has_value());
}

TEST_CASE("AbiKernel structFieldsOf: generic instantiation swaps ty_idx, keeps original as uLabel") {
  ContractABI abi = load_fixture_abi("lots-of-messages");
  auto k = AbiKernel::create(abi).move_as_ok();
  // ty_idx 13 = StructRef ResetTo<int64>; decl fields counter_id:int32(8),
  // reset_to:T(14 genericT); instantiation monomorphic_fields = [8, 10].
  auto fr = k.struct_fields_of(13);
  REQUIRE(fr.is_ok());
  const auto &fields = *fr.ok();
  REQUIRE(fields.size() == 2);
  CHECK(fields[0].name() == "counter_id");
  CHECK(fields[0].ty_idx == 8);
  REQUIRE(fields[0].u_label_ty_idx.has_value());
  CHECK(*fields[0].u_label_ty_idx == 8);
  CHECK(fields[1].name() == "reset_to");
  CHECK(fields[1].ty_idx == 10);            // T -> int64 (monomorph)
  REQUIRE(fields[1].u_label_ty_idx.has_value());
  CHECK(*fields[1].u_label_ty_idx == 14);   // original genericT idx retained
}

TEST_CASE("AbiKernel structFieldsOf: client_ty_idx swaps to cell type and drops uLabel") {
  // Hand-built: field `f` has cell-side client_ty_idx pointing at `cell` (idx 1)
  // while its stack ty_idx is int (idx 0). Cell side must use the client type.
  ContractABI abi = load_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [ {"kind": "int"}, {"kind": "cell"},
      {"kind": "StructRef", "struct_name": "S"} ],
    "struct_instantiations": [], "alias_instantiations": [],
    "declarations": [
      {"kind": "struct", "name": "S", "ty_idx": 2, "fields": [
        {"name": "f", "ty_idx": 0, "client_ty_idx": 1}
      ]}
    ],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  auto k = AbiKernel::create(abi).move_as_ok();
  auto fr = k.struct_fields_of(2);
  REQUIRE(fr.is_ok());
  const auto &fields = *fr.ok();
  REQUIRE(fields.size() == 1);
  CHECK(fields[0].ty_idx == 1);            // swapped to client (cell)
  CHECK_FALSE(fields[0].u_label_ty_idx.has_value());
}

TEST_CASE("AbiKernel structFieldsOf: not a StructRef -> error") {
  ContractABI abi = load_fixture_abi("lots-of-messages");
  auto k = AbiKernel::create(abi).move_as_ok();
  CHECK(k.struct_fields_of(8).is_error());  // ty 8 = intN, not a StructRef
}


TEST_CASE("AbiKernel aliasTargetOf: generic instantiation -> monomorph target + uLabel") {
  ContractABI abi = load_fixture_abi("generic-union-labels");
  auto k = AbiKernel::create(abi).move_as_ok();
  // ty 47 = AliasRef TlbEither<StateInit, Cell<StateInit>>; decl target 57,
  // instantiation monomorphic_target 53.
  auto tr = k.alias_target_of(47);
  REQUIRE(tr.is_ok());
  CHECK(tr.ok().ty_idx == 53);
  REQUIRE(tr.ok().u_label_ty_idx.has_value());
  CHECK(*tr.ok().u_label_ty_idx == 57);
}

TEST_CASE("AbiKernel aliasTargetOf: non-instantiated alias -> decl target, no uLabel") {
  ContractABI abi = load_fixture_abi("generic-union-labels");
  auto k = AbiKernel::create(abi).move_as_ok();
  // ty 29 = AliasRef AliasOrInt8<T> (generic decl form, no instantiation at 29);
  // decl target 26.
  auto tr = k.alias_target_of(29);
  REQUIRE(tr.is_ok());
  CHECK(tr.ok().ty_idx == 26);
  CHECK_FALSE(tr.ok().u_label_ty_idx.has_value());
}

TEST_CASE("AbiKernel aliasTargetOf: not an AliasRef -> error") {
  ContractABI abi = load_fixture_abi("generic-union-labels");
  auto k = AbiKernel::create(abi).move_as_ok();
  CHECK(k.alias_target_of(8).is_error());  // intN
}


TEST_CASE("AbiKernel createLabelsForUnion: primitive variants get $ + value labels") {
  ContractABI abi = load_fixture_abi("generic-union-labels");
  auto k = AbiKernel::create(abi).move_as_ok();
  // ty 14 = union int32 | int64 (the monomorphic form of GenericPair<int32,int64>).
  const auto &u = union_at(k, 14);
  auto lr = k.create_labels_for_union(u.variants, std::nullopt);
  REQUIRE(lr.is_ok());
  const auto &labels = lr.ok();
  REQUIRE(labels.size() == 2);
  CHECK(labels[0].label_str == "int32");
  CHECK(labels[0].has_value_field);
  CHECK(labels[1].label_str == "int64");
  CHECK(labels[1].has_value_field);
}

TEST_CASE("AbiKernel createLabelsForUnion: uLabel makes generic union label as T1|T2 (invariant i)") {
  ContractABI abi = load_fixture_abi("generic-union-labels");
  auto k = AbiKernel::create(abi).move_as_ok();
  // Same interned union ty 14, but arriving from GenericPair<int32,int64>.value
  // whose uLabel back-ref is the generic union ty 18 (T1 | T2). Labels must be
  // T1/T2, NOT int32/int64 -- the use-site property that invariant (i) guards.
  const auto &u = union_at(k, 14);
  auto lr = k.create_labels_for_union(u.variants, 18);
  REQUIRE(lr.is_ok());
  const auto &labels = lr.ok();
  REQUIRE(labels.size() == 2);
  CHECK(labels[0].label_str == "T1");
  CHECK(labels[0].variant_ty_idx == 8);   // still (de)serializes the ACTUAL int32
  CHECK(labels[0].has_value_field);
  CHECK(labels[1].label_str == "T2");
  CHECK(labels[1].variant_ty_idx == 10);
}

TEST_CASE("AbiKernel createLabelsForUnion: struct variants inline (own label, no value)") {
  ContractABI abi = load_fixture_abi("generic-union-labels");
  auto k = AbiKernel::create(abi).move_as_ok();
  // ty 53 = union TlbEitherLeft<StateInit> | TlbEitherRight<Cell<StateInit>>.
  const auto &u = union_at(k, 53);
  auto lr = k.create_labels_for_union(u.variants, std::nullopt);
  REQUIRE(lr.is_ok());
  const auto &labels = lr.ok();
  REQUIRE(labels.size() == 2);
  CHECK(labels[0].label_str == "TlbEitherLeft");
  CHECK_FALSE(labels[0].has_value_field);
  CHECK(labels[1].label_str == "TlbEitherRight");
  CHECK_FALSE(labels[1].has_value_field);
}

TEST_CASE("AbiKernel createLabelsForUnion: duplicate short labels fall back to full renderTy + value") {
  // Hand-built union int32 | int32 (prefix-free via implicit 0/1). createLabel
  // collides ('int32','int32') -> full renderTy labels + forced value field.
  ContractABI abi = load_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [
      {"kind": "intN", "n": 32},
      {"kind": "union", "variants": [
        {"variant_ty_idx": 0, "prefix_num": 0, "prefix_len": 1, "is_prefix_implicit": true},
        {"variant_ty_idx": 0, "prefix_num": 1, "prefix_len": 1, "is_prefix_implicit": true}
      ]}
    ],
    "struct_instantiations": [], "alias_instantiations": [], "declarations": [],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  auto k = AbiKernel::create(abi).move_as_ok();
  const auto &u = union_at(k, 1);
  auto lr = k.create_labels_for_union(u.variants, std::nullopt);
  REQUIRE(lr.is_ok());
  const auto &labels = lr.ok();
  REQUIRE(labels.size() == 2);
  CHECK(labels[0].label_str == "int32");
  CHECK(labels[0].has_value_field);
  CHECK(labels[1].label_str == "int32");
  CHECK(labels[1].has_value_field);
}

TEST_CASE("AbiKernel createLabelsForUnion: nullLiteral variant -> empty label, no value") {
  ContractABI abi = load_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [
      {"kind": "intN", "n": 32},
      {"kind": "nullLiteral"},
      {"kind": "union", "variants": [
        {"variant_ty_idx": 1, "prefix_num": 0, "prefix_len": 1, "is_prefix_implicit": true},
        {"variant_ty_idx": 0, "prefix_num": 1, "prefix_len": 1, "is_prefix_implicit": true}
      ]}
    ],
    "struct_instantiations": [], "alias_instantiations": [], "declarations": [],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  auto k = AbiKernel::create(abi).move_as_ok();
  const auto &u = union_at(k, 2);
  auto lr = k.create_labels_for_union(u.variants, std::nullopt);
  REQUIRE(lr.is_ok());
  const auto &labels = lr.ok();
  REQUIRE(labels.size() == 2);
  CHECK(labels[0].label_str == "");
  CHECK_FALSE(labels[0].has_value_field);
  CHECK(labels[1].label_str == "int32");
  CHECK(labels[1].has_value_field);
}


TEST_CASE("AbiKernel resolve_union: dispatch order sorted by prefix, labels attached") {
  // Hand-built union with declaration order NOT sorted by prefix (len 3 before
  // len 1). Dispatch order must come out sorted by (prefix_len, prefix_num);
  // prefix-freedom (loader-validated) makes this behaviour-preserving.
  ContractABI abi = load_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [
      {"kind": "intN", "n": 32},
      {"kind": "union", "variants": [
        {"variant_ty_idx": 0, "prefix_num": 7, "prefix_len": 3},
        {"variant_ty_idx": 0, "prefix_num": 0, "prefix_len": 1}
      ]}
    ],
    "struct_instantiations": [], "alias_instantiations": [], "declarations": [],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  auto k = AbiKernel::create(abi).move_as_ok();
  auto ru = k.resolve_union(1, std::nullopt);
  REQUIRE(ru.is_ok());
  const auto &u = ru.ok();
  CHECK_FALSE(u.has_void);
  REQUIRE(u.variants.size() == 2);
  REQUIRE(u.dispatch_order.size() == 2);
  // variants stay in DECLARATION order; dispatch_order indexes them sorted.
  CHECK(u.dispatch_order[0] == 1);   // prefix_len 1 first
  CHECK(u.dispatch_order[1] == 0);   // prefix_len 3 second
}

TEST_CASE("AbiKernel resolve_union: trailing void detected, excluded from dispatch") {
  ContractABI abi = load_json(R"JSON({
    "contract_name": "X", "abi_schema_version": "1.0",
    "unique_types": [
      {"kind": "intN", "n": 32},
      {"kind": "void"},
      {"kind": "union", "variants": [
        {"variant_ty_idx": 0, "prefix_num": 0, "prefix_len": 1, "is_prefix_implicit": true},
        {"variant_ty_idx": 1, "prefix_num": 0, "prefix_len": 0}
      ]}
    ],
    "struct_instantiations": [], "alias_instantiations": [], "declarations": [],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name": "tolk", "compiler_version": "1.4.1"
  })JSON");
  auto k = AbiKernel::create(abi).move_as_ok();
  auto ru = k.resolve_union(2, std::nullopt);
  REQUIRE(ru.is_ok());
  const auto &u = ru.ok();
  CHECK(u.has_void);
  REQUIRE(u.variants.size() == 2);          // void variant still present, last
  REQUIRE(u.dispatch_order.size() == 1);    // but excluded from dispatch
  CHECK(u.dispatch_order[0] == 0);
}

TEST_CASE("AbiKernel resolve_union: not a union -> error") {
  ContractABI abi = load_fixture_abi("lots-of-messages");
  auto k = AbiKernel::create(abi).move_as_ok();
  CHECK(k.resolve_union(8, std::nullopt).is_error());  // intN
}
