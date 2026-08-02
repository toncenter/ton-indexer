// ABI-inspection tests port the pure ABI-parse assertions from OnlyHeader/
// LotsOfAnnotations/
// LotsOfThrows/LotsOfStorage/HasNotInitializedStorage.spec.ts onto our already
// -committed .abi.json fixtures, using AbiModel/AbiLoader/AbiKernel accessors
// directly (no AbiInterp needed -- these are declaration/type-tree
// inspections, not (de)serialization).
//
// Deliberately out of scope:
//  - LotsOfThrows' `isDefaultValueSupported` / `CodegenCtx` check -- a CODEGEN
//    concern (emit-field-defs.ts), belongs with the emitter's defaults
//    support gate, not the loader/kernel. We still assert every
//    WithUnsupportedDefaults field HAS a default_value (that much is a loader
//    fact), just not whether the emitter could legally encode it.
//  - HasNotInitializedStorage's address/shard-derivation tests ("toShard and
//    workchain", "fromStorage takes only 2 args") -- these need real contract-
//    address derivation (StateInit hash + shard-prefix rewriting via
//    fixedPrefixLength/closeTo), which is new, non-trivial surface (shard
//    prefix math), not "genuinely cheap" per the lead's delegated criterion.
//    SKIPPED, deferred register. Only the pure ABI-parse fact (the
//    storage_at_deployment_ty_idx points at the right struct) is ported below.

#include "AbiTestSupport.h"

namespace {

using namespace ton_abi;
using ton_abi_test::load_fixture_abi;

const ABIStruct *find_struct(const ContractABI &abi, const std::string &name) {
  for (const auto &d : abi.declarations) {
    if (d.kind == DeclKind::Struct && d.as_struct.name == name) return &d.as_struct;
  }
  return nullptr;
}
const ABIAlias *find_alias(const ContractABI &abi, const std::string &name) {
  for (const auto &d : abi.declarations) {
    if (d.kind == DeclKind::Alias && d.as_alias.name == name) return &d.as_alias;
  }
  return nullptr;
}
const ABIEnum *find_enum(const ContractABI &abi, const std::string &name) {
  for (const auto &d : abi.declarations) {
    if (d.kind == DeclKind::Enum && d.as_enum.name == name) return &d.as_enum;
  }
  return nullptr;
}

}  // namespace

TEST_CASE("W10 OnlyHeader: outgoing/emitted/thrown_errors from the contract header (spec :18-54)") {
  ContractABI abi = load_fixture_abi("only-header");

  SUBCASE("outgoing messages: StructRef then AliasRef, descriptions on both sides of the alias") {
    REQUIRE(abi.outgoing_messages.size() == 2);
    const Ty &t0 = abi.unique_types[static_cast<std::size_t>(abi.outgoing_messages[0].body_ty_idx)];
    const Ty &t1 = abi.unique_types[static_cast<std::size_t>(abi.outgoing_messages[1].body_ty_idx)];
    REQUIRE(t0.kind == TyKind::StructRef);
    CHECK(std::get<TyStructRef>(t0.data).struct_name == "OutMsgA");
    REQUIRE(t1.kind == TyKind::AliasRef);
    CHECK(std::get<TyAliasRef>(t1.data).alias_name == "AliasOutMsgB");

    const ABIAlias *a = find_alias(abi, "AliasOutMsgB");
    const ABIStruct *s = find_struct(abi, "OutMsgB");
    REQUIRE(a != nullptr);
    REQUIRE(s != nullptr);
    CHECK(a->description == "desc OutMsgB");
    CHECK(s->description == "desc OutMsgB");
  }

  SUBCASE("emitted events: single StructRef") {
    REQUIRE(abi.emitted_events.size() == 1);
    const Ty &t = abi.unique_types[static_cast<std::size_t>(abi.emitted_events[0].body_ty_idx)];
    REQUIRE(t.kind == TyKind::StructRef);
    CHECK(std::get<TyStructRef>(t.data).struct_name == "OutMsgExtA");
  }

  SUBCASE("thrown errors: enum_member with description, matches the enum's own member") {
    REQUIRE(abi.thrown_errors.size() == 1);
    const ABIThrownError &te = abi.thrown_errors[0];
    CHECK(te.kind == "enum_member");
    CHECK(te.name.value() == "ErrCo.NotFound");
    CHECK(te.description.value() == "desc NotFound");
    CHECK(te.err_code == 404);

    const ABIEnum *e = find_enum(abi, "ErrCo");
    REQUIRE(e != nullptr);
    REQUIRE(e->members.size() == 1);
    CHECK(e->members[0].name == "NotFound");
    CHECK(e->members[0].value->to_dec_string() == "404");
    // ABIEnumMember carries
    // {name, value, description}; parse_enum reads the JSON's per-member
    // "description" field (previously silently dropped).
    REQUIRE(e->members[0].description.has_value());
    CHECK(e->members[0].description.value() == "desc NotFound");
  }
}

TEST_CASE("W10 LotsOfAnnotations: contract properties, message/getter descriptions (spec :29-85)") {
  ContractABI abi = load_fixture_abi("lots-of-annotations");

  SUBCASE("ABI common properties (compiler_version PINNED exact, not the reference's regex)") {
    CHECK(abi.contract_name == "LotsOfAnnotations");
    CHECK(abi.author.value() == "A K");
    CHECK(abi.version.value() == "1.0");
    CHECK(abi.description.value() == "some d");
    CHECK(abi.compiler_name == "tolk");
    CHECK(abi.compiler_version == "1.4.1");
  }

  SUBCASE("incoming messages: Msg1 description, generic ResetTo<int8> description, external shape") {
    auto ty_of = [&](int ty_idx) -> const Ty & { return abi.unique_types[static_cast<std::size_t>(ty_idx)]; };

    const ABIInternalMessage *msg1 = nullptr;
    const ABIInternalMessage *reset = nullptr;
    for (const auto &m : abi.incoming_messages) {
      const Ty &t = ty_of(m.body_ty_idx);
      if (t.kind == TyKind::StructRef && std::get<TyStructRef>(t.data).struct_name == "Msg1") msg1 = &m;
      if (t.kind == TyKind::StructRef && !std::get<TyStructRef>(t.data).type_args_ty_idx.empty()) reset = &m;
    }
    REQUIRE(msg1 != nullptr);
    REQUIRE(reset != nullptr);
    CHECK(find_struct(abi, "Msg1")->description == "mmm1\nmmm2");
    CHECK(find_struct(abi, "ResetTo")->description == "mmmReset");

    REQUIRE(abi.incoming_external.size() == 1);
    const Ty &ext_ty = ty_of(abi.incoming_external[0].body_ty_idx);
    REQUIRE(ext_ty.kind == TyKind::StructRef);
    CHECK(std::get<TyStructRef>(ext_ty.data).struct_name == "ActualExternalShape");
    CHECK(find_struct(abi, "ActualExternalShape")->description == "mmmShape");
  }

  SUBCASE("get methods: parsed even though getter EXECUTION is out of scope (plan decision 1) -- the parse itself is cheap and free") {
    const ABIGetMethod *getFirst = nullptr;
    for (const auto &gm : abi.get_methods) {
      if (gm.name == "getFirst") getFirst = &gm;
    }
    REQUIRE(getFirst != nullptr);
    CHECK(getFirst->description.value() == "get1");
    CHECK(getFirst->tvm_method_id == 90137);
    REQUIRE(getFirst->parameters.size() >= 1);
    CHECK(getFirst->parameters[0].name == "spec");
    CHECK(getFirst->parameters[0].description.value() == "some number");
    REQUIRE(getFirst->parameters[0].default_value.has_value());
    CHECK(getFirst->parameters[0].default_value->kind == ConstExprKind::Int);
    CHECK(std::get<ConstExprInt>(getFirst->parameters[0].default_value->data).v->to_dec_string() == "50");
  }

  SUBCASE("outgoing messages: intN, plain StructRef, and a monomorphized StructRef with 1 type arg") {
    auto ty_of = [&](int ty_idx) -> const Ty & { return abi.unique_types[static_cast<std::size_t>(ty_idx)]; };
    REQUIRE(abi.outgoing_messages.size() == 4);
    CHECK(ty_of(abi.outgoing_messages[0].body_ty_idx).kind == TyKind::IntN);
    const Ty &t1 = ty_of(abi.outgoing_messages[1].body_ty_idx);
    REQUIRE(t1.kind == TyKind::StructRef);
    CHECK(std::get<TyStructRef>(t1.data).struct_name == "Transfer");
    const Ty &t2 = ty_of(abi.outgoing_messages[2].body_ty_idx);
    REQUIRE(t2.kind == TyKind::StructRef);
    CHECK(std::get<TyStructRef>(t2.data).struct_name == "Out2");
    const Ty &t3 = ty_of(abi.outgoing_messages[3].body_ty_idx);
    REQUIRE(t3.kind == TyKind::StructRef);
    CHECK(std::get<TyStructRef>(t3.data).struct_name == "Out3");
    CHECK(std::get<TyStructRef>(t3.data).type_args_ty_idx.size() == 1);
  }

  SUBCASE("emitted events + field description") {
    REQUIRE(abi.emitted_events.size() == 1);
    const Ty &t = abi.unique_types[static_cast<std::size_t>(abi.emitted_events[0].body_ty_idx)];
    REQUIRE(t.kind == TyKind::StructRef);
    CHECK(std::get<TyStructRef>(t.data).struct_name == "OutExt4");
    CHECK(find_struct(abi, "OutExt4")->description == "mmmOut4");

    const ABIStruct *transfer = find_struct(abi, "Transfer");
    REQUIRE(transfer != nullptr);
    const ABIStructField *fp = nullptr;
    for (const auto &f : transfer->fields) {
      if (f.name == "forwardPayload") fp = &f;
    }
    REQUIRE(fp != nullptr);
    CHECK(fp->description.value() == "actually it's not a slice");
  }
}

TEST_CASE("W10 LotsOfThrows: unnamed/plain_int throws, descriptions, external slice type, defaults presence (spec :17-50)") {
  ContractABI abi = load_fixture_abi("lots-of-throws");

  SUBCASE("unnamed throws (plain_int, no name) -- exactly 3, one at err_code 200, none at 201") {
    int unnamed = 0;
    bool found_200_plain_int = false;
    bool found_201 = false;
    for (const auto &t : abi.thrown_errors) {
      if (!t.name.has_value()) ++unnamed;
      if (t.err_code == 200 && t.kind == "plain_int") found_200_plain_int = true;
      if (t.err_code == 201) found_201 = true;
    }
    CHECK(unnamed == 3);
    CHECK(found_200_plain_int);
    CHECK_FALSE(found_201);
  }

  SUBCASE("throws descriptions") {
    const ABIThrownError *err105 = nullptr;
    const ABIThrownError *enum2 = nullptr;
    for (const auto &t : abi.thrown_errors) {
      if (t.name.has_value() && t.name.value() == "ERR_105") err105 = &t;
      if (t.name.has_value() && t.name.value() == "Err.EInEnum2") enum2 = &t;
    }
    REQUIRE(err105 != nullptr);
    REQUIRE(enum2 != nullptr);
    CHECK(err105->description.value() == "desc for 105");
    CHECK(enum2->description.value() == "desc for EInEnum2");
  }

  SUBCASE("external message body type is bare 'slice' (non-serializable, get-method/stack domain -- ABI-parse still succeeds)") {
    REQUIRE(abi.incoming_external.size() == 1);
    CHECK(abi.unique_types[static_cast<std::size_t>(abi.incoming_external[0].body_ty_idx)].kind == TyKind::Slice);
  }

  SUBCASE("WithUnsupportedDefaults: every field carries a default_value (isDefaultValueSupported is a W11/codegen concern, not asserted here)") {
    const ABIStruct *s = find_struct(abi, "WithUnsupportedDefaults");
    REQUIRE(s != nullptr);
    REQUIRE_FALSE(s->fields.empty());
    for (const auto &f : s->fields) {
      CAPTURE(f.name);
      CHECK(f.default_value.has_value());
    }
  }
}

TEST_CASE("W10 LotsOfStorage: default_value ABIConstExpression trees, incl. the big bigint (spec :51-94)") {
  ContractABI abi = load_fixture_abi("lots-of-storage");
  const ABIStruct *st = find_struct(abi, "StWithAllDefaults");
  REQUIRE(st != nullptr);

  auto get_def = [&](const std::string &name) -> const ABIConstExpression & {
    for (const auto &f : st->fields) {
      if (f.name == name) {
        REQUIRE(f.default_value.has_value());
        return *f.default_value;
      }
    }
    FAIL("field not found: " << name);
    static ABIConstExpression dummy;
    return dummy;
  };

  SUBCASE("every field has a default_value") {
    for (const auto &f : st->fields) {
      CAPTURE(f.name);
      CHECK(f.default_value.has_value());
    }
  }

  SUBCASE("i2: castTo(int '50000000')") {
    const auto &i2 = get_def("i2");
    REQUIRE(i2.kind == ConstExprKind::CastTo);
    const auto &inner = *std::get<ConstExprCastTo>(i2.data).inner;
    REQUIRE(inner.kind == ConstExprKind::Int);
    CHECK(std::get<ConstExprInt>(inner.data).v->to_dec_string() == "50000000");
  }

  SUBCASE("i5: int, the big bigint (>256 bits' worth of decimal digits)") {
    const auto &i5 = get_def("i5");
    REQUIRE(i5.kind == ConstExprKind::Int);
    CHECK(std::get<ConstExprInt>(i5.data).v->to_dec_string() == "1267650600228229401496703205376");
  }

  SUBCASE("i7: null") {
    CHECK(get_def("i7").kind == ConstExprKind::Null);
  }

  SUBCASE("b3: bool false") {
    const auto &b3 = get_def("b3");
    REQUIRE(b3.kind == ConstExprKind::Bool);
    CHECK(std::get<ConstExprBool>(b3.data).v == false);
  }

  SUBCASE("s1: slice hex '0102'") {
    const auto &s1 = get_def("s1");
    REQUIRE(s1.kind == ConstExprKind::Slice);
    CHECK(std::get<ConstExprSlice>(s1.data).hex == "0102");
  }

  SUBCASE("s4: slice hex '68656c6c6f312340'") {
    const auto &s4 = get_def("s4");
    REQUIRE(s4.kind == ConstExprKind::Slice);
    CHECK(std::get<ConstExprSlice>(s4.data).hex == "68656c6c6f312340");
  }

  SUBCASE("a2: address literal") {
    const auto &a2 = get_def("a2");
    REQUIRE(a2.kind == ConstExprKind::Address);
    CHECK(std::get<ConstExprAddress>(a2.data).addr == "EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c");
  }

  SUBCASE("a3: null") {
    CHECK(get_def("a3").kind == ConstExprKind::Null);
  }

  SUBCASE("a4: castTo(address), addr string length 48") {
    const auto &a4 = get_def("a4");
    REQUIRE(a4.kind == ConstExprKind::CastTo);
    const auto &inner = *std::get<ConstExprCastTo>(a4.data).inner;
    REQUIRE(inner.kind == ConstExprKind::Address);
    CHECK(std::get<ConstExprAddress>(inner.data).addr.size() == 48);
  }

  SUBCASE("t3: tensor(null, tensor(20,30))") {
    const auto &t3 = get_def("t3");
    REQUIRE(t3.kind == ConstExprKind::Tensor);
    const auto &items = std::get<ConstExprTensor>(t3.data).items;
    REQUIRE(items.size() == 2);
    CHECK(items[0]->kind == ConstExprKind::Null);
    CHECK(items[1]->kind == ConstExprKind::Tensor);
  }

  SUBCASE("t4: tensor of 5 ints, incl. a ~256-bit one, literal values") {
    const auto &t4 = get_def("t4");
    REQUIRE(t4.kind == ConstExprKind::Tensor);
    const auto &items = std::get<ConstExprTensor>(t4.data).items;
    REQUIRE(items.size() == 5);
    static const char *expect[] = {"907060870", "50018",
                                    "20329878786436204988385760252021328656300425018755239228739303522659023427620",
                                    "754077114", "448378203247"};
    for (std::size_t i = 0; i < items.size(); ++i) {
      CAPTURE(i);
      REQUIRE(items[i]->kind == ConstExprKind::Int);
      CHECK(std::get<ConstExprInt>(items[i]->data).v->to_dec_string() == expect[i]);
    }
  }

  SUBCASE("sh1: castTo(shapedTuple(int 1, null))") {
    const auto &sh1 = get_def("sh1");
    REQUIRE(sh1.kind == ConstExprKind::CastTo);
    const auto &inner = *std::get<ConstExprCastTo>(sh1.data).inner;
    REQUIRE(inner.kind == ConstExprKind::ShapedTuple);
    const auto &items = std::get<ConstExprShapedTuple>(inner.data).items;
    REQUIRE(items.size() == 2);
    REQUIRE(items[0]->kind == ConstExprKind::Int);
    CHECK(std::get<ConstExprInt>(items[0]->data).v->to_dec_string() == "1");
    CHECK(items[1]->kind == ConstExprKind::Null);
  }

  SUBCASE("sh2: nested castTo(shapedTuple(tensor(2 items), castTo(shapedTuple(string '10'))))") {
    const auto &sh2 = get_def("sh2");
    REQUIRE(sh2.kind == ConstExprKind::CastTo);
    const auto &inner = *std::get<ConstExprCastTo>(sh2.data).inner;
    REQUIRE(inner.kind == ConstExprKind::ShapedTuple);
    const auto &items = std::get<ConstExprShapedTuple>(inner.data).items;
    REQUIRE(items.size() == 2);
    REQUIRE(items[0]->kind == ConstExprKind::Tensor);
    CHECK(std::get<ConstExprTensor>(items[0]->data).items.size() == 2);
    REQUIRE(items[1]->kind == ConstExprKind::CastTo);
    const auto &inner2 = *std::get<ConstExprCastTo>(items[1]->data).inner;
    REQUIRE(inner2.kind == ConstExprKind::ShapedTuple);
    const auto &items2 = std::get<ConstExprShapedTuple>(inner2.data).items;
    REQUIRE(items2.size() == 1);
    REQUIRE(items2[0]->kind == ConstExprKind::String);
    CHECK(std::get<ConstExprString>(items2[0]->data).str == "10");
  }

  SUBCASE("o2: object literal, struct_name 'Inner', 2 fields") {
    const auto &o2 = get_def("o2");
    REQUIRE(o2.kind == ConstExprKind::Object);
    const auto &obj = std::get<ConstExprObject>(o2.data);
    CHECK(obj.struct_name == "Inner");
    CHECK(obj.fields.size() == 2);
  }

  SUBCASE("o3: null") {
    CHECK(get_def("o3").kind == ConstExprKind::Null);
  }
}

TEST_CASE("W10 HasNotInitializedStorage: ABI-parse-only fact (spec :62-66); address/shard derivation SKIPPED (see file header)") {
  ContractABI abi = load_fixture_abi("has-not-init-storage");
  REQUIRE(abi.storage.storage_at_deployment_ty_idx.has_value());
  const Ty &t = abi.unique_types[static_cast<std::size_t>(*abi.storage.storage_at_deployment_ty_idx)];
  REQUIRE(t.kind == TyKind::StructRef);
  CHECK(std::get<TyStructRef>(t.data).struct_name == "NftItemStorageNotInitialized");
}
