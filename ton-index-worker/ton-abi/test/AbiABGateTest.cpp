// Conformance gate: every testdata/abi_vectors.json vector runs through
// generated from_slice -> to_abi_value -> canonical JSON and must match.
// Error vectors must fail from_slice; others also round-trip store hash.
// Compiling this TU proves every committed generated pair links.
// Custom-serializer vectors use register_abi_custom<T>, keyed
// <contract_name>::<decl>. TelegramString and CustomPoint are AbiValue-form;
// Custom8 is td::uint64.

#include "AbiTestSupport.h"

#include "tolk_counter_gen.h"
#include "lots_of_messages_gen.h"
#include "lots_of_wrappers_gen.h"
#include "lots_of_storage_gen.h"
#include "generic_union_labels_gen.h"
#include "w8_hand_union_gen.h"
#include "client_type_anno_gen.h"

#include <functional>
#include <map>
#include <set>
#include <type_traits>

#ifndef TON_ABI_VECTORS_FILE
#error "TON_ABI_VECTORS_FILE must be defined by CMake"
#endif

namespace {

using namespace ton_abi;
using Json = td::JsonValue;
using JType = td::JsonValue::Type;

const Json *jfield(const Json &e, td::Slice name) {
  if (e.type() != JType::Object) return nullptr;
  for (const auto &kv : e.get_object().field_values_) {
    if (kv.first == name) return &kv.second;
  }
  return nullptr;
}
std::string jstr(const Json &e, td::Slice name, const std::string &dflt = {}) {
  const Json *f = jfield(e, name);
  return (f != nullptr && f->type() == JType::String) ? f->get_string().str() : dflt;
}
bool jbool(const Json &e, td::Slice name, bool dflt = false) {
  const Json *f = jfield(e, name);
  return (f != nullptr && f->type() == JType::Boolean) ? f->get_boolean() : dflt;
}
bool json_equal(const Json &a, const Json &b) {
  if (a.type() != b.type()) return false;
  switch (a.type()) {
    case JType::Null: return true;
    case JType::Boolean: return a.get_boolean() == b.get_boolean();
    case JType::Number: return a.get_number() == b.get_number();
    case JType::String: return a.get_string() == b.get_string();
    case JType::Array: {
      const auto &aa = a.get_array();
      const auto &bb = b.get_array();
      if (aa.size() != bb.size()) return false;
      for (std::size_t i = 0; i < aa.size(); ++i)
        if (!json_equal(aa[i], bb[i])) return false;
      return true;
    }
    case JType::Object: {
      const auto &ao = a.get_object().field_values_;
      const auto &bo = b.get_object();
      if (ao.size() != bo.field_values_.size()) return false;
      for (const auto &kv : ao) {
        const Json *bv = jfield(b, kv.first);
        if (bv == nullptr || !json_equal(kv.second, *bv)) return false;
      }
      return true;
    }
  }
  return false;
}

// Fift-hex parser ("x{..}" + indented refs)
struct FiftNode {
  std::string body;
  std::vector<FiftNode> children;
};
int leading_spaces(const std::string &s) {
  int n = 0;
  while (n < static_cast<int>(s.size()) && s[n] == ' ') ++n;
  return n;
}
std::size_t parse_fift_node(const std::vector<std::string> &lines, std::size_t idx, int depth, FiftNode &out) {
  out.body = lines[idx].substr(static_cast<std::size_t>(depth));
  std::size_t i = idx + 1;
  while (i < lines.size()) {
    int d = leading_spaces(lines[i]);
    if (d <= depth) break;
    FiftNode child;
    i = parse_fift_node(lines, i, d, child);
    out.children.push_back(std::move(child));
  }
  return i;
}
td::Result<td::Ref<vm::Cell>> fift_node_cell(const FiftNode &node) {
  if (node.body.size() < 3 || node.body[0] != 'x' || node.body[1] != '{' || node.body.back() != '}')
    return td::Status::Error(PSLICE() << "bad fift-hex node: '" << node.body << "'");
  unsigned char buff[128];
  const std::string inner = node.body.substr(2, node.body.size() - 3);
  long bits = td::bitstring::parse_bitstring_hex_literal(buff, sizeof(buff), inner.data(), inner.data() + inner.size());
  if (bits < 0) return td::Status::Error(PSLICE() << "failed to parse hex literal: '" << node.body << "'");
  vm::CellBuilder cb;
  cb.store_bits(td::ConstBitPtr{buff}, static_cast<std::size_t>(bits));
  for (const auto &child : node.children) {
    TRY_RESULT(child_cell, fift_node_cell(child));
    if (!cb.store_ref_bool(child_cell)) return td::Status::Error("store_ref failed");
  }
  return cb.finalize();
}
td::Result<td::Ref<vm::Cell>> parse_fift_hex(const std::string &text) {
  std::vector<std::string> lines;
  std::size_t pos = 0;
  while (true) {
    std::size_t nl = text.find('\n', pos);
    if (nl == std::string::npos) {
      lines.push_back(text.substr(pos));
      break;
    }
    lines.push_back(text.substr(pos, nl - pos));
    pos = nl + 1;
  }
  if (lines.empty() || lines[0].empty()) return td::Status::Error("empty fift-hex text");
  FiftNode root;
  std::size_t consumed = parse_fift_node(lines, 0, 0, root);
  if (consumed != lines.size()) return td::Status::Error("trailing garbage in fift-hex");
  return fift_node_cell(root);
}

// Generated dispatch: fixture struct to from_slice/to_abi/store
struct GenEntry {
  std::function<td::Result<AbiValue>(vm::CellSlice &)> to_value;         // from_slice -> to_abi_value
  std::function<td::Result<td::Ref<vm::Cell>>(vm::CellSlice &)> repack;  // from_slice -> store -> cell
};

template <class T>
GenEntry entry() {
  return {[](vm::CellSlice &cs) -> td::Result<AbiValue> {
            TRY_RESULT(v, T::from_slice(cs));
            return v.to_abi_value();
          },
          [](vm::CellSlice &cs) -> td::Result<td::Ref<vm::Cell>> {
            TRY_RESULT(v, T::from_slice(cs));
            vm::CellBuilder cb;
            TRY_STATUS(v.store(cb));
            return cb.finalize();
          }};
}

// CustomPoint is AbiValue-form (free functions, not member methods).
GenEntry custom_point_entry() {
  namespace lw = ton_abi::gen::lots_of_wrappers;
  return {[](vm::CellSlice &cs) -> td::Result<AbiValue> {
            TRY_RESULT(v, lw::CustomPoint_from_slice(cs));
            return lw::CustomPoint_to_abi_value(v);
          },
          [](vm::CellSlice &cs) -> td::Result<td::Ref<vm::Cell>> {
            TRY_RESULT(v, lw::CustomPoint_from_slice(cs));
            vm::CellBuilder cb;
            TRY_STATUS(lw::CustomPoint_store(cb, v));
            return cb.finalize();
          }};
}

std::map<std::string, GenEntry> build_dispatch() {
  namespace tc = ton_abi::gen::tolk_counter;
  namespace lm = ton_abi::gen::lots_of_messages;
  namespace lw = ton_abi::gen::lots_of_wrappers;
  namespace gul = ton_abi::gen::generic_union_labels;
  namespace whu = ton_abi::gen::w8_hand_union;
  std::map<std::string, GenEntry> m;
  m["tolk_counter::IncreaseCounter"] = entry<tc::IncreaseCounter>();
  m["tolk_counter::ResetCounter"] = entry<tc::ResetCounter>();
  m["lots-of-messages::IncreaseBy"] = entry<lm::IncreaseBy>();
  m["lots-of-messages::EmptyMsg"] = entry<lm::EmptyMsg>();
  m["lots-of-wrappers::MsgSinglePrefix32"] = entry<lw::MsgSinglePrefix32>();
  m["lots-of-wrappers::CounterIncrement"] = entry<lw::CounterIncrement>();
  m["lots-of-wrappers::CounterDecrement"] = entry<lw::CounterDecrement>();
  m["lots-of-wrappers::CounterReset0"] = entry<lw::CounterReset0>();
  m["lots-of-wrappers::CounterResetTo"] = entry<lw::CounterResetTo>();
  m["lots-of-wrappers::JustAddress"] = entry<lw::JustAddress>();
  m["lots-of-wrappers::JustMaybeInt32"] = entry<lw::JustMaybeInt32>();
  m["lots-of-wrappers::TwoInts32AndRef64"] = entry<lw::TwoInts32AndRef64>();
  m["lots-of-wrappers::WithArrays2"] = entry<lw::WithArrays2>();
  m["lots-of-wrappers::WithLispLists1"] = entry<lw::WithLispLists1>();
  m["lots-of-wrappers::TransferParams2"] = entry<lw::TransferParams2>();
  m["lots-of-wrappers::WithMaps0"] = entry<lw::WithMaps0>();
  m["lots-of-wrappers::WithEnums"] = entry<lw::WithEnums>();
  m["lots-of-wrappers::WithAnyAddress"] = entry<lw::WithAnyAddress>();
  m["lots-of-wrappers::StorWithStr"] = entry<lw::StorWithStr>();
  m["lots-of-wrappers::PointWithCustomInt"] = entry<lw::PointWithCustomInt>();
  m["lots-of-wrappers::CustomPoint"] = custom_point_entry();
  m["lots-of-wrappers::WithStrings1"] = entry<lw::WithStrings1>();
  m["generic-union-labels::MsgPair"] = entry<gul::MsgPair>();
  m["generic-union-labels::MsgOrInt16"] = entry<gul::MsgOrInt16>();
  m["generic-union-labels::MsgAliasInt16"] = entry<gul::MsgAliasInt16>();
  m["w8-hand-union::Holder"] = entry<whu::Holder>();
  return m;
}

// Typed custom serializers
void register_customs() {
  // Custom8 -> td::uint64 because a uint8 target uses the native member type.
  auto st = ton_abi::gen::register_abi_custom<td::uint64>(
      "LotsOfWrappers::Custom8",
      [](const td::uint64 &v, vm::CellBuilder &cb) -> td::Status { return store_uint64(cb, v, 8); },
      [](vm::CellSlice &cs) -> td::Result<td::uint64> { return load_uint64(cs, 8); },
      [](const td::uint64 &v) -> td::Result<AbiValue> { return AbiValue::make_int(td::make_refint(v)); });
  REQUIRE(st.is_ok());

  // TelegramString -> AbiValue (Bits): uint8 byte-length then that many bytes.
  st = ton_abi::gen::register_abi_custom<AbiValue>(
      "LotsOfWrappers::TelegramString",
      [](const AbiValue &v, vm::CellBuilder &cb) -> td::Status {
        if (v.kind != AbiValueKind::Bits) return td::Status::Error("TelegramString pack: expected Bits");
        int bytes = (static_cast<int>(v.bits_v->size()) + 7) / 8;
        TRY_STATUS(store_uint(cb, td::make_refint(bytes), 8));
        cb.store_bits(v.bits_v->data_bits(), v.bits_v->size());
        return td::Status::OK();
      },
      [](vm::CellSlice &cs) -> td::Result<AbiValue> {
        TRY_RESULT(bytes, load_uint(cs, 8));
        TRY_RESULT(bits, load_bits(cs, static_cast<int>(bytes->to_long()) * 8));
        return AbiValue::make_bits(std::move(bits));
      },
      [](const AbiValue &v) -> td::Result<AbiValue> { return AbiValue::make_bits(v.bits_v); });
  REQUIRE(st.is_ok());

  // CustomPoint -> AbiValue (Struct{x,y}): two uint8.
  st = ton_abi::gen::register_abi_custom<AbiValue>(
      "LotsOfWrappers::CustomPoint",
      [](const AbiValue &v, vm::CellBuilder &cb) -> td::Status {
        const td::RefInt256 *x = nullptr;
        const td::RefInt256 *y = nullptr;
        for (const auto &f : v.struct_fields) {
          if (f.first == "x") x = &f.second.int_v;
          if (f.first == "y") y = &f.second.int_v;
        }
        if (x == nullptr || y == nullptr) return td::Status::Error("CustomPoint pack: missing x/y");
        TRY_STATUS(store_uint(cb, *x, 8));
        return store_uint(cb, *y, 8);
      },
      [](vm::CellSlice &cs) -> td::Result<AbiValue> {
        TRY_RESULT(x, load_uint(cs, 8));
        TRY_RESULT(y, load_uint(cs, 8));
        AbiValue r = AbiValue::make_struct("CustomPoint");
        r.add_field("x", AbiValue::make_int(std::move(x)));
        r.add_field("y", AbiValue::make_int(std::move(y)));
        return r;
      },
      [](const AbiValue &v) -> td::Result<AbiValue> {
        AbiValue r = AbiValue::make_struct("CustomPoint");
        for (const auto &f : v.struct_fields) r.add_field(f.first, AbiValue::make_int(f.second.int_v));
        return r;
      });
  REQUIRE(st.is_ok());

  // MyBorderedInt -> td::RefInt256 (representable target): a lossy 3-range
  // encoding -- pack stores a 4-bit range TAG, unpack returns a fixed
  // representative per range, which is non-idempotent by design.
  st = ton_abi::gen::register_abi_custom<td::RefInt256>(
      "LotsOfWrappers::MyBorderedInt",
      [](const td::RefInt256 &v, vm::CellBuilder &cb) -> td::Status {
        int tag = v > 10 ? 1 : (v > 0 ? 2 : 3);
        return store_uint(cb, td::make_refint(tag), 4);
      },
      [](vm::CellSlice &cs) -> td::Result<td::RefInt256> {
        TRY_RESULT(tag, load_uint(cs, 4));
        long long t = tag->to_long();
        if (t == 1) return td::dec_string_to_int256(std::string("10"));
        if (t == 2) return td::dec_string_to_int256(std::string("0"));
        if (t == 3) return td::dec_string_to_int256(std::string("-1"));
        return td::Status::Error("MyBorderedInt: bad tag");
      },
      [](const td::RefInt256 &v) -> td::Result<AbiValue> { return AbiValue::make_int(v); });
  REQUIRE(st.is_ok());
}

// Register the typed customs exactly once across all TEST_CASEs (the registry
// is a process-global; re-registration errors).
void ensure_customs() {
  static bool once = [] {
    register_customs();
    return true;
  }();
  (void)once;
}

using ton_abi_test::read_file;

}  // namespace

TEST_CASE("A/B gate: generated structs match the oracle vectors, value + repack") {
  ensure_customs();
  auto dispatch = build_dispatch();

  std::string buf = read_file(TON_ABI_VECTORS_FILE);
  auto r_root = td::json_decode(td::MutableSlice(buf));
  REQUIRE(r_root.is_ok());
  Json root = r_root.move_as_ok();
  const Json *vectors = jfield(root, "vectors");
  REQUIRE(vectors != nullptr);

  int checked = 0;
  int idx = 0;
  for (const auto &v : vectors->get_array()) {
    std::string fixture = jstr(v, "fixture");
    std::string struct_name = jstr(v, "struct");
    std::string key = fixture + "::" + struct_name;
    std::string name = key + "#" + std::to_string(idx++);

    SUBCASE(name.c_str()) {
      auto it = dispatch.find(key);
      REQUIRE_MESSAGE(it != dispatch.end(), name << ": no generated dispatch wired");
      const GenEntry &ge = it->second;

      auto r_cell = parse_fift_hex(jstr(v, "golden_hex"));
      REQUIRE_MESSAGE(r_cell.is_ok(), name << ": fift-hex: " << r_cell.error().message().str());
      td::Ref<vm::Cell> cell = r_cell.move_as_ok();

      if (jbool(v, "expect_error")) {
        auto cs = vm::load_cell_slice(cell);
        auto r = ge.to_value(cs);
        CHECK_MESSAGE(r.is_error(), name << ": generated from_slice should have failed");
        ++checked;
        return;
      }

      const Json *expected = jfield(v, "value");
      REQUIRE(expected != nullptr);

      auto cs = vm::load_cell_slice(cell);
      auto r_val = ge.to_value(cs);
      REQUIRE_MESSAGE(r_val.is_ok(), name << ": generated from_slice/to_abi: "
                                          << (r_val.is_error() ? r_val.error().message().str() : ""));
      std::string dumped = r_val.ok().to_json();
      auto r_dj = td::json_decode(td::MutableSlice(dumped));
      REQUIRE_MESSAGE(r_dj.is_ok(), name << ": reparse dump: " << dumped);
      Json dj = r_dj.move_as_ok();
      CHECK_MESSAGE(json_equal(dj, *expected), name << ": generated dump != expected\n  got: " << dumped);

      if (!jbool(v, "unpack_only")) {
        auto cs2 = vm::load_cell_slice(cell);
        auto r_pack = ge.repack(cs2);
        REQUIRE_MESSAGE(r_pack.is_ok(), name << ": generated repack: "
                                             << (r_pack.is_error() ? r_pack.error().message().str() : ""));
        CHECK_MESSAGE(r_pack.ok()->get_hash() == cell->get_hash(), name << ": repacked hash != golden");
      }
      ++checked;
    }
  }
  MESSAGE("A/B gate: ", checked, " vectors checked (this doctest pass)");
}

// Corpus-structure gate. The per-vector checks above live inside SUBCASEs, so
// doctest re-enters the case body once per subcase and any counter incremented
// there is a per-pass fact, never a whole-corpus one. These two asserts are the
// whole-corpus facts, and they run exactly once:
//   (a) the vector file still carries the full conformance corpus (a truncated
//       or partially-regenerated abi_vectors.json must FAIL, not silently
//       shrink the gate);
//   (b) every build_dispatch() key is exercised by at least one vector (catches
//       an entry left behind after its vectors were renamed or dropped).
TEST_CASE("A/B gate corpus: full vector set present, no orphan dispatch entries") {
  constexpr std::size_t kExpectedVectorCount = 33;

  std::string buf = read_file(TON_ABI_VECTORS_FILE);
  auto r_root = td::json_decode(td::MutableSlice(buf));
  REQUIRE(r_root.is_ok());
  Json root = r_root.move_as_ok();
  const Json *vectors = jfield(root, "vectors");
  REQUIRE(vectors != nullptr);
  REQUIRE(vectors->type() == JType::Array);

  const auto &varr = vectors->get_array();
  CHECK_MESSAGE(varr.size() == kExpectedVectorCount,
                "vector corpus size " << varr.size() << " != expected " << kExpectedVectorCount
                                      << " (regenerate with tools/gen_vectors.mjs, or update the constant "
                                         "deliberately when the corpus grows)");

  std::set<std::string> vector_keys;
  for (const auto &v : varr) {
    vector_keys.insert(jstr(v, "fixture") + "::" + jstr(v, "struct"));
  }
  for (const auto &kv : build_dispatch()) {
    CHECK_MESSAGE(vector_keys.count(kv.first) == 1, "orphan dispatch entry: '" << kv.first
                                                                              << "' is wired but no vector uses it");
  }
}

// Union dispatch forms on the generated path.
//   - an EXPLICIT prefix is peeked, never eaten (the variant re-reads it);
//   - exact T|void: an empty slice is the void, anything else is the T;
//   - a general union with a trailing void matches void LAST, and a non-empty
//     no-match is an ERROR rather than a silent void.
// Implicit-prefix EAT and no-match-without-void are vector-covered.
TEST_CASE("union dispatch forms (generated): explicit peek, T|void, void-last") {
  namespace lw = ton_abi::gen::lots_of_wrappers;
  namespace whu = ton_abi::gen::w8_hand_union;

  SUBCASE("explicit prefix is peeked, not consumed") {
    // W8HandUnion::Holder = int8|int8 behind EXPLICIT 8-bit prefixes 10 / 11.
    // The variant is a bare int8, so it re-reads those very bits as its value:
    // value == prefix is the proof that dispatch consumed nothing.
    vm::CellBuilder cb;
    REQUIRE(cb.store_long_bool(11, 8));
    auto cs = vm::load_cell_slice(cb.finalize());
    auto r = whu::Holder::from_slice(cs);
    REQUIRE(r.is_ok());
    REQUIRE(r.ok().u.index() == 1);
    CHECK(std::get<1>(r.ok().u) == 11);
    CHECK(cs.size() == 0);
  }
  SUBCASE("no prefix matches and no void -> error") {
    vm::CellBuilder cb;
    REQUIRE(cb.store_long_bool(12, 8));  // neither 10 nor 11
    auto cs = vm::load_cell_slice(cb.finalize());
    CHECK(whu::Holder::from_slice(cs).is_error());
  }
  SUBCASE("T|void: empty slice is the void") {
    vm::CellBuilder cb;
    auto cs = vm::load_cell_slice(cb.finalize());
    auto r = lw::Int32OrVoid_from_slice(cs);
    REQUIRE(r.is_ok());
    CHECK(r.ok().index() == 1);  // std::monostate
  }
  SUBCASE("T|void: non-empty slice is the T") {
    vm::CellBuilder cb;
    REQUIRE(cb.store_long_bool(0x55, 32));
    auto cs = vm::load_cell_slice(cb.finalize());
    auto r = lw::Int32OrVoid_from_slice(cs);
    REQUIRE(r.is_ok());
    REQUIRE(r.ok().index() == 0);
    CHECK(std::get<0>(r.ok()) == 0x55);
  }
  SUBCASE("void-last: a matching prefix wins over the void") {
    vm::CellBuilder cb;
    REQUIRE(cb.store_long_bool(1, 2));  // ThreeP2's explicit 2-bit prefix
    REQUIRE(cb.store_long_bool(7, 16));  // ThreeP2.v is int16
    auto cs = vm::load_cell_slice(cb.finalize());
    auto r = lw::ThreeWayWithVoid_from_slice(cs);
    REQUIRE(r.is_ok());
    REQUIRE(r.ok().index() == 1);
    CHECK(std::get<1>(r.ok()).v == 7);
  }
  SUBCASE("void-last: empty slice is the void, matched after the probes") {
    vm::CellBuilder cb;
    auto cs = vm::load_cell_slice(cb.finalize());
    auto r = lw::ThreeWayWithVoid_from_slice(cs);
    REQUIRE(r.is_ok());
    CHECK(r.ok().index() == 2);
  }
  SUBCASE("void-last: non-empty no-match errors instead of falling into void") {
    vm::CellBuilder cb;
    REQUIRE(cb.store_long_bool(3, 2));  // neither 0b00 nor 0b01
    REQUIRE(cb.store_long_bool(7, 8));
    auto cs = vm::load_cell_slice(cb.finalize());
    CHECK(lw::ThreeWayWithVoid_from_slice(cs).is_error());
  }
}

// MyBorderedInt: typed-side 3-range lossy encoding, driven
// through the GENERATED MyBorderedInt_store / _from_slice + typed registry).
// Deliberately vector-less (non-idempotent), so a dedicated native unit.
TEST_CASE("typed custom: MyBorderedInt 3-range lossy encoding") {
  ensure_customs();
  namespace lw = ton_abi::gen::lots_of_wrappers;

  struct Case {
    const char *in;
    const char *rep;
  };
  // pack(v) -> tag by range; unpack(tag) -> fixed representative.
  for (Case c : {Case{"20", "10"}, Case{"5", "0"}, Case{"-7", "-1"}, Case{"11", "10"}, Case{"1", "0"},
                 Case{"0", "-1"}}) {
    vm::CellBuilder cb;
    REQUIRE(lw::MyBorderedInt_store(cb, td::dec_string_to_int256(std::string(c.in))).is_ok());
    auto cs = vm::load_cell_slice(cb.finalize());
    auto r = lw::MyBorderedInt_from_slice(cs);
    REQUIRE(r.is_ok());
    CHECK_MESSAGE(r.ok()->to_dec_string() == c.rep, "MyBorderedInt(" << c.in << ") -> " << r.ok()->to_dec_string()
                                                                     << " expected " << c.rep);
  }
}

// LotsOfStorage default-fill: create() with no explicit args
// materializes every field's ABI default_value ConstExpr. Exactness gate
// on a representative spread incl. the big 256-bit bigints and a nullable tensor.
TEST_CASE("create() default-fill: LotsOfStorage StWithAllDefaults") {
  namespace ls = ton_abi::gen::lots_of_storage;
  ls::StWithAllDefaults::CreateArgs args;  // all fields defaulted -> all optional, left empty
  ls::StWithAllDefaults s = ls::StWithAllDefaults::create(args);

  CHECK(s.i1 == 0);  // int32 -> td::int64
  CHECK(s.i5->to_dec_string() == "1267650600228229401496703205376");   // uint256 -> RefInt256
  CHECK(s.i6->to_dec_string() == "-1267650600228229401496703205375");  // int256 -> RefInt256
  CHECK(s.b1 == true);
  CHECK(s.b2 == false);
  CHECK(s.s5 == "kopi");
  // t1: (int8,int8,int8)? = (1,2,3) -> present optional<tuple<td::int64,...>>.
  REQUIRE(s.t1.has_value());
  CHECK(std::get<0>(*s.t1) == 1);
  CHECK(std::get<1>(*s.t1) == 2);
  CHECK(std::get<2>(*s.t1) == 3);
}

// client_ty_idx cell-side swap, now exercised through generated
// path). ClientPayload.note is declared 'remaining' but carries
// @abi.clientType 'string'; the kernel swaps to the cell type, so the emitted
// member is std::string and the wire is a snake string ref-tail. Proves the
// emitter consumed the kernel's client_ty_idx resolution end-to-end.
TEST_CASE("client_ty_idx: ClientPayload note reads as string (generated)") {
  namespace ct = ton_abi::gen::client_type_anno;
  static_assert(std::is_same_v<decltype(ct::ClientPayload::note), std::string>,
                "note must be std::string (client_ty_idx swap), not a slice");
  vm::CellBuilder cb;
  REQUIRE(store_prefix(cb, 0x12345678u, 32).is_ok());
  REQUIRE(store_string(cb, "hello").is_ok());
  td::Ref<vm::Cell> cell = cb.finalize();

  auto cs = vm::load_cell_slice(cell);
  auto g = ct::ClientPayload::from_slice(cs);
  REQUIRE_MESSAGE(g.is_ok(), "from_slice: " << (g.is_error() ? g.error().message().str() : ""));
  CHECK(g.ok().to_abi_value().move_as_ok().to_json() == R"({"$":"ClientPayload","note":"hello"})");

  vm::CellBuilder cb2;
  REQUIRE(g.ok().store(cb2).is_ok());
  CHECK(cb2.finalize()->get_hash() == cell->get_hash());
}

// Custom declared-but-not-registered -> runtime error. Color is a custom
// enum never registered here, so every direction must fail with a
// 'not registered' runtime error (NOT a compile/link error).
TEST_CASE("custom not registered: Color errors at runtime, all directions (generated)") {
  namespace lw = ton_abi::gen::lots_of_wrappers;
  ensure_customs();  // registers Custom8/TelegramString/CustomPoint/MyBorderedInt, NOT Color
  vm::CellBuilder cb;
  REQUIRE(store_uint(cb, td::make_refint(1), 2).is_ok());
  auto cs = vm::load_cell_slice(cb.finalize());
  auto r_un = lw::Color_from_slice(cs);
  CHECK(r_un.is_error());
  CHECK(r_un.error().message().str().find("not registered") != std::string::npos);

  vm::CellBuilder got;
  auto r_pk = lw::Color_store(got, td::make_refint(1));
  CHECK(r_pk.is_error());
  CHECK(r_pk.error().message().str().find("not registered") != std::string::npos);
}
