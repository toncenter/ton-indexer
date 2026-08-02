// ton-abi-gen emitter units cover codegen-level facts that the
// vector gate structurally CANNOT see, because they are about the emitted TEXT
// or about generation failing, not about wire behaviour.
//  (A) create() default materialization: defaults live only in
//      the ABI's ConstExprs, never on the wire, so no vector can reach them.
//      (The wire-behaviour cases that used to sit here -- IncreaseCounter,
//      IncreaseBy, EmptyMsg, MsgPair, MsgAliasInt16 round-trips and the prefix
//      mismatch -- were hand-copies of conformance vectors and were deleted;
//      AbiABGateTest drives those same shapes from testdata/abi_vectors.json,
//      and it is also where "every generated pair compiles + links" is proven.)
//  (B) Per-direction custom gate: a ONE-SIDED
//      custom STRUCT delegates ONLY the registered direction to the typed
//      registry; the other direction keeps the normal baked path. This is the
//      reference-emitter bug (unpack gated on the pack flag) that we must NOT
//      port -- proven at codegen level since the A/B gate cannot catch it.
//  (C) Decl-level non-standard map key -> HARD generation failure.
//  (D) Non-serializable field (callable) -> compiling stubs, both directions.

#include "AbiTestSupport.h"

#include "lots_of_messages_gen.h"

#include <cstdlib>
#include <filesystem>
#include <memory>

#ifndef _WIN32
#include <sys/wait.h>  // std::system() returns a wait status, not an exit code
#endif

namespace {

using namespace ton_abi;
using ton_abi_test::fixture_path;
using ton_abi_test::load_fixture_abi;
using ton_abi_test::read_file;

std::string emit_source_of(const std::string &fixture) {
  auto box = std::make_unique<ContractABI>(load_fixture_abi(fixture));
  auto kernel = AbiKernel::create(*box).move_as_ok();
  auto r = emit_abi(*box, kernel);
  REQUIRE_MESSAGE(r.is_ok(), fixture << " emit: " << (r.is_error() ? r.error().message().str() : ""));
  return r.move_as_ok().source;
}

}  // namespace

// (A) create() default materialization: a defaulted field left out of
// CreateArgs is filled from the ABI's ConstExpr. Defaults never appear on the
// wire, so this is unreachable from the vector corpus.
TEST_CASE("emit: lots-of-messages IncreaseBy create()-with-default") {
  gen::lots_of_messages::IncreaseBy::CreateArgs args;
  args.counter_id = 7;  // int8 -> td::int64
  auto made = gen::lots_of_messages::IncreaseBy::create(args);
  CHECK(made.inc_by == 1);  // int32 -> td::int64, ABI default 1
  CHECK(made.counter_id == 7);
}

// (B) per-direction custom gate: do not port the reference bug
namespace {
// Minimal hand-authored ABI: struct S with a single uint8 field `a`, carrying a
// custom_pack_unpack with the two booleans templated in.
std::string one_sided_custom_json(bool pack, bool unpack) {
  std::string p = pack ? "true" : "false";
  std::string u = unpack ? "true" : "false";
  return R"({
    "abi_schema_version": "1.0",
    "contract_name": "C",
    "unique_types": [ {"kind":"uintN","n":8}, {"kind":"StructRef","struct_name":"S"} ],
    "struct_instantiations": [], "alias_instantiations": [],
    "declarations": [
      { "kind":"struct", "name":"S", "ty_idx":1,
        "custom_pack_unpack": { "pack_to_builder": )" +
         p + R"(, "unpack_from_slice": )" + u + R"( },
        "fields": [ {"name":"a","ty_idx":0} ] }
    ],
    "storage": {}, "incoming_messages": [], "incoming_external": [],
    "outgoing_messages": [], "emitted_events": [], "get_methods": [], "thrown_errors": [],
    "compiler_name":"hand", "compiler_version":"n/a"
  })";
}

std::string emit_json_source(const std::string &json) {
  auto abi = load_abi_from_json(json).move_as_ok();
  auto box = std::make_unique<ContractABI>(std::move(abi));
  auto kernel = AbiKernel::create(*box).move_as_ok();
  return emit_abi(*box, kernel).move_as_ok().source;
}

// crude "does the body of method `sig` mention `needle`" check.
bool body_mentions(const std::string &src, const std::string &sig, const std::string &needle) {
  auto start = src.find(sig);
  REQUIRE(start != std::string::npos);
  auto next = src.find("\n}\n", start);
  std::string body = src.substr(start, next == std::string::npos ? std::string::npos : next - start);
  return body.find(needle) != std::string::npos;
}
}  // namespace

TEST_CASE("emit: pack-only custom struct delegates ONLY store, not from_slice") {
  std::string src = emit_json_source(one_sided_custom_json(/*pack*/ true, /*unpack*/ false));
  CHECK(body_mentions(src, "S::store(", "abi_custom_lookup"));         // pack -> registry
  CHECK_FALSE(body_mentions(src, "S::from_slice(", "abi_custom_lookup"));  // unpack -> normal
  CHECK(body_mentions(src, "S::from_slice(", "load_uint"));            // normal baked path
}

TEST_CASE("emit: unpack-only custom struct delegates ONLY from_slice, not store") {
  std::string src = emit_json_source(one_sided_custom_json(/*pack*/ false, /*unpack*/ true));
  CHECK(body_mentions(src, "S::from_slice(", "abi_custom_lookup"));    // unpack -> registry
  CHECK_FALSE(body_mentions(src, "S::store(", "abi_custom_lookup"));   // pack -> normal
  CHECK(body_mentions(src, "S::store(", "store_uint"));               // normal baked path
}

// (C) decl-level non-standard map key -> HARD failure (ErrorsAtCodegen parity)
TEST_CASE("emit: err-invalid-map-key hard-fails with map-key error") {
  for (const char *fx : {"err-invalid-map-key-1", "err-invalid-map-key-2"}) {
    auto box = std::make_unique<ContractABI>(load_fixture_abi(fx));
    auto kernel = AbiKernel::create(*box).move_as_ok();
    auto r = emit_abi(*box, kernel);
    REQUIRE_MESSAGE(r.is_error(), fx << " expected hard failure");
    CHECK(r.error().message().str().find("map-key") != std::string::npos);
  }
}

// (D) non-serializable field (callable) -> compiling stubs
TEST_CASE("emit: err-cont-on-stack emits cleanly; callable field -> stub") {
  // -1 has `struct D { n: callable }` -> the callable member is omitted and both
  // directions become runtime-error stubs. The file still emits cleanly because
  // err-cont ABIs must not hard-fail.
  std::string src1 = emit_source_of("err-cont-on-stack-1");
  CHECK(src1.find("not serializable") != std::string::npos);
  CHECK(src1.find("D::from_slice") != std::string::npos);

  // -2 carries `callable` only in a get-method (out of scope): no struct field
  // is non-serializable, so it emits cleanly with no stub.
  std::string src2 = emit_source_of("err-cont-on-stack-2");
  CHECK(src2.find("not serializable") == std::string::npos);
}

// The ton-abi-gen executable writes a pair to disk; the emitted header contains
// the struct's signature.
#if defined(TON_ABI_GEN_EXE) && defined(TON_ABI_TEST_TMP)
TEST_CASE("cli: ton-abi-gen writes a header containing the struct signature") {
  namespace fs = std::filesystem;
  fs::path out = TON_ABI_TEST_TMP;
  std::error_code ec;
  fs::create_directories(out, ec);

  std::string abi = fixture_path("tolk_counter");
#ifdef _WIN32
  // cmd.exe strips one outer quote pair from the whole command line
  std::string cmd = "\"\"" + std::string(TON_ABI_GEN_EXE) + "\" \"" + abi + "\" --out-dir \"" + out.string() + "\"\"";
#else
  std::string cmd = "\"" + std::string(TON_ABI_GEN_EXE) + "\" \"" + abi + "\" --out-dir \"" + out.string() + "\"";
#endif
  int rc = std::system(cmd.c_str());
#ifdef _WIN32
  int exit_status = rc;
#else
  int exit_status = WIFEXITED(rc) ? WEXITSTATUS(rc) : -1;  // -1 == died by signal, see raw
#endif
  REQUIRE_MESSAGE(rc == 0, "ton-abi-gen exit status " << exit_status << " (raw " << rc << ") for: " << cmd);

  std::string header = read_file((out / "tolk_counter_gen.h").string());
  CHECK(header.find("struct IncreaseCounter {") != std::string::npos);
  CHECK(header.find("static td::Result<IncreaseCounter> from_slice(vm::CellSlice& cs);") != std::string::npos);
}
#endif
