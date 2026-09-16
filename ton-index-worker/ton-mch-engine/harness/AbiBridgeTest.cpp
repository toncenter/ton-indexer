// ABI bridge self-test. See AbiBridgeTest.h.
#include "AbiBridgeTest.h"

#include "AbiBridge.h"
#include "AbiProjection.h"
#include "AbiTryFirst.h"
#include "MsgParse.h"
#include "Value.h"
#include "parse/PSlice.h"

#include "AbiGenSupport.h"  // ton_abi::gen::address_from_string
#include "AbiValue.h"
#include "AbiLeavesAddress.h"

#include "dedust_gen.h"
#include "jetton_gen.h"
#include "jetton_payloads_gen.h"
#include "jetton_wallet_contract_gen.h"
#include "lots_of_wrappers_gen.h"
#include "dedust_v2_gen.h"  // DeDust V2 production pair

#include "common/refint.h"
#include "vm/boc.h"
#include "vm/cells/CellBuilder.h"
#include "vm/cellslice.h"

#include <cstdio>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace mch {

namespace {

namespace jw = ton_abi::gen::jetton_wallet_contract;
namespace lw = ton_abi::gen::lots_of_wrappers;
namespace d = ton_abi::gen::dedust;
namespace dv = ton_abi::gen::dedust_v2;
namespace j = ton_abi::gen::jetton;
namespace jp = ton_abi::gen::jetton_payloads;
using ton_abi::AbiAddress;
using ton_abi::AbiAddressKind;
using ton_abi::AbiValue;

int g_fail = 0;

void check(const std::string &name, bool ok) {
  std::printf("%s %s\n", ok ? "PASS" : "FAIL", name.c_str());
  if (!ok) {
    g_fail++;
  }
}

// Value inspectors
bool is_int(const Value *v, long long want) {
  return v && v->t == VType::Int && v->num.not_null() && v->num->to_dec_string() == std::to_string(want);
}
bool is_bool(const Value *v, bool want) {
  return v && v->t == VType::Bool && v->boolean == want;
}
bool is_account(const Value *v) {  // non-none Account
  return v && v->t == VType::Account && !v->addr_none && !v->str.empty();
}
// Exact canonical raw rendering: the adapter must yield "WC:HEX_UPPER".
bool is_account_str(const Value *v, const std::string &want) {
  return v && v->t == VType::Account && !v->addr_none && v->str == want;
}
const Value *fld(const Value &v, const char *name) { return v.field(name); }

// Cell-building helpers
td::Ref<vm::Cell> empty_cell() { return vm::CellBuilder().finalize(); }
td::Ref<vm::CellSlice> empty_slice() { return vm::load_cell_slice_ref(empty_cell()); }
AbiAddress test_addr() { return ton_abi::gen::address_from_string("0:" + std::string(64, 'a')); }
// The canonical render of test_addr(): uppercase raw.
const std::string kTestAddrCanon = "0:" + std::string(64, 'A');

template <class T>
td::Result<Value> round_trip(const T &s) {
  vm::CellBuilder cb;
  auto st = s.store(cb);
  if (st.is_error()) {
    return std::move(st);
  }
  return abi_parse_body<T>(cb.finalize());
}

template <class T>
td::Result<td::Ref<vm::Cell>> store_abi_value(const T &value) {
  vm::CellBuilder cb;
  TRY_STATUS(value.store(cb));
  return cb.finalize();
}

td::Result<td::Ref<vm::Cell>> inline_jetton_transfer(
    const td::Ref<vm::Cell> &payload,
    std::optional<td::Ref<vm::Cell>> custom_payload = std::nullopt) {
  j::JettonTransfer::CreateArgs args;
  args.query_id = 7;
  args.amount = td::make_refint(1000);
  args.destination = ton_abi::gen::address_from_string("0:" + std::string(64, '1'));
  args.response_destination =
      ton_abi::gen::address_from_string("0:" + std::string(64, '2'));
  args.custom_payload = std::move(custom_payload);
  args.forward_ton_amount = td::make_refint(1);

  vm::CellBuilder cb;
  TRY_STATUS(j::JettonTransfer::create(std::move(args)).store(cb));
  auto payload_slice = vm::load_cell_slice_ref(payload);
  TRY_STATUS(j::JettonForwardPayloadInline::create({std::move(payload_slice)}).store(cb));
  return cb.finalize();
}

td::Result<td::Ref<vm::Cell>> vesting_whitelist_body(
    td::uint64 query_id, const std::vector<AbiAddress> &addresses) {
  if (addresses.empty()) {
    return td::Status::Error("test: vesting whitelist requires a head node");
  }

  td::Ref<vm::Cell> next;
  for (std::size_t i = addresses.size(); i-- > 1;) {
    vm::CellBuilder node;
    TRY_STATUS(ton_abi::store_address_any(node, addresses[i]));
    if (next.not_null() && !node.store_ref_bool(next)) {
      return td::Status::Error("test: cannot store vesting whitelist node ref");
    }
    next = node.finalize();
  }

  vm::CellBuilder body;
  if (!body.store_long_bool(0x7258a69b, 32) ||
      !body.store_ulong_rchk_bool(query_id, 64)) {
    return td::Status::Error("test: cannot store vesting whitelist header");
  }
  TRY_STATUS(ton_abi::store_address_any(body, addresses[0]));
  if (next.not_null() && !body.store_ref_bool(next)) {
    return td::Status::Error("test: cannot store vesting whitelist head ref");
  }
  return body.finalize();
}

const Value *parsed_field(const td::Result<Value> &parsed, const char *name) {
  return parsed.is_ok() ? parsed.ok().field(name) : nullptr;
}

td::Result<td::Ref<vm::Cell>> boc_field_cell(const Value *value) {
  if (!value || value->t != VType::Bytes) {
    return td::Status::Error("test: expected BOC bytes field");
  }
  return vm::std_boc_deserialize(td::Slice(value->str));
}

void test_try_parse_first_jetton_payloads() {
  using Match = std::variant<jp::TextCommentPayload,
                             jp::EncryptedCommentPayload,
                             jp::StonfiSwapPayload>;
  auto parse = [](const td::Ref<vm::Cell> &cell) -> std::optional<Match> {
    return try_parse_first<jp::TextCommentPayload,
                           jp::EncryptedCommentPayload,
                           jp::StonfiSwapPayload>(cell);
  };

  {
    auto stored = store_abi_value(jp::TextCommentPayload::create(
        {vm::load_cell_slice_ref(vm::CellBuilder().store_bytes("hello").finalize())}));
    bool ok = stored.is_ok();
    auto match = ok ? parse(stored.ok()) : std::nullopt;
    check("try_first/text_comment",
          match && std::holds_alternative<jp::TextCommentPayload>(*match));
  }
  {
    auto stored = store_abi_value(jp::EncryptedCommentPayload::create(
        {vm::load_cell_slice_ref(vm::CellBuilder().store_bytes("cipher").finalize())}));
    bool ok = stored.is_ok();
    auto match = ok ? parse(stored.ok()) : std::nullopt;
    check("try_first/encrypted_comment",
          match && std::holds_alternative<jp::EncryptedCommentPayload>(*match));
  }
  {
    auto stored = store_abi_value(jp::StonfiSwapPayload::create(
        {test_addr(), td::make_refint(123), test_addr()}));
    bool ok = stored.is_ok();
    auto match = ok ? parse(stored.ok()) : std::nullopt;
    check("try_first/stonfi_swap",
          match && std::holds_alternative<jp::StonfiSwapPayload>(*match));
  }
  {
    auto garbage = vm::CellBuilder().store_long(0x12345678, 32).finalize();
    check("try_first/garbage_fallthrough", !parse(garbage));
  }
  {
    auto short_payload = vm::CellBuilder().store_long(0, 7).finalize();
    check("try_first/short_fallthrough", !parse(short_payload));

    auto body = inline_jetton_transfer(short_payload);
    auto parsed = body.is_ok()
                      ? parse_message_body("JettonTransfer", body.ok())
                      : td::Result<Value>(body.move_as_error());
    const Value *forward = parsed_field(parsed, "forward_payload");
    check("jetton_transfer/short_is_unknown",
          parsed.is_ok() && fld(parsed.ok(), "sum_type") &&
              fld(parsed.ok(), "sum_type")->str == "Unknown" &&
              fld(parsed.ok(), "payload_sum_type") &&
              fld(parsed.ok(), "payload_sum_type")->is_null() &&
              forward && forward->t == VType::Bytes);
  }
}

void test_jetton_snake_failure_keeps_boc() {
  auto payload = vm::CellBuilder().store_long(0, 32).store_long(1, 1).finalize();
  auto body = inline_jetton_transfer(payload);
  auto parsed = body.is_ok()
                    ? parse_message_body("JettonTransfer", body.ok())
                    : td::Result<Value>(body.move_as_error());
  auto retained = boc_field_cell(parsed_field(parsed, "forward_payload"));
  check("jetton_transfer/snake_failure_unknown",
        parsed.is_ok() && fld(parsed.ok(), "sum_type") &&
            fld(parsed.ok(), "sum_type")->str == "Unknown" &&
            fld(parsed.ok(), "comment") && fld(parsed.ok(), "comment")->is_null() &&
            is_bool(fld(parsed.ok(), "encrypted_comment"), false));
  check("jetton_transfer/snake_failure_keeps_boc",
        retained.is_ok() && retained.ok()->get_hash() == payload->get_hash());
}

void test_jetton_inline_copybug_regression() {
  auto comment_tail = vm::CellBuilder().store_bytes("!!").finalize();
  auto payload = vm::CellBuilder()
                     .store_long(0, 32)
                     .store_bytes("hi")
                     .store_ref(comment_tail)
                     .finalize();
  auto custom = vm::CellBuilder().store_long(0xdead, 16).finalize();
  auto body = inline_jetton_transfer(payload, custom);
  if (body.is_error()) {
    check("jetton_transfer/copybug_fixture_build", false);
    return;
  }

  vm::CellSlice legacy_cs = vm::load_cell_slice(body.ok());
  auto prefix = j::JettonTransfer::from_slice(legacy_cs);
  bool inline_selector = prefix.is_ok() && legacy_cs.have(1) && legacy_cs.fetch_ulong(1) == 0;
  PSlice legacy_payload;
  legacy_payload.cs = legacy_cs;
  auto full_body = vm::load_cell_slice(body.ok());
  for (unsigned i = 0; i < full_body.size_refs(); ++i) {
    legacy_payload.refs.push_back(full_body.prefetch_ref(i));
  }
  auto legacy_cell = inline_selector
                         ? pslice_to_cell(legacy_payload)
                         : td::Result<td::Ref<vm::Cell>>(
                               td::Status::Error("test: expected inline selector"));

  auto parsed = parse_message_body("JettonTransfer", body.ok());
  auto actual = boc_field_cell(parsed_field(parsed, "forward_payload"));
  const std::string true_hash = payload->get_hash().to_hex();
  const std::string legacy_hash =
      legacy_cell.is_ok() ? legacy_cell.ok()->get_hash().to_hex() : std::string("<error>");
  std::printf("JETTON-COPYBUG-HASHES true=%s legacy=%s\n",
              true_hash.c_str(), legacy_hash.c_str());

  check("jetton_transfer/copybug_fixture_true_hash",
        true_hash.rfind("E0644D55DDD9B079", 0) == 0);
  check("jetton_transfer/copybug_fixture_legacy_hash",
        legacy_hash.rfind("D383129802F81F87", 0) == 0);
  check("jetton_transfer/copybug_fixed_hash",
        actual.is_ok() && actual.ok()->get_hash() == payload->get_hash());
  check("jetton_transfer/copybug_not_legacy_hash",
        actual.is_ok() && legacy_cell.is_ok() &&
            actual.ok()->get_hash() != legacy_cell.ok()->get_hash());
  check("jetton_transfer/copybug_comment",
        parsed.is_ok() && fld(parsed.ok(), "comment") &&
            fld(parsed.ok(), "comment")->t == VType::Bytes &&
            fld(parsed.ok(), "comment")->str == "hi!!");
}

void test_vesting_add_whitelist_flattening() {
  const AbiAddress addr1 = ton_abi::gen::address_from_string("0:" + std::string(64, '1'));
  const AbiAddress addr2 = ton_abi::gen::address_from_string("0:" + std::string(64, '2'));
  const AbiAddress addr3 = ton_abi::gen::address_from_string("0:" + std::string(64, '3'));

  {
    auto body = vesting_whitelist_body(17, {addr1, addr2, addr3});
    auto parsed = body.is_ok()
                      ? parse_message_body("VestingAddWhiteList", body.ok())
                      : td::Result<Value>(body.move_as_error());
    const Value *addresses = parsed_field(parsed, "addresses");
    bool ordered = addresses && addresses->t == VType::List && addresses->items &&
                   addresses->items->size() == 3 &&
                   is_account_str(&(*addresses->items)[0], "0:" + std::string(64, '1')) &&
                   is_account_str(&(*addresses->items)[1], "0:" + std::string(64, '2')) &&
                   is_account_str(&(*addresses->items)[2], "0:" + std::string(64, '3'));
    check("vesting_whitelist/three_node_parses", parsed.is_ok());
    check("vesting_whitelist/three_node_query_id", is_int(parsed_field(parsed, "query_id"), 17));
    check("vesting_whitelist/three_node_ordered_void_tail", ordered);
  }

  {
    auto body = vesting_whitelist_body(23, {addr1});
    auto parsed = body.is_ok()
                      ? parse_message_body("VestingAddWhiteList", body.ok())
                      : td::Result<Value>(body.move_as_error());
    const Value *addresses = parsed_field(parsed, "addresses");
    bool singleton = addresses && addresses->t == VType::List && addresses->items &&
                     addresses->items->size() == 1 &&
                     is_account_str(&(*addresses->items)[0], "0:" + std::string(64, '1'));
    check("vesting_whitelist/single_node_parses", parsed.is_ok());
    check("vesting_whitelist/single_node_query_id", is_int(parsed_field(parsed, "query_id"), 23));
    check("vesting_whitelist/single_node_void_tail", singleton);
  }
}

// Full-trampoline-path fixtures
// ClaimRewardMessage below covers the prefix check, uintN-to-Int conversion,
// and Struct-to-Obj("$") path on a production row.

// Jetton fixture: RefInt256 coins -> Int + AbiAddress Std -> Account + Cell.
// The ONLY case carrying a Cell field through the trampoline.
void test_jetton_wallet_data() {
  jw::JettonWalletDataReply::CreateArgs args;
  args.jettonBalance = td::make_refint(123456);
  args.ownerAddress = test_addr();
  args.minterAddress = test_addr();
  args.jettonWalletCode = empty_cell();
  auto r = round_trip(jw::JettonWalletDataReply::create(std::move(args)));
  bool ok = r.is_ok();
  check("jetton/parses", ok);
  if (!ok) return;
  const Value &v = r.ok();
  check("jetton/discriminator", fld(v, "$") && fld(v, "$")->str == "JettonWalletDataReply");
  check("jetton/balance", is_int(fld(v, "jettonBalance"), 123456));
  check("jetton/owner_account", is_account_str(fld(v, "ownerAddress"), kTestAddrCanon));
  check("jetton/minter_account", is_account_str(fld(v, "minterAddress"), kTestAddrCanon));
  check("jetton/code_cell", fld(v, "jettonWalletCode") && fld(v, "jettonWalletCode")->t == VType::Cell);
}

// has_value_field union: primitive variant -> Union kind -> Obj{"$","value"}.
void test_hasvalue_union() {
  lw::AbiUnion_0 u{td::int64(5)};  // int8 variant
  auto r = round_trip(lw::IntAndEitherInt8Or256::create({td::int64(9), u}));
  bool ok = r.is_ok();
  check("union_hasvalue/parses", ok);
  if (!ok) return;
  const Value &v = r.ok();
  check("union_hasvalue/op", is_int(fld(v, "op"), 9));
  const Value *un = fld(v, "i8or256");
  check("union_hasvalue/is_obj", un && un->t == VType::Obj);
  check("union_hasvalue/has_label", un && fld(*un, "$") && fld(*un, "$")->t == VType::Str);
  check("union_hasvalue/value_int", un && is_int(fld(*un, "value"), 5));
}

// Struct-labeled union: variant IS its inner struct (carries own "$"), NOT
// wrapped in {"$","value"}, the key design distinction. Also covers Address +
// Bool + Bits inside the inner struct + an 8-bit prefix.
void test_structlabeled_union() {
  lw::BodyPayload body{lw::BodyPayload1::create({/*should_forward*/ true, /*n_times*/ 3, empty_slice()})};
  auto r = round_trip(lw::SayHiAndGoodbye::create({test_addr(), std::move(body)}));
  bool ok = r.is_ok();
  check("union_structlabeled/parses", ok);
  if (!ok) return;
  const Value &v = r.ok();
  check("union_structlabeled/dest_account", is_account(fld(v, "dest_addr")));
  const Value *b = fld(v, "body");
  check("union_structlabeled/body_is_obj", b && b->t == VType::Obj);
  // Inner struct carries its OWN "$" = variant struct name (NOT a "value" wrapper).
  check("union_structlabeled/inner_discriminator", b && fld(*b, "$") && fld(*b, "$")->str == "BodyPayload1");
  check("union_structlabeled/no_value_wrapper", b && fld(*b, "value") == nullptr);
  check("union_structlabeled/inner_bool", b && is_bool(fld(*b, "should_forward"), true));
  check("union_structlabeled/inner_int", b && is_int(fld(*b, "n_times"), 3));
}

// Direct AbiValue-to-Value adapter units
void test_adapter_direct() {
  {
    Value v = abi_value_to_mch(AbiValue::make_bool(true));
    check("adapter/bool", is_bool(&v, true));
  }
  check("adapter/null", abi_value_to_mch(AbiValue::make_null()).is_null());

  {
    Value v = abi_value_to_mch(AbiValue::make_void());
    check("adapter/void", v.t == VType::Obj && fld(v, "$") && fld(v, "$")->str == "void");
  }
  {
    AbiAddress none;  // kind == None by default
    Value v = abi_value_to_mch(AbiValue::make_address(none));
    check("adapter/addr_none", v.t == VType::Account && v.addr_none);
  }
  {
    AbiAddress ext;
    ext.kind = AbiAddressKind::Extern;
    ext.ext_bits = 10;
    ext.ext_value = empty_slice();
    Value v = abi_value_to_mch(AbiValue::make_address(ext));
    check("adapter/extern", v.t == VType::Obj && is_int(fld(v, "bits"), 10) &&
                                fld(v, "value") && fld(v, "value")->t == VType::Cell);
  }
  {
    Value v = abi_value_to_mch(AbiValue::make_bits(empty_slice()));
    check("adapter/bits_is_cell", v.t == VType::Cell);
  }
  {
    Value v = abi_value_to_mch(AbiValue::make_cell_of(AbiValue::make_int(td::make_refint(5))));
    check("adapter/cellof", v.t == VType::Obj && is_int(fld(v, "ref"), 5));
  }
  {
    auto raw_cell = empty_cell();
    Value v = abi_value_to_mch(
        AbiValue::make_cell_of(AbiValue::make_int(td::make_refint(5)), raw_cell));
    const Value *cell = fld(v, "cell");
    check("adapter/cellof_raw_cell", cell && cell->t == VType::Cell && cell->cell.not_null() &&
                                         cell->cell->get_hash() == raw_cell->get_hash());
  }
  {
    std::vector<AbiValue> items;
    items.push_back(AbiValue::make_int(td::make_refint(1)));
    items.push_back(AbiValue::make_int(td::make_refint(2)));
    Value v = abi_value_to_mch(AbiValue::make_list(std::move(items)));
    check("adapter/list", v.t == VType::List && v.items && v.items->size() == 2 &&
                              is_int(&(*v.items)[0], 1) && is_int(&(*v.items)[1], 2));
  }
  {
    std::vector<std::pair<AbiValue, AbiValue>> entries;
    entries.emplace_back(AbiValue::make_int(td::make_refint(1)), AbiValue::make_int(td::make_refint(2)));
    Value v = abi_value_to_mch(AbiValue::make_map(std::move(entries)));
    bool ok = v.t == VType::List && v.items && v.items->size() == 1;
    ok = ok && (*v.items)[0].t == VType::List && (*v.items)[0].items && (*v.items)[0].items->size() == 2;
    ok = ok && is_int(&(*(*v.items)[0].items)[0], 1) && is_int(&(*(*v.items)[0].items)[1], 2);
    check("adapter/map_is_pairlist", ok);
  }
  {
    Value v = abi_value_to_mch(AbiValue::make_cell(empty_cell()));
    check("adapter/cell", v.t == VType::Cell);
  }
}

void test_abi_projection() {
  Value enum_hit = enum_name(lw::EStoredAsInt8_name_table(), td::make_refint(-100));
  check("projection/enum_hit", enum_hit.t == VType::Str && enum_hit.str == "M100");
  check("projection/enum_miss",
        enum_name(lw::EStoredAsInt8_name_table(), td::make_refint(42)).is_null());

  Value zero_hex = minimal_hex(td::uint64{0});
  check("projection/minimal_hex_zero", zero_hex.t == VType::Str && zero_hex.str == "0x0");
  Value uint_hex = minimal_hex(td::uint64{0x123});
  check("projection/minimal_hex_uint64", uint_hex.t == VType::Str && uint_hex.str == "0x123");
  auto wide = td::dec_string_to_int256(std::string("1208925819614629174706177"));  // 2^80 + 1
  Value wide_hex = minimal_hex(wide);
  check("projection/minimal_hex_wide",
        wide_hex.t == VType::Str && wide_hex.str == "0x100000000000000000001");

  const unsigned char raw[] = {0x00, 0xa8};
  vm::CellBuilder cb;
  cb.store_bits(raw, 13);
  auto bits = vm::load_cell_slice_ref(cb.finalize());
  Value root_hex = root_bits_hex(*bits);
  check("projection/root_bits_hex_partial_byte",
        root_hex.t == VType::Str && root_hex.str == "0x00a8");
}

// Registry and trampoline-guard policy
void test_registry_policy() {
  // Production registry has no collision.
  check("registry/prod_clean", validate_registries().is_ok());

  // Unknown name fails closed.
  auto r = parse_message_body("__NoSuchType__", empty_cell());
  check("registry/unknown_name", r.is_error());

  // Abort-safe open guard: null body -> clean error result (never an abort);
  // the exotic-cell branch shares this code path (special flag).
  auto rn = abi_parse_body<dv::ClaimRewardMessage>(td::Ref<vm::Cell>());
  check("trampoline/null_body_error", rn.is_error());
}

// EVERY production ABI row must be reachable under its OWN key: present in the
// merged registry with ITS function pointer (not shadowed by an earlier source)
// and routable through parse_message_body. Closes the typo'd-/shadowed-key hole
// that per-row hand-written asserts leave open once a row is added but never
// named in a test.
void test_abi_rows_dispatch() {
  const auto &registry = message_parsers();
  for (const auto &row : abi_message_parsers()) {
    auto it = registry.find(row.first);
    check("abi_rows/registered/" + row.first, it != registry.end() && it->second == row.second);
    // An empty body legitimately fails each row's own prefix check; what must
    // NOT come back is a registration error (unknown or ambiguous key).
    auto rr = parse_message_body(row.first, empty_cell());
    bool routed = true;
    if (rr.is_error()) {
      const std::string msg = rr.error().message().str();
      routed = msg.find("is not registered") == std::string::npos &&
               msg.find("ambiguously registered") == std::string::npos;
    }
    check("abi_rows/dispatch/" + row.first, routed);
  }
}

// DeDust swap notification: Cell<DedustSwapInfo> preserves both the decoded
// payload and the exact raw child cell through generated load + registry parse.
void test_dedust_swap_notification_cellof() {
  auto info = d::DedustSwapInfo::create(
      {test_addr(), test_addr(), td::make_refint(111), td::make_refint(222)});
  vm::CellBuilder info_cb;
  auto info_st = info.store(info_cb);
  check("dedust/swap_notification/info_store_ok", info_st.is_ok());
  auto info_cell = info_cb.finalize();

  d::DedustSwapNotification::CreateArgs args;
  args.asset_in = d::Asset{d::AssetTon::create({})};
  args.asset_out = d::Asset{d::AssetTon::create({})};
  args.amount_in = td::make_refint(1000);
  args.amount_out = td::make_refint(900);
  args.info = {std::make_shared<d::DedustSwapInfo>(std::move(info)), info_cell};

  vm::CellBuilder cb;
  auto st = d::DedustSwapNotification::create(std::move(args)).store(cb);
  check("dedust/swap_notification/store_ok", st.is_ok());
  auto r = parse_message_body("DedustSwapNotification", cb.finalize());
  bool ok = r.is_ok();
  check("dedust/swap_notification/parses", ok);
  if (!ok) return;

  const Value &v = r.ok();
  const Value *info_v = fld(v, "info");
  const Value *info_ref = info_v ? fld(*info_v, "ref") : nullptr;
  const Value *raw_cell = info_v ? fld(*info_v, "cell") : nullptr;
  check("dedust/swap_notification/info_ref",
        info_ref && info_ref->t == VType::Obj && fld(*info_ref, "$") &&
            fld(*info_ref, "$")->str == "DedustSwapInfo");
  check("dedust/swap_notification/info_sender",
        info_ref && is_account_str(fld(*info_ref, "sender_address"), kTestAddrCanon));
  check("dedust/swap_notification/info_ref_address",
        info_ref && is_account_str(fld(*info_ref, "ref_address"), kTestAddrCanon));
  check("dedust/swap_notification/info_reserves",
        info_ref && is_int(fld(*info_ref, "reserve_0"), 111) &&
            is_int(fld(*info_ref, "reserve_1"), 222));
  check("dedust/swap_notification/info_raw_cell",
        raw_cell && raw_cell->t == VType::Cell && raw_cell->cell.not_null() &&
            raw_cell->cell->get_hash() == info_cell->get_hash());
}

// DeDust CPMM V2 claim_reward
// Exercises the real registry rows (parse_message_body by BARE name) + the
// mandatory address-render parity check on ownerAddress/excessesTo.
void test_dedust_v2_claim_reward() {
  const std::string A = "0:" + std::string(64, 'A');  // canonical of test_addr()

  // PayoutRewardMessage: the amount-bearing leg. coins -> Int, uint2 -> Int,
  // two addresses -> Account (exact-string parity).
  {
    dv::PayoutRewardMessage::CreateArgs args;
    args.queryId = 42ULL;
    args.amount = td::make_refint(150000000);  // 0.15 TON
    args.rewardIndex = 1ULL;
    args.ownerAddress = test_addr();
    args.excessesTo = test_addr();
    auto r = round_trip(dv::PayoutRewardMessage::create(std::move(args)));
    bool ok = r.is_ok();
    check("dedust_v2/payout_reward/parses", ok);
    if (ok) {
      const Value &v = r.ok();
      check("dedust_v2/payout_reward/discriminator",
            fld(v, "$") && fld(v, "$")->str == "PayoutRewardMessage");
      check("dedust_v2/payout_reward/queryId", is_int(fld(v, "queryId"), 42));
      check("dedust_v2/payout_reward/amount", is_int(fld(v, "amount"), 150000000));
      check("dedust_v2/payout_reward/rewardIndex", is_int(fld(v, "rewardIndex"), 1));
      // Verify canonical address rendering at the production adapter boundary.
      check("dedust_v2/payout_reward/owner_addr_parity", is_account_str(fld(v, "ownerAddress"), A));
      check("dedust_v2/payout_reward/excesses_addr_parity", is_account_str(fld(v, "excessesTo"), A));
    }
  }

  // ClaimRewardMessage: prefix check + uint2 + address.
  {
    dv::ClaimRewardMessage::CreateArgs args;
    args.queryId = 7ULL;
    args.rewardIndex = 2ULL;
    args.excessesTo = test_addr();
    auto r = round_trip(dv::ClaimRewardMessage::create(std::move(args)));
    bool ok = r.is_ok();
    check("dedust_v2/claim_reward/parses", ok);
    if (ok) {
      const Value &v = r.ok();
      check("dedust_v2/claim_reward/discriminator",
            fld(v, "$") && fld(v, "$")->str == "ClaimRewardMessage");
      check("dedust_v2/claim_reward/rewardIndex", is_int(fld(v, "rewardIndex"), 2));
      check("dedust_v2/claim_reward/excesses_addr_parity", is_account_str(fld(v, "excessesTo"), A));
    }
  }

  // PayoutMessage (0x3216ca09): the TON payout leg. int32 exitCode -> Int,
  // nullable cell (absent) -> Null.
  {
    dv::PayoutMessage::CreateArgs args;
    args.queryId = 9ULL;
    args.amount = td::make_refint(70400000);
    args.exitCode = 0;
    args.payload = std::nullopt;
    auto r = round_trip(dv::PayoutMessage::create(std::move(args)));
    bool ok = r.is_ok();
    check("dedust_v2/payout_msg/parses", ok);
    if (ok) {
      const Value &v = r.ok();
      check("dedust_v2/payout_msg/discriminator",
            fld(v, "$") && fld(v, "$")->str == "PayoutMessage");
      check("dedust_v2/payout_msg/amount", is_int(fld(v, "amount"), 70400000));
      check("dedust_v2/payout_msg/exitCode", is_int(fld(v, "exitCode"), 0));
      check("dedust_v2/payout_msg/payload_null", fld(v, "payload") && fld(v, "payload")->is_null());
    }
  }

  // Real registry dispatch by bare name: parse_message_body routes
  // the production row, not just the direct abi_parse_body<T> call above.
  {
    vm::CellBuilder cb;
    dv::ClaimRewardMessage::CreateArgs args;
    args.queryId = 1ULL;
    args.rewardIndex = 0ULL;
    args.excessesTo = test_addr();
    auto st = dv::ClaimRewardMessage::create(std::move(args)).store(cb);
    check("dedust_v2/registry/store_ok", st.is_ok());
    auto r = parse_message_body("ClaimRewardMessage", cb.finalize());
    check("dedust_v2/registry/dispatch_by_name", r.is_ok() && r.ok().t == VType::Obj);
  }
}

}  // namespace

int run_abi_bridge_test() {
  g_fail = 0;
  test_try_parse_first_jetton_payloads();
  test_jetton_snake_failure_keeps_boc();
  test_jetton_inline_copybug_regression();
  test_vesting_add_whitelist_flattening();
  test_jetton_wallet_data();
  test_hasvalue_union();
  test_structlabeled_union();
  test_adapter_direct();
  test_abi_projection();
  test_dedust_swap_notification_cellof();
  test_dedust_v2_claim_reward();
  test_registry_policy();
  test_abi_rows_dispatch();
  std::printf("ABI-BRIDGE-TEST %s\n", g_fail == 0 ? "ALL PASS" : "FAILURES");
  return g_fail == 0 ? 0 : 1;
}

}  // namespace mch
