// ABI bridge self-test. See AbiBridgeTest.h.
#include "AbiBridgeTest.h"

#include "AbiBridge.h"
#include "MsgParse.h"
#include "Value.h"

#include "AbiGenSupport.h"  // ton_abi::gen::address_from_string
#include "AbiValue.h"
#include "AbiLeavesAddress.h"

#include "jetton_wallet_contract_gen.h"
#include "lots_of_wrappers_gen.h"
#include "dedust_v2_gen.h"  // DeDust V2 production pair

#include "common/refint.h"
#include "vm/cells/CellBuilder.h"
#include "vm/cellslice.h"

#include <cstdio>
#include <string>
#include <variant>
#include <vector>

namespace mch {

namespace {

namespace jw = ton_abi::gen::jetton_wallet_contract;
namespace lw = ton_abi::gen::lots_of_wrappers;
namespace dv = ton_abi::gen::dedust_v2;
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
// Exact canonical raw rendering: the
// adapter must yield "WC:HEX_UPPER" == Python AccountId.as_str().upper().
bool is_account_str(const Value *v, const std::string &want) {
  return v && v->t == VType::Account && !v->addr_none && v->str == want;
}
const Value *fld(const Value &v, const char *name) { return v.field(name); }

// Cell-building helpers
td::Ref<vm::Cell> empty_cell() { return vm::CellBuilder().finalize(); }
td::Ref<vm::CellSlice> empty_slice() { return vm::load_cell_slice_ref(empty_cell()); }
AbiAddress test_addr() { return ton_abi::gen::address_from_string("0:" + std::string(64, 'a')); }
// The canonical render of test_addr(): uppercase raw, == Python AccountId.as_str().
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

// Registry and trampoline-guard policy
void test_registry_policy() {
  // Duplicate keys across parser sources are reported.
  const std::vector<std::pair<std::string, MsgParserFn>> src_a = {{"A", nullptr}, {"B", nullptr}};
  const std::vector<std::pair<std::string, MsgParserFn>> src_b = {{"B", nullptr},
                                                                 {"C", nullptr}};  // B collides
  auto dups = duplicate_parser_keys(ParserSources{&src_a, &src_b});
  check("registry/dup_detected", dups.size() == 1 && dups[0] == "B");

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
  test_jetton_wallet_data();
  test_hasvalue_union();
  test_structlabeled_union();
  test_adapter_direct();
  test_dedust_v2_claim_reward();
  test_registry_policy();
  test_abi_rows_dispatch();
  std::printf("ABI-BRIDGE-TEST %s\n", g_fail == 0 ? "ALL PASS" : "FAILURES");
  return g_fail == 0 ? 0 : 1;
}

}  // namespace mch
