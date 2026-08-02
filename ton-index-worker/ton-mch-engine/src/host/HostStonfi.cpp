// Stonfi v1/v2 host bindings (builders/stonfi_v1.py, builders/stonfi_v2.py +
// blocks/swaps.py cores). See host/HostImpls.h for the internal registry
// surface and HostRegistry.h for the public one.
#include "host/HostImpls.h"

#include "host/BlockViews.h"
#include "host/DexPton.h"
#include "host/DexRecords.h"
#include "host/HostCommon.h"

#include "BlockTree.h"
#include "BuildRuntime.h"
#include "ExprRuntime.h"
#include "MsgParse.h"
#include "parse/PSlice.h"

#include "common/refint.h"
#include "vm/cellslice.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace mch {

namespace {

constexpr std::uint32_t kStonfiPaymentRequest = 0xf93bb43f;
constexpr std::uint32_t kStonfiSwap = 0x25938561;      // v1 swap request call
constexpr std::uint32_t kStonfiSwapOk = 0xc64370e5;
constexpr std::uint32_t kStonfiSwapOkRef = 0x45078540;  // ok_ref (referral leg)
constexpr std::uint32_t kStonfiSwapNoLiq = 0x5ffe1295;
constexpr std::uint32_t kStonfiSwapReserveErr = 0x38976e9b;

// stonfi v2 opcodes (blocks/swaps.py StonfiV2SwapBlockMatcher). The swap
// REQUEST call carries op 0x6664de2a (swap_opcode) with a StonfiSwapV2 body;
// the pay_to call carries op 0x657b54f5 with a StonfiV2PayTo body (the
// StonfiSwapV2.opcode class attr is misleading/unused, its __init__ never
// checks the tag). 0x6664de2a doubles as the forward-payload swap opcode and
// one of the cross-swap sum-type tags.
constexpr std::uint32_t kV2Swap = 0x6664de2a;       // swap request call
constexpr std::uint32_t kV2PayTo = 0x657b54f5;      // pay_to call
constexpr std::uint32_t kV2CrossSwapB = 0x69cf1a5b;
constexpr std::uint32_t kPTonTransfer = 0x01f3835d;
// Only used to say, in a [mch-reject] reason, whether an absent out leg was a
// jetton transfer that never became a jetton_transfer BLOCK (its own matcher
// rejects on a missing wallet interface) rather than an absent message.
constexpr std::uint32_t kJettonTransfer = 0x0f8a7ea5;

// acc_str is shared with the tonco core through host/BlockViews.h.

// messages/swaps.py StonfiPaymentRequest.__init__ IN FULL: opcode(32),
// query_id(64), owner(load_address), exit_code(32), then a ref whose slice
// carries amount0/token0/amount1/token1. Python constructs the whole object
// before the caller reads .exit_code, so any failure past exit_code (e.g. a
// missing ref) still raises -> the pred's try/except returns False. Reproduce
// the full parse and only report exit_code when every field parsed.
// messages/swaps.py StonfiPaymentRequest, full field set (the v1 swap core
// reads exit_code + amount0/token0/amount1/token1 from the ref).
struct PaymentReq {
  std::uint32_t exit_code{0};
  td::RefInt256 amount0_out, amount1_out;
  Value token0, token1;  // account values
};
// The body is parsed by the ABI-faithful StonfiPaymentRequest declaration;
// unpack its Obj into the host struct. A parse failure propagates the same
// td::Status the hand walk raised.
td::Result<PaymentReq> parse_payment_request(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(v, parse_message_body("StonfiPaymentRequest", body));
  const Value *info_cell = v.field("info");
  const Value *info = info_cell != nullptr ? info_cell->field("ref") : nullptr;
  if (info == nullptr) {
    return td::Status::Error("payment request: info.ref missing");
  }
  PaymentReq pr;
  pr.exit_code = static_cast<std::uint32_t>(v.field("exit_code")->num->to_long());
  pr.amount0_out = info->field("amount0_out")->num;
  pr.token0 = *info->field("token0");
  pr.amount1_out = info->field("amount1_out")->num;
  pr.token1 = *info->field("token1");
  return pr;
}

td::Result<std::uint32_t> stonfi_payment_exit_code(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(pr, parse_payment_request(body));
  return pr.exit_code;
}

// messages/swaps.py StonfiSwapMessage. Fields the v1 core reads:
// from_user_address, token_wallet, amount, from_real_user (in the ref).
struct SwapV1 {
  Value from_user_address;
  Value token_wallet;
  td::RefInt256 amount;
  Value from_real_user;
};
// ABI-faithful StonfiSwapMessage unpacker.
td::Result<SwapV1> parse_swap_v1(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(v, parse_message_body("StonfiSwapMessage", body));
  const Value *info_cell = v.field("info");
  const Value *info = info_cell != nullptr ? info_cell->field("ref") : nullptr;
  if (info == nullptr) {
    return td::Status::Error("stonfi swap: info.ref missing");
  }
  SwapV1 sw;
  sw.from_user_address = *v.field("from_user_address");
  sw.token_wallet = *v.field("token_wallet");
  sw.amount = v.field("amount")->num;
  sw.from_real_user = *info->field("from_real_user");
  return sw;
}

// sender-related exit codes (stonfi_sender_related_exit_codes).
bool sender_related(std::uint32_t ec) {
  return ec == kStonfiSwapOk || ec == kStonfiSwapNoLiq || ec == kStonfiSwapReserveErr;
}

// Stonfi v2

// open_ref_cell is shared with the tonco and dedust parsers.

// messages/swaps.py StonfiV2PayTo. Fields the core reads: exit_code,
// amount0_out, token0_address, amount1_out, token1_address (canonical upper).
struct PayTo {
  std::uint32_t exit_code{0};
  td::RefInt256 amount0_out, amount1_out;
  Value token0, token1;  // account values
};
// ABI-faithful StonfiV2PayTo unpacker.
td::Result<PayTo> parse_pay_to(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(v, parse_message_body("StonfiV2PayTo", body));
  const Value *info_cell = v.field("info");
  const Value *info = info_cell != nullptr ? info_cell->field("ref") : nullptr;
  if (info == nullptr) {
    return td::Status::Error("stonfi pay_to: info.ref missing");
  }
  PayTo pt;
  pt.exit_code = static_cast<std::uint32_t>(v.field("exit_code")->num->to_long());
  pt.amount0_out = info->field("amount0_out")->num;
  pt.token0 = *info->field("token0");
  pt.amount1_out = info->field("amount1_out")->num;
  pt.token1 = *info->field("token1");
  return pt;
}

// messages/swaps.py StonfiSwapV2: token_wallet1 + custom_payload cell (2 ref
// levels deep). The other fields are not read by the core.
struct SwapV2 {
  Value token_wallet1;             // account
  td::Ref<vm::Cell> custom_payload;  // null when absent
};
td::Result<SwapV2> parse_swap_v2(const td::Ref<vm::Cell> &body) {
  TRY_RESULT(ctx, open_body(body));
  auto &cs = ctx.cs;
  if (!cs.have(32 + 64) || !cs.advance(32 + 64)) {
    return td::Status::Error("swap_v2: header underflow");
  }
  TRY_RESULT(from_user, load_address_py(cs));
  (void)from_user;
  TRY_RESULT(left, load_coins_py(cs));
  (void)left;
  TRY_RESULT(right, load_coins_py(cs));
  (void)right;
  if (cs.size_refs() < 1) {
    return td::Status::Error("swap_v2: dex_payload ref missing");
  }
  TRY_RESULT(dex, open_ref_cell(cs.fetch_ref()));
  if (!dex.have(32) || !dex.advance(32)) {  // transferred_op
    return td::Status::Error("swap_v2: transferred_op underflow");
  }
  SwapV2 out;
  TRY_RESULT_ASSIGN(out.token_wallet1, load_address_py(dex));
  TRY_RESULT(refund, load_address_py(dex));
  (void)refund;
  TRY_RESULT(exc, load_address_py(dex));
  (void)exc;
  if (!dex.have(64) || !dex.advance(64)) {  // tx_deadline
    return td::Status::Error("swap_v2: tx_deadline underflow");
  }
  if (dex.size_refs() < 1) {
    return td::Status::Error("swap_v2: swap_body ref missing");
  }
  TRY_RESULT(sb, open_ref_cell(dex.fetch_ref()));
  TRY_RESULT(min_out, load_coins_py(sb));
  (void)min_out;
  TRY_RESULT(receiver, load_address_py(sb));
  (void)receiver;
  TRY_RESULT(fwd_gas, load_coins_py(sb));
  (void)fwd_gas;
  // custom_payload maybe-ref (first maybe-ref in swap_body).
  if (!sb.have(1)) {
    return td::Status::Error("swap_v2: custom_payload bit underflow");
  }
  if (sb.fetch_ulong(1)) {
    if (sb.size_refs() < 1) {
      return td::Status::Error("swap_v2: custom_payload ref missing");
    }
    out.custom_payload = sb.fetch_ref();
  }
  return out;
}

// StonfiSwapV2.get_pool_accounts_recursive: walk the custom_payload cross-swap
// chain, collecting pool wallet addresses (canonical upper; "" where a decoded
// address is addr_none). Faults on a malformed head (Python load_uint underflow
// raises), which propagates to a match rejection.
td::Result<std::vector<std::string>> pool_accounts(const SwapV2 &sv) {
  std::vector<std::string> accounts;
  accounts.push_back(acc_str(sv.token_wallet1).value_or(std::string{}));
  if (sv.custom_payload.is_null()) {
    return accounts;
  }
  TRY_RESULT(cur, open_ref_cell(sv.custom_payload));
  while (true) {
    if (!cur.have(32)) {
      return td::Status::Error("pool_accounts: sum_type underflow");
    }
    auto sum_type = static_cast<std::uint32_t>(cur.fetch_ulong(32));
    if (sum_type != kV2Swap && sum_type != kV2CrossSwapB) {
      break;
    }
    TRY_RESULT(addr, load_address_py(cur));
    accounts.push_back(acc_str(addr).value_or(std::string{}));
    if (cur.size_refs() == 0) {
      break;
    }
    TRY_RESULT(cross, open_ref_cell(cur.fetch_ref()));
    TRY_RESULT(min_out, load_coins_py(cross));
    (void)min_out;
    TRY_RESULT(c2, load_coins_py(cross));
    (void)c2;
    if (cross.size_refs() == 0) {
      break;
    }
    if (!cross.have(1)) {
      return td::Status::Error("pool_accounts: maybe bit underflow");
    }
    if (cross.fetch_ulong(1)) {
      if (cross.size_refs() < 1) {
        return td::Status::Error("pool_accounts: inner ref missing");
      }
      TRY_RESULT_ASSIGN(cur, open_ref_cell(cross.fetch_ref()));
    } else {
      break;
    }
  }
  return accounts;
}

// address_selectors.py extract_target_wallet_stonfi_v2_swap: the single target
// wallet (canonical upper) from a swap-request body, or nullopt.
std::optional<std::string> extract_target_wallet(const td::Ref<vm::Cell> &body) {
  auto r_ctx = open_body(body);
  if (r_ctx.is_error()) {
    return std::nullopt;
  }
  auto ctx = r_ctx.move_as_ok();
  auto &cs = ctx.cs;
  if (!cs.have(32 + 64) || !cs.advance(32 + 64)) {
    return std::nullopt;
  }
  if (load_coins_py(cs).is_error() || load_address_py(cs).is_error()) {
    return std::nullopt;
  }
  if (!cs.have(1)) {
    return std::nullopt;
  }
  vm::CellSlice payload;
  if (cs.fetch_ulong(1)) {
    if (cs.size_refs() < 1) {
      return std::nullopt;
    }
    auto r = open_ref_cell(cs.fetch_ref());
    if (r.is_error()) {
      return std::nullopt;
    }
    payload = r.move_as_ok();
  } else {
    payload = cs;  // Slice.copy()
  }
  if (payload.size() == 0 || !payload.have(32)) {
    return std::nullopt;
  }
  auto op = static_cast<std::uint32_t>(payload.fetch_ulong(32));
  if (op != kV2Swap) {
    return std::nullopt;
  }
  auto r_addr = load_address_py(payload);
  if (r_addr.is_error()) {
    return std::nullopt;
  }
  return acc_str(r_addr.ok());
}

}  // namespace

// builders/stonfi_v1.py stonfi_v1_sender_payment: parse the payment request
// body (abort-safe) and accept the sender-related exit codes (ok / no_liq /
// reserve_err); the ok_ref referral payment fails this gate and stays
// unconsumed. Any parse failure -> False (Python try/except).
bool stonfi_v1_sender_payment(const Block *block) {
  auto r_body = block_body(block);
  if (r_body.is_error()) {
    return false;
  }
  auto r = stonfi_payment_exit_code(r_body.ok());
  if (r.is_error()) {
    return false;
  }
  std::uint32_t ec = r.ok();
  return ec == kStonfiSwapOk || ec == kStonfiSwapNoLiq || ec == kStonfiSwapReserveErr;
}

// builders/stonfi_v1.py _stonfi_v1_swap_data -> blocks/swaps.py
// build_stonfi_v1_swap_core. Arguments are the in_transfer anchor and
// StonfiSwap call). Resolve the outgoing transfer by btype to match the generic
// declarative jetton_transfer block. There is no try/except in the Python
// wrapper, so any nav/parse/lookup miss raises -> rt_fault -> match rejection.
EvalResult stonfi_v1_swap_data(BuildEnv &env, const std::vector<Value> &args) {
  if (args.size() != 2) {
    return rt_fault("stonfi_v1_swap_data: bad arguments");
  }
  const Block *block = as_block(args[0]);  // in_transfer (anchor jetton_transfer)
  const Block *swap = as_block(args[1]);   // StonfiSwap call
  if (block == nullptr || swap == nullptr) {
    return rt_fault("stonfi_v1: null capture");
  }
  // AccountId(x): None -> addr_none; a present address stays as-is.
  auto acc_wrap = [](const Value &v) {
    return v.is_null() ? Value::make_account_none() : v;
  };

  auto r_sb = block_body(swap);
  if (r_sb.is_error()) return rt_fault("stonfi_v1: swap body");
  auto r_sw = parse_swap_v1(r_sb.move_as_ok());
  if (r_sw.is_error()) return rt_fault("stonfi_v1: swap parse");
  SwapV1 sw = r_sw.move_as_ok();

  // payment_requests: swap children that are StonfiPaymentRequest calls with a
  // jetton_transfer child, in tree order (matches the Python builder).
  struct PR { PaymentReq msg; const Block *blk; };
  std::vector<PR> prs;
  for (Block *x : swap->next_blocks) {
    if (!is_call_op(x, kStonfiPaymentRequest)) continue;
    bool has_jt = false;
    for (Block *nb : x->next_blocks) {
      if (nb->btype == "jetton_transfer") { has_jt = true; break; }
    }
    if (!has_jt) continue;
    auto rb = block_body(x);
    if (rb.is_error()) return rt_fault("stonfi_v1: payment body");
    auto rp = parse_payment_request(rb.move_as_ok());
    if (rp.is_error()) return rt_fault("stonfi_v1: payment parse");
    prs.push_back({rp.move_as_ok(), x});
  }
  if (prs.empty()) return rt_fault("stonfi_v1: no payment requests");  // assert len>0

  auto first_jt_child = [](const Block *pb) -> const Block * {
    for (Block *b : pb->next_blocks) {
      if (b->btype == "jetton_transfer") return b;
    }
    return nullptr;
  };

  td::RefInt256 out_amt, ref_amt;   // null == None
  Value out_addr, ref_addr;         // Null == None
  const Block *outgoing_jt = nullptr;
  bool success_swap = false;
  const Block *in_jt = swap->previous_block;

  for (auto &pr : prs) {
    td::RefInt256 amount;
    Value addr;
    if (!pr.msg.amount0_out.is_null() && pr.msg.amount0_out->sgn() > 0) {
      amount = pr.msg.amount0_out;
      addr = pr.msg.token0;
    } else {
      amount = pr.msg.amount1_out;
      addr = pr.msg.token1;
    }
    if (sender_related(pr.msg.exit_code)) {
      success_swap = (pr.msg.exit_code == kStonfiSwapOk);
      if (out_amt.is_null()) {
        outgoing_jt = first_jt_child(pr.blk);
        out_amt = amount;
        out_addr = addr;
      } else if (td::cmp(out_amt, amount) < 0) {
        outgoing_jt = first_jt_child(pr.blk);
        ref_amt = out_amt;
        ref_addr = out_addr;
        out_amt = amount;
        out_addr = addr;
      }
    } else if (pr.msg.exit_code == kStonfiSwapOkRef) {
      ref_amt = amount;
      ref_addr = addr;  // referral_request_blocks dropped (never consumed)
    }
  }

  Value actual_out_addr = out_addr;
  // stonfi_swap_body override of out_addr (jetton_wallet), if present.
  if (block->btype == "jetton_transfer") {
    Value sb = data_field(block, "stonfi_swap_body");
    if (sb.t == VType::Dict) {
      const Value *jw = sb.field("jetton_wallet");
      if (jw != nullptr) out_addr = *jw;
    }
  }
  // Three jetton_wallet lookups, ISSUED before any deref so the two-phase
  // collect pass records all keys before a null-lookup fault. A None address
  // here mirrors Python's `addr.to_str()` on None -> raise -> reject.
  auto s_out = acc_str(out_addr);
  auto s_act = acc_str(actual_out_addr);
  auto s_in = acc_str(sw.token_wallet);
  if (!s_out || !s_act || !s_in) {
    return rt_fault(std::string("stonfi_v1: null address for wallet lookup (out=") +
                    (s_out ? *s_out : "none") + " actual_out=" + (s_act ? *s_act : "none") +
                    " dex_in=" + (s_in ? *s_in : "none") + ")");
  }
  Value out_wallet = env.lookups->get("jetton_wallet", std::vector<Value>{Value::make_str(*s_out)});
  Value actual_out_wallet =
      env.lookups->get("jetton_wallet", std::vector<Value>{Value::make_str(*s_act)});
  Value dex_in_wallet =
      env.lookups->get("jetton_wallet", std::vector<Value>{Value::make_str(*s_in)});

  // dex_in_wallet.owner is dereferenced unconditionally in Python -> a miss
  // raises -> reject; outgoing_jt likewise (its .data is read unguarded).
  if (dex_in_wallet.is_null()) {
    return rt_fault("stonfi_v1: dex_in wallet lookup miss (jetton_wallet=" + *s_in + ")");
  }
  if (outgoing_jt == nullptr) {
    return rt_fault("stonfi_v1: no outgoing jetton transfer (payment_requests=" +
                    std::to_string(prs.size()) + ")");
  }

  Value in_source_jw =
      data_truthy(in_jt, "has_internal_transfer") ? data_field(in_jt, "sender_wallet")
                                                  : Value::null();
  Value out_dest_jw =
      data_truthy(outgoing_jt, "has_internal_transfer") ? data_field(outgoing_jt, "receiver_wallet")
                                                        : Value::null();

  const Value *in_owner = dex_in_wallet.field("owner");
  Value in_dest = (in_owner != nullptr && in_owner->t == VType::Str)
                      ? account_from_opt(in_owner->str)
                      : Value::make_account_none();

  Value::Fields incoming{
      {"asset", wallet_jetton_asset(dex_in_wallet, /*pton_conversion=*/false)},
      {"amount", Value::make_amount(sw.amount.is_null() ? td::make_refint(0) : sw.amount)},
      {"source", acc_wrap(sw.from_real_user)},
      {"source_jetton_wallet", in_source_jw},
      {"destination", in_dest},
      {"destination_jetton_wallet", acc_wrap(sw.token_wallet)}};

  Value::Fields outgoing{
      {"asset", wallet_jetton_asset(actual_out_wallet, /*pton_conversion=*/false)},
      {"amount", Value::make_amount(out_amt.is_null() ? td::make_refint(0) : out_amt)},
      {"source", data_field(outgoing_jt, "sender")},
      {"source_jetton_wallet", data_field(outgoing_jt, "sender_wallet")}};
  if (!out_dest_jw.is_null()) {
    outgoing.emplace_back("destination_jetton_wallet", out_dest_jw);
    outgoing.emplace_back("destination", data_field(outgoing_jt, "receiver"));
  } else {
    Value sb = data_field(in_jt, "stonfi_swap_body");
    if (sb.t == VType::Dict) {
      const Value *ua = sb.field("user_address");
      outgoing.emplace_back("destination", acc_wrap(ua != nullptr ? *ua : Value::null()));
      outgoing.emplace_back("destination_jetton_wallet", Value::null());
    } else {
      outgoing.emplace_back("destination", acc_wrap(sw.from_user_address));
      outgoing.emplace_back("destination_jetton_wallet", Value::null());
    }
  }

  // target/destination asset from out_wallet (no pton conversion; None when the
  // wallet is absent or carries no jetton master).
  Value target_asset = Value::null();
  if (!out_wallet.is_null()) {
    const Value *jf = out_wallet.field("jetton");
    if (jf != nullptr && jf->t == VType::Str) {
      auto norm = normalize_raw_address(jf->str);
      target_asset = Value::make_asset_jetton(norm ? *norm : jf->str);
    }
  }

  Value::Fields d;
  d.emplace_back("dex", Value::make_str("stonfi"));
  d.emplace_back("sender", acc_wrap(sw.from_real_user));
  d.emplace_back("receiver", acc_wrap(sw.from_user_address));
  d.emplace_back("dex_incoming_transfer", Value::make_dict(std::move(incoming)));
  d.emplace_back("dex_outgoing_transfer", Value::make_dict(std::move(outgoing)));
  d.emplace_back("destination_asset", std::move(target_asset));
  d.emplace_back("destination_wallet", out_addr.is_null() ? Value::null() : acc_wrap(out_addr));
  d.emplace_back("referral_amount",
                 ref_amt.is_null() ? Value::make_amount_none() : Value::make_amount(ref_amt));
  d.emplace_back("referral_address", ref_addr.is_null() ? Value::null() : acc_wrap(ref_addr));
  d.emplace_back("peer_swaps", Value::make_list({}));
  d.emplace_back("success", Value::make_bool(success_swap));
  return rt_ok(Value::make_obj(std::move(d)));
}

// The stonfi_v2 jetton-transfer output arm uses the inline predicate
// `where (not .has_internal_transfer)`.

// builders/stonfi.py pton_self_transfer -> validate_stonfi_v2_intermediate_pton_transfer
// (try/except -> False). The intermediate pton call's previous jetton_transfer
// must be non-internal, address the same receiver, and the pton forward payload
// must carry the swap opcode.
bool pton_self_transfer(const Block *block) {
  const Block *prev = block->previous_block;
  if (prev == nullptr) {
    return false;
  }
  if (data_truthy(prev, "has_internal_transfer")) {
    return false;
  }
  Value recv = data_field(prev, "receiver");
  const Message *m = block_msg(block);
  if (m == nullptr || !m->opcode32() || *m->opcode32() != kPTonTransfer) {
    return false;
  }
  std::optional<std::string> dst = m != nullptr ? m->destination : std::nullopt;
  Value dst_acc = account_from_opt(dst);
  // AccountId(receiver) != message.destination -> canonical compare.
  auto rs = acc_str(recv), ds = acc_str(dst_acc);
  if (rs != ds) {
    return false;
  }
  auto r_body = block_body(block);
  if (r_body.is_error()) {
    return false;
  }
  auto r = parse_message_body("PTonTransfer", r_body.ok());
  if (r.is_error()) {
    return false;
  }
  const Value *fp = r.ok().field("forward_payload");
  if (fp == nullptr) {
    return false;
  }
  EvalResult unwrapped = rt_builtin_tail_unwrap(*fp);
  if (unwrapped.faulted || unwrapped.value.t != VType::Cell) {
    return false;
  }
  auto r_cs = open_ref_cell(unwrapped.value.cell);
  if (r_cs.is_error()) {
    return false;
  }
  auto cs = r_cs.move_as_ok();
  if (!cs.have(32)) {
    return false;
  }
  return static_cast<std::uint32_t>(cs.fetch_ulong(32)) == kV2Swap;
}

namespace {

// The bigger-amount (amount, token) of a pay_to's two out-legs (assets.sort by
// amount desc, take [0]; stable on ties -> token0).
std::pair<td::RefInt256, Value> bigger_leg(const PayTo &pt) {
  bool zero0 = pt.amount0_out.is_null();
  bool zero1 = pt.amount1_out.is_null();
  int cmp = (zero0 || zero1) ? 0 : td::cmp(pt.amount0_out, pt.amount1_out);
  if (cmp >= 0) {
    return {pt.amount0_out, pt.token0};
  }
  return {pt.amount1_out, pt.token1};
}

// dict {amount, asset}.
Value leg_dict(Value amount, Value asset) {
  Value::Fields f;
  f.emplace_back("amount", std::move(amount));
  f.emplace_back("asset", std::move(asset));
  return Value::make_dict(std::move(f));
}

// wallet.jetton -> Asset with the pTON conversion (a missing jetton yields Null,
// the caller treats it as "no asset").
Value asset_from_wallet_jetton(const Value &wallet) {
  return wallet_jetton_asset(wallet, /*pton_conversion=*/true);
}

}  // namespace

// builders/stonfi.py _stonfi_v2_swap_data -> blocks/swaps.py
// build_stonfi_v2_swap_core. No try/except in the Python wrapper, so any parse/
// nav failure raises -> match rejection (rt_fault here). The incoming btype
// selects the leg construction: jetton_transfer reads the produced block data;
// the pTON call path synthesizes the message-backed TON leg.
EvalResult stonfi_v2_swap_data(BuildEnv &env, const std::vector<Value> &args) {
  if (args.size() != 1 || args[0].t != VType::List || args[0].items->empty()) {
    return rt_fault("stonfi_v2_swap_data: bad arguments");
  }
  std::vector<const Block *> consumed;
  for (const Value &v : *args[0].items) {
    const Block *b = as_block(v);
    if (b != nullptr) {
      consumed.push_back(b);
    }
  }
  if (consumed.empty()) {
    return rt_fault("stonfi_v2_swap_data: empty consumed");
  }
  const Block *anchor = consumed.front();

  // --- _derive_parts ---
  std::vector<const Block *> payouts;
  for (const Block *b : consumed) {
    if (is_call_op(b, kV2PayTo)) {
      payouts.push_back(b);
    }
  }
  auto in_payouts = [&](const Block *p) {
    return std::find(payouts.begin(), payouts.end(), p) != payouts.end();
  };
  std::vector<const Block *> swaps;
  for (const Block *b : consumed) {
    if (is_call_op(b, kV2Swap)) {
      swaps.push_back(b);
    }
  }
  std::stable_sort(swaps.begin(), swaps.end(),
                   [](const Block *a, const Block *b) { return a->min_lt < b->min_lt; });
  std::vector<std::pair<const Block *, const Block *>> peer_swap_blocks;
  for (const Block *s : swaps) {
    const Block *pay = nullptr;
    for (Block *p : s->next_blocks) {
      if (in_payouts(p)) {
        pay = p;
        break;
      }
    }
    if (pay == nullptr) {
      return rt_fault("stonfi_v2: swap without payout child");  // next() no default
    }
    peer_swap_blocks.emplace_back(s, pay);
  }
  const Block *prev = anchor->previous_block;
  if (prev == nullptr) {
    return rt_fault("stonfi_v2: anchor has no previous block");
  }
  const Block *in_transfer =
      prev->btype == "jetton_transfer" ? prev : prev->previous_block;
  const Block *out_transfer = nullptr;
  std::int64_t best_lt = 0;
  for (const Block *b : consumed) {
    if (b->btype == "jetton_transfer" && in_payouts(b->previous_block)) {
      if (out_transfer == nullptr || b->min_lt > best_lt) {
        out_transfer = b;
        best_lt = b->min_lt;
      }
    }
  }

  // --- build_stonfi_v2_swap_core ---
  bool ok = true;
  struct Step {
    Value token_wallet1;
    std::int64_t min_lt;
    PayTo pay_to;
    SwapV2 swap;
  };
  std::vector<Step> steps;
  for (auto &pr : peer_swap_blocks) {
    auto rb_pay = block_body(pr.second);
    if (rb_pay.is_error()) {
      return rt_fault("stonfi_v2: pay_to body");
    }
    auto r_pt = parse_pay_to(rb_pay.move_as_ok());
    if (r_pt.is_error()) {
      return rt_fault("stonfi_v2: pay_to parse");
    }
    PayTo pt = r_pt.move_as_ok();
    if (pt.exit_code != kStonfiSwapOk) {
      ok = false;
    }
    auto rb_sw = block_body(pr.first);
    if (rb_sw.is_error()) {
      return rt_fault("stonfi_v2: swap body");
    }
    auto r_sv = parse_swap_v2(rb_sw.move_as_ok());
    if (r_sv.is_error()) {
      return rt_fault("stonfi_v2: swap parse");
    }
    SwapV2 sv = r_sv.move_as_ok();
    steps.push_back({sv.token_wallet1, pr.first->min_lt, std::move(pt), std::move(sv)});
  }
  std::stable_sort(steps.begin(), steps.end(),
                   [](const Step &a, const Step &b) { return a.min_lt < b.min_lt; });

  // target pool asset (only observed via the failed-swap fallback).
  auto r_accts = pool_accounts(steps.front().swap);
  if (r_accts.is_error()) {
    return rt_fault("stonfi_v2: pool accounts");
  }
  std::vector<std::string> accts = r_accts.move_as_ok();
  Value destination_asset = Value::null();
  {
    const std::string &target = accts.back();
    Value tpw = env.lookups->get("jetton_wallet", std::vector<Value>{Value::make_str(target)});
    if (!tpw.is_null()) {
      destination_asset = asset_from_wallet_jetton(tpw);  // null when wallet.jetton null
    }
  }

  // per-hop pool wallet -> asset map.
  std::map<std::string, Value> pool_map;
  for (const Step &st : steps) {
    auto key = acc_str(st.token_wallet1);
    if (!key) {
      return rt_fault("stonfi_v2: step wallet addr_none");  // .to_str on None
    }
    Value jw = env.lookups->get("jetton_wallet", std::vector<Value>{Value::make_str(*key)});
    if (!jw.is_null()) {
      pool_map[*key] = asset_from_wallet_jetton(jw);
    } else {
      break;  // block.broken = True (unobserved); stop filling the map
    }
  }

  // Dispatch by incoming btype: a produced jetton_transfer supplies its data;
  // the single-leaf PTonTransfer call synthesizes the message-backed TON leg.
  if (in_transfer == nullptr) {
    return rt_fault("stonfi_v2: no in_transfer");
  }
  Value sender;
  Value in_data;
  if (in_transfer->btype == "jetton_transfer") {
    Value asset_v = data_field(in_transfer, "asset");
    // asset.jetton_address.as_str(): a TON asset (jetton_address None) would
    // AttributeError before the pton test -> reject.
    if (asset_v.t != VType::Asset || asset_v.is_ton) {
      // The asset came from the in-leg's own jetton_wallet lookup
      // (specs/jettons_decl.mch `asset(wallet.jetton)`), so name the wallet:
      // a TON/absent asset here means that wallet's interface carried no master.
      return rt_fault("stonfi_v2: in asset has no jetton_address (asset=" + asset_v.describe() +
                      " receiver_wallet=" +
                      acc_str(data_field(in_transfer, "receiver_wallet")).value_or("none") + ")");
    }
    Value in_asset = is_pton_master(asset_v.str) ? Value::make_asset_ton() : asset_v;
    sender = data_field(in_transfer, "sender");
    TransferLeg leg = TransferLeg::from_jetton_transfer(in_transfer);
    leg.asset = std::move(in_asset);  // pton-normalized asset overrides the data field
    in_data = leg.encode();
  } else {
    const Message *im = block_msg(in_transfer);
    if (im == nullptr) {
      return rt_fault("stonfi_v2: in_transfer has no message");
    }
    Value in_amount;
    if (im->opcode32() && *im->opcode32() == kPTonTransfer) {
      auto rb = block_body(in_transfer);
      if (rb.is_error()) {
        return rt_fault("stonfi_v2: pton in body");
      }
      auto r = parse_message_body("PTonTransfer", rb.ok());
      if (r.is_error()) {
        return rt_fault("stonfi_v2: pton in parse");
      }
      const Value *ta = r.ok().field("ton_amount");
      in_amount = ta != nullptr ? to_amount(*ta) : Value::make_amount_none();
    } else {
      in_amount = im->value ? Value::make_amount(td::make_refint(*im->value))
                            : Value::make_amount_none();
    }
    sender = account_from_opt(im->source);
    const Message *anchor_msg = block_msg(anchor);
    in_data = Value::make_dict(Value::Fields{
        {"asset", Value::make_asset_ton()},
        {"amount", std::move(in_amount)},
        {"source", account_from_opt(im->source)},
        {"source_jetton_wallet", Value::null()},
        {"destination",
         account_from_opt(anchor_msg != nullptr ? anchor_msg->source : std::nullopt)},
        {"destination_jetton_wallet", account_from_opt(im->destination)}});
  }

  // peer_swaps (multi-hop only).
  std::vector<Value> peer_swaps;
  auto map_asset = [&](const Value &tok) -> const Value * {
    auto k = acc_str(tok);
    if (!k) {
      return nullptr;
    }
    auto it = pool_map.find(*k);
    return it == pool_map.end() ? nullptr : &it->second;
  };
  if (steps.size() > 1) {
    auto big = bigger_leg(steps[0].pay_to);
    const Value *oa = map_asset(big.second);
    if (oa == nullptr) {
      // KeyError / addr_none. pool_map is short of `steps` when a per-hop
      // jetton_wallet lookup missed and truncated the fill loop above.
      return rt_fault("stonfi_v2: peer out asset missing (jetton_wallet=" +
                      acc_str(big.second).value_or("none") + " pool_map=" +
                      std::to_string(pool_map.size()) + "/" + std::to_string(steps.size()) + ")");
    }
    Value out_leg = leg_dict(Value::make_amount(big.first.is_null() ? td::make_refint(0)
                                                                     : big.first),
                             *oa);
    const Value *ia = in_data.field("amount");
    const Value *ias = in_data.field("asset");
    peer_swaps.push_back(Value::make_dict(Value::Fields{
        {"in", leg_dict(ia != nullptr ? *ia : Value::null(),
                        ias != nullptr ? *ias : Value::null())},
        {"out", out_leg}}));
    if (steps[0].pay_to.exit_code == kStonfiSwapOk) {
      for (std::size_t i = 0; i + 1 < steps.size(); i++) {
        const PayTo &pti = steps[i + 1].pay_to;
        auto b2 = bigger_leg(pti);
        if (pti.exit_code != kStonfiSwapOk) {
          continue;
        }
        const Value *oa2 = map_asset(b2.second);
        if (oa2 == nullptr) {
          return rt_fault("stonfi_v2: peer out asset missing (hop) (jetton_wallet=" +
                          acc_str(b2.second).value_or("none") + " pool_map=" +
                          std::to_string(pool_map.size()) + "/" + std::to_string(steps.size()) +
                          ")");
        }
        const Value *prev_out = peer_swaps.back().field("out");
        Value out2 = leg_dict(Value::make_amount(b2.first.is_null() ? td::make_refint(0)
                                                                    : b2.first),
                              *oa2);
        peer_swaps.push_back(Value::make_dict(Value::Fields{
            {"in", prev_out != nullptr ? *prev_out : Value::null()}, {"out", out2}}));
      }
    }
  }

  // out-transfer.
  if (out_transfer == nullptr) {
    // out_transfer.next_blocks. _derive_parts finds it by btype, so a raw
    // JettonTransfer CALL still in the consumed set means the jetton_transfer
    // matcher rejected it (missing wallet interface) rather than the trace
    // lacking an out leg, that distinction is the whole point of the count.
    std::size_t jt_calls = 0;
    for (const Block *b : consumed) {
      if (is_call_op(b, kJettonTransfer)) jt_calls++;
    }
    return rt_fault("stonfi_v2: no out_transfer (consumed=" + std::to_string(consumed.size()) +
                    " steps=" + std::to_string(steps.size()) + " raw_jetton_transfer_calls=" +
                    std::to_string(jt_calls) + ")");
  }
  const Block *pton_transfer = nullptr;
  for (Block *n : out_transfer->next_blocks) {
    if (is_call_op(n, kPTonTransfer)) {
      pton_transfer = n;
      break;
    }
  }
  bool out_has_internal = data_truthy(out_transfer, "has_internal_transfer");
  Value out_data;
  if (pton_transfer == nullptr && out_has_internal) {
    Value asset_f = data_field(out_transfer, "asset");
    // asset.jetton_address.as_str(): a TON asset has jetton_address None ->
    // AttributeError -> reject. Only the non-pton jetton path is live here.
    if (!(asset_f.t == VType::Asset) || asset_f.is_ton) {
      return rt_fault("stonfi_v2: out asset has no jetton_address (asset=" + asset_f.describe() +
                      " receiver_wallet=" +
                      acc_str(data_field(out_transfer, "receiver_wallet")).value_or("none") + ")");
    }
    Value out_asset = is_pton_master(asset_f.str) ? Value::make_asset_ton() : asset_f;
    TransferLeg leg = TransferLeg::from_jetton_transfer(out_transfer);
    leg.asset = std::move(out_asset);
    out_data = leg.encode();
  } else {
    if (pton_transfer == nullptr) {
      // PTonTransfer(None) -> raise. Reached only when the out leg is NOT an
      // internal-transfer jetton_transfer, so name what it was instead.
      return rt_fault("stonfi_v2: pton out missing (out_transfer=" + out_transfer->btype +
                      " has_internal=" + (out_has_internal ? "1" : "0") + ")");
    }
    auto rb = block_body(pton_transfer);
    if (rb.is_error()) {
      return rt_fault("stonfi_v2: pton out body");
    }
    auto r = parse_message_body("PTonTransfer", rb.ok());
    if (r.is_error()) {
      return rt_fault("stonfi_v2: pton out parse");
    }
    const Value *ta = r.ok().field("ton_amount");
    const Message *pm = block_msg(pton_transfer);
    out_data = Value::make_dict(Value::Fields{
        {"asset", Value::make_asset_ton()},
        {"amount", ta != nullptr ? to_amount(*ta) : Value::make_amount_none()},
        {"source", data_field(out_transfer, "sender")},
        {"source_jetton_wallet", data_field(out_transfer, "sender_wallet")},
        {"destination", account_from_opt(pm != nullptr ? pm->destination : std::nullopt)},
        {"destination_jetton_wallet", Value::null()}});
  }

  const Value *src_asset = in_data.field("asset");
  Value dst_asset = [&]() {
    const Value *a = out_data.field("asset");
    return a != nullptr ? *a : Value::null();
  }();

  // failed-swap destination_asset override.
  if (!ok) {
    if (!destination_asset.is_null()) {
      dst_asset = destination_asset;
    } else if (prev != nullptr) {
      // _get_stonfi_v2_target_asset_from_notification(anchor.previous_block msg)
      auto rb = block_body(prev);
      if (!rb.is_error()) {
        auto tw = extract_target_wallet(rb.ok());
        if (tw) {
          Value jw =
              env.lookups->get("jetton_wallet", std::vector<Value>{Value::make_str(*tw)});
          if (!jw.is_null()) {
            const Value *jf = jw.field("jetton");
            if (jf != nullptr && jf->t == VType::Str) {
              auto norm = normalize_raw_address(jf->str);
              dst_asset = Value::make_asset_jetton(norm ? *norm : jf->str);
            }
          }
        }
      }
    }
  }

  Value::Fields d;
  d.emplace_back("failed", Value::make_bool(!ok));
  d.emplace_back("dex", Value::make_str("stonfi_v2"));
  d.emplace_back("source_asset", src_asset != nullptr ? *src_asset : Value::null());
  d.emplace_back("destination_asset", std::move(dst_asset));
  d.emplace_back("sender", std::move(sender));
  d.emplace_back("dex_incoming_transfer", std::move(in_data));
  d.emplace_back("dex_outgoing_transfer", std::move(out_data));
  d.emplace_back("referral_amount", Value::null());
  d.emplace_back("referral_address", Value::null());
  d.emplace_back("peer_swaps",
                 Value::make_list(peer_swaps.size() > 1 ? peer_swaps : std::vector<Value>{}));
  return rt_ok(Value::make_obj(std::move(d)));
}

}  // namespace mch
