#include "host/HostImpls.h"

#include "host/BlockViews.h"
#include "host/HostAdapter.h"
#include "host/HostCommon.h"

#include "BlockTree.h"
#include "BuildRuntime.h"
#include "ExprRuntime.h"
#include "MsgParse.h"
#include "parse/PSlice.h"
#include "btypes_gen.h"

#include "td/utils/base64.h"
#include "td/utils/misc.h"
#include "vm/boc.h"
#include "vm/cellslice.h"

#include <algorithm>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

namespace mch {

namespace {


constexpr std::uint32_t kDedustSwapExternal = 0x61ee542d;
constexpr std::uint32_t kDedustSwapPeer = 0x72aca8aa;
constexpr std::uint32_t kDedustSwapNotification = 0x9c610de3;
constexpr std::uint32_t kDedustPayoutFromPool = 0xad4eb6f5;
constexpr std::uint32_t kDedustPayout = 0x474f86cf;
constexpr std::uint32_t kDedustSwapPayload = 0xe3a0d482;

// Walk the step list to the final (destination) asset. A missing pool
// lookup faults and rejects.
EvalResult dedust_seq_destination(BuildEnv &env, const std::vector<std::string> &steps,
                                  const Value &seed) {
  Value prev = seed;
  for (const std::string &step : steps) {
    Value pool = env.lookups->get("dedust_pool", std::vector<Value>{Value::make_str(step)});
    if (pool.is_null()) {
      return rt_fault("dedust_pool missing for " + step);
    }
    const Value *assets = pool.field("assets");
    if (assets == nullptr || assets->t != VType::List) {
      return rt_fault("dedust_pool has no assets list");
    }
    for (const Value &a : *assets->items) {
      const Value *is_ton_v = a.field("is_ton");
      const Value *addr_v = a.field("address");
      bool is_ton = is_ton_v != nullptr && value_truthy(*is_ton_v);
      bool prev_ton = prev.t == VType::Asset && prev.is_ton;
      if (is_ton && prev_ton) {
        continue;
      }
      if (!prev_ton && addr_v != nullptr && addr_v->t == VType::Str && addr_v->str == prev.str) {
        continue;
      }
      if (is_ton) {
        prev = Value::make_asset_ton();
      } else {
        std::string addr = addr_v != nullptr && addr_v->t == VType::Str ? addr_v->str : std::string{};
        prev = Value::make_asset_jetton(addr);
      }
      break;
    }
  }
  return rt_ok(prev);
}

}  // namespace

// Walk the SwapStep chain (already positioned past the sum-type/header).
// Each link: pool addr, 1 flag bit, coins, maybe-ref to the next step.
// Returns pool addresses in walk order (addr_none -> "addr_none").
static td::Result<std::vector<std::string>> parse_dedust_steps(vm::CellSlice cs) {
  std::vector<std::string> steps;
  for (;;) {
    TRY_RESULT(addr, load_address_py(cs));
    std::string pool = (addr.t == VType::Account && !addr.addr_none) ? addr.str : "addr_none";
    if (!cs.have(1)) {
      return td::Status::Error("dedust steps: flag bit underflow");
    }
    cs.advance(1);
    TRY_RESULT(_coins, load_coins_py(cs));
    (void)_coins;
    if (!cs.have(1)) {                // maybe-ref bit
      return td::Status::Error("dedust steps: maybe-ref bit underflow");
    }
    bool has_next = cs.fetch_ulong(1) != 0;
    steps.push_back(std::move(pool));
    if (!has_next) {
      return steps;
    }
    if (cs.size_refs() == 0) {
      return td::Status::Error("dedust steps: next-step ref missing");
    }
    TRY_RESULT_ASSIGN(cs, open_ref_cell(cs.fetch_ref()));
  }
}

// args = (consumed, in_transfer, swap_request). Returns the swap-legs
// object, or Null to reject (payload opcode mismatch), or faults to reject
// (malformed trace / missing pool). include == the consumed set verbatim.
EvalResult dedust_swap_legs(BuildEnv &env, const std::vector<Value> &args) {
  ConsumedBlocks decoded;
  EvalResult decode = decode_consumed({args[0]}, "dedust_swap_legs", decoded);
  if (decode.faulted) {
    return decode;
  }
  std::vector<const Block *> consumed = std::move(decoded.blocks);
  std::vector<const Block *> other_blocks(consumed.begin() + 1, consumed.end());
  std::unordered_set<const Block *> include(consumed.begin(), consumed.end());

  const Block *in_transfer = as_block(args[1]);
  const Block *swap_request = as_block(args[2]);

  // Successful hops, one per DedustSwapNotification, in lt order. Collected
  // from the ORDERED `consumed` vector, not from the `include` set: the
  // stable_sort below preserves insertion order for EQUAL min_lt, and an
  // unordered_set iterates in address-dependent order, so two passes over the
  // same trace could assign different in_leg/out_leg. `include` stays for the
  // membership tests further down.
  std::vector<const Block *> notif_blocks;
  for (const Block *b : consumed) {
    if (is_call_op(b, kDedustSwapNotification)) {
      notif_blocks.push_back(b);
    }
  }
  std::stable_sort(notif_blocks.begin(), notif_blocks.end(),
                   [](const Block *a, const Block *b) { return a->min_lt < b->min_lt; });
  std::vector<Value> peer_swaps;
  Value first_in;
  Value last_out;
  Value first_in_amount;
  Value first_in_asset;
  Value last_out_amount;
  Value last_out_asset;
  for (const Block *b : notif_blocks) {
    auto r_body = block_body(b);
    if (r_body.is_error()) {
      return rt_fault("dedust notify: body");
    }
    auto r = parse_message_body("DedustSwapNotification", r_body.ok());
    if (r.is_error()) {
      return rt_fault("dedust notify: parse");
    }
    Value n = r.move_as_ok();
    const Value *amount_in_f = n.field("amount_in");
    const Value *asset_in_f = n.field("asset_in");
    const Value *amount_out_f = n.field("amount_out");
    const Value *asset_out_f = n.field("asset_out");
    if (amount_in_f == nullptr || asset_in_f == nullptr || amount_out_f == nullptr ||
        asset_out_f == nullptr) {
      return rt_fault("dedust notify: missing leg field");
    }
    EvalResult asset_in = rt_builtin_asset_of(*asset_in_f);
    EvalResult asset_out = rt_builtin_asset_of(*asset_out_f);
    if (asset_in.faulted) return asset_in;
    if (asset_out.faulted) return asset_out;
    Value in_amount_value = to_amount(*amount_in_f);
    Value in_asset_value = std::move(asset_in.value);
    Value out_amount_value = to_amount(*amount_out_f);
    Value out_asset_value = std::move(asset_out.value);
    Value in_leg = Value::make_dict(
        Value::Fields{{"amount", in_amount_value}, {"asset", in_asset_value}});
    Value out_leg = Value::make_dict(
        Value::Fields{{"amount", out_amount_value}, {"asset", out_asset_value}});
    if (peer_swaps.empty()) {
      first_in = in_leg;
      first_in_amount = in_amount_value;
      first_in_asset = in_asset_value;
    }
    last_out = out_leg;
    last_out_amount = out_amount_value;
    last_out_asset = out_asset_value;
    peer_swaps.push_back(Value::make_dict(Value::Fields{
        {"in", std::move(in_leg)}, {"out", std::move(out_leg)}}));
  }

  Value sender = Value::null();
  Value sender_wallet = Value::null();
  Value dex_incoming_jetton_wallet = Value::null();
  Value dex_incoming_wallet = Value::null();
  Value amount_in = Value::null();
  Value asset_in = Value::null();
  std::vector<std::string> steps;
  if (in_transfer != nullptr) {
    dex_incoming_jetton_wallet = data_field(in_transfer, "receiver_wallet");
    dex_incoming_wallet = data_field(in_transfer, "receiver");
    sender_wallet = data_field(in_transfer, "sender_wallet");
    sender = data_field(in_transfer, "sender");
    asset_in = data_field(in_transfer, "asset");
    Value payload_opcode = data_field(in_transfer, "payload_opcode");
    if (payload_opcode.t != VType::Int || payload_opcode.num.is_null()) {
      return rt_fault("dedust legs: payload_opcode not an integer");
    }
    if (payload_opcode.num != kDedustSwapPayload) {
      return rt_ok(Value::null());
    }
    amount_in = data_field(in_transfer, "amount");
    Value fp = data_field(in_transfer, "forward_payload");
    if (fp.t != VType::Str) {
      return rt_fault("dedust legs: forward_payload not a string");
    }
    auto r_cell = cell_from_pystr(fp.str);
    if (r_cell.is_error()) {
      return rt_fault("dedust legs: forward_payload BOC");
    }
    bool special = false;
    vm::CellSlice cs;
    try {
      cs = vm::load_cell_slice_special(r_cell.ok(), special);
    } catch (...) {
      return rt_fault("dedust legs: forward_payload slice");
    }
    if (!cs.have(32) || !cs.advance(32)) {
      return rt_fault("dedust legs: forward_payload sum-type underflow");
    }
    auto r_steps = parse_dedust_steps(cs);
    if (r_steps.is_error()) {
      return rt_fault("dedust legs: steps");
    }
    steps = r_steps.move_as_ok();
  } else if (swap_request != nullptr) {
    const Message *m = block_msg(swap_request);
    sender = account_from_opt(m != nullptr ? m->source : std::nullopt);
    dex_incoming_wallet = account_from_opt(m != nullptr ? m->destination : std::nullopt);
    auto r_body = block_body(swap_request);
    if (r_body.is_error()) {
      return rt_fault("dedust legs: swap_request body");
    }
    bool special = false;
    vm::CellSlice cs;
    try {
      cs = vm::load_cell_slice_special(r_body.ok(), special);
    } catch (...) {
      return rt_fault("dedust legs: swap_request slice");
    }
    if (!cs.have(32 + 64) || !cs.advance(32 + 64)) {
      return rt_fault("dedust legs: swap_request header underflow");
    }
    auto r_amt = load_coins_py(cs);
    if (r_amt.is_error()) {
      return rt_fault("dedust legs: swap_request amount");
    }
    amount_in = Value::make_int(r_amt.move_as_ok());
    auto r_steps = parse_dedust_steps(cs);
    if (r_steps.is_error()) {
      return rt_fault("dedust legs: steps");
    }
    steps = r_steps.move_as_ok();
    asset_in = Value::make_asset_ton();
  }

  std::vector<const Block *> payout_from_pool;
  for (const Block *b : other_blocks) {
    if (is_call_op(b, kDedustPayoutFromPool)) {
      payout_from_pool.push_back(b);
    }
  }
  if (payout_from_pool.size() != 1) {
    return rt_fault("dedust legs: expected one payout from pool");
  }
  Value receiver = sender;
  Value receiver_wallet = Value::null();
  Value dex_outgoing_jetton_wallet = Value::null();
  Value dex_outgoing_wallet = Value::null();
  Value actual_asset_out = Value::null();
  Value actual_amount_out;
  {
    auto r_body = block_body(payout_from_pool[0]);
    if (r_body.is_error()) {
      return rt_fault("dedust legs: payout body");
    }
    auto r = parse_message_body("DedustPayoutFromPool", r_body.ok());
    if (r.is_error()) {
      return rt_fault("dedust legs: payout parse");
    }
    const Value *amt = r.ok().field("amount");
    actual_amount_out = amt != nullptr ? *amt : Value::null();
  }
  for (Block *n : payout_from_pool[0]->next_blocks) {
    if (include.count(n) == 0) {
      continue;
    }
    // IR jetton transfers use the generic Block representation.
    if (n->btype == mch::btype::kJettonTransfer) {
      receiver_wallet = data_field(n, "receiver_wallet");
      receiver = data_field(n, "receiver");
      dex_outgoing_wallet = data_field(n, "sender");
      dex_outgoing_jetton_wallet = data_field(n, "sender_wallet");
      actual_asset_out = data_field(n, "asset");
      actual_amount_out = data_field(n, "amount");
    } else if (is_call_op(n, kDedustPayout)) {
      const Message *m = block_msg(n);
      dex_outgoing_wallet = account_from_opt(m != nullptr ? m->source : std::nullopt);
      receiver = account_from_opt(m != nullptr ? m->destination : std::nullopt);
      actual_asset_out = Value::make_asset_ton();
    }
  }

  // Desired out asset: BOC steps + pool-hop lookups.
  Value seed = asset_in.is_null() ? Value::make_asset_ton() : asset_in;
  EvalResult r_dest = dedust_seq_destination(env, steps, seed);
  if (r_dest.faulted) {
    return r_dest;
  }
  Value destination_asset = std::move(r_dest.value);

  // No successful hop at all: synthesize the single leg from actual values.
  if (peer_swaps.empty()) {
    first_in_amount = to_amount(amount_in);
    first_in_asset = asset_in;
    last_out_amount = to_amount(actual_amount_out);
    last_out_asset = actual_asset_out;
    first_in = Value::make_dict(
        Value::Fields{{"amount", first_in_amount}, {"asset", first_in_asset}});
    last_out = Value::make_dict(
        Value::Fields{{"amount", last_out_amount}, {"asset", last_out_asset}});
    peer_swaps.push_back(Value::make_dict(Value::Fields{
        {"in", first_in}, {"out", last_out}}));
  }

  Value dex_incoming_transfer = Value::make_dict(Value::Fields{
      {"asset", first_in_asset},
      {"amount", first_in_amount},
      {"source", sender},
      {"source_jetton_wallet", sender_wallet},
      {"destination", dex_incoming_wallet},
      {"destination_jetton_wallet", dex_incoming_jetton_wallet}});
  Value dex_outgoing_transfer = Value::make_dict(Value::Fields{
      {"asset", last_out_asset},
      {"amount", last_out_amount},
      {"source", dex_outgoing_wallet},
      {"source_jetton_wallet", dex_outgoing_jetton_wallet},
      {"destination", receiver},
      {"destination_jetton_wallet", receiver_wallet}});

  std::vector<Value> peer_swaps_out;
  if (peer_swaps.size() > 1) {
    peer_swaps_out = peer_swaps;
  }

  Value::Fields ns;
  ns.emplace_back("sender", std::move(sender));
  ns.emplace_back("asset_in", std::move(asset_in));
  ns.emplace_back("destination_asset", std::move(destination_asset));
  ns.emplace_back("dex_incoming_transfer", std::move(dex_incoming_transfer));
  ns.emplace_back("dex_outgoing_transfer", std::move(dex_outgoing_transfer));
  ns.emplace_back("in_leg", std::move(first_in));
  ns.emplace_back("out_leg", std::move(last_out));
  ns.emplace_back("peer_swaps", Value::make_list(std::move(peer_swaps_out)));
  return rt_ok(Value::make_obj(std::move(ns)));
}

namespace {

// DedustV2SwapStep: pool:MsgAddressInt minimal_amount_out:Coins deadline:uint40
// next:(Maybe ^SwapStep), an inline struct, so no opcode prefix.
td::Status dv2_read_step(vm::CellSlice &cs, td::RefInt256 &min_out,
                         td::Ref<vm::Cell> &next) {
  TRY_RESULT(pool, load_address_py(cs));
  (void)pool;
  TRY_RESULT(amount, load_coins_py(cs));
  min_out = std::move(amount);
  if (!cs.have(40)) {
    return td::Status::Error("swap step: deadline underflow");
  }
  cs.advance(40);
  if (!cs.have(1)) {
    return td::Status::Error("swap step: next flag underflow");
  }
  if (cs.fetch_ulong(1) == 1) {
    next = cs.fetch_ref();
    if (next.is_null()) {
      return td::Status::Error("swap step: next ref missing");
    }
  }
  return td::Status::OK();
}

}  // namespace

// Return the final minimum output from a parsed swap payload's step chain.
EvalResult dedust_v2_swap_min_out(BuildEnv &, const std::vector<Value> &args) {
  if (args[0].is_null()) {
    return rt_ok(Value::null());
  }
  const Value *min_out_value = args[0].field("minimal_amount_out");
  const Value *next_value = args[0].field("next");
  if (min_out_value == nullptr || min_out_value->t != VType::Int ||
      min_out_value->num.is_null() || next_value == nullptr) {
    return rt_ok(Value::null());
  }

  td::RefInt256 min_out = min_out_value->num;
  td::Ref<vm::Cell> next;
  if (next_value->t == VType::Cell) {
    next = next_value->cell;
    if (next.is_null()) {
      return rt_ok(Value::null());
    }
  } else if (!next_value->is_null()) {
    return rt_ok(Value::null());
  }

  bool chain_ok = true;
  for (int depth = 0; !next.is_null() && depth < 16; depth++) {
    auto step_cs = open_ref_cell(next);
    if (step_cs.is_error()) {
      chain_ok = false;
      break;
    }
    vm::CellSlice step = step_cs.move_as_ok();
    next = td::Ref<vm::Cell>{};
    if (dv2_read_step(step, min_out, next).is_error()) {
      chain_ok = false;
      break;
    }
  }
  return chain_ok ? rt_ok(Value::make_int(std::move(min_out)))
                  : rt_ok(Value::null());
}

}  // namespace mch
