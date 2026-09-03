// Message bodies are parsed inline (the spec parses none of them).
// coffee_swap_data decodes the consumed set and derives a SwapRecord.
#include "host/HostImpls.h"

#include "host/BlockViews.h"
#include "host/DexRecords.h"
#include "host/HostAdapter.h"
#include "host/HostCommon.h"

#include "BlockTree.h"
#include "BuildRuntime.h"
#include "ExprRuntime.h"
#include "btypes_gen.h"
#include "MsgParse.h"
#include "parse/PSlice.h"

#include "common/refint.h"
#include "vm/cellslice.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace mch {

namespace {

constexpr std::uint32_t kCoffeeSwapNative = 0xc0ffee00;
constexpr std::uint32_t kCoffeeSwapInternal = 0xc0ffee20;
constexpr std::uint32_t kCoffeeSwapSuccessfulEvent = 0xc0ffee30;
constexpr std::uint32_t kCoffeePayoutInternal = 0xc0ffee21;
constexpr std::uint32_t kCoffeePayout = 0xc0ffee32;
constexpr std::uint32_t kCoffeeNotification = 0xc0ffee36;

// A TON-arm transfer leg (native / payout / notification): jetton wallets null.
TransferLeg ton_leg(Value amount, Value source, Value dest) {
  TransferLeg leg;
  leg.asset = Value::make_asset_ton();
  leg.amount = std::move(amount);
  leg.source = std::move(source);
  leg.source_jetton_wallet = Value::null();
  leg.destination = std::move(dest);
  leg.destination_jetton_wallet = Value::null();
  return leg;
}

// TON-arm is reachable (CoffeeSwapNative in, CoffeePayout / CoffeeNotification
// out); jetton legs need a jetton_transfer block. Encoded SwapRecord, or Null
// where a role is missing / no hop.
EvalResult coffee_swap(const ConsumedBlocks &consumed) {
  const Block *block = consumed.anchor();
  std::vector<const Block *> others = consumed.others();

  // in_transfer = prev if in others and (jetton_transfer | CoffeeSwapNative).
  const Block *in_transfer = nullptr;
  const Block *prev = block->previous_block;
  if (prev != nullptr && block_in(others, prev) &&
      (prev->btype == mch::btype::kJettonTransfer || is_call_op(prev, kCoffeeSwapNative))) {
    in_transfer = prev;
  }
  // payout = first CoffeePayoutInternal in others.
  const Block *payout = first_call(others, kCoffeePayoutInternal);
  // out_transfer = first in others whose previous_block is payout and is a
  // jetton_transfer / CoffeePayout / CoffeeNotification.
  const Block *out_transfer = nullptr;
  if (payout != nullptr) {
    for (const Block *b : others) {
      if (b->previous_block == payout &&
          (b->btype == mch::btype::kJettonTransfer || is_call_op(b, kCoffeePayout) ||
           is_call_op(b, kCoffeeNotification))) {
        out_transfer = b;
        break;
      }
    }
  }

  if (in_transfer == nullptr || payout == nullptr || out_transfer == nullptr) {
    return host_reject("coffee_swap_data: missing role");
  }

  // swap_internal blocks (others + anchor), unique, sorted by min_lt.
  std::vector<const Block *> swaps = all_calls(others, kCoffeeSwapInternal);
  swaps.push_back(block);  // the anchor is a CoffeeSwapInternal
  unique_lt_sorted(swaps);

  bool ok = true;
  std::vector<PeerSwap> peer_swaps;  // each {in:{asset,amount}, out:{asset,amount}}
  for (const Block *sw : swaps) {
    const Block *event = first_next_call(sw, kCoffeeSwapSuccessfulEvent);
    if (event == nullptr) {
      ok = false;
      continue;
    }
    // Decode the event body through the ABI-faithful CoffeeSwapEvent parser.
    auto r_body = block_body(event);
    if (r_body.is_error()) {
      return rt_fault("coffee_swap_data: event body");
    }
    auto r_ev = parse_message_body("CoffeeSwapEvent", r_body.move_as_ok());
    if (r_ev.is_error()) {
      return rt_fault("coffee_swap_data: event parse");
    }
    Value ev = r_ev.move_as_ok();
    const Value *asset = ev.field("asset");
    if (asset == nullptr) {
      return rt_fault("coffee_swap_data: event asset missing");
    }
    EvalResult converted = rt_builtin_asset_of(*asset);
    if (converted.faulted) {
      return rt_fault(std::string("coffee_swap_data: ") + converted.message);
    }
    PeerSwap p;
    p.in.asset = std::move(converted.value);
    p.in.amount = to_amount(*ev.field("amount_in"));
    p.out.asset = Value::null();  // filled after all hops (fill_peer_out_assets)
    p.out.amount = to_amount(*ev.field("amount_out"));
    peer_swaps.push_back(std::move(p));
  }
  if (peer_swaps.empty()) {
    return host_reject("coffee_swap_data: no hop");
  }

  // Jetton in-leg is a jetton_transfer whose data carries the transfer fields;
  // otherwise use the CoffeeSwapNative TON arm.
  Value sender = Value::null();
  TransferLeg in_leg;
  if (in_transfer->btype == mch::btype::kJettonTransfer) {
    sender = data_field(in_transfer, "sender");
    in_leg = TransferLeg::from_jetton_transfer(in_transfer);
  } else {  // is_call_op(in_transfer, kCoffeeSwapNative)
    auto r_body = block_body(in_transfer);
    if (r_body.is_error()) {
      return rt_fault("coffee_swap_data: native body");
    }
    auto r_ctx = open_body(r_body.ok());
    if (r_ctx.is_error()) {
      return rt_fault("coffee_swap_data: native header");
    }
    auto ctx = r_ctx.move_as_ok();
    auto &cs = ctx.cs;
    if (!cs.have(32 + 64)) {
      return rt_fault("coffee_swap_data: native header");
    }
    cs.advance(32 + 64);
    auto r_amt = load_coins_py(cs);
    if (r_amt.is_error()) {
      return rt_fault("coffee_swap_data: native amount");
    }
    const Message *m = block_msg(in_transfer);
    sender = account_from_opt(m != nullptr ? m->source : std::nullopt);
    in_leg = ton_leg(Value::make_amount(r_amt.move_as_ok()), sender,
                     account_from_opt(m != nullptr ? m->destination : std::nullopt));
  }

  // Jetton out-leg uses btype and data; else CoffeePayout / CoffeeNotification.
  TransferLeg out_leg;
  if (out_transfer->btype == mch::btype::kJettonTransfer) {
    out_leg = TransferLeg::from_jetton_transfer(out_transfer);
  } else if (is_call_op(out_transfer, kCoffeePayout)) {
    auto r_body = block_body(payout);
    if (r_body.is_error()) {
      return rt_fault("coffee_swap_data: payout body");
    }
    auto r_ctx = open_body(r_body.ok());
    if (r_ctx.is_error()) {
      return rt_fault("coffee_swap_data: payout header");
    }
    auto ctx = r_ctx.move_as_ok();
    auto &cs = ctx.cs;
    if (!cs.have(32 + 64)) {
      return rt_fault("coffee_swap_data: payout header");
    }
    cs.advance(32 + 64);
    auto r_rcpt = load_address_py(cs);
    auto r_amt = load_coins_py(cs);
    if (r_rcpt.is_error() || r_amt.is_error()) {
      return rt_fault("coffee_swap_data: payout fields");
    }
    const Message *m = block_msg(payout);
    out_leg = ton_leg(Value::make_amount(r_amt.move_as_ok()),
                      account_from_opt(m != nullptr ? m->source : std::nullopt),
                      r_rcpt.move_as_ok());
  } else if (is_call_op(out_transfer, kCoffeeNotification)) {
    const Message *m = block_msg(out_transfer);
    Value amt = msg_value_amount(m);
    out_leg = ton_leg(std::move(amt), account_from_opt(m != nullptr ? m->source : std::nullopt),
                      account_from_opt(m != nullptr ? m->destination : std::nullopt));
  }

  SwapRecord rec;
  rec.dex = "coffee";
  rec.sender = sender;
  rec.source_asset = in_leg.asset;
  rec.destination_asset = out_leg.asset;
  rec.dex_incoming_transfer = std::move(in_leg);
  rec.dex_outgoing_transfer = std::move(out_leg);
  rec.peer_swaps = std::move(peer_swaps);
  rec.referral_amount = Value::null();
  rec.referral_address = Value::null();
  rec.failed = !ok;
  // Fill peer out-assets from the next hop or final payout.
  rec.fill_peer_out_assets();
  return rt_ok(rec.encode());
}

}  // namespace

// Registered thunk: decode the consumed set, then the typed core.
EvalResult coffee_swap_data(BuildEnv &env, const std::vector<Value> &args) {
  (void)env;
  ConsumedBlocks consumed;
  EvalResult decoded = decode_consumed(args, "coffee_swap_data", consumed);
  if (decoded.faulted) return decoded;
  return coffee_swap(consumed);
}

}  // namespace mch
