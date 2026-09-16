#include "ActionBuild.h"

#include "MsgParse.h"

#include "td/utils/base64.h"
#include "td/utils/crypto.h"
#include "td/utils/logging.h"

#include "common/refint.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace mch {

namespace {

// Account/Asset/none -> canonical address STRING (Str) or Null.
// Composites and directional fields store addresses as plain strings, so they
// render with the "str:" prefix on both sides.
Value av_addr(const Value &v) {
  if (v.is_null()) return Value::null();
  if (v.t == VType::Asset) return v.has_jetton ? Value::make_str(v.str) : Value::null();
  if (v.t == VType::Account) return v.addr_none ? Value::null() : Value::make_str(v.str);
  return Value::null();
}

// Amount -> its underlying Int or Null; a plain Int (jvault/evaa/tgbtc raw
// amounts) passes through unchanged.
Value av_amount(const Value &v) {
  if (v.is_null()) return Value::null();
  if (v.t == VType::Amount) {
    return v.num.is_null() ? Value::null() : Value::make_int(v.num);
  }
  return v;
}

Value dg(const Value &d, const char *k) {
  const Value *v = d.field(k);
  return v != nullptr ? *v : Value::null();
}
bool has(const Value &d, const char *k) { return d.field(k) != nullptr; }

// "0x" + lowercase magnitude, no leading zeros. Only used for EVAA asset_id
// (a positive 256-bit id). Null -> Null.
Value av_hex(const Value &v) {
  if (v.is_null() || v.t != VType::Int || v.num.is_null()) return Value::null();
  return Value::make_str("0x" + td::hex_string(v.num, false));
}

// Same lowercase magnitude as av_hex, WITHOUT the "0x". Exists solely for
// Cocoon's `new_secret_hash`.
Value av_hex_bare(const Value &v) {
  if (v.is_null() || v.t != VType::Int || v.num.is_null()) return Value::null();
  return Value::make_str(td::hex_string(v.num, false));
}

// jetton/nft `comment`: produced field is RAW bytes (Bytes) or Null. encrypted
// -> base64; else utf-8 backslashreplace + U+0000 strip.
Value comment_value(const Value &d) {
  const Value *c = d.field("comment");
  if (c == nullptr || c->is_null()) return Value::null();
  bool enc = false;
  const Value *e = d.field("encrypted_comment");
  if (e != nullptr && e->t == VType::Bool) enc = e->boolean;
  if (enc) return Value::make_str(td::base64_encode(td::Slice(c->str)));
  return Value::make_str(decode_comment_bytes(c->str));
}

// tick_tock -> tx.now; created_lt set -> created_at; else tx.now.
std::int64_t node_utime(const EventNode *n) {
  if (n->is_tick_tock && n->tx != nullptr) return n->tx->now;
  if (n->msg != nullptr && n->msg->created_lt) {
    return n->msg->created_at ? *n->msg->created_at : 0;
  }
  return n->tx != nullptr ? n->tx->now : 0;
}
Value mkdict(Value::Fields f) { return Value::make_dict(std::move(f)); }

// Extra participant the generic source/destination assembly cannot derive.
// build_action seeds its account list from here, then dedups, so a duplicate
// push is harmless.
void push_account(Action &a, const Value &v) {
  if (v.t == VType::Str) a.accounts.push_back(v.str);
}

void fill_jetton_transfer(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.source_secondary = av_addr(dg(d, "sender_wallet"));
  a.destination = av_addr(dg(d, "receiver"));
  a.destination_secondary = av_addr(dg(d, "receiver_wallet"));
  a.amount = av_amount(dg(d, "amount"));
  Value asset = dg(d, "asset");
  a.asset = av_addr(asset);
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  f.emplace_back("response_destination", av_addr(dg(d, "response_address")));
  f.emplace_back("forward_amount", av_amount(dg(d, "forward_amount")));
  f.emplace_back("custom_payload", dg(d, "custom_payload"));
  f.emplace_back("forward_payload", dg(d, "forward_payload"));
  f.emplace_back("comment", comment_value(d));
  f.emplace_back("is_encrypted_comment", dg(d, "encrypted_comment"));
  a.jetton_transfer_data = mkdict(std::move(f));
}

void fill_jetton_burn(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "owner"));
  a.source_secondary = av_addr(dg(d, "jetton_wallet"));
  a.asset = av_addr(dg(d, "asset"));
  a.amount = av_amount(dg(d, "amount"));
}

void fill_jetton_mint(const Value &d, Action &a) {
  a.destination = av_addr(dg(d, "to"));
  a.destination_secondary = av_addr(dg(d, "to_jetton_wallet"));
  a.asset = av_addr(dg(d, "asset"));
  a.amount = av_amount(dg(d, "amount"));
  a.value = av_amount(dg(d, "ton_amount"));
}

Value convert_transfer(const Value &t) {
  Value::Fields f;
  f.emplace_back("amount", av_amount(dg(t, "amount")));
  f.emplace_back("source", av_addr(dg(t, "source")));
  f.emplace_back("source_jetton_wallet", av_addr(dg(t, "source_jetton_wallet")));
  f.emplace_back("destination", av_addr(dg(t, "destination")));
  f.emplace_back("destination_jetton_wallet", av_addr(dg(t, "destination_jetton_wallet")));
  f.emplace_back("asset", av_addr(dg(t, "asset")));
  return mkdict(std::move(f));
}

Value convert_peer_swap(const Value &ps) {
  Value in = dg(ps, "in"), out = dg(ps, "out");
  Value::Fields f;
  f.emplace_back("amount_in", av_amount(dg(in, "amount")));
  f.emplace_back("asset_in", av_addr(dg(in, "asset")));
  f.emplace_back("amount_out", av_amount(dg(out, "amount")));
  f.emplace_back("asset_out", av_addr(dg(out, "asset")));
  return mkdict(std::move(f));
}

void fill_jetton_swap(const Value &d, Action &a) {
  Value dit = dg(d, "dex_incoming_transfer");
  Value dot = dg(d, "dex_outgoing_transfer");
  Value in_conv = convert_transfer(dit);
  Value out_conv = convert_transfer(dot);
  a.asset = av_addr(dg(dit, "asset"));
  a.asset2 = av_addr(dg(dot, "asset"));
  Value dex = dg(d, "dex");
  if (dex.t == VType::Str && (dex.str == "stonfi_v2" || dex.str == "dedust" || dex.str == "tonco")) {
    a.asset = av_addr(dg(d, "source_asset"));
    a.asset2 = av_addr(dg(d, "destination_asset"));
  }
  a.source = dg(in_conv, "source");
  a.source_secondary = dg(in_conv, "source_jetton_wallet");
  a.destination = dg(out_conv, "destination");
  a.destination_secondary = dg(out_conv, "destination_jetton_wallet");
  if (!dg(d, "destination_wallet").is_null()) {
    a.destination_secondary = av_addr(dg(d, "destination_wallet"));
  }
  if (!dg(d, "destination_asset").is_null()) {
    a.asset2 = av_addr(dg(d, "destination_asset"));
  }
  Value min_out = Value::null();
  if (has(d, "min_out_amount")) min_out = av_amount(dg(d, "min_out_amount"));
  Value::Fields f;
  f.emplace_back("dex", dex);
  f.emplace_back("sender", av_addr(dg(d, "sender")));
  f.emplace_back("dex_incoming_transfer", in_conv);
  f.emplace_back("dex_outgoing_transfer", out_conv);
  f.emplace_back("min_out_amount", min_out);
  if (!dg(d, "peer_swaps").is_null()) {
    Value ps = dg(d, "peer_swaps");
    std::vector<Value> conv;
    if (ps.items) {
      for (const Value &s : *ps.items) conv.push_back(convert_peer_swap(s));
    }
    f.emplace_back("peer_swaps", Value::make_list(std::move(conv)));
  }
  a.jetton_swap_data = mkdict(std::move(f));
}

void fill_nft_transfer(const Value &d, Action &a) {
  if (!dg(d, "prev_owner").is_null()) {
    a.source = av_addr(dg(d, "prev_owner"));
  }
  a.destination = av_addr(dg(d, "new_owner"));
  Value nft = dg(d, "nft");
  a.asset_secondary = av_addr(dg(nft, "address"));
  Value coll = dg(nft, "collection");
  if (!coll.is_null()) a.asset = av_addr(dg(coll, "address"));
  bool is_purchase = dg(d, "is_purchase").t == VType::Bool && dg(d, "is_purchase").boolean;
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  f.emplace_back("is_purchase", dg(d, "is_purchase"));
  f.emplace_back("price", is_purchase ? av_amount(dg(d, "price")) : Value::null());
  f.emplace_back("nft_item_index", dg(nft, "index"));
  Value fwd = dg(d, "forward_amount");
  f.emplace_back("forward_amount", av_amount(fwd));
  f.emplace_back("custom_payload", dg(d, "custom_payload"));
  f.emplace_back("forward_payload", dg(d, "forward_payload"));
  Value resp = dg(d, "response_destination");
  f.emplace_back("response_destination", av_addr(resp));
  f.emplace_back("marketplace", dg(d, "marketplace"));
  f.emplace_back("marketplace_address", av_addr(dg(d, "marketplace_address")));
  f.emplace_back("real_prev_owner", av_addr(dg(d, "real_prev_owner")));
  f.emplace_back("payout_amount", av_amount(dg(d, "payout_amount")));
  f.emplace_back("payout_comment_encrypted", dg(d, "payout_comment_encrypted"));
  f.emplace_back("payout_comment_encoded", dg(d, "payout_comment_encoded"));
  f.emplace_back("payout_comment", dg(d, "payout_comment"));
  f.emplace_back("royalty_amount", av_amount(dg(d, "royalty_amount")));
  a.nft_transfer_data = mkdict(std::move(f));
  Value::Fields l;
  l.emplace_back("marketplace_fee_address", av_addr(dg(d, "payout_address")));
  l.emplace_back("royalty_address", av_addr(dg(d, "royalty_address")));
  a.nft_listing_data = mkdict(std::move(l));
}

void fill_dedust_deposit(const Value &d, Action &a) {
  a.type = "dex_deposit_liquidity";
  a.source = av_addr(dg(d, "sender"));
  a.destination = av_addr(dg(d, "pool_address"));
  a.destination_secondary = av_addr(dg(d, "deposit_contract"));
  std::vector<Value> excesses;
  Value ve = dg(d, "vault_excesses");
  if (ve.items) {
    for (const Value &e : *ve.items) {
      Value::Fields ef;
      Value a0 = Value::null(), a1 = Value::null();
      if (e.items && e.items->size() >= 2) {
        a0 = (*e.items)[0];
        a1 = (*e.items)[1];
      }
      ef.emplace_back("asset", av_addr(a0));
      ef.emplace_back("amount", av_amount(a1));
      excesses.push_back(mkdict(std::move(ef)));
    }
  }
  Value::Fields f;
  f.emplace_back("dex", dg(d, "dex"));
  f.emplace_back("asset1", av_addr(dg(d, "asset_1")));
  f.emplace_back("amount1", av_amount(dg(d, "amount_1")));
  f.emplace_back("asset2", av_addr(dg(d, "asset_2")));
  f.emplace_back("amount2", av_amount(dg(d, "amount_2")));
  f.emplace_back("user_jetton_wallet_1", av_addr(dg(d, "user_jetton_wallet_1")));
  f.emplace_back("user_jetton_wallet_2", av_addr(dg(d, "user_jetton_wallet_2")));
  f.emplace_back("lp_tokens_minted", av_amount(dg(d, "lp_tokens_minted")));
  f.emplace_back("target_asset_1", av_addr(dg(d, "target_asset_1")));
  f.emplace_back("target_amount_1", av_amount(dg(d, "target_amount_1")));
  f.emplace_back("target_asset_2", av_addr(dg(d, "target_asset_2")));
  f.emplace_back("target_amount_2", av_amount(dg(d, "target_amount_2")));
  f.emplace_back("vault_excesses", Value::make_list(std::move(excesses)));
  a.dex_deposit_liquidity_data = mkdict(std::move(f));
}

void fill_dedust_deposit_partial(const Value &d, Action &a) {
  a.type = "dex_deposit_liquidity";
  a.source = av_addr(dg(d, "sender"));
  a.destination_secondary = av_addr(dg(d, "deposit_contract"));
  Value::Fields f;
  f.emplace_back("dex", dg(d, "dex"));
  f.emplace_back("asset1", av_addr(dg(d, "asset_1")));
  f.emplace_back("amount1", av_amount(dg(d, "amount_1")));
  f.emplace_back("asset2", av_addr(dg(d, "asset_2")));
  f.emplace_back("amount2", av_amount(dg(d, "amount_2")));
  f.emplace_back("user_jetton_wallet_1", av_addr(dg(d, "user_jetton_wallet_1")));
  f.emplace_back("user_jetton_wallet_2", av_addr(dg(d, "user_jetton_wallet_2")));
  f.emplace_back("lp_tokens_minted", Value::null());
  f.emplace_back("target_asset_1", av_addr(dg(d, "target_asset_1")));
  f.emplace_back("target_amount_1", av_amount(dg(d, "target_amount_1")));
  f.emplace_back("target_asset_2", av_addr(dg(d, "target_asset_2")));
  f.emplace_back("target_amount_2", av_amount(dg(d, "target_amount_2")));
  a.dex_deposit_liquidity_data = mkdict(std::move(f));
}

// DeDust CPMM V2 LP trading-fees claim. Reuses the dex_withdraw_liquidity_data
// composite, both slots populated; leaves a.type at the block's btype.
void fill_dedust_v2_claim_fees(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.destination = av_addr(dg(d, "pool"));
  a.destination_secondary = av_addr(dg(d, "position"));
  Value::Fields f;
  f.emplace_back("dex", dg(d, "dex"));
  f.emplace_back("amount1", av_amount(dg(d, "amount_x")));
  f.emplace_back("amount2", av_amount(dg(d, "amount_y")));
  f.emplace_back("asset1_out", av_addr(dg(d, "asset_x")));
  f.emplace_back("asset2_out", av_addr(dg(d, "asset_y")));
  f.emplace_back("user_jetton_wallet_1", av_addr(dg(d, "user_wallet_x")));
  f.emplace_back("user_jetton_wallet_2", av_addr(dg(d, "user_wallet_y")));
  f.emplace_back("dex_wallet_1", av_addr(dg(d, "dex_wallet_x")));
  f.emplace_back("dex_wallet_2", av_addr(dg(d, "dex_wallet_y")));
  f.emplace_back("dex_jetton_wallet_1", av_addr(dg(d, "dex_jetton_wallet_x")));
  f.emplace_back("dex_jetton_wallet_2", av_addr(dg(d, "dex_jetton_wallet_y")));
  f.emplace_back("is_refund", Value::make_bool(false));
  f.emplace_back("lp_tokens_burnt", Value::null());
  a.dex_withdraw_liquidity_data = mkdict(std::move(f));
}

// DeDust CPMM V2 reward claim. Reuses the dex_withdraw_liquidity_data composite;
// reward_index is carried in the block data but is NOT serialized.
void fill_dedust_v2_claim_reward(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.destination = av_addr(dg(d, "pool"));
  a.destination_secondary = av_addr(dg(d, "position"));
  Value::Fields f;
  f.emplace_back("dex", dg(d, "dex"));
  f.emplace_back("amount1", av_amount(dg(d, "amount")));
  f.emplace_back("amount2", Value::null());
  f.emplace_back("asset1_out", av_addr(dg(d, "asset")));
  f.emplace_back("asset2_out", Value::null());
  f.emplace_back("user_jetton_wallet_1", av_addr(dg(d, "user_wallet")));
  f.emplace_back("user_jetton_wallet_2", Value::null());
  f.emplace_back("dex_wallet_1", av_addr(dg(d, "dex_wallet")));
  f.emplace_back("dex_wallet_2", Value::null());
  f.emplace_back("dex_jetton_wallet_1", av_addr(dg(d, "dex_jetton_wallet")));
  f.emplace_back("dex_jetton_wallet_2", Value::null());
  f.emplace_back("is_refund", Value::make_bool(false));
  f.emplace_back("lp_tokens_burnt", Value::null());
  a.dex_withdraw_liquidity_data = mkdict(std::move(f));
}

// Tonco LP shares live in position NFTs rather than LP jettons. Withdrawals
// hard-null source_secondary/asset and always emit is_refund=false.
void fill_tonco_withdraw(const Value &d, Action &a) {
  a.type = "dex_withdraw_liquidity";
  a.source = av_addr(dg(d, "sender"));
  a.source_secondary = Value::null();
  a.destination = av_addr(dg(d, "pool"));
  a.asset = Value::null();
  Value::Fields f;
  f.emplace_back("dex", Value::make_str("tonco"));
  f.emplace_back("amount1", av_amount(dg(d, "amount1_out")));
  f.emplace_back("amount2", av_amount(dg(d, "amount2_out")));
  f.emplace_back("asset1_out", av_addr(dg(d, "asset1_out")));
  f.emplace_back("asset2_out", av_addr(dg(d, "asset2_out")));
  f.emplace_back("user_jetton_wallet_1", av_addr(dg(d, "wallet1")));
  f.emplace_back("user_jetton_wallet_2", av_addr(dg(d, "wallet2")));
  f.emplace_back("dex_jetton_wallet_1", av_addr(dg(d, "dex_jetton_wallet_1")));
  f.emplace_back("dex_wallet_1", av_addr(dg(d, "dex_wallet_1")));
  f.emplace_back("dex_wallet_2", av_addr(dg(d, "dex_wallet_2")));
  f.emplace_back("dex_jetton_wallet_2", av_addr(dg(d, "dex_jetton_wallet_2")));
  f.emplace_back("is_refund", Value::make_bool(false));
  f.emplace_back("lp_tokens_burnt", av_amount(dg(d, "liquidity_burnt")));
  f.emplace_back("burned_nft_index", dg(d, "burned_nft_index"));
  f.emplace_back("burned_nft_address", av_addr(dg(d, "burned_nft_address")));
  f.emplace_back("tick_lower", dg(d, "tick_lower"));
  f.emplace_back("tick_upper", dg(d, "tick_upper"));
  a.dex_withdraw_liquidity_data = mkdict(std::move(f));
}

void fill_tonco_deposit(const Value &d, Action &a) {
  a.type = "dex_deposit_liquidity";
  a.source = av_addr(dg(d, "sender"));
  Value sw1 = dg(d, "sender_wallet_1"), sw2 = dg(d, "sender_wallet_2");
  a.source_secondary = av_addr(!sw1.is_null() ? sw1 : sw2);
  a.destination = av_addr(dg(d, "pool"));
  a.destination_secondary = av_addr(dg(d, "account_contract"));
  std::vector<Value> excesses;
  Value ve = dg(d, "excesses");
  if (ve.items) {
    for (const Value &e : *ve.items) {
      Value::Fields ef;
      Value a0 = Value::null(), a1 = Value::null();
      if (e.items && e.items->size() >= 2) {
        a0 = (*e.items)[0];
        a1 = (*e.items)[1];
      }
      ef.emplace_back("asset", av_addr(a0));
      ef.emplace_back("amount", av_amount(a1));
      excesses.push_back(mkdict(std::move(ef)));
    }
  }
  Value amt1 = dg(d, "amount_1"), as1 = dg(d, "asset_1");
  Value amt2 = dg(d, "amount_2"), as2 = dg(d, "asset_2");
  Value actual_amount_1 = Value::null(), actual_asset_1 = Value::null();
  Value actual_amount_2 = Value::null(), actual_asset_2 = Value::null();
  Value pairs_amt[2] = {amt1, amt2};
  Value pairs_asset[2] = {as1, as2};
  for (int i = 0; i < 2; i++) {
    if (pairs_amt[i].is_null()) continue;
    if (actual_amount_1.is_null()) {
      actual_amount_1 = pairs_amt[i];
      actual_asset_1 = pairs_asset[i];
    } else {
      actual_amount_2 = pairs_amt[i];
      actual_asset_2 = pairs_asset[i];
    }
  }
  Value::Fields f;
  f.emplace_back("dex", Value::make_str("tonco"));
  f.emplace_back("amount1", av_amount(actual_amount_1));
  f.emplace_back("amount2", av_amount(actual_amount_2));
  f.emplace_back("asset1", av_addr(actual_asset_1));
  f.emplace_back("asset2", av_addr(actual_asset_2));
  f.emplace_back("user_jetton_wallet_1", av_addr(sw1));
  f.emplace_back("user_jetton_wallet_2", av_addr(sw2));
  f.emplace_back("lp_tokens_minted", av_amount(dg(d, "lp_tokens_minted")));
  f.emplace_back("tick_lower", dg(d, "tick_lower"));
  f.emplace_back("tick_upper", dg(d, "tick_upper"));
  f.emplace_back("nft_index", dg(d, "nft_index"));
  f.emplace_back("nft_address", av_addr(dg(d, "nft_address")));
  f.emplace_back("target_amount_1", av_amount(dg(d, "position_amount_1")));
  f.emplace_back("target_amount_2", av_amount(dg(d, "position_amount_2")));
  f.emplace_back("target_asset_1", av_addr(dg(d, "asset_1")));
  f.emplace_back("target_asset_2", av_addr(dg(d, "asset_2")));
  f.emplace_back("vault_excesses", Value::make_list(std::move(excesses)));
  a.dex_deposit_liquidity_data = mkdict(std::move(f));
}

// `amount` is a raw Int here (not an Amount), like the withdraw twin.
void fill_evaa_supply(const Value &d, Action &a, bool block_failed) {
  a.source = av_addr(dg(d, "sender"));
  a.source_secondary = av_addr(dg(d, "sender_jetton_wallet"));
  a.destination = av_addr(dg(d, "recipient"));
  a.destination_secondary = av_addr(dg(d, "recipient_contract"));
  a.amount = av_amount(dg(d, "amount"));
  a.asset = av_addr(dg(d, "asset"));
  Value succ = dg(d, "is_success");
  a.success = succ.t == VType::Bool && succ.boolean;
  if (block_failed) a.success = false;
  Value::Fields f;
  f.emplace_back("is_ton", dg(d, "is_ton"));
  f.emplace_back("asset_id", av_hex(dg(d, "asset_id")));
  f.emplace_back("master", av_addr(dg(d, "master")));
  Value rjw = dg(d, "recipient_jetton_wallet");
  f.emplace_back("recipient_jetton_wallet", av_addr(rjw));
  Value mjw = dg(d, "master_jetton_wallet");
  f.emplace_back("master_jetton_wallet", av_addr(mjw));
  a.evaa_supply_data = mkdict(std::move(f));
}

void fill_evaa_withdraw(const Value &d, Action &a, bool block_failed) {
  a.source = av_addr(dg(d, "owner"));
  a.destination = av_addr(dg(d, "recipient"));
  a.destination_secondary = av_addr(dg(d, "owner_contract"));
  a.amount = av_amount(dg(d, "amount"));
  a.asset = av_addr(dg(d, "asset"));
  Value succ = dg(d, "is_success");
  a.success = succ.t == VType::Bool && succ.boolean;
  if (block_failed) a.success = false;
  Value::Fields f;
  f.emplace_back("is_ton", dg(d, "is_ton"));
  Value rjw = dg(d, "recipient_jetton_wallet");
  f.emplace_back("recipient_jetton_wallet", av_addr(rjw));
  Value mjw = dg(d, "master_jetton_wallet");
  f.emplace_back("master_jetton_wallet", av_addr(mjw));
  f.emplace_back("fail_reason", dg(d, "fail_reason"));
  f.emplace_back("master", av_addr(dg(d, "master")));
  f.emplace_back("asset_id", av_hex(dg(d, "asset_id")));
  a.evaa_withdraw_data = mkdict(std::move(f));
}

}  // namespace

void fill_evaa_liquidate(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "liquidator"));
  a.destination = av_addr(dg(d, "borrower"));
  a.destination_secondary = av_addr(dg(d, "borrower_contract"));
  a.amount = dg(d, "collateral_amount");
  Value succ = dg(d, "is_success");
  a.success = succ.t == VType::Bool && succ.boolean;
  Value::Fields f;
  f.emplace_back("fail_reason", dg(d, "fail_reason"));
  f.emplace_back("debt_amount", dg(d, "debt_amount"));
  f.emplace_back("asset_id", av_hex(dg(d, "collateral_asset_id")));
  a.evaa_liquidate_data = mkdict(std::move(f));
}

namespace {

void fill_jvault_stake(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.source_secondary = av_addr(dg(d, "sender_wallet"));
  a.asset = av_addr(dg(d, "asset"));
  a.destination = av_addr(dg(d, "staking_pool"));
  a.amount = av_amount(dg(d, "staked_amount"));
  Value::Fields f;
  f.emplace_back("period", dg(d, "period"));
  f.emplace_back("stake_wallet", av_addr(dg(d, "stake_wallet")));
  a.jvault_stake_data = mkdict(std::move(f));
}

void fill_jvault_unstake(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.source_secondary = av_addr(dg(d, "stake_wallet"));
  a.destination = av_addr(dg(d, "staking_pool"));
  a.amount = av_amount(dg(d, "unstaked_amount"));
  a.opcode = dg(d, "exit_code");
  a.asset = av_addr(dg(d, "asset"));
  a.asset2 = av_addr(dg(d, "jvault_asset"));
}

void fill_jvault_unstake_request(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.source_secondary = av_addr(dg(d, "stake_wallet"));
  a.destination = av_addr(dg(d, "staking_pool"));
  a.amount = av_amount(dg(d, "requested_amount"));
  a.asset = av_addr(dg(d, "asset"));
  a.asset2 = av_addr(dg(d, "jvault_asset"));
  a.opcode = dg(d, "exit_code");
}

void fill_jvault_claim(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.source_secondary = av_addr(dg(d, "stake_wallet"));
  a.destination = av_addr(dg(d, "staking_pool"));
  std::vector<Value> jettons;
  Value cj = dg(d, "claimed_jettons");
  if (cj.items) {
    for (const Value &j : *cj.items) jettons.push_back(av_addr(j));
  }
  Value::Fields f;
  f.emplace_back("claimed_jettons", Value::make_list(std::move(jettons)));
  f.emplace_back("claimed_amounts", dg(d, "claimed_amounts"));
  a.jvault_claim_data = mkdict(std::move(f));
}

void fill_ethena_deposit(const Value &d, Action &a) {
  a.type = "stake_deposit";
  a.source = av_addr(dg(d, "source"));
  a.source_secondary = av_addr(dg(d, "user_jetton_wallet"));
  a.destination = av_addr(dg(d, "pool"));
  a.amount = av_amount(dg(d, "value"));
  a.asset = av_addr(dg(d, "asset"));
  a.asset2 = av_addr(dg(d, "source_asset"));
  Value::Fields f;
  f.emplace_back("provider", Value::make_str("ethena"));
  f.emplace_back("tokens_minted", av_amount(dg(d, "tokens_minted")));
  a.staking_data = mkdict(std::move(f));
}

void fill_layerzero_receive(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.destination = av_addr(dg(d, "oapp"));
  a.destination_secondary = av_addr(dg(d, "channel"));
  a.layerzero_packet_data = dg(d, "packet_data");
}

// Populate the LayerZero send composites shared by native and token sends.
// msglib_manager/msglib are payload hex strings, not accounts.
void fill_layerzero_send_fields(const Value &d, Action &a) {
  Value::Fields f;
  f.emplace_back("send_request_id", dg(d, "send_request_id"));
  f.emplace_back("msglib_manager", dg(d, "msglib_manager"));
  f.emplace_back("msglib", dg(d, "msglib"));
  f.emplace_back("uln", av_addr(dg(d, "uln")));
  f.emplace_back("native_fee", dg(d, "native_fee"));
  f.emplace_back("zro_fee", dg(d, "zro_fee"));
  f.emplace_back("endpoint", av_addr(dg(d, "endpoint")));
  f.emplace_back("channel", av_addr(dg(d, "channel")));
  a.layerzero_send_data = mkdict(std::move(f));
  a.layerzero_packet_data = dg(d, "packet_data");
}

void fill_layerzero_send(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "initiator"));
  fill_layerzero_send_fields(d, a);
}

void fill_layerzero_send_tokens(const Value &d, Action &a) {
  a.source_secondary = av_addr(dg(d, "sender_wallet"));
  a.destination = av_addr(dg(d, "oapp"));
  a.destination_secondary = av_addr(dg(d, "oapp_wallet"));
  a.amount = av_amount(dg(d, "amount"));
  a.asset = av_addr(dg(d, "asset"));
  fill_layerzero_send_fields(dg(d, "layerzero_send_data"), a);
  a.source = av_addr(dg(d, "sender"));
}

// `uln` and `uln_connection` both carry the ULN CONNECTION's address; the
// same local is assigned to both.
void fill_layerzero_commit_packet(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.source_secondary = av_addr(dg(d, "endpoint"));
  a.destination = av_addr(dg(d, "uln"));
  a.destination_secondary = av_addr(dg(d, "uln_connection"));
  a.asset = av_addr(dg(d, "channel"));
  a.asset_secondary = av_addr(dg(d, "msglib_connection"));
  a.layerzero_packet_data = dg(d, "packet_data");
}

void fill_layerzero_dvn_verify(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  Value::Fields f;
  f.emplace_back("nonce", dg(d, "nonce"));
  f.emplace_back("status", dg(d, "status"));
  f.emplace_back("dvn", av_addr(dg(d, "dvn")));
  f.emplace_back("proxy", av_addr(dg(d, "proxy")));
  f.emplace_back("uln", av_addr(dg(d, "uln")));
  f.emplace_back("uln_connection", av_addr(dg(d, "uln_connection")));
  a.layerzero_dvn_verify_data = mkdict(std::move(f));
}

void fill_coffee_create_vault(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.destination = av_addr(dg(d, "vault"));
  a.asset = av_addr(dg(d, "asset"));
  a.value = av_amount(dg(d, "amount"));
}

// `pool_first`/`pool_second` carry the flat pool-asset pair rather than a live
// PoolParams parser object.

void fill_coffee_create_pool_creator(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.source_secondary = av_addr(dg(d, "sender_jetton_wallet"));
  a.destination = av_addr(dg(d, "deposit_recipient"));
  a.destination_secondary = av_addr(dg(d, "pool_creator_contract"));
  a.asset = av_addr(dg(d, "provided_asset"));
  a.asset2 = av_addr(dg(d, "pool_first"));
  a.asset2_secondary = av_addr(dg(d, "pool_second"));
  a.amount = av_amount(dg(d, "amount"));
}

void fill_coffee_create_pool(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "source"));
  a.source_secondary = av_addr(dg(d, "source_jetton_wallet"));
  a.amount = av_amount(dg(d, "amount"));
  a.asset = av_addr(dg(d, "asset_1"));
  a.asset2 = av_addr(dg(d, "asset_2"));
  a.destination = av_addr(dg(d, "pool"));
  a.destination_secondary = av_addr(dg(d, "pool_creator_contract"));
  Value::Fields f;
  f.emplace_back("amount_1", av_amount(dg(d, "amount_1")));
  f.emplace_back("amount_2", av_amount(dg(d, "amount_2")));
  f.emplace_back("initiator_1", av_addr(dg(d, "initiator_1")));
  f.emplace_back("initiator_2", av_addr(dg(d, "initiator_2")));
  f.emplace_back("provided_asset", av_addr(dg(d, "provided_asset")));
  f.emplace_back("lp_tokens_minted", av_amount(dg(d, "lp_tokens_minted")));
  a.coffee_create_pool_data = mkdict(std::move(f));
}

void fill_coffee_mev_protect_hold_funds(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.source_secondary = av_addr(dg(d, "sender_wallet"));
  a.destination = av_addr(dg(d, "mev_contract"));
  a.destination_secondary = av_addr(dg(d, "mev_contract_wallet"));
  a.asset = av_addr(dg(d, "asset"));
  a.amount = av_amount(dg(d, "amount"));
}

void fill_coffee_staking_deposit(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "source"));
  a.source_secondary = av_addr(dg(d, "user_jetton_wallet"));
  a.destination = av_addr(dg(d, "pool"));
  a.destination_secondary = av_addr(dg(d, "pool_jetton_wallet"));
  a.asset = av_addr(dg(d, "asset"));
  a.amount = av_amount(dg(d, "value"));
  Value::Fields f;
  f.emplace_back("minted_item_address", av_addr(dg(d, "minted_item_address")));
  // Raw passthrough: the index comes off the nft-item interface record, where
  // it can be a float.
  f.emplace_back("minted_item_index", dg(d, "minted_item_index"));
  a.coffee_staking_deposit_data = mkdict(std::move(f));
}

void fill_coffee_staking_withdraw(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "source"));
  a.source_secondary = av_addr(dg(d, "user_jetton_wallet"));
  a.destination = av_addr(dg(d, "pool"));
  a.destination_secondary = av_addr(dg(d, "pool_jetton_wallet"));
  a.asset = av_addr(dg(d, "asset"));
  a.amount = av_amount(dg(d, "amount"));
  Value::Fields f;
  f.emplace_back("nft_address", av_addr(dg(d, "nft_address")));
  f.emplace_back("nft_index", dg(d, "nft_index"));
  f.emplace_back("points", dg(d, "points"));
  a.coffee_staking_withdraw_data = mkdict(std::move(f));
}

void fill_coffee_staking_claim_rewards(const Value &d, Action &a) {
  // `admin` is deliberately dropped (always the same highload wallet, and no
  // directional field fits it); the block still carries it.
  a.source = av_addr(dg(d, "pool"));
  a.source_secondary = av_addr(dg(d, "pool_jetton_wallet"));
  a.destination = av_addr(dg(d, "recipient"));
  a.destination_secondary = av_addr(dg(d, "recipient_jetton_wallet"));
  a.asset = av_addr(dg(d, "asset"));
  a.amount = av_amount(dg(d, "amount"));
}

void fill_vesting_send_message(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.destination = av_addr(dg(d, "vesting"));
  a.destination_secondary = av_addr(dg(d, "message_destination"));
  a.amount = av_amount(dg(d, "message_value"));
  Value succ = dg(d, "success");  // the message was actually sent
  a.success = succ.t == VType::Bool && succ.boolean;
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  f.emplace_back("message_boc", dg(d, "message_boc_str"));
  a.vesting_send_message_data = mkdict(std::move(f));
}

// `accounts_added` is a LIST of addresses (the whitelist's unbounded ref chain),
// mapped element-wise through av_addr.
void fill_vesting_add_whitelist(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "adder"));
  a.destination = av_addr(dg(d, "vesting"));
  std::vector<Value> added;
  Value acc = dg(d, "accounts_added");
  if (acc.items) {
    for (const Value &x : *acc.items) added.push_back(av_addr(x));
  }
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  f.emplace_back("accounts_added", Value::make_list(std::move(added)));
  a.vesting_add_whitelist_data = mkdict(std::move(f));
}

void fill_multisig_create_order(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "created_by"));
  a.destination = av_addr(dg(d, "multisig"));
  a.destination_secondary = av_addr(dg(d, "order_contract_address"));
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  f.emplace_back("order_seqno", dg(d, "order_seqno"));
  f.emplace_back("is_created_by_signer", dg(d, "is_created_by_signer"));
  f.emplace_back("is_signed_by_creator", dg(d, "creator_approved"));
  f.emplace_back("creator_index", dg(d, "creator_index"));
  f.emplace_back("expiration_date", dg(d, "expiration_date"));
  f.emplace_back("order_boc", dg(d, "order_boc_str"));
  a.multisig_create_order_data = mkdict(std::move(f));
  Value sg = dg(d, "signers");
  if (sg.items) { for (const Value &x : *sg.items) push_account(a, x); }
}

void fill_multisig_approve(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "signer"));
  a.destination = av_addr(dg(d, "order"));
  Value success = dg(d, "success");
  a.success = success.t == VType::Bool && success.boolean;
  Value::Fields f;
  f.emplace_back("signer_index", dg(d, "signer_index"));
  f.emplace_back("exit_code", dg(d, "exit_code"));
  a.multisig_approve_data = mkdict(std::move(f));
  Value sg = dg(d, "signers");
  if (sg.items) { for (const Value &x : *sg.items) push_account(a, x); }
}

void fill_multisig_execute(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "order_contract_address"));
  a.destination = av_addr(dg(d, "multisig"));
  Value success = dg(d, "success");
  a.success = success.t == VType::Bool && success.boolean;
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  f.emplace_back("order_seqno", dg(d, "order_seqno"));
  f.emplace_back("expiration_date", dg(d, "expiration_date"));
  f.emplace_back("approvals_num", dg(d, "approvals_num"));
  f.emplace_back("signers_hash", dg(d, "signers_hash_str"));
  f.emplace_back("order_boc", dg(d, "order_boc_str"));
  a.multisig_execute_data = mkdict(std::move(f));
  Value sg = dg(d, "signers");
  if (sg.items) { for (const Value &x : *sg.items) push_account(a, x); }
}

// `success` is the literal true written by the matcher: reaching this fill
// means POOLV3_INIT was found.
void fill_tonco_deploy_pool(const Value &d, Action &a) {
  Value succ = dg(d, "success");
  a.success = succ.t == VType::Bool && succ.boolean;
  a.source = av_addr(dg(d, "deployer"));
  a.destination = av_addr(dg(d, "router"));
  a.destination_secondary = av_addr(dg(d, "pool"));
  Value::Fields f;
  f.emplace_back("jetton0_router_wallet", av_addr(dg(d, "jetton0_router_wallet")));
  f.emplace_back("jetton1_router_wallet", av_addr(dg(d, "jetton1_router_wallet")));
  f.emplace_back("jetton0_minter", av_addr(dg(d, "jetton0_minter")));
  f.emplace_back("jetton1_minter", av_addr(dg(d, "jetton1_minter")));
  f.emplace_back("tick_spacing", dg(d, "tick_spacing"));
  f.emplace_back("initial_price_x96", dg(d, "initial_price_x96"));
  f.emplace_back("protocol_fee", dg(d, "protocol_fee"));
  f.emplace_back("lp_fee_base", dg(d, "lp_fee_base"));
  f.emplace_back("lp_fee_current", dg(d, "lp_fee_current"));
  f.emplace_back("pool_active", dg(d, "pool_active"));
  a.tonco_deploy_pool_data = mkdict(std::move(f));
}

// Rewrites the row type like the deposit above: the ethena block btype is
// ethena_withdrawal_request, the ACTION type is the generic
// stake_withdrawal_request with a provider-tagged staking_data.
void fill_ethena_withdrawal_request(const Value &d, Action &a) {
  a.type = "stake_withdrawal_request";
  a.source = av_addr(dg(d, "source"));
  a.source_secondary = av_addr(dg(d, "source_wallet"));
  a.destination = av_addr(dg(d, "pool"));
  a.asset = av_addr(dg(d, "asset"));
  a.amount = av_amount(dg(d, "amount"));
  Value::Fields f;
  f.emplace_back("provider", Value::make_str("ethena"));
  f.emplace_back("tokens_minted", av_amount(dg(d, "ts_usde_amount")));
  a.staking_data = mkdict(std::move(f));
}

void fill_cocoon_worker_payout(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "proxy_contract"));
  a.source_secondary = av_addr(dg(d, "worker_contract"));
  a.destination = av_addr(dg(d, "worker_owner"));
  a.amount = av_amount(dg(d, "payout_amount"));
  Value::Fields f;
  f.emplace_back("payout_type", dg(d, "payout_type"));
  f.emplace_back("query_id", dg(d, "query_id"));
  f.emplace_back("new_tokens", dg(d, "new_tokens"));
  f.emplace_back("worker_state", dg(d, "worker_state"));
  f.emplace_back("worker_tokens", dg(d, "worker_tokens"));
  a.cocoon_worker_payout_data = mkdict(std::move(f));
}

void fill_cocoon_proxy_payout(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "proxy_contract"));
  a.destination = av_addr(dg(d, "proxy_owner"));
  a.destination_secondary = av_addr(dg(d, "excesses_recipient"));
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  a.cocoon_proxy_payout_data = mkdict(std::move(f));
}

// `amount = 0` is a LITERAL ("no actual transfer"), not a value read off the
// block.
void fill_cocoon_proxy_charge(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "proxy_contract"));
  a.destination = av_addr(dg(d, "client_contract"));
  a.amount = Value::make_int64(0);
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  f.emplace_back("new_tokens_used", dg(d, "new_tokens_used"));
  f.emplace_back("expected_address", dg(d, "expected_address"));
  a.cocoon_proxy_charge_data = mkdict(std::move(f));
}

void fill_cocoon_client_top_up(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.destination = av_addr(dg(d, "client_contract"));
  a.destination_secondary = av_addr(dg(d, "proxy_contract"));
  a.amount = av_amount(dg(d, "top_up_amount"));
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  a.cocoon_client_top_up_data = mkdict(std::move(f));
}

void fill_cocoon_register_proxy(const Value &d, Action &a) {
  a.destination = av_addr(dg(d, "root_contract"));
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  a.cocoon_register_proxy_data = mkdict(std::move(f));
}

void fill_cocoon_unregister_proxy(const Value &d, Action &a) {
  a.destination = av_addr(dg(d, "root_contract"));
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  f.emplace_back("seqno", dg(d, "seqno"));
  a.cocoon_unregister_proxy_data = mkdict(std::move(f));
}

void fill_cocoon_client_register(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "owner"));
  a.destination = av_addr(dg(d, "client_contract"));
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  f.emplace_back("nonce", dg(d, "nonce"));
  a.cocoon_client_register_data = mkdict(std::move(f));
}

// Composite stores the uint256 as lowercase hex without "0x"; the block data
// keeps the raw integer and rendering happens here.
void fill_cocoon_client_change_secret_hash(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "owner"));
  a.destination = av_addr(dg(d, "client_contract"));
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  f.emplace_back("new_secret_hash", av_hex_bare(dg(d, "new_secret_hash")));
  a.cocoon_client_change_secret_hash_data = mkdict(std::move(f));
}

void fill_cocoon_client_request_refund(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "owner"));
  a.destination = av_addr(dg(d, "client_contract"));
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  f.emplace_back("via_wallet", dg(d, "via_wallet"));
  a.cocoon_client_request_refund_data = mkdict(std::move(f));
}

void fill_cocoon_grant_refund(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "proxy_contract"));
  a.source_secondary = av_addr(dg(d, "client_contract"));
  a.destination = av_addr(dg(d, "refund_recipient"));
  a.amount = av_amount(dg(d, "payout_amount"));
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  f.emplace_back("new_tokens_used", dg(d, "new_tokens_used"));
  f.emplace_back("expected_address", dg(d, "expected_address"));
  a.cocoon_grant_refund_data = mkdict(std::move(f));
}

// `new_stake` / `withdraw_amount` are each written twice: once as the
// row-level `amount`, once inside the composite.
void fill_cocoon_client_increase_stake(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "owner"));
  a.destination = av_addr(dg(d, "client_contract"));
  a.amount = av_amount(dg(d, "new_stake"));
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  f.emplace_back("new_stake", av_amount(dg(d, "new_stake")));
  a.cocoon_client_increase_stake_data = mkdict(std::move(f));
}

void fill_cocoon_client_withdraw(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "owner"));
  a.destination = av_addr(dg(d, "client_contract"));
  a.amount = av_amount(dg(d, "withdraw_amount"));
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  f.emplace_back("withdraw_amount", av_amount(dg(d, "withdraw_amount")));
  a.cocoon_client_withdraw_data = mkdict(std::move(f));
}

void fill_tgbtc_mint(const Value &d, Action &a) {
  a.type = "tgbtc_mint";
  Value cr = dg(d, "crippled");
  if (cr.t == VType::Bool && cr.boolean) a.type += "_fallback";
  a.source = av_addr(dg(d, "sender"));
  a.destination = av_addr(dg(d, "recipient"));
  a.amount = av_amount(dg(d, "amount"));
  a.asset = av_addr(dg(d, "asset"));
  Value succ = dg(d, "success");
  a.success = succ.t == VType::Bool && succ.boolean;
  Value::Fields ex;
  ex.emplace_back("btc_txid", dg(d, "bitcoin_txid"));
  a.extra = mkdict(std::move(ex));
  a.source_secondary = av_addr(dg(d, "teleport_contract"));
  a.destination_secondary = av_addr(dg(d, "recipient_wallet"));
}

// `crippled` (set by tgbtc_burn_log_only) appends the `_fallback` suffix,
// same pattern as the mint.
void fill_tgbtc_burn(const Value &d, Action &a) {
  a.type = "tgbtc_burn";
  Value cr = dg(d, "crippled");
  if (cr.t == VType::Bool && cr.boolean) a.type += "_fallback";
  a.source = av_addr(dg(d, "sender"));
  a.source_secondary = av_addr(dg(d, "jetton_wallet"));
  a.destination = av_addr(dg(d, "pegout_address"));
  a.amount = av_amount(dg(d, "amount"));
  a.asset = av_addr(dg(d, "asset"));
}

// `amount` is a raw int (av_amount is the Int passthrough). `value` carries
// the DKG timestamp and `extra.pubkey` the raw hex; neither is a TON address.
void fill_tgbtc_new_key(const Value &d, Action &a) {
  a.type = "tgbtc_new_key";
  Value cr = dg(d, "crippled");
  if (cr.t == VType::Bool && cr.boolean) a.type += "_fallback";
  a.source = av_addr(dg(d, "teleport_contract"));
  Value::Fields ex;
  ex.emplace_back("pubkey", dg(d, "pubkey"));
  a.extra = mkdict(std::move(ex));
  a.destination = av_addr(dg(d, "coordinator_contract"));
  a.destination_secondary = av_addr(dg(d, "pegout_address"));
  a.amount = av_amount(dg(d, "amount"));
  a.value = dg(d, "timestamp");
}

// Type is hardcoded `_fallback` (no non-fallback producer, so no `crippled`
// field). Extra key is `pubkey` while the data field is `internal_pubkey`.
void fill_tgbtc_dkg_log(const Value &d, Action &a) {
  a.type = "tgbtc_dkg_log_fallback";
  a.source = av_addr(dg(d, "coordinator_contract"));
  Value::Fields ex;
  ex.emplace_back("pubkey", dg(d, "internal_pubkey"));
  a.extra = mkdict(std::move(ex));
  a.value = dg(d, "timestamp");
}

void fill_dns_renew(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "source"));
  a.destination = av_addr(dg(d, "destination"));
  a.asset = av_addr(dg(d, "collection_address"));
}

// Lowercase unseparated hex, no 0x prefix. td::buffer_to_hex is uppercase and
// must not be substituted: Go takes these fields as *string, so a case slip
// would pass the decoder and surface a silently different key.
Value bytes_hex(const Value &v) {
  if (v.t != VType::Bytes) {
    return Value::null();
  }
  static const char kHex[] = "0123456789abcdef";
  std::string out(v.str.size() * 2, '\0');
  for (std::size_t i = 0; i < v.str.size(); i++) {
    const auto c = static_cast<unsigned char>(v.str[i]);
    out[2 * i] = kHex[c >> 4];
    out[2 * i + 1] = kHex[c & 15];
  }
  return Value::make_str(std::move(out));
}

// DNS records always emit the four database fields. Unknown schemas carry a
// null value and flags.
void fill_change_dns(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "source"));
  a.destination = av_addr(dg(d, "destination"));
  a.asset = av_addr(dg(d, "collection_address"));

  const Value rec = dg(d, "value");
  const Value schema = dg(rec, "schema");
  const std::string s = schema.t == VType::Str ? schema.str : std::string();

  Value value_field = Value::null();
  Value flags = Value::null();
  if (s == "DNSNextResolver" || s == "DNSSmcAddress") {
    value_field = av_addr(dg(rec, "address"));
    if (s == "DNSSmcAddress") {
      flags = dg(rec, "flags");
    }
  } else if (s == "DNSAdnlAddress") {
    value_field = bytes_hex(dg(rec, "address"));
    flags = dg(rec, "flags");
  } else if (s == "DNSStorageAddress") {
    value_field = bytes_hex(dg(rec, "address"));
  } else if (s == "DNSText") {
    value_field = dg(rec, "dns_text");
  }

  Value::Fields f;
  f.emplace_back("value_schema", schema);
  // Int, not Str: the serializer stringifies every non-excluded int inside a
  // composite, which Go's *string + ParseInt expects.
  f.emplace_back("flags", flags);
  f.emplace_back("key", bytes_hex(dg(d, "key")));
  f.emplace_back("value", value_field);
  a.change_dns_record_data = mkdict(std::move(f));
}

// Same column with everything but the key nulled. A deletion names the record
// it removes and nothing else.
void fill_delete_dns(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "source"));
  a.destination = av_addr(dg(d, "destination"));
  a.asset = av_addr(dg(d, "collection_address"));
  Value::Fields f;
  f.emplace_back("value_schema", Value::null());
  f.emplace_back("flags", Value::null());
  f.emplace_back("key", bytes_hex(dg(d, "key")));
  a.change_dns_record_data = mkdict(std::move(f));
}

void fill_nominator_pool_deposit(const Value &d, Action &a) {
  a.type = "stake_deposit";
  a.source = av_addr(dg(d, "source"));
  a.destination = av_addr(dg(d, "pool"));
  a.amount = av_amount(dg(d, "value"));
  Value::Fields f;
  f.emplace_back("provider", Value::make_str("nominator"));
  a.staking_data = mkdict(std::move(f));
}

void fill_nominator_pool_withdraw_request(const Value &d, Action &a) {
  // The btype does NOT decide the action type here: a request that already
  // carries its payout is a COMPLETED withdrawal.
  Value payout = dg(d, "payout_amount");
  if (payout.is_null()) {
    a.type = "stake_withdrawal_request";
  } else {
    a.type = "stake_withdrawal";
    a.amount = av_amount(payout);
  }
  Value::Fields f;
  f.emplace_back("provider", Value::make_str("nominator"));
  a.staking_data = mkdict(std::move(f));
  a.source = av_addr(dg(d, "source"));
  a.destination = av_addr(dg(d, "pool"));
}

// Both elector btypes: source + amount, no composite. Recover omits `amount`
// when the elector never confirmed; dg answers Null for a missing key.
void fill_election(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "stake_holder"));
  a.amount = av_amount(dg(d, "amount"));
}

void fill_subscribe(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "subscriber"));
  a.destination = av_addr(dg(d, "beneficiary"));
  a.destination_secondary = av_addr(dg(d, "subscription"));
  a.amount = av_amount(dg(d, "amount"));
}

void fill_unsubscribe(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "subscriber"));
  a.destination = av_addr(dg(d, "beneficiary"));
  a.destination_secondary = av_addr(dg(d, "subscription"));
}

void fill_tonstakers_deposit(const Value &d, Action &a) {
  a.type = "stake_deposit";
  a.source = av_addr(dg(d, "source"));
  a.destination = av_addr(dg(d, "pool"));
  a.amount = av_amount(dg(d, "value"));
  a.asset = av_addr(dg(d, "asset"));
  Value::Fields f;
  f.emplace_back("provider", Value::make_str("liquid_staking"));
  f.emplace_back("tokens_minted", av_amount(dg(d, "tokens_minted")));
  a.staking_data = mkdict(std::move(f));
}

void fill_tonstakers_withdraw(const Value &d, Action &a) {
  a.type = "stake_withdrawal";
  a.source = av_addr(dg(d, "stake_holder"));
  a.destination = av_addr(dg(d, "pool"));
  a.amount = av_amount(dg(d, "amount"));
  a.asset = av_addr(dg(d, "asset"));
  Value::Fields f;
  f.emplace_back("provider", Value::make_str("liquid_staking"));
  f.emplace_back("ts_nft", av_addr(dg(d, "burnt_nft")));
  f.emplace_back("tokens_burnt", av_amount(dg(d, "tokens_burnt")));
  a.staking_data = mkdict(std::move(f));
}

void fill_tonstakers_withdraw_request(const Value &d, Action &a) {
  a.type = "stake_withdrawal_request";
  a.source = av_addr(dg(d, "source"));
  a.source_secondary = av_addr(dg(d, "tsTON_wallet"));
  a.destination = av_addr(dg(d, "pool"));
  a.amount = av_amount(dg(d, "tokens_burnt"));
  a.asset = av_addr(dg(d, "asset"));
  Value::Fields f;
  f.emplace_back("provider", Value::make_str("liquid_staking"));
  f.emplace_back("ts_nft", av_addr(dg(d, "minted_nft")));
  a.staking_data = mkdict(std::move(f));
}

// Distinct from the dedust deposit fills: same composite column, different
// data field spelling (amount_1 here, amount1 there).
void fill_dex_deposit_liquidity(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.destination = av_addr(dg(d, "pool"));
  Value::Fields f;
  f.emplace_back("dex", dg(d, "dex"));
  f.emplace_back("amount1", av_amount(dg(d, "amount_1")));
  f.emplace_back("amount2", av_amount(dg(d, "amount_2")));
  f.emplace_back("asset1", av_addr(dg(d, "asset_1")));
  f.emplace_back("asset2", av_addr(dg(d, "asset_2")));
  f.emplace_back("user_jetton_wallet_1", av_addr(dg(d, "sender_wallet_1")));
  f.emplace_back("user_jetton_wallet_2", av_addr(dg(d, "sender_wallet_2")));
  f.emplace_back("lp_tokens_minted", av_amount(dg(d, "lp_tokens_minted")));
  a.dex_deposit_liquidity_data = mkdict(std::move(f));
}

// Reuses the dex_deposit_liquidity_data composite. Not typos vs V1: pool
// lives under `pool` (V1: `pool_address`), user wallets under
// `sender_wallet_N` (V1: `user_jetton_wallet_N`); `target_asset_*` aliases
// asset_1/asset_2 (V2 has no separate target asset, only target amounts).
// `destination_secondary` is written only when the deposit contract is present.
void fill_dedust_v2_deposit(const Value &d, Action &a) {
  a.type = "dex_deposit_liquidity";
  a.source = av_addr(dg(d, "sender"));
  a.destination = av_addr(dg(d, "pool"));
  Value deposit_contract = dg(d, "deposit_contract");
  if (!deposit_contract.is_null()) {
    a.destination_secondary = av_addr(deposit_contract);
  }
  std::vector<Value> excesses;
  Value ve = dg(d, "vault_excesses");
  if (ve.items) {
    for (const Value &e : *ve.items) {
      Value::Fields ef;
      Value a0 = Value::null(), a1 = Value::null();
      if (e.items && e.items->size() >= 2) {
        a0 = (*e.items)[0];
        a1 = (*e.items)[1];
      }
      ef.emplace_back("asset", av_addr(a0));
      ef.emplace_back("amount", av_amount(a1));
      excesses.push_back(mkdict(std::move(ef)));
    }
  }
  Value::Fields f;
  f.emplace_back("dex", dg(d, "dex"));
  f.emplace_back("asset1", av_addr(dg(d, "asset_1")));
  f.emplace_back("amount1", av_amount(dg(d, "amount_1")));
  f.emplace_back("asset2", av_addr(dg(d, "asset_2")));
  f.emplace_back("amount2", av_amount(dg(d, "amount_2")));
  f.emplace_back("user_jetton_wallet_1", av_addr(dg(d, "sender_wallet_1")));
  f.emplace_back("user_jetton_wallet_2", av_addr(dg(d, "sender_wallet_2")));
  f.emplace_back("lp_tokens_minted", av_amount(dg(d, "lp_tokens_minted")));
  f.emplace_back("target_asset_1", av_addr(dg(d, "asset_1")));
  f.emplace_back("target_amount_1", av_amount(dg(d, "target_amount_1")));
  f.emplace_back("target_asset_2", av_addr(dg(d, "asset_2")));
  f.emplace_back("target_amount_2", av_amount(dg(d, "target_amount_2")));
  f.emplace_back("vault_excesses", Value::make_list(std::move(excesses)));
  a.dex_deposit_liquidity_data = mkdict(std::move(f));
}

// Bare dex_deposit_liquidity fill plus the deposit contract. A partial carries
// no target amounts and no vault excesses, so its composite is eight keys,
// deliberately not the twelve-key shape of the fill above.
void fill_dedust_v2_deposit_partial(const Value &d, Action &a) {
  a.type = "dex_deposit_liquidity";
  fill_dex_deposit_liquidity(d, a);
  Value deposit_contract = dg(d, "deposit_contract");
  if (!deposit_contract.is_null()) {
    a.destination_secondary = av_addr(deposit_contract);
  }
}

void fill_dex_withdraw_liquidity(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.source_secondary = av_addr(dg(d, "sender_wallet"));
  a.destination = av_addr(dg(d, "pool"));
  a.asset = av_addr(dg(d, "asset"));
  Value::Fields f;
  f.emplace_back("dex", dg(d, "dex"));
  f.emplace_back("amount1", av_amount(dg(d, "amount1_out")));
  f.emplace_back("amount2", av_amount(dg(d, "amount2_out")));
  f.emplace_back("asset1_out", av_addr(dg(d, "asset1_out")));
  f.emplace_back("asset2_out", av_addr(dg(d, "asset2_out")));
  f.emplace_back("user_jetton_wallet_1", av_addr(dg(d, "wallet1")));
  f.emplace_back("user_jetton_wallet_2", av_addr(dg(d, "wallet2")));
  f.emplace_back("dex_jetton_wallet_1", av_addr(dg(d, "dex_jetton_wallet_1")));
  f.emplace_back("dex_wallet_1", av_addr(dg(d, "dex_wallet_1")));
  f.emplace_back("dex_wallet_2", av_addr(dg(d, "dex_wallet_2")));
  f.emplace_back("dex_jetton_wallet_2", av_addr(dg(d, "dex_jetton_wallet_2")));
  f.emplace_back("is_refund", dg(d, "is_refund"));
  f.emplace_back("lp_tokens_burnt", av_amount(dg(d, "lp_tokens_burnt")));
  a.dex_withdraw_liquidity_data = mkdict(std::move(f));
}

// getgems / telegram NFT purchase. Sets nft_transfer_data ONLY, unlike
// fill_nft_transfer, which also writes nft_listing_data.
void fill_nft_purchase(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "prev_owner"));
  a.destination = av_addr(dg(d, "new_owner"));
  a.asset_secondary = av_addr(dg(d, "nft_address"));
  a.asset = av_addr(dg(d, "collection_address"));
  Value::Fields f;
  f.emplace_back("query_id", dg(d, "query_id"));
  f.emplace_back("is_purchase", Value::make_bool(true));
  f.emplace_back("price", av_amount(dg(d, "price")));
  f.emplace_back("nft_item_index", dg(d, "nft_index"));
  f.emplace_back("forward_amount", av_amount(dg(d, "forward_amount")));
  f.emplace_back("custom_payload", dg(d, "custom_payload"));
  f.emplace_back("forward_payload", dg(d, "forward_payload"));
  f.emplace_back("response_destination", av_addr(dg(d, "response_destination")));
  f.emplace_back("marketplace", dg(d, "marketplace"));
  f.emplace_back("marketplace_address", av_addr(dg(d, "marketplace_address")));
  f.emplace_back("real_prev_owner", av_addr(dg(d, "real_prev_owner")));
  f.emplace_back("payout_amount", av_amount(dg(d, "payout_amount")));
  f.emplace_back("payout_comment_encrypted", dg(d, "payout_comment_encrypted"));
  f.emplace_back("payout_comment_encoded", dg(d, "payout_comment_encoded"));
  f.emplace_back("payout_comment", dg(d, "payout_comment"));
  f.emplace_back("royalty_amount", Value::null());
  a.nft_transfer_data = mkdict(std::move(f));
}

void fill_dns_release(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "source"));
  a.destination = av_addr(dg(d, "nft_address"));
  a.asset = av_addr(dg(d, "nft_collection"));
  Value::Fields t;
  t.emplace_back("query_id", dg(d, "query_id"));
  t.emplace_back("is_purchase", Value::null());
  t.emplace_back("price", Value::null());
  t.emplace_back("nft_item_index", dg(d, "nft_index"));
  t.emplace_back("forward_amount", Value::null());
  t.emplace_back("custom_payload", Value::null());
  t.emplace_back("forward_payload", Value::null());
  t.emplace_back("response_destination", Value::null());
  t.emplace_back("marketplace", Value::null());
  t.emplace_back("marketplace_address", Value::null());
  t.emplace_back("real_prev_owner", Value::null());
  t.emplace_back("payout_amount", Value::null());
  t.emplace_back("payout_comment_encrypted", Value::null());
  t.emplace_back("payout_comment_encoded", Value::null());
  t.emplace_back("payout_comment", Value::null());
  t.emplace_back("royalty_amount", Value::null());
  a.nft_transfer_data = mkdict(std::move(t));
  a.value = av_amount(dg(d, "value"));
}

void fill_nft_put_on_auction(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "owner"));
  a.source_secondary = av_addr(dg(d, "listing_address"));
  a.destination = av_addr(dg(d, "auction_address"));
  a.asset = av_addr(dg(d, "nft_collection"));
  a.asset_secondary = av_addr(dg(d, "nft_address"));
  Value::Fields t;
  t.emplace_back("marketplace_address", av_addr(dg(d, "marketplace_address")));
  a.nft_transfer_data = mkdict(std::move(t));
  Value::Fields l;
  l.emplace_back("nft_item_index", dg(d, "nft_index"));
  l.emplace_back("mp_fee_factor", av_amount(dg(d, "mp_fee_factor")));
  l.emplace_back("mp_fee_base", av_amount(dg(d, "mp_fee_base")));
  l.emplace_back("royalty_fee_base", av_amount(dg(d, "royalty_fee_base")));
  l.emplace_back("max_bid", av_amount(dg(d, "max_bid")));
  l.emplace_back("min_bid", av_amount(dg(d, "min_bid")));
  l.emplace_back("marketplace_fee_address", av_addr(dg(d, "mp_fee_address")));
  l.emplace_back("marketplace", dg(d, "marketplace"));
  l.emplace_back("royalty_address", av_addr(dg(d, "royalty_fee_addr")));
  // Sale fields are deliberately null on auction listings.
  l.emplace_back("full_price", Value::null());
  l.emplace_back("marketplace_fee", Value::null());
  l.emplace_back("royalty_amount", Value::null());
  a.nft_listing_data = mkdict(std::move(l));
}

// Shared by teleitem_cancel_auction and the three GetGems cancel/finish
// btypes. For GetGems the destination is the sale/auction contract and
// asset_secondary is the NFT — an account generic assembly cannot reach,
// because those matchers do not consume the nft_transfer that would have
// carried it. push_account + dedup covers both cases.
void fill_cancel_nft_trade(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "owner"));
  a.destination = av_addr(dg(d, "trade_contract"));
  a.asset_secondary = av_addr(dg(d, "nft_address"));
  a.asset = av_addr(dg(d, "nft_collection"));
  Value::Fields f;
  f.emplace_back("query_id", Value::null());
  f.emplace_back("is_purchase", Value::null());
  f.emplace_back("price", Value::null());
  f.emplace_back("nft_item_index", Value::null());
  f.emplace_back("forward_amount", Value::null());
  f.emplace_back("custom_payload", Value::null());
  f.emplace_back("forward_payload", Value::null());
  f.emplace_back("response_destination", Value::null());
  f.emplace_back("marketplace", Value::null());
  f.emplace_back("marketplace_address", av_addr(dg(d, "marketplace_address")));
  f.emplace_back("real_prev_owner", Value::null());
  f.emplace_back("payout_amount", Value::null());
  f.emplace_back("payout_comment_encrypted", Value::null());
  f.emplace_back("payout_comment_encoded", Value::null());
  f.emplace_back("payout_comment", Value::null());
  f.emplace_back("royalty_amount", Value::null());
  a.nft_transfer_data = mkdict(std::move(f));
  push_account(a, a.asset_secondary);
}

// Same two composites as fill_nft_put_on_auction with sale/auction key
// groups swapped: the sale carries full_price / marketplace_fee /
// royalty_amount and nulls the five auction-only keys.
void fill_nft_put_on_sale(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "owner"));
  a.source_secondary = av_addr(dg(d, "listing_address"));
  a.destination = av_addr(dg(d, "sale_address"));
  a.asset = av_addr(dg(d, "nft_collection"));
  a.asset_secondary = av_addr(dg(d, "nft_address"));
  Value::Fields t;
  t.emplace_back("marketplace_address", av_addr(dg(d, "marketplace_address")));
  a.nft_transfer_data = mkdict(std::move(t));
  Value::Fields l;
  l.emplace_back("nft_item_index", dg(d, "nft_index"));
  l.emplace_back("full_price", av_amount(dg(d, "full_price")));
  l.emplace_back("marketplace_fee", av_amount(dg(d, "marketplace_fee")));
  l.emplace_back("royalty_amount", av_amount(dg(d, "royalty_amount")));
  l.emplace_back("marketplace_fee_address", av_addr(dg(d, "marketplace_fee_address")));
  l.emplace_back("marketplace", dg(d, "marketplace"));
  l.emplace_back("royalty_address", av_addr(dg(d, "royalty_address")));
  // Auction fields are deliberately null on sale listings.
  l.emplace_back("mp_fee_factor", Value::null());
  l.emplace_back("mp_fee_base", Value::null());
  l.emplace_back("royalty_fee_base", Value::null());
  l.emplace_back("max_bid", Value::null());
  l.emplace_back("min_bid", Value::null());
  a.nft_listing_data = mkdict(std::move(l));
}

// asset_secondary here is the NFT (neither source nor destination), so it
// needs the extra push.
void fill_sale_update(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.destination = av_addr(dg(d, "sale_contract"));
  a.asset_secondary = av_addr(dg(d, "nft_address"));
  Value::Fields l;
  l.emplace_back("nft_item_index", Value::null());
  l.emplace_back("full_price", av_amount(dg(d, "new_full_price")));
  l.emplace_back("marketplace_fee", av_amount(dg(d, "new_marketplace_fee")));
  l.emplace_back("royalty_amount", av_amount(dg(d, "new_royalty_amount")));
  l.emplace_back("marketplace_fee_address", Value::null());
  l.emplace_back("marketplace", Value::null());
  l.emplace_back("royalty_address", Value::null());
  l.emplace_back("mp_fee_factor", Value::null());
  l.emplace_back("mp_fee_base", Value::null());
  l.emplace_back("royalty_fee_base", Value::null());
  l.emplace_back("max_bid", Value::null());
  l.emplace_back("min_bid", Value::null());
  a.nft_listing_data = mkdict(std::move(l));
  Value::Fields t;
  t.emplace_back("query_id", Value::null());
  t.emplace_back("is_purchase", Value::null());
  t.emplace_back("price", Value::null());
  t.emplace_back("nft_item_index", Value::null());
  t.emplace_back("forward_amount", Value::null());
  t.emplace_back("custom_payload", Value::null());
  t.emplace_back("forward_payload", Value::null());
  t.emplace_back("response_destination", Value::null());
  t.emplace_back("marketplace", Value::null());
  t.emplace_back("marketplace_address", av_addr(dg(d, "marketplace_address")));
  t.emplace_back("real_prev_owner", Value::null());
  t.emplace_back("payout_amount", Value::null());
  t.emplace_back("payout_comment_encrypted", Value::null());
  t.emplace_back("payout_comment_encoded", Value::null());
  t.emplace_back("payout_comment", Value::null());
  t.emplace_back("royalty_amount", Value::null());
  a.nft_transfer_data = mkdict(std::move(t));
  push_account(a, a.asset_secondary);
}

void fill_auction_bid(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "bidder"));
  a.destination = av_addr(dg(d, "auction"));
  a.asset_secondary = av_addr(dg(d, "nft_address"));
  a.asset = av_addr(dg(d, "nft_collection"));
  Value::Fields t;
  t.emplace_back("nft_item_index", dg(d, "nft_item_index"));
  t.emplace_back("marketplace", dg(d, "auction_type"));
  a.nft_transfer_data = mkdict(std::move(t));
  push_account(a, a.asset_secondary);
  a.value = dg(d, "amount");  // raw int, not av_amount
}

void fill_auction_outbid(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "auction_address"));
  a.destination = av_addr(dg(d, "bidder"));
  a.source_secondary = av_addr(dg(d, "new_bidder"));
  a.asset_secondary = av_addr(dg(d, "nft"));
  a.asset = av_addr(dg(d, "nft_collection"));
  Value::Fields t;
  t.emplace_back("marketplace", dg(d, "auction_type"));
  a.nft_transfer_data = mkdict(std::move(t));
  a.amount = av_amount(dg(d, "amount"));
  Value::Fields tt;
  tt.emplace_back("comment", dg(d, "comment"));
  a.ton_transfer_data = mkdict(std::move(tt));
  push_account(a, a.asset_secondary);
}

// `destination` is deliberately never set.
void fill_nft_discovery(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "sender"));
  a.asset = av_addr(dg(d, "result_collection"));
  a.asset_secondary = av_addr(dg(d, "nft"));
  Value::Fields t;
  t.emplace_back("query_id", Value::null());
  t.emplace_back("is_purchase", Value::null());
  t.emplace_back("price", Value::null());
  t.emplace_back("nft_item_index", dg(d, "result_index"));
  t.emplace_back("forward_amount", Value::null());
  t.emplace_back("custom_payload", Value::null());
  t.emplace_back("forward_payload", Value::null());
  t.emplace_back("response_destination", Value::null());
  t.emplace_back("marketplace", Value::null());
  t.emplace_back("marketplace_address", Value::null());
  t.emplace_back("real_prev_owner", Value::null());
  t.emplace_back("payout_amount", Value::null());
  t.emplace_back("payout_comment_encrypted", Value::null());
  t.emplace_back("payout_comment_encoded", Value::null());
  t.emplace_back("payout_comment", Value::null());
  t.emplace_back("royalty_amount", Value::null());
  a.nft_transfer_data = mkdict(std::move(t));
}

// The only producer of the nft_mint_data composite. `opcode` and
// `nft_item_index` are raw passthroughs; the index is already an Int.
void fill_nft_mint(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "source"));
  a.destination = av_addr(dg(d, "address"));
  a.asset_secondary = a.destination;
  a.opcode = dg(d, "opcode");
  a.asset = av_addr(dg(d, "collection"));
  Value::Fields f;
  f.emplace_back("nft_item_index", dg(d, "index"));
  a.nft_mint_data = mkdict(std::move(f));
}

// Fallback-path fills only (ClassifyResult.failure). The main dump never
// emits these leaf btypes.
void fill_call_contract(const Value &d, Action &a) {
  a.opcode = dg(d, "opcode");
  a.value = av_amount(dg(d, "value"));
  a.source = av_addr(dg(d, "source"));
  a.destination = av_addr(dg(d, "destination"));
  // value_extra_currencies is not a rendered column; `extra_currencies` is
  // absent from the leaf data.
}

void fill_ton_transfer(const Value &d, Action &a) {
  a.value = av_amount(dg(d, "value"));
  a.source = av_addr(dg(d, "source"));
  a.destination = av_addr(dg(d, "destination"));
  Value::Fields f;
  f.emplace_back("content", dg(d, "comment"));  // leaf_comment_info already strips U+0000
  f.emplace_back("encrypted", dg(d, "encrypted"));
  a.ton_transfer_data = mkdict(std::move(f));
}

void fill_tick_tock(const Value &d, Action &a) {
  a.source = av_addr(dg(d, "account"));
}

// Returns false for a btype outside the fill set (skip-table entry).
bool fill_action(Block *b, Action &a) {
  const Value &d = b->data;
  const std::string &t = b->btype;
  if (t == "call_contract" || t == "contract_deploy" || t == "gasless_request" ||
      t == "change_wallet_key") {
    fill_call_contract(d, a);  // the two wallet types carry only source/destination/value
  }
  else if (t == "ton_transfer") fill_ton_transfer(d, a);
  else if (t == "tick_tock") fill_tick_tock(d, a);
  else if (t == "jetton_transfer") fill_jetton_transfer(d, a);
  else if (t == "jetton_burn") fill_jetton_burn(d, a);
  else if (t == "jetton_mint") fill_jetton_mint(d, a);
  else if (t == "jetton_swap") fill_jetton_swap(d, a);
  else if (t == "nft_transfer") fill_nft_transfer(d, a);
  else if (t == "dedust_deposit_liquidity") fill_dedust_deposit(d, a);
  // Same DepositLiquidityData field set as dedust deposit; withdraw produces
  // `dex_withdraw_liquidity`, already dispatched below.
  else if (t == "coffee_deposit_liquidity") fill_dedust_deposit(d, a);
  else if (t == "dedust_deposit_liquidity_partial") fill_dedust_deposit_partial(d, a);
  else if (t == "dedust_v2_deposit_liquidity") fill_dedust_v2_deposit(d, a);
  else if (t == "dedust_v2_deposit_liquidity_partial") fill_dedust_v2_deposit_partial(d, a);
  else if (t == "tonco_deposit_liquidity") fill_tonco_deposit(d, a);
  else if (t == "tonco_withdraw_liquidity") fill_tonco_withdraw(d, a);
  else if (t == "dedust_v2_claim_fees") fill_dedust_v2_claim_fees(d, a);
  else if (t == "dedust_v2_claim_reward") fill_dedust_v2_claim_reward(d, a);
  else if (t == "evaa_supply") fill_evaa_supply(d, a, b->failed);
  else if (t == "evaa_withdraw") fill_evaa_withdraw(d, a, b->failed);
  else if (t == "evaa_liquidate") fill_evaa_liquidate(d, a);
  else if (t == "jvault_stake") fill_jvault_stake(d, a);
  else if (t == "jvault_unstake") fill_jvault_unstake(d, a);
  else if (t == "jvault_unstake_request") fill_jvault_unstake_request(d, a);
  else if (t == "jvault_claim") fill_jvault_claim(d, a);
  else if (t == "ethena_deposit") fill_ethena_deposit(d, a);
  else if (t == "layerzero_receive") fill_layerzero_receive(d, a);
  else if (t == "layerzero_send") fill_layerzero_send(d, a);
  else if (t == "layerzero_send_tokens") fill_layerzero_send_tokens(d, a);
  else if (t == "layerzero_commit_packet") fill_layerzero_commit_packet(d, a);
  else if (t == "layerzero_dvn_verify") fill_layerzero_dvn_verify(d, a);
  else if (t == "coffee_create_vault") fill_coffee_create_vault(d, a);
  else if (t == "coffee_create_pool_creator") fill_coffee_create_pool_creator(d, a);
  else if (t == "coffee_create_pool") fill_coffee_create_pool(d, a);
  else if (t == "coffee_mev_protect_hold_funds") fill_coffee_mev_protect_hold_funds(d, a);
  else if (t == "coffee_staking_deposit") fill_coffee_staking_deposit(d, a);
  else if (t == "coffee_staking_withdraw") fill_coffee_staking_withdraw(d, a);
  else if (t == "coffee_staking_claim_rewards") fill_coffee_staking_claim_rewards(d, a);
  else if (t == "vesting_send_message") fill_vesting_send_message(d, a);
  else if (t == "vesting_add_whitelist") fill_vesting_add_whitelist(d, a);
  else if (t == "multisig_create_order") fill_multisig_create_order(d, a);
  else if (t == "multisig_approve") fill_multisig_approve(d, a);
  else if (t == "multisig_execute") fill_multisig_execute(d, a);
  else if (t == "tonco_deploy_pool") fill_tonco_deploy_pool(d, a);
  else if (t == "ethena_withdrawal_request") fill_ethena_withdrawal_request(d, a);
  else if (t == "cocoon_worker_payout") fill_cocoon_worker_payout(d, a);
  else if (t == "cocoon_proxy_payout") fill_cocoon_proxy_payout(d, a);
  else if (t == "cocoon_proxy_charge") fill_cocoon_proxy_charge(d, a);
  else if (t == "cocoon_client_top_up") fill_cocoon_client_top_up(d, a);
  else if (t == "cocoon_register_proxy") fill_cocoon_register_proxy(d, a);
  else if (t == "cocoon_unregister_proxy") fill_cocoon_unregister_proxy(d, a);
  else if (t == "cocoon_client_register") fill_cocoon_client_register(d, a);
  else if (t == "cocoon_client_change_secret_hash") fill_cocoon_client_change_secret_hash(d, a);
  else if (t == "cocoon_client_request_refund") fill_cocoon_client_request_refund(d, a);
  else if (t == "cocoon_grant_refund") fill_cocoon_grant_refund(d, a);
  else if (t == "cocoon_client_increase_stake") fill_cocoon_client_increase_stake(d, a);
  else if (t == "cocoon_client_withdraw") fill_cocoon_client_withdraw(d, a);
  else if (t == "tgbtc_mint") fill_tgbtc_mint(d, a);
  else if (t == "tgbtc_burn") fill_tgbtc_burn(d, a);
  else if (t == "tgbtc_new_key") fill_tgbtc_new_key(d, a);
  else if (t == "tgbtc_dkg_log") fill_tgbtc_dkg_log(d, a);
  else if (t == "renew_dns") fill_dns_renew(d, a);
  else if (t == "nominator_pool_deposit") fill_nominator_pool_deposit(d, a);
  else if (t == "nominator_pool_withdraw_request") fill_nominator_pool_withdraw_request(d, a);
  else if (t == "election_deposit" || t == "election_recover") fill_election(d, a);
  else if (t == "subscribe") fill_subscribe(d, a);
  else if (t == "unsubscribe") fill_unsubscribe(d, a);
  else if (t == "tonstakers_deposit") fill_tonstakers_deposit(d, a);
  else if (t == "tonstakers_withdraw") fill_tonstakers_withdraw(d, a);
  else if (t == "tonstakers_withdraw_request") fill_tonstakers_withdraw_request(d, a);
  else if (t == "dex_deposit_liquidity") fill_dex_deposit_liquidity(d, a);
  else if (t == "dex_withdraw_liquidity") fill_dex_withdraw_liquidity(d, a);
  else if (t == "nft_purchase") fill_nft_purchase(d, a);
  else if (t == "dns_purchase") fill_nft_purchase(d, a);
  else if (t == "dns_release") fill_dns_release(d, a);
  else if (t == "change_dns") fill_change_dns(d, a);
  else if (t == "delete_dns") fill_delete_dns(d, a);
  else if (t == "teleitem_start_auction" || t == "nft_put_on_auction")
    fill_nft_put_on_auction(d, a);
  else if (t == "teleitem_cancel_auction" || t == "nft_cancel_sale" ||
           t == "nft_cancel_auction" || t == "nft_finish_auction")
    fill_cancel_nft_trade(d, a);
  else if (t == "nft_put_on_sale") fill_nft_put_on_sale(d, a);
  else if (t == "nft_update_sale") fill_sale_update(d, a);
  else if (t == "auction_bid") fill_auction_bid(d, a);
  else if (t == "auction_outbid") fill_auction_outbid(d, a);
  else if (t == "nft_discovery") fill_nft_discovery(d, a);
  else if (t == "nft_mint") fill_nft_mint(d, a);
  else return false;
  return true;
}
}  // namespace

const EventNode *root_event_node(const Block *b) {
  const EventNode *root = nullptr;
  for (const EventNode *n : b->event_nodes) {
    if (root == nullptr || n->lt() < root->lt()) root = n;
  }
  return root;
}

std::string calc_action_id(const Block *b) {
  const EventNode *root = root_event_node(b);
  std::string key;
  if (root != nullptr) {
    key = (root->msg != nullptr) ? root->msg->msg_hash : root->tx_hash();
  }
  key += b->btype;
  return td::base64_encode(td::Slice(td::sha256(td::Slice(key))));
}

bool build_action(Block *b, Action &a) {
  a.type = b->btype;
  a.success = !b->failed;
  a.action_id = calc_action_id(b);
  std::set<std::string> ths;
  std::int64_t maxlt = std::numeric_limits<std::int64_t>::min();
  std::int64_t minu = std::numeric_limits<std::int64_t>::max();
  std::int64_t maxu = std::numeric_limits<std::int64_t>::min();
  std::int64_t seqmax = std::numeric_limits<std::int64_t>::min();
  for (const EventNode *n : b->event_nodes) {
    ths.insert(n->tx_hash());
    std::int64_t tlt = n->tx != nullptr ? n->tx->lt : 0;
    maxlt = std::max(maxlt, tlt);
    minu = std::min(minu, node_utime(n));
    maxu = std::max(maxu, n->tx != nullptr ? n->tx->now : 0);
    if (n->tx != nullptr) seqmax = std::max(seqmax, n->tx->mc_block_seqno);
  }
  if (b->initiating_event_node != nullptr) {
    ths.insert(b->initiating_event_node->tx_hash());
  }
  a.tx_hashes.assign(ths.begin(), ths.end());
  a.start_lt = b->min_lt;
  a.end_lt = b->event_nodes.empty() ? 0 : maxlt;
  a.start_utime = b->event_nodes.empty() ? 0 : minu;
  a.end_utime = b->event_nodes.empty() ? 0 : maxu;
  a.mc_seqno_end = b->event_nodes.empty() ? 0 : seqmax;
  if (!fill_action(b, a)) return false;

  // Directional fields are strings by the time an action row exists; the
  // serializer relies on this boundary.
  const std::pair<const char *, const Value *> directional_fields[]{
      {"source", &a.source},
      {"source_secondary", &a.source_secondary},
      {"destination", &a.destination},
      {"destination_secondary", &a.destination_secondary},
      {"asset", &a.asset},
      {"asset_secondary", &a.asset_secondary},
      {"asset2", &a.asset2},
      {"asset2_secondary", &a.asset2_secondary},
  };
  for (const auto &[name, value] : directional_fields) {
    if (value->t != VType::Str && value->t != VType::Null) {
      LOG(ERROR) << "action build produced non-string directional field " << name
                 << " for " << b->btype;
      return false;
    }
  }

// Role-less accounts: each event_node's tx account; + source/source_secondary;
// + destination/destination_secondary unless ghost; + the initiating tx
// account (non-tick_tock). Dedup, drop nulls. Seeded from whatever the fill
// already pushed (NFT sale/auction asset_secondary and similar extras).
  std::vector<std::string> accts = std::move(a.accounts);
  a.accounts.clear();
  for (const EventNode *n : b->event_nodes) {
    if (n->tx != nullptr) accts.push_back(n->tx->account);
  }
  auto push_acc = [&](const Value &v) {
    if (v.t == VType::Str) accts.push_back(v.str);
  };
  push_acc(a.source);
  push_acc(a.source_secondary);
  if (!b->is_ghost_block) {
    push_acc(a.destination);
    push_acc(a.destination_secondary);
  }
  if (b->initiating_event_node != nullptr && !b->initiating_event_node->is_tick_tock &&
      b->initiating_event_node->tx != nullptr) {
    accts.push_back(b->initiating_event_node->tx->account);
  }
  std::sort(accts.begin(), accts.end());
  accts.erase(std::unique(accts.begin(), accts.end()), accts.end());
  a.accounts = std::move(accts);
  return true;
}

bool build_action(const ActionRow &row, Action &a) {
  if (!build_action(row.block, a)) return false;
  a.parent_action_id = row.parent_action_id;
  a.ancestor_type = row.ancestor_type;
  if (!row.parent_gasless_action.empty()) {
    Value::Fields f = a.extra.fields ? *a.extra.fields : Value::Fields{};
    f.emplace_back("parent_gasless_action", Value::make_str(row.parent_gasless_action));
    a.extra = mkdict(std::move(f));
  }
  return true;
}

Action create_unknown_action(const Trace &trace) {
  Action a;
  a.type = "unknown";
  a.action_id = trace.trace_id;
  // Every loader must populate these trace aggregates.
  a.start_lt = trace.start_lt;
  a.end_lt = trace.end_lt;
  a.start_utime = trace.start_utime;
  a.end_utime = trace.end_utime;
  a.mc_seqno_end = trace.mc_seqno_end;
  // success = not any(tx.aborted): a property of the whole trace, not a block.
  bool failed = false;
  std::set<std::string> accts;
  for (const auto &tx : trace.transactions) {
    a.tx_hashes.push_back(tx->hash);
    accts.insert(tx->account);
    failed = failed || tx->aborted;
  }
  a.success = !failed;
  // Sorted for determinism; consumers and A/B comparators sort anyway.
  std::sort(a.tx_hashes.begin(), a.tx_hashes.end());
  a.accounts.assign(accts.begin(), accts.end());
  return a;
}

}  // namespace mch
