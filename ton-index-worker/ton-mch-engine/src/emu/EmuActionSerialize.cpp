#include "EmuActionSerialize.h"

#include "Value.h"

#include "td/utils/base64.h"

#include <msgpack.hpp>

#include <cstdint>
#include <cstring>
#include <set>
#include <string>
#include <vector>

namespace mch {

namespace {

using Packer = msgpack::packer<msgpack::sbuffer>;

void pack_key(Packer &pk, const char *k) {
  const auto n = static_cast<std::uint32_t>(std::strlen(k));
  pk.pack_str(n);
  pk.pack_str_body(k, n);
}

void pack_text(Packer &pk, const std::string &s) {
  const auto n = static_cast<std::uint32_t>(s.size());
  pk.pack_str(n);
  pk.pack_str_body(s.data(), n);
}

// These shapes omit an unfilled top-level action field. Composite maps retain
// their full key set and encode unfilled members as nil.
bool omitted(const Value &v) {
  switch (v.t) {
    case VType::Null:
      return true;
    case VType::Account:
      return v.addr_none;
    case VType::Asset:
      return v.is_ton || !v.has_jetton;
    case VType::Amount:
      return !v.amount_float && v.num.is_null();  // make_amount_none()
    default:
      return false;
  }
}

// Keys in this set encode integer values as decimal strings at every map depth.
// Other integers use native msgpack types. See
// Plain MsgPack payload; integers that can exceed int64 are decimal strings by field name.
bool decimal_string_key(const std::string &k) {
  static const std::set<std::string> keys{
      // top level
      "amount", "value",
      // jetton_transfer_data / vesting_send_message_data / nft_transfer_data
      "forward_amount", "query_id",
      // jetton_swap_data (+ its nested transfer / peer-swap maps)
      "min_out_amount", "amount_in", "amount_out",
      // nft_transfer_data
      "price", "payout_amount", "royalty_amount", "nft_item_index",
      // dex_deposit_liquidity_data / dex_withdraw_liquidity_data
      // (`amount` above also covers vault_excesses[].amount)
      "amount1", "amount2", "lp_tokens_minted", "lp_tokens_burnt",
      "target_amount_1", "target_amount_2", "nft_index",
      // staking_data
      "tokens_minted", "tokens_burnt",
      // evaa_liquidate_data (Go *string)
      "debt_amount",
      // Coffee amount, item-index, and points fields may exceed int64.
      "amount_1", "amount_2", "minted_item_index", "points",
      // jvault_claim_data uses []string, whose elements bypass scalar decoders.
      "claimed_amounts",
      // LayerZero fees are uint128 and the request ID is uint64.
      "send_request_id", "native_fee", "zro_fee",
      // The Tonco Q64.96 initial price is uint160.
      "initial_price_x96",
      // Multisig order sequence numbers are uint256.
      "order_seqno",
      // Cocoon token, stake, and nonce fields can exceed int64. The shared key
      // rule also stringifies bounded LayerZero nonce values losslessly.
      "nonce", "new_tokens", "worker_tokens", "new_tokens_used", "new_stake",
      "withdraw_amount",
  };
  return keys.count(k) != 0;
}

// Native integer fields must fit signed int64. Encode an unexpected wider value
// as nil rather than truncating it or invalidating the complete Go decode.
void pack_int(Packer &pk, const Value &v, ActionSerializeStats &st) {
  if (v.num.is_null() || !v.num->fits_bits(64)) {
    st.unrenderable++;
    pk.pack_nil();
    return;
  }
  pk.pack(static_cast<std::int64_t>(v.num->to_long()));
}

void pack_value(Packer &pk, const Value &v, bool as_decimal_string, ActionSerializeStats &st);

// Encode composite maps in insertion order and retain unfilled keys as nil.
void pack_fields(Packer &pk, const Value::Fields &fs, ActionSerializeStats &st) {
  pk.pack_map(static_cast<std::uint32_t>(fs.size()));
  for (const auto &[key, val] : fs) {
    pack_text(pk, key);
    pack_value(pk, val, decimal_string_key(key), st);
  }
}

void pack_value(Packer &pk, const Value &v, bool as_decimal_string, ActionSerializeStats &st) {
  switch (v.t) {
    case VType::Null:
      // omitted() gates every top-level call site; reachable from a composite
      // key (present-as-nil) and from a List, where position is significant and
      // the element cannot be dropped.
      pk.pack_nil();
      return;
    case VType::Bool:
      pk.pack(v.boolean);
      return;
    case VType::Int:
      if (v.num.is_null()) {
        st.unrenderable++;
        pk.pack_nil();
        return;
      }
      if (as_decimal_string) {
        pack_text(pk, v.num->to_dec_string());
      } else {
        pack_int(pk, v, st);
      }
      return;
    case VType::Float:
      // msgpack-c encodes integral doubles as integers and fractional values as
      // float64. Flexible string fields accept both representations.
      st.float_values++;
      pk.pack(v.dnum);
      return;
    case VType::Amount:
      // Value-domain by definition, so always a decimal string regardless of
      // the key. (ActionBuild's av_amount converts Amount -> Int before it ever
      // reaches a fill's output, so this arm is a backstop.)
      if (v.amount_float) {
        st.float_values++;
        pk.pack(v.dnum);
        return;
      }
      if (v.num.is_null()) {
        pk.pack_nil();
        return;
      }
      pack_text(pk, v.num->to_dec_string());
      return;
    case VType::Str:
      pack_text(pk, v.str);
      return;
    case VType::Account:
      if (v.addr_none) {
        pk.pack_nil();
        return;
      }
      pack_text(pk, v.str);  // canonical wc:HEXUPPER, == AccountId.as_str()
      return;
    case VType::Asset:
      if (v.is_ton || !v.has_jetton) {
        pk.pack_nil();
        return;
      }
      pack_text(pk, v.str);
      return;
    case VType::Bytes:
      // Python holds these as base64 str already (jettons.py custom_payload /
      // forward_payload); the C++ side carries the raw bytes.
      pack_text(pk, td::base64_encode(td::Slice(v.str)));
      return;
    case VType::Cell: {
      // Preserve unexpected cells as base64 BOCs and count the occurrence.
      st.cell_values++;
      if (v.cell.is_null()) {
        st.unrenderable++;
        pk.pack_nil();
        return;
      }
      auto boc = td_boc_serialize(v.cell);
      if (boc.is_error()) {
        st.unrenderable++;
        pk.pack_nil();
        return;
      }
      pack_text(pk, td::base64_encode(td::Slice(boc.ok())));
      return;
    }
    case VType::List:
      pk.pack_array(static_cast<std::uint32_t>(v.items->size()));
      for (const Value &item : *v.items) {
        pack_value(pk, item, as_decimal_string, st);  // elements inherit the key's decision
      }
      return;
    case VType::Dict:
    case VType::Obj:
      pack_fields(pk, *v.fields, st);
      return;
    case VType::Block:
      // Nulled before the rows leave the classifier actor (scrub_arena_refs);
      // a survivor holds a dead arena pointer, so it is never dereferenced here.
      st.unrenderable++;
      pk.pack_nil();
      return;
  }
  pk.pack_nil();
}

struct ActionField {
  const char *key;
  Value Action::*member;
};

// Keep this field set and order synchronized with scrub_arena_refs().
const std::vector<ActionField> &optional_fields() {
  static const std::vector<ActionField> fields{
      {"source", &Action::source},
      {"source_secondary", &Action::source_secondary},
      {"destination", &Action::destination},
      {"destination_secondary", &Action::destination_secondary},
      {"asset", &Action::asset},
      {"asset_secondary", &Action::asset_secondary},
      {"asset2", &Action::asset2},
      {"asset2_secondary", &Action::asset2_secondary},
      {"amount", &Action::amount},
      {"value", &Action::value},
      {"opcode", &Action::opcode},
      {"ton_transfer_data", &Action::ton_transfer_data},
      {"jetton_transfer_data", &Action::jetton_transfer_data},
      {"jetton_swap_data", &Action::jetton_swap_data},
      {"nft_transfer_data", &Action::nft_transfer_data},
      {"nft_listing_data", &Action::nft_listing_data},
      {"nft_mint_data", &Action::nft_mint_data},
      {"dex_deposit_liquidity_data", &Action::dex_deposit_liquidity_data},
      {"dex_withdraw_liquidity_data", &Action::dex_withdraw_liquidity_data},
      {"staking_data", &Action::staking_data},
      {"evaa_supply_data", &Action::evaa_supply_data},
      {"evaa_withdraw_data", &Action::evaa_withdraw_data},
      {"evaa_liquidate_data", &Action::evaa_liquidate_data},
      {"vesting_send_message_data", &Action::vesting_send_message_data},
      {"vesting_add_whitelist_data", &Action::vesting_add_whitelist_data},
      {"tonco_deploy_pool_data", &Action::tonco_deploy_pool_data},
      {"multisig_create_order_data", &Action::multisig_create_order_data},
      {"multisig_approve_data", &Action::multisig_approve_data},
      {"multisig_execute_data", &Action::multisig_execute_data},
      {"cocoon_worker_payout_data", &Action::cocoon_worker_payout_data},
      {"cocoon_proxy_payout_data", &Action::cocoon_proxy_payout_data},
      {"cocoon_proxy_charge_data", &Action::cocoon_proxy_charge_data},
      {"cocoon_client_top_up_data", &Action::cocoon_client_top_up_data},
      {"cocoon_register_proxy_data", &Action::cocoon_register_proxy_data},
      {"cocoon_unregister_proxy_data", &Action::cocoon_unregister_proxy_data},
      {"cocoon_client_register_data", &Action::cocoon_client_register_data},
      {"cocoon_client_change_secret_hash_data", &Action::cocoon_client_change_secret_hash_data},
      {"cocoon_client_request_refund_data", &Action::cocoon_client_request_refund_data},
      {"cocoon_grant_refund_data", &Action::cocoon_grant_refund_data},
      {"cocoon_client_increase_stake_data", &Action::cocoon_client_increase_stake_data},
      {"cocoon_client_withdraw_data", &Action::cocoon_client_withdraw_data},
      {"layerzero_packet_data", &Action::layerzero_packet_data},
      {"layerzero_send_data", &Action::layerzero_send_data},
      {"layerzero_dvn_verify_data", &Action::layerzero_dvn_verify_data},
      {"jvault_stake_data", &Action::jvault_stake_data},
      {"jvault_claim_data", &Action::jvault_claim_data},
      {"change_dns_record_data", &Action::change_dns_record_data},
      {"coffee_create_pool_data", &Action::coffee_create_pool_data},
      {"coffee_staking_deposit_data", &Action::coffee_staking_deposit_data},
      {"coffee_staking_withdraw_data", &Action::coffee_staking_withdraw_data},
      {"extra", &Action::extra},
  };
  return fields;
}

// Number of unconditional keys below. ancestor_type and optional fields are
// counted separately; fields unused by the Go consumer are not serialized.
constexpr std::uint32_t kFixedKeys = 12;

void pack_action(Packer &pk, const Action &a, const std::string &trace_key, std::int64_t finality,
                 ActionSerializeStats &st) {
  const std::vector<ActionField> &opt = optional_fields();
  std::uint32_t n = kFixedKeys + (a.ancestor_type.empty() ? 0 : 1);
  for (const ActionField &f : opt) {
    if (!omitted(a.*f.member)) {
      n++;
    }
  }
  pk.pack_map(n);

  pack_key(pk, "action_id");
  pack_text(pk, a.action_id);
  pack_key(pk, "type");
  pack_text(pk, a.type);
  pack_key(pk, "success");
  pk.pack(a.success);
  // []string on the Go side, where element decoding bypasses the flexible
  // decoders: nothing numeric may ever be written into one of these arrays.
  pack_key(pk, "tx_hashes");
  pk.pack_array(static_cast<std::uint32_t>(a.tx_hashes.size()));
  for (const std::string &h : a.tx_hashes) {
    pack_text(pk, h);
  }
  pack_key(pk, "accounts");
  pk.pack_array(static_cast<std::uint32_t>(a.accounts.size()));
  for (const std::string &acc : a.accounts) {
    pack_text(pk, acc);
  }
  pack_key(pk, "start_lt");
  pk.pack(a.start_lt);
  pack_key(pk, "end_lt");
  pk.pack(a.end_lt);
  pack_key(pk, "start_utime");
  pk.pack(a.start_utime);
  pack_key(pk, "end_utime");
  pk.pack(a.end_utime);
  // Both fields come from the view rather than mch::Action.
  pack_key(pk, "finality");
  pk.pack(finality);
  // Go only surfaces it when it differs from trace_external_hash
  // (models.go:930), so it must be written.
  pack_key(pk, "trace_external_hash_norm");
  pack_text(pk, trace_key);
  // parent_action_id is always present and nil for top-level rows.
  // ancestor_type is present only for child rows.
  pack_key(pk, "parent_action_id");
  if (a.parent_action_id.empty()) {
    pk.pack_nil();
  } else {
    pack_text(pk, a.parent_action_id);
  }
  if (!a.ancestor_type.empty()) {
    pack_key(pk, "ancestor_type");
    pk.pack_array(static_cast<std::uint32_t>(a.ancestor_type.size()));
    for (const std::string &t : a.ancestor_type) {
      pack_text(pk, t);
    }
  }

  for (const ActionField &f : opt) {
    const Value &v = a.*f.member;
    if (omitted(v)) {
      continue;
    }
    pack_key(pk, f.key);
    pack_value(pk, v, decimal_string_key(f.key), st);
  }
}

}  // namespace

std::string serialize_actions(const std::vector<Action> &actions, const EmuTraceView &view,
                              ActionSerializeStats *stats) {
  ActionSerializeStats discarded;
  ActionSerializeStats &st = stats != nullptr ? *stats : discarded;
  // The payload and insert guard share the minimum node finality.
  const auto finality = static_cast<std::int64_t>(view_finality(view));

  msgpack::sbuffer buf;
  Packer pk(&buf);
  pk.pack_array(static_cast<std::uint32_t>(actions.size()));
  for (const Action &a : actions) {
    pack_action(pk, a, view.trace_id, finality, st);
  }
  return std::string(buf.data(), buf.size());
}

}  // namespace mch
