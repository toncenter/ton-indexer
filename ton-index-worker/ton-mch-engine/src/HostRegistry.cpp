// Host binding registries and dispatch. Per-protocol predicate
// and fn implementations live in src/host/*; this file owns the host_fns() /
// host_predicates() maps and the rt_call_hostfn dispatch.
#include "HostRegistry.h"

#include "BuildRuntime.h"
#include "host/HostImpls.h"

namespace mch {

const std::map<std::string, HostFnEntry> &host_fns() {
  static const std::map<std::string, HostFnEntry> fns = {
      {"dedust_swap_legs", {dedust_swap_legs, 3}},
      {"dedust_v2_swap_min_out", {dedust_v2_swap_min_out, 1}},
      {"layerzero_dst_oapp_matches", {layerzero_dst_oapp_matches, 2}},
      {"tonstakers_pool_addr", {tonstakers_pool_addr, 1}},
      {"vesting_message_was_sent", {vesting_message_was_sent, 2}},
      {"nominator_withdraw_payout_amount", {nominator_withdraw_payout_amount, 1}},
      {"nft_transfer_data", {nft_transfer_data, 3}},
      {"telegram_nft_purchase_data", {telegram_nft_purchase_data, 4}},
      {"is_teleitem", {is_teleitem, 1}},
      {"tgbtc_mint_data", {tgbtc_mint_data, 1}},
      {"tgbtc_mint_log", {tgbtc_mint_log, 1}},
      {"tgbtc_burn_log", {tgbtc_burn_log, 1}},
      {"tgbtc_new_key_log", {tgbtc_new_key_log, 1}},
      {"tgbtc_dkg_completed_log", {tgbtc_dkg_completed_log, 1}},
      {"coffee_swap_data", {coffee_swap_data, 1}},
      {"stonfi_v1_swap_data", {stonfi_v1_swap_data, 2}},
      {"dedust_deposit_final_data", {dedust_deposit_final_data, 1}},
      {"dedust_deposit_partial_data", {dedust_deposit_partial_data, 1}},
      {"tonco_deposit_liquidity_data", {tonco_deposit_liquidity_data, 1}},
      {"tonco_withdraw_payouts", {tonco_withdraw_payouts, 1}},
      {"evaa_supply_data", {evaa_supply_data, 2}},
      {"evaa_liquidate_data", {evaa_liquidate_data, 1}},
      {"tonco_swap_data", {tonco_swap_data, 1}},
      // stonfi_v2_swap_data reads the in-leg by block type instead of a
      // hash-seed-dependent first event node, so it is deterministic.
      {"stonfi_v2_swap_data", {stonfi_v2_swap_data, 1}},
      // GetGems StateInit readers and chained-lookup bid builder.
      {"getgems_sale_init", {getgems_sale_init, 2}},
      {"getgems_auction_init", {getgems_auction_init, 2}},
      {"auction_bid_data", {auction_bid_data, 1}},
      // expectedMyAddress string renderer shared by three matchers.
      {"cocoon_expected_address", {cocoon_expected_address, 1}},
  };
  return fns;
}

EvalResult rt_call_hostfn(BuildEnv &env, const std::string &name,
                          const std::vector<Value> &args) {
  auto it = host_fns().find(name);
  if (it == host_fns().end()) {
    return rt_fault("unknown host fn '" + name + "'");
  }
  // Name the fn (and its anchor) for the [mch-reject] line: the fault below and
  // any host_reject() inside the fn both read them off the context.
  RejectCtx ctx = reject_ctx();
  ctx.fn = &name;
  ctx.anchor = env.anchor;
  RejectScope scope(ctx);
  if (it->second.arity >= 0 &&
      args.size() != static_cast<std::size_t>(it->second.arity)) {
    EvalResult r = rt_fault(name + ": bad arguments");
    reject_log(r.message);
    return r;
  }
  EvalResult r = it->second.fn(env, args);
  if (r.faulted) {
    reject_log(r.message);
  }
  return r;
}

const std::map<std::string, HostPredFn> &host_predicates() {
  static const std::map<std::string, HostPredFn> preds = {
      {"evaa_user_withdraw_user", evaa_user_withdraw_user},
      {"evaa_user_withdraw_success", evaa_user_withdraw_success},
      {"evaa_user_withdraw_fail", evaa_user_withdraw_fail},
      {"evaa_supply_anchor", evaa_supply_anchor},
      {"evaa_user_supply", evaa_user_supply},
      {"evaa_liquidate_anchor", evaa_liquidate_anchor},
      {"evaa_user_liquidate", evaa_user_liquidate},
      {"evaa_liquidate_success_header", evaa_liquidate_success_header},
      {"evaa_bounced_call", evaa_bounced_call},
      // Predicate anchor has no opcode or btype prefilter.
      {"single_contract_deploy", single_contract_deploy},
      {"stonfi_v1_sender_payment", stonfi_v1_sender_payment},
      // pton_self_transfer gates stonfi_v2_swap; no_internal_transfer is an
      // inline where expression.
      {"pton_self_transfer", pton_self_transfer},
      {"sale_contract_deploy", sale_contract_deploy},
      {"nft_trade_cancel_comment", nft_trade_cancel_comment},
      {"nft_trade_finish_comment", nft_trade_finish_comment},
      {"auction_bid_candidate", auction_bid_candidate},
      {"auction_outbid_leg", auction_outbid_leg},
  };
  return preds;
}

const std::map<std::string, HostShaperFn> &host_shapers() {
  static const std::map<std::string, HostShaperFn> shapers = {
      {"getgems_proxy_insert", getgems_proxy_insert},
      {"nft_transfer_parent_absorb", nft_transfer_parent_absorb},
      {"nominator_withdraw_absorb_payouts", nominator_withdraw_absorb_payouts},
      {"tgbtc_deploy_absorb", tgbtc_deploy_absorb},
  };
  return shapers;
}

}  // namespace mch
