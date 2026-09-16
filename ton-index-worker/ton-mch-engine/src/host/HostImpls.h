// Internal registry surface: the registered host predicates and fns, one TU per
// protocol (HostNominator.cpp, HostEvaa.cpp, ...). Declared here so
// HostRegistry.cpp's host_predicates()/host_fns() maps can reference them across
// TU boundaries. NOT part of the public HostRegistry.h surface.
#pragma once

#include "ExprRuntime.h"
#include "Value.h"

#include <vector>

namespace mch {

struct Block;     // BlockTree.h
struct BuildEnv;  // BuildRuntime.h
struct ShaperMatch;  // HostRegistry.h

// Getgems (HostNftSale.cpp): finish/stop sibling-payout shaper.
void getgems_proxy_insert(Block *produced, const ShaperMatch &m);

// NFT (HostNft.cpp): base nft_transfer host fn + parent-absorb shaper + telegram.
EvalResult nft_transfer_data(BuildEnv &env, const std::vector<Value> &args);
void nft_transfer_parent_absorb(Block *produced, const ShaperMatch &m);
EvalResult telegram_nft_purchase_data(BuildEnv &env, const std::vector<Value> &args);
// Substring test the expression language cannot express.
EvalResult is_teleitem(BuildEnv &env, const std::vector<Value> &args);
// Exactly one distinct contract deployment across the block's event nodes.
bool single_contract_deploy(const Block *block);

// GetGems sale/auction family (HostNftSale.cpp).
bool sale_contract_deploy(const Block *block);
bool nft_trade_cancel_comment(const Block *block);
bool nft_trade_finish_comment(const Block *block);
bool auction_bid_candidate(const Block *block);
bool auction_outbid_leg(const Block *bid);
EvalResult getgems_sale_init(BuildEnv &env, const std::vector<Value> &args);
EvalResult getgems_auction_init(BuildEnv &env, const std::vector<Value> &args);
EvalResult auction_bid_data(BuildEnv &env, const std::vector<Value> &args);

// tgBTC (HostTgbtc.cpp): mint host fn, the four shared event-log parse fns (one
// per event opcode, each shared by a full matcher and its `*_log_only` fallback
// twin), and the deploy-absorb shaper.
EvalResult tgbtc_mint_data(BuildEnv &env, const std::vector<Value> &args);
EvalResult tgbtc_mint_log(BuildEnv &env, const std::vector<Value> &args);
EvalResult tgbtc_burn_log(BuildEnv &env, const std::vector<Value> &args);
EvalResult tgbtc_new_key_log(BuildEnv &env, const std::vector<Value> &args);
EvalResult tgbtc_dkg_completed_log(BuildEnv &env, const std::vector<Value> &args);
void tgbtc_deploy_absorb(Block *produced, const ShaperMatch &m);

// Coffee (HostCoffee.cpp): swap host fn.
EvalResult coffee_swap_data(BuildEnv &env, const std::vector<Value> &args);

// Stonfi (HostStonfi.cpp): v1 sender-payment pred + v1 swap fn;
// v2 pton_self_transfer pred + v2 swap fn.
bool stonfi_v1_sender_payment(const Block *block);
EvalResult stonfi_v1_swap_data(BuildEnv &env, const std::vector<Value> &args);
bool pton_self_transfer(const Block *block);
EvalResult stonfi_v2_swap_data(BuildEnv &env, const std::vector<Value> &args);

// Tonco (HostTonco.cpp): deposit-liquidity, withdraw-payout, and swap fns.
EvalResult tonco_deposit_liquidity_data(BuildEnv &env, const std::vector<Value> &args);
EvalResult tonco_withdraw_payouts(BuildEnv &env, const std::vector<Value> &args);
EvalResult tonco_swap_data(BuildEnv &env, const std::vector<Value> &args);

// Nominator (HostNominator.cpp).
EvalResult nominator_withdraw_payout_amount(BuildEnv &env, const std::vector<Value> &args);
void nominator_withdraw_absorb_payouts(Block *produced, const ShaperMatch &m);

// EVAA (HostEvaa.cpp).
bool evaa_user_withdraw_user(const Block *b);
bool evaa_user_withdraw_success(const Block *b);
bool evaa_user_withdraw_fail(const Block *b);
bool evaa_supply_anchor(const Block *b);
bool evaa_user_supply(const Block *b);
bool evaa_liquidate_anchor(const Block *b);
bool evaa_user_liquidate(const Block *b);
bool evaa_liquidate_success_header(const Block *b);
bool evaa_bounced_call(const Block *b);
EvalResult evaa_supply_data(BuildEnv &env, const std::vector<Value> &args);
EvalResult evaa_liquidate_data(BuildEnv &env, const std::vector<Value> &args);

// DeDust (HostDedust.cpp).
EvalResult dedust_swap_legs(BuildEnv &env, const std::vector<Value> &args);
EvalResult dedust_v2_swap_min_out(BuildEnv &env, const std::vector<Value> &args);

// DeDust liquidity deposit (HostDedustDeposit.cpp).
EvalResult dedust_deposit_final_data(BuildEnv &env, const std::vector<Value> &args);
EvalResult dedust_deposit_partial_data(BuildEnv &env, const std::vector<Value> &args);

// LayerZero (HostLayerZero.cpp).
EvalResult layerzero_dst_oapp_matches(BuildEnv &env, const std::vector<Value> &args);

// Tonstakers (HostTonstakers.cpp).
EvalResult tonstakers_pool_addr(BuildEnv &env, const std::vector<Value> &args);

// Vesting (HostVesting.cpp).
EvalResult vesting_message_was_sent(BuildEnv &env, const std::vector<Value> &args);

// Cocoon (HostCocoon.cpp): the shared expectedMyAddress renderer.
EvalResult cocoon_expected_address(BuildEnv &env, const std::vector<Value> &args);

}  // namespace mch
