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

// Getgems (HostGetgems.cpp): shaper + purchase/seller-payout preds + purchase fn.
void getgems_proxy_insert(Block *produced, const ShaperMatch &m);
bool getgems_purchase(const Block *block);
bool getgems_seller_payout(const Block *block);
EvalResult getgems_nft_purchase_data(BuildEnv &env, const std::vector<Value> &args);

// NFT (HostNft.cpp): base nft_transfer host fn + parent-absorb shaper + telegram.
EvalResult nft_transfer_data(BuildEnv &env, const std::vector<Value> &args);
void nft_transfer_parent_absorb(Block *produced, const ShaperMatch &m);
EvalResult telegram_nft_purchase_data(BuildEnv &env, const std::vector<Value> &args);
// specs/teleitem_auction.mch: blocks/auction.py `_is_teleitem` (a substring test
// the expression language deliberately cannot express).
EvalResult is_teleitem(BuildEnv &env, const std::vector<Value> &args);
// specs/nft_mint.mch anchor: NftMintBlockMatcher.test_self — exactly one
// distinct contract deployment across the block's event nodes.
bool single_contract_deploy(const Block *block);

// GetGems sale/auction family (HostNftSale.cpp), specs/nft_sale.mch.
bool sale_contract_deploy(const Block *block);
bool nft_trade_cancel_comment(const Block *block);
bool nft_trade_finish_comment(const Block *block);
bool auction_bid_candidate(const Block *block);
bool auction_outbid_leg(const Block *block);
EvalResult getgems_sale_init(BuildEnv &env, const std::vector<Value> &args);
EvalResult getgems_auction_init(BuildEnv &env, const std::vector<Value> &args);
EvalResult auction_bid_data(BuildEnv &env, const std::vector<Value> &args);
EvalResult nft_trade_is_finish(BuildEnv &env, const std::vector<Value> &args);
EvalResult nft_trade_returned(BuildEnv &env, const std::vector<Value> &args);
EvalResult auction_outbid_data(BuildEnv &env, const std::vector<Value> &args);

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
// v2 pton_self_transfer pred + v2 swap fn. (no_internal_transfer moved to an
// inline where expression.)
bool stonfi_v1_sender_payment(const Block *block);
EvalResult stonfi_v1_swap_data(BuildEnv &env, const std::vector<Value> &args);
bool pton_self_transfer(const Block *block);
EvalResult stonfi_v2_swap_data(BuildEnv &env, const std::vector<Value> &args);

// Tonco (HostTonco.cpp): deposit-liquidity, withdraw-payout, and swap fns.
EvalResult tonco_deposit_liquidity_data(BuildEnv &env, const std::vector<Value> &args);
EvalResult tonco_withdraw_payouts(BuildEnv &env, const std::vector<Value> &args);
EvalResult tonco_swap_data(BuildEnv &env, const std::vector<Value> &args);

// Nominator (HostNominator.cpp).
bool nominator_pool_withdraw_parent(const Block *b);
EvalResult nominator_withdraw_payout_amount(BuildEnv &env, const std::vector<Value> &args);
void nominator_withdraw_absorb_payouts(Block *produced, const ShaperMatch &m);

// EVAA (HostEvaa.cpp).
bool evaa_user_withdraw_user(const Block *b);
bool evaa_user_withdraw_success(const Block *b);
bool evaa_user_withdraw_fail(const Block *b);
bool evaa_service_comment(const Block *b);
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
EvalResult dedust_swap_failed(BuildEnv &env, const std::vector<Value> &args);
EvalResult dedust_v2_swap_payload(BuildEnv &env, const std::vector<Value> &args);
EvalResult dedust_v2_deposit_payload(BuildEnv &env, const std::vector<Value> &args);

// DeDust liquidity deposit (HostDedustDeposit.cpp).
EvalResult dedust_deposit_final_data(BuildEnv &env, const std::vector<Value> &args);
EvalResult dedust_deposit_partial_data(BuildEnv &env, const std::vector<Value> &args);

// LayerZero (HostLayerZero.cpp).
EvalResult layerzero_dst_oapp_matches(BuildEnv &env, const std::vector<Value> &args);

// Tonstakers (HostTonstakers.cpp).
EvalResult tonstakers_minted_nft(BuildEnv &env, const std::vector<Value> &args);
EvalResult tonstakers_pool_addr(BuildEnv &env, const std::vector<Value> &args);

// DNS auction tail (HostDns.cpp).
EvalResult dns_purchase_buyer(BuildEnv &env, const std::vector<Value> &args);

// Vesting (HostVesting.cpp).
EvalResult vesting_message_was_sent(BuildEnv &env, const std::vector<Value> &args);

// JVault (HostJvault.cpp). jvault_account_list is defined by a spec G-LC map.
EvalResult jvault_stake_period(BuildEnv &env, const std::vector<Value> &args);

// Cocoon (HostCocoon.cpp): the shared expectedMyAddress renderer and the nested
// ClientProxyRequest.payload -> ClientProxyRefundGranted coins read.
EvalResult cocoon_expected_address(BuildEnv &env, const std::vector<Value> &args);
EvalResult cocoon_withdraw_amount(BuildEnv &env, const std::vector<Value> &args);

}  // namespace mch
