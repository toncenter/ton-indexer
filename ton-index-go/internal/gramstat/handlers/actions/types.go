package actions

import "strings"

// TotalMetric freezes the accepted taxonomy and nesting policy. A changed set
// requires a new version and historical rebuild, not a mixture of definitions.
const TotalMetric = "actions_total_v1"

// Keep in sync with Gramstat ui/action-types.mjs. Technical wrappers (including
// gasless_request), unknown calls, deployments and validator elections are not
// counted and do not suppress a recognized child. UI filtering never changes this
// set: a counted ancestor always suppresses its descendants, even when hidden.
var TotalTypes = strings.Fields(`
ton_transfer jetton_transfer jetton_swap
dex_deposit_liquidity dex_withdraw_liquidity dedust_v2_claim_fees dedust_v2_claim_reward
stake_deposit stake_withdrawal stake_withdrawal_request
jvault_stake jvault_unstake jvault_claim jvault_unstake_request
coffee_staking_claim_rewards coffee_staking_deposit coffee_staking_withdraw
nft_transfer
nft_purchase nft_put_on_sale nft_put_on_auction nft_cancel_sale nft_cancel_auction
nft_finish_auction nft_update_sale auction_bid auction_outbid teleitem_start_auction teleitem_cancel_auction
jetton_mint jetton_burn nft_mint
change_dns delete_dns renew_dns dns_purchase dns_release
evaa_supply evaa_withdraw evaa_liquidate
layerzero_send layerzero_send_tokens layerzero_receive
subscribe unsubscribe multisig_create_order multisig_approve multisig_execute
vesting_send_message vesting_add_whitelist change_wallet_key
cocoon_worker_payout cocoon_proxy_payout cocoon_proxy_charge cocoon_client_top_up
cocoon_register_proxy cocoon_unregister_proxy cocoon_client_register cocoon_client_change_secret_hash
cocoon_client_request_refund cocoon_grant_refund cocoon_client_increase_stake cocoon_client_withdraw
`)

var totalTypeSet = func() map[string]bool {
	m := make(map[string]bool, len(TotalTypes))
	for _, typ := range TotalTypes {
		m[typ] = true
	}
	return m
}()

// A one-off initial backfill needs only type counts, not the already prepared
// distinct users, Stars distributions or money. Ordinary refresh uses daily.sql.
const totalOnlySQL = `WITH counts AS (
 SELECT (to_timestamp(start_utime) AT TIME ZONE 'UTC')::date AS day,type,count(*) AS n
 FROM public.actions WHERE trace_mc_seqno_end >= $1 AND trace_mc_seqno_end <= $2
 AND start_utime >= $3 AND start_utime < $4 AND success=true AND type=ANY($5::varchar[])
 AND NOT(coalesce(ancestor_type,ARRAY[]::varchar[]) && $5::varchar[])
 GROUP BY 1,2)
 SELECT day,0,0,0,0,0,0,0,0,'{}'::jsonb,0,'0'::text,'{}'::jsonb,0,0,'[]'::jsonb,
 jsonb_object_agg(type,n) FROM counts GROUP BY day ORDER BY day`
