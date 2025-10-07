package index

import (
	"sort"
	"sync"

	"github.com/xssnick/tonutils-go/tlb"
)

var (
	interfacesCache []Interface
	interfacesOnce  sync.Once
)

type Interface struct {
	Name       string
	Methods    []uint32
	CodeHashes []string
}

func getInterfaces() []Interface {
	interfacesOnce.Do(func() {
		interfacesCache = []Interface{
			{
				Name: "airdrop_interlocker_v1",
				CodeHashes: []string{
					"rNK47E23QUOW+lNphlPswEpBbuNknY9bILQfF2rYM/A=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_contract_data")),
				},
			},
			{
				Name: "airdrop_interlocker_v2",
				CodeHashes: []string{
					"D/RKlvwkgRESNtjZkg/W4pthCCFePsW6dh6tlgKtraQ=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_distribution_info")),
					uint32(tlb.MethodNameHash("get_unlocks_info")),
					uint32(tlb.MethodNameHash("get_available_claim_amount")),
				},
			},
			{
				Name: "bidask_pool",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_pool_info")),
					uint32(tlb.MethodNameHash("get_current_bin")),
					uint32(tlb.MethodNameHash("get_sqrt_p")),
					uint32(tlb.MethodNameHash("get_active_range")),
				},
			},
			{
				Name: "bidask_range",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_pool_addr")),
					uint32(tlb.MethodNameHash("get_lp_multitoken_wallet")),
				},
			},
			{
				Name: "bidask_lp_multitoken",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_bins_number")),
					uint32(tlb.MethodNameHash("get_nft_data")),
				},
			},
			{
				Name: "bidask_internal_liquidity_vault",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_liquidity_data")),
				},
			},
			{
				Name: "cron",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_cron_info")),
				},
			},
			{
				Name: "daolama_vault",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_pool_data")),
				},
			},
			{
				Name: "dedust_factory",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_vault_address")),
					uint32(tlb.MethodNameHash("get_pool_address")),
					uint32(tlb.MethodNameHash("get_liquidity_deposit_address")),
				},
			},
			{
				Name: "dedust_liquidity_deposit ",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_balances")),
					uint32(tlb.MethodNameHash("get_target_balances")),
				},
			},
			{
				Name: "dedust_pool",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_reserves")),
					uint32(tlb.MethodNameHash("get_assets")),
					uint32(tlb.MethodNameHash("get_jetton_data")),
					uint32(tlb.MethodNameHash("is_stable")),
					uint32(tlb.MethodNameHash("get_trade_fee")),
					uint32(tlb.MethodNameHash("estimate_swap_out")),
				},
			},
			{
				Name: "dedust_vault",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_asset")),
				},
			},
			{
				Name: "dns",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("dnsresolve")),
				},
			},
			{
				Name: "gram_miner",
				CodeHashes: []string{
					"zK5v+2A8fT53mrWewmf/wi3B6+CvmDmQIomnqD5MAPE=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_pow_params")),
				},
			},
			{
				Name: "jetton_master",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_jetton_data")),
					uint32(tlb.MethodNameHash("get_wallet_address")),
				},
			},
			{
				Name: "jetton_wallet",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_wallet_data")),
				},
			},
			{
				Name: "jetton_wallet_v1",
				CodeHashes: []string{
					"vrBoPr64kn/p/I7AoYvH3ReJlomCWhIeq0bFo6hg0M4=",
				},
			},
			{
				Name: "jetton_wallet_v2",
				CodeHashes: []string{
					"jSjqQht36AX+pSrPM1KWSZ8Drsjp/SHdtfJWSqZcSN4=",
				},
			},
			{
				Name: "jetton_wallet_governed",
				CodeHashes: []string{
					"iUaPAseOVwgC45l5yFFvw43wfqdqSDV+BTbyuns+43s=",
				},
			},
			{
				Name: "tonstake_pool",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_pool_full_data")),
				},
			},
			{
				Name: "validator_controller",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_validator_controller_data")),
				},
			},
			{
				Name: "locker",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_locker_data")),
					uint32(tlb.MethodNameHash("get_bill_address")),
				},
			},
			{
				Name: "locker_bill",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_locker_bill_data")),
				},
			},
			{
				Name: "lockup_vesting",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_lockup_data")),
				},
			},
			{
				Name: "lockup_universal",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_balances")),
				},
			},
			{
				Name: "megatonfi_router",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_mining_data")),
					uint32(tlb.MethodNameHash("get_lp_data")),
				},
			},
			{
				Name: "megatonfi_exchange",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_lp_swap_data")),
					uint32(tlb.MethodNameHash("get_lp_mining_data")),
				},
			},
			{
				Name: "moon_pool",
				CodeHashes: []string{
					"KAgWUlMoah5P5j76ubo1SBE3qsCxoPntzLj2RC3eRsQ=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_jetton_data")),
					uint32(tlb.MethodNameHash("get_reserves")),
					uint32(tlb.MethodNameHash("get_assets")),
					uint32(tlb.MethodNameHash("get_wallet_address")),
				},
			},
			{
				Name: "moon_booster",
				CodeHashes: []string{
					"zoTDgsi2rA0FISu6o00F5U4eMOLMkony2cnWRyahEqg=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_status")),
					uint32(tlb.MethodNameHash("get_pool")),
				},
			},
			{
				Name: "moon_order_factory",
				CodeHashes: []string{
					"0Ec31vZ4EDqeRIwiEJkZ0PnL4F7xsag8XnuIZoOlRwQ=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_id")),
					uint32(tlb.MethodNameHash("get_pipe")),
				},
			},
			{
				Name: "moon_order",
				CodeHashes: []string{
					"iMONKVhrXXSLrbx7Tb/iOcJcuFypxFhqROBVB6bt9qY=",
					"7gcUCzzP4mHtKFgPYKFr2kxLnG19p64F9bfH4rHf4iY=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_status")),
					uint32(tlb.MethodNameHash("get_order_amount")),
					uint32(tlb.MethodNameHash("get_fill_out")),
					uint32(tlb.MethodNameHash("get_vesting_data")),
				},
			},
			{
				Name: "multisig_v2",
				CodeHashes: []string{
					"09FNqaYn8Ow1MzQYKXYq+SuVQLIb8DZl+sCcK0bqu6w=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_multisig_data")),
					uint32(tlb.MethodNameHash("get_order_address")),
				},
			},
			{
				Name: "multisig_order_v2",
				CodeHashes: []string{
					"oB4Ff71CiEArmJjXjWe9TpAlTJPFhmh5vC0dEoZUNrw=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_order_data")),
				},
			},
			{
				Name: "nft_sale",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_sale_data")),
				},
			},
			{
				Name: "nft_auction_v1",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_sale_data")),
				},
			},
			{
				Name: "nft_offer_getgems_v1",
				CodeHashes: []string{
					"bl1mf6bvqBh8bQKe/UAVYBIy/S42wpHbw0be2rbcgCQ=",
				},
			},
			{
				Name: "nft_sale_getgems_v4",
				CodeHashes: []string{
					"a5WmQYucnSNZBF0edVm41UmuDlBvJMqrWPowyPsf64Y=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_fix_price_data_v4")),
				},
			},
			{
				Name: "nft_sale_getgems_v3",
				CodeHashes: []string{
					"JCIfpXHlQuBVx3vt/b9SfHr0YM/cfzRMRQeHtM+h600=",
					"3rU7bFdlwebNI4v0e8XoO6WWvcwEsLhM1Qqx5HSgjzE=",
					"MgUN+sRPZIZrzIbyzZ4TBf6dyts5WcACI3z7CQLUQyM=",
				},
			},
			{
				Name: "nft_sale_getgems_v2",
				CodeHashes: []string{
					"gnj0xSM95vvtyWmvUZNEp6m//FRIVtuphqlcC8+Fcck=",
				},
			},
			{
				Name: "nft_auction_getgems_v3",
				CodeHashes: []string{
					"G9nFo5v/t6DzQViLXdkrgTqEK/Ze8UEJOCIAzq+Pct8=",
					"ZmiHL6eXBUQ//UdSPo6eqfdquZ+aC1nSfej4GhwnudQ=",
				},
			},
			{
				Name: "nft_auction_getgems_v4",
				CodeHashes: []string{
					"zlp4U06qps7tja/UhtB262CpsNbb+1Nnb2YmScBomVY=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_auction_data_v4")),
				},
			},
			{
				Name: "nft_collection",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_nft_content")),
					uint32(tlb.MethodNameHash("get_collection_data")),
					uint32(tlb.MethodNameHash("get_nft_address_by_index")),
				},
			},
			{
				Name: "nft_item",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_nft_data")),
				},
			},
			{
				Name: "nft_item_simple",
				CodeHashes: []string{
					"TJEjgoaC+m9DeXq0FzK8qJDK4Bdm4GdBACUFFuC/jUI=",
				},
			},
			{
				Name: "editable",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_editor")),
				},
			},
			{
				Name: "sbt",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_authority_address")),
				},
			},
			{
				Name: "payment_channel",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_channel_data")),
				},
			},
			{
				Name: "pyth_price_oracle",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_update_fee")),
					uint32(tlb.MethodNameHash("get_single_update_fee")),
					uint32(tlb.MethodNameHash("get_governance_data_source_index")),
					uint32(tlb.MethodNameHash("get_governance_data_source")),
					uint32(tlb.MethodNameHash("get_last_executed_governance_sequence")),
					uint32(tlb.MethodNameHash("get_is_valid_data_source")),
					uint32(tlb.MethodNameHash("get_price_unsafe")),
					uint32(tlb.MethodNameHash("get_price_no_older_than")),
					uint32(tlb.MethodNameHash("get_ema_price_unsafe")),
					uint32(tlb.MethodNameHash("get_ema_price_no_older_than")),
					uint32(tlb.MethodNameHash("get_chain_id")),
					uint32(tlb.MethodNameHash("get_current_guardian_set_index")),
					uint32(tlb.MethodNameHash("get_guardian_set")),
					uint32(tlb.MethodNameHash("get_governance_chain_id")),
					uint32(tlb.MethodNameHash("get_governance_contract")),
					uint32(tlb.MethodNameHash("governance_action_is_consumed")),
				},
			},
			{
				Name: "stonfi_pool",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_pool_data")),
					uint32(tlb.MethodNameHash("get_expected_outputs")),
					uint32(tlb.MethodNameHash("get_expected_tokens")),
					uint32(tlb.MethodNameHash("get_expected_liquidity")),
					uint32(tlb.MethodNameHash("get_lp_account_address")),
				},
			},
			{
				Name: "stonfi_pool_v2",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_pool_data")),
					uint32(tlb.MethodNameHash("get_pool_type")),
					uint32(tlb.MethodNameHash("get_lp_account_address")),
					uint32(tlb.MethodNameHash("get_jetton_data")),
					uint32(tlb.MethodNameHash("get_wallet_address")),
				},
			},
			{
				Name: "stonfi_router_v2",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_vault_address")),
					uint32(tlb.MethodNameHash("get_pool_address")),
					uint32(tlb.MethodNameHash("get_router_data")),
					uint32(tlb.MethodNameHash("get_router_version")),
				},
			},
			{
				Name: "stonfi_router",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_router_data")),
					uint32(tlb.MethodNameHash("get_pool_address")),
				},
			},
			{
				Name: "stonfi_lp_account_v2",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_lp_account_data")),
				},
			},
			{
				Name: "stonfi_vault_v2",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_vault_data")),
				},
			},
			{
				Name: "omniston_referral",
				CodeHashes: []string{
					"7cxi8XUil/vaJAjAnKQgs6+VKBAuM7ngoOvjhVPBj+w=",
					"bNo1HDwO42iHOl6hD1O5pHpVqs3I3rP9EDwvloyVJSc=",
				},
			},
			{
				Name: "storage_provider",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_wallet_params")),
					uint32(tlb.MethodNameHash("get_storage_params")),
					uint32(tlb.MethodNameHash("seqno")),
					uint32(tlb.MethodNameHash("get_public_key")),
					uint32(tlb.MethodNameHash("get_storage_contract_address")),
				},
			},
			{
				Name: "storage_contract",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_storage_contract_data")),
					uint32(tlb.MethodNameHash("get_torrent_hash")),
					uint32(tlb.MethodNameHash("is_active")),
					uint32(tlb.MethodNameHash("get_next_proof_info")),
				},
			},
			{
				Name: "storm_vamm",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_amm_name")),
					uint32(tlb.MethodNameHash("get_amm_status")),
					uint32(tlb.MethodNameHash("get_amm_contract_data")),
					uint32(tlb.MethodNameHash("get_exchange_settings")),
					uint32(tlb.MethodNameHash("get_spot_price")),
					uint32(tlb.MethodNameHash("get_terminal_amm_price")),
					uint32(tlb.MethodNameHash("get_vamm_type")),
				},
			},
			{
				Name: "storm_referral",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_nft_data")),
					uint32(tlb.MethodNameHash("get_referral_data")),
				},
			},
			{
				Name: "storm_referral_collection",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_referral_vaults_whitelist")),
				},
			},
			{
				Name: "storm_executor",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_nft_data")),
					uint32(tlb.MethodNameHash("get_executor_balances")),
				},
			},
			{
				Name: "storm_executor_collection",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_amm_name")),
				},
			},
			{
				Name: "storm_vault",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_executor_collection_address")),
					uint32(tlb.MethodNameHash("get_referral_collection_address")),
					uint32(tlb.MethodNameHash("get_vault_contract_data")),
					uint32(tlb.MethodNameHash("get_lp_minter_address")),
					uint32(tlb.MethodNameHash("get_vault_whitelisted_addresses")),
					uint32(tlb.MethodNameHash("get_vault_data")),
					uint32(tlb.MethodNameHash("get_vault_type")),
				},
			},
			{
				Name: "storm_position_manager",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_position_manager_contract_data")),
				},
			},
			{
				Name: "subscription_v1",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_subscription_data")),
				},
			},
			{
				Name: "subscription_v2",
				CodeHashes: []string{
					"XUG7cGLs/yqv7/IFppEtfFyyIwjbHRLCCH3IzAORwy4=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_subscription_info")),
					uint32(tlb.MethodNameHash("get_payment_info")),
					uint32(tlb.MethodNameHash("get_cron_info")),
				},
			},
			{
				Name: "coffee_mev_protector",
				CodeHashes: []string{
					"WPvC26JoTTi4C/zj3Fqf/OAK+bRkx7TcIc9X46rhHxw=",
				},
			},
			{
				Name: "coffee_cross_dex",
				CodeHashes: []string{
					"/qnGoe9eqyxcqU1r7x4zmUxXhqvrUmR2DhUNozv7Nnc=",
				},
			},
			{
				Name: "coffee_staking_master",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_stored_data")),
					uint32(tlb.MethodNameHash("get_collection_data")),
					uint32(tlb.MethodNameHash("get_nft_address_by_index")),
					uint32(tlb.MethodNameHash("get_nft_content")),
				},
			},
			{
				Name: "coffee_staking_vault",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_stored_data")),
					uint32(tlb.MethodNameHash("get_master_address")),
				},
			},
			{
				Name: "coffee_staking_item",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_stored_data")),
					uint32(tlb.MethodNameHash("get_nft_data")),
				},
			},
			{
				Name: "coffee_factory",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_vault_address")),
					uint32(tlb.MethodNameHash("get_pool_address")),
					uint32(tlb.MethodNameHash("get_pool_address_no_settings")),
					uint32(tlb.MethodNameHash("get_pool_creator_address")),
					uint32(tlb.MethodNameHash("get_pool_creator_address_no_settings")),
					uint32(tlb.MethodNameHash("get_liquidity_depository_address")),
					uint32(tlb.MethodNameHash("get_liquidity_depository_address_no_settings")),
					uint32(tlb.MethodNameHash("get_admin_address")),
					uint32(tlb.MethodNameHash("get_code")),
				},
			},
			{
				Name: "coffee_vault",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_asset")),
					uint32(tlb.MethodNameHash("is_active")),
				},
			},
			{
				Name: "coffee_vault_native",
			},
			{
				Name: "coffee_vault_jetton",
			},
			{
				Name: "coffee_vault_extra",
			},
			{
				Name: "coffee_pool",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_jetton_data")),
					uint32(tlb.MethodNameHash("get_wallet_address")),
					uint32(tlb.MethodNameHash("get_pool_data")),
					uint32(tlb.MethodNameHash("estimate_swap_amount")),
					uint32(tlb.MethodNameHash("estimate_liquidity_withdraw_amount")),
					uint32(tlb.MethodNameHash("estimate_liquidity_deposit_amount")),
				},
			},
			{
				Name: "jetton_wallet_coffee_lp",
				CodeHashes: []string{
					"18OPqZSoKCZXIJ+T68Xf6Qil8G0uQfDAhekXvE9UorM=",
				},
			},
			{
				Name: "teleitem",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_telemint_auction_state")),
					uint32(tlb.MethodNameHash("get_telemint_auction_config")),
					uint32(tlb.MethodNameHash("get_telemint_token_name")),
				},
			},
			{
				Name: "tonco_pool",
				CodeHashes: []string{
					"vJR9FjRsL9yEW3pEY4L1nIassHXiox2/qVbNDJ5UZ44=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_collection_data")),
					uint32(tlb.MethodNameHash("getIsActive")),
					uint32(tlb.MethodNameHash("getPoolStateAndConfiguration")),
					uint32(tlb.MethodNameHash("getChildContracts")),
					uint32(tlb.MethodNameHash("getAllTickInfos")),
				},
			},
			{
				Name: "tonco_router",
				CodeHashes: []string{
					"m5iR6qfbe+zGzN2hvZqNJdw98oF9V+SyfsAD2vgaRDk=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("getRouterState")),
					uint32(tlb.MethodNameHash("getPoolAddress")),
					uint32(tlb.MethodNameHash("getChildContracts")),
				},
			},
			{
				Name: "tonkeeper_2fa",
				CodeHashes: []string{
					"xe8Z3yKu6LcHvXoYEXTkAKQiUiPFrkDYMg9d3XB9NKE=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_wallet_addr")),
					uint32(tlb.MethodNameHash("get_root_pubkey")),
					uint32(tlb.MethodNameHash("get_seed_pubkey")),
					uint32(tlb.MethodNameHash("get_delegation_state")),
					uint32(tlb.MethodNameHash("get_estimated_attached_value")),
				},
			},
			{
				Name: "tv_pool",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_pool_data")),
					uint32(tlb.MethodNameHash("get_nominator_data")),
					uint32(tlb.MethodNameHash("list_nominators")),
					uint32(tlb.MethodNameHash("list_votes")),
				},
			},
			{
				Name: "wallet_v1r1",
				CodeHashes: []string{
					"oM/CxIruFqJx8s/AtzgtgXVs7LEBfQd/qqs7tgL2how=",
				},
			},
			{
				Name: "wallet_v1r2",
				CodeHashes: []string{
					"1JAvzJ+tdGmPqONTIgpo2g3PcuMryy657gQhfBfTBiw=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("seqno")),
				},
			},
			{
				Name: "wallet_v1r3",
				CodeHashes: []string{
					"WHzHie/xyE9G7DeX5F/ICaFP9a4k8eDHpqmcydyQYf8=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("seqno")),
					uint32(tlb.MethodNameHash("get_public_key")),
				},
			},
			{
				Name: "wallet_v2r1",
				CodeHashes: []string{
					"XJpeaMEI4YchoHxC+ZVr+zmtd+xtYktgxXbsiO7mUyk=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("seqno")),
				},
			},
			{
				Name: "wallet_v2r2",
				CodeHashes: []string{
					"/pUw0yQ4Uwg+8u8LTCkIwKv2+hwx6iQ6rKpb+MfXU/E=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("seqno")),
					uint32(tlb.MethodNameHash("get_public_key")),
				},
			},
			{
				Name: "wallet_v3r1",
				CodeHashes: []string{
					"thBBpYp5gLlG6PueGY48kE0keZ/6NldOpCUcQaVm9YE=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("seqno")),
				},
			},
			{
				Name: "wallet_v3r2",
				CodeHashes: []string{
					"hNr6RJ+Ypph3ibojI1gHK8D3bcRSQAKl0JGLmnXS1Zk=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_public_key")),
					uint32(tlb.MethodNameHash("seqno")),
				},
			},
			{
				Name: "wallet_v4r1",
				CodeHashes: []string{
					"ZN1UgFUixb6KnbWc6gEFzPDQh4bKeb64y3nogKjXMi0=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_plugin_list")),
					uint32(tlb.MethodNameHash("is_plugin_installed")),
					uint32(tlb.MethodNameHash("get_public_key")),
					uint32(tlb.MethodNameHash("seqno")),
					uint32(tlb.MethodNameHash("get_subwallet_id")),
				},
			},
			{
				Name: "wallet_v4r2",
				CodeHashes: []string{
					"/rX/aCDi/w2Ug+fg1iyBfYRniftK5YDIeIZtlZ2r1cA=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_plugin_list")),
					uint32(tlb.MethodNameHash("is_plugin_installed")),
					uint32(tlb.MethodNameHash("get_public_key")),
					uint32(tlb.MethodNameHash("seqno")),
					uint32(tlb.MethodNameHash("get_subwallet_id")),
				},
			},
			{
				Name: "wallet_v5_beta",
				CodeHashes: []string{
					"89fKU0k97trCizgZhqhJQDy6w9LFhHea8IEGWvCsS5M=",
					"5M87L0xtamHqDytUR9JmeFsmrzY32y3u5rzRqoJvNBI=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("seqno")),
				},
			},
			{
				Name: "wallet_v5r1",
				CodeHashes: []string{
					"IINLe3KxEhR+Gy+0V7hOdNGjDwT3N9T2KmaOlVLSty8=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("seqno")),
					uint32(tlb.MethodNameHash("get_public_key")),
					uint32(tlb.MethodNameHash("get_subwallet_id")),
					uint32(tlb.MethodNameHash("get_extensions")),
					uint32(tlb.MethodNameHash("is_signature_allowed")),
				},
			},
			{
				Name: "wallet_highload_v1r1",
				CodeHashes: []string{
					"2M27t58sXKpnesRQdwvgNRviHhJQSG3oXMUqoz3RZIQ=",
				},
			},
			{
				Name: "wallet_highload_v1r2",
				CodeHashes: []string{
					"Dc7tISadZgE+lbGfu1xVpvAa2tQIN7qo5SHN46AqpGw=",
				},
			},
			{
				Name: "wallet_highload_v2",
				CodeHashes: []string{
					"lJTRzI7fEvBWcaGpugmSEJbrUIEeGSTsZcPGKfu4CBI=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_public_key")),
				},
			},
			{
				Name: "wallet_highload_v2r1",
				CodeHashes: []string{
					"jOtFs81LXMYOquHBO5wJI5Jnf+U2sumy2AG2Lv+TH+E=",
				},
			},
			{
				Name: "wallet_highload_v2r2",
				CodeHashes: []string{
					"ID3U81ittJmTEpqpJcrDmRa2ig5PeNJujywraer6Vnk=",
				},
			},
			{
				Name: "wallet_highload_v3r1",
				CodeHashes: []string{
					"EayteVWEQJDyg78ji8FEmHH3g+fMCXlAjT9IWUg+hSU=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_public_key")),
					uint32(tlb.MethodNameHash("get_subwallet_id")),
					uint32(tlb.MethodNameHash("get_timeout")),
				},
			},
			{
				Name: "wallet_preprocessed_v2",
				CodeHashes: []string{
					"Reu86bXSNYhstr/hw62TtwjeBYJEiSNlye4N/kOct7U=",
				},
			},
			{
				Name: "wallet_vesting",
				CodeHashes: []string{
					"tItTGr7DtxRjgpH3137W3J9qJynvyiBHcTc3TUrotZA=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("seqno")),
					uint32(tlb.MethodNameHash("get_public_key")),
					uint32(tlb.MethodNameHash("get_vesting_data")),
				},
			},
			{
				Name: "whales_pool",
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_staking_status")),
					uint32(tlb.MethodNameHash("get_pool_status")),
					uint32(tlb.MethodNameHash("get_member")),
					uint32(tlb.MethodNameHash("get_members_raw")),
					uint32(tlb.MethodNameHash("get_params")),
				},
			},
			{
				Name: "x1000_wallet_v1",
				CodeHashes: []string{
					"Q9RHFtMmqDLO1WprYKAstnYw4E9Xhf6J+HhKEQzzYVE=",
				},
			},
			{
				Name: "x1000_affiliate_account",
				CodeHashes: []string{
					"RFb60SpDTEiYsFrGW6td6A2zPydcYCB0bejhEaXNpOY=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_affiliate_account_data")),
				},
			},
			{
				Name: "x1000_affiliate_program",
				CodeHashes: []string{
					"yLuWL5k+Jp+RQ1u+dsG7rdb6Tq0+msgcoJdeGCDL7ws=",
				},
				Methods: []uint32{
					uint32(tlb.MethodNameHash("get_affiliate_account_address")),
				},
			},
		}
	})
	return interfacesCache
}

func DetectInterface(codeHash string, methodIDs []uint32) []string {
	var matchingInterfaces []string
	interfaces := getInterfaces()

	// first check code hash matches
	if codeHash != "" {
		for _, iface := range interfaces {
			for _, hash := range iface.CodeHashes {
				if codeHash == hash {
					return []string{iface.Name} // exact match, return immediately
				}
			}
		}
	}

	// if no code hash match, check methods
	if len(methodIDs) == 0 {
		return []string{}
	}

	methodSet := make(map[uint32]bool)
	for _, id := range methodIDs {
		methodSet[id] = true
	}

	for _, iface := range interfaces {
		// skip interfaces with empty method lists
		if len(iface.Methods) == 0 {
			continue
		}

		allMethodsPresent := true
		for _, methodID := range iface.Methods {
			if !methodSet[methodID] {
				allMethodsPresent = false
				break
			}
		}
		if allMethodsPresent {
			matchingInterfaces = append(matchingInterfaces, iface.Name)
		}
	}

	sort.Strings(matchingInterfaces)
	return matchingInterfaces
}

func MarkAccountStates(states []AccountStateFull) error {
	for i := range states {
		var methods []uint32
		if states[i].ContractMethods != nil {
			methods = *states[i].ContractMethods
		}
		codeHash := ""
		if states[i].CodeHash != nil {
			codeHash = string(*states[i].CodeHash)
		}
		interfaces := DetectInterface(codeHash, methods)
		states[i].Interfaces = &interfaces
	}
	return nil
}
