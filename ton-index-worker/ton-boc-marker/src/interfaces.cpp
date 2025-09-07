#include "interfaces.h"

const std::vector<Interface> g_interfaces = {
    {"airdrop_interlocker_v1", {
        Interface::calc_method_id("get_contract_data")
    }},

    {"airdrop_interlocker_v2", {
        Interface::calc_method_id("get_distribution_info"),
        Interface::calc_method_id("get_unlocks_info"),
        Interface::calc_method_id("get_available_claim_amount")
    }},

    {"bidask_pool", {
        Interface::calc_method_id("get_pool_info"),
        Interface::calc_method_id("get_current_bin"),
        Interface::calc_method_id("get_sqrt_p"),
        Interface::calc_method_id("get_active_range")
    }},

    {"bidask_range", {
        Interface::calc_method_id("get_pool_addr"),
        Interface::calc_method_id("get_lp_multitoken_wallet")
    }},

    {"bidask_lp_multitoken", {
        Interface::calc_method_id("get_bins_number"),
        Interface::calc_method_id("get_nft_data")
    }},

    {"bidask_internal_liquidity_vault", {
        Interface::calc_method_id("get_liquidity_data")
    }},

    {"cron", {
        Interface::calc_method_id("get_cron_info")
    }},

    {"daolama_vault", {
        Interface::calc_method_id("get_pool_data")
    }},

    {"dedust_factory", {
        Interface::calc_method_id("get_vault_address"),
        Interface::calc_method_id("get_pool_address"),
        Interface::calc_method_id("get_liquidity_deposit_address")
    }},

    {"dedust_liquidity_deposit ", {
        Interface::calc_method_id("get_balances"),
        Interface::calc_method_id("get_target_balances")
    }},

    {"dedust_pool", {
        Interface::calc_method_id("get_reserves"),
        Interface::calc_method_id("get_assets"),
        Interface::calc_method_id("get_jetton_data"),
        Interface::calc_method_id("is_stable"),
        Interface::calc_method_id("get_trade_fee"),
        Interface::calc_method_id("estimate_swap_out")
    }},

    {"dedust_vault", {
        Interface::calc_method_id("get_asset")
    }},

    {"dns", {
        Interface::calc_method_id("dnsresolve")
    }},

    {"gram_miner", {
        Interface::calc_method_id("get_pow_params")
    }},

    {"jetton_master", {
        Interface::calc_method_id("get_jetton_data"),
        Interface::calc_method_id("get_wallet_address")
    }},

    {"jetton_wallet", {
        Interface::calc_method_id("get_wallet_data")
    }},

    {"tonstake_pool", {
        Interface::calc_method_id("get_pool_full_data")
    }},

    {"validator_controller", {
        Interface::calc_method_id("get_validator_controller_data")
    }},

    {"locker", {
        Interface::calc_method_id("get_locker_data"),
        Interface::calc_method_id("get_bill_address")
    }},

    {"locker_bill", {
        Interface::calc_method_id("get_locker_bill_data")
    }},

    {"lockup_vesting", {
        Interface::calc_method_id("get_lockup_data")
    }},

    {"lockup_universal", {
        Interface::calc_method_id("get_balances")
    }},

    {"megatonfi_router", {
        Interface::calc_method_id("get_mining_data"),
        Interface::calc_method_id("get_lp_data")
    }},

    {"megatonfi_exchange", {
        Interface::calc_method_id("get_lp_swap_data"),
        Interface::calc_method_id("get_lp_mining_data")
    }},

    {"moon_pool", {
        Interface::calc_method_id("get_jetton_data"),
        Interface::calc_method_id("get_reserves"),
        Interface::calc_method_id("get_assets"),
        Interface::calc_method_id("get_wallet_address")
    }},

    {"moon_booster", {
        Interface::calc_method_id("get_status"),
        Interface::calc_method_id("get_pool")
    }},

    {"moon_order_factory", {
        Interface::calc_method_id("get_id"),
        Interface::calc_method_id("get_pipe")
    }},

    {"moon_order", {
        Interface::calc_method_id("get_status"),
        Interface::calc_method_id("get_order_amount"),
        Interface::calc_method_id("get_fill_out"),
        Interface::calc_method_id("get_vesting_data")
    }},

    {"multisig_v2", {
        Interface::calc_method_id("get_multisig_data"),
        Interface::calc_method_id("get_order_address")
    }},

    {"multisig_order_v2", {
        Interface::calc_method_id("get_order_data")
    }},

    {"nft_sale_v1", {
        Interface::calc_method_id("get_sale_data")
    }},

    {"nft_sale_v2", {
        Interface::calc_method_id("get_sale_data")
    }},

    {"nft_auction_v1", {
        Interface::calc_method_id("get_sale_data")
    }},

    {"nft_sale_getgems_v4", {
        Interface::calc_method_id("get_fix_price_data_v4")
    }},

    {"nft_auction_getgems_v4", {
        Interface::calc_method_id("get_auction_data_v4")
    }},

    {"nft_collection", {
        Interface::calc_method_id("get_nft_content"),
        Interface::calc_method_id("get_collection_data"),
        Interface::calc_method_id("get_nft_address_by_index")
    }},

    {"nft_item", {
        Interface::calc_method_id("get_nft_data")
    }},

    {"editable", {
        Interface::calc_method_id("get_editor")
    }},

    {"sbt", {
        Interface::calc_method_id("get_authority_address")
    }},

    {"payment_channel", {
        Interface::calc_method_id("get_channel_data")
    }},

    {"stonfi_pool", {
        Interface::calc_method_id("get_pool_data")
    }},

    {"stonfi_pool_v2", {
        Interface::calc_method_id("get_pool_data"),
        Interface::calc_method_id("get_lp_account_address")
    }},

    {"stonfi_router", {
        Interface::calc_method_id("get_router_data")
    }},

    {"stonfi_router_v2", {
        Interface::calc_method_id("get_vault_address"),
        Interface::calc_method_id("get_pool_address"),
        Interface::calc_method_id("get_router_data"),
        Interface::calc_method_id("get_router_version")
    }},

    {"stonfi_lp_account_v2", {
        Interface::calc_method_id("get_lp_account_data")
    }},

    {"stonfi_vault_v2", {
        Interface::calc_method_id("get_vault_data")
    }},

    {"storage_provider", {
        Interface::calc_method_id("get_wallet_params"),
        Interface::calc_method_id("get_storage_params"),
        Interface::calc_method_id("seqno"),
        Interface::calc_method_id("get_public_key"),
        Interface::calc_method_id("get_storage_contract_address")
    }},

    {"storage_contract", {
        Interface::calc_method_id("get_storage_contract_data"),
        Interface::calc_method_id("get_torrent_hash"),
        Interface::calc_method_id("is_active"),
        Interface::calc_method_id("get_next_proof_info")
    }},

    {"storm_vamm", {
        Interface::calc_method_id("get_amm_name"),
        Interface::calc_method_id("get_amm_status"),
        Interface::calc_method_id("get_amm_contract_data"),
        Interface::calc_method_id("get_exchange_settings"),
        Interface::calc_method_id("get_spot_price"),
        Interface::calc_method_id("get_terminal_amm_price"),
        Interface::calc_method_id("get_vamm_type")
    }},

    {"storm_referral", {
        Interface::calc_method_id("get_nft_data"),
        Interface::calc_method_id("get_referral_data")
    }},

    {"storm_referral_collection", {
        Interface::calc_method_id("get_referral_vaults_whitelist")
    }},

    {"storm_executor", {
        Interface::calc_method_id("get_nft_data"),
        Interface::calc_method_id("get_executor_balances")
    }},

    {"storm_executor_collection", {
        Interface::calc_method_id("get_amm_name")
    }},

    {"storm_vault", {
        Interface::calc_method_id("get_executor_collection_address"),
        Interface::calc_method_id("get_referral_collection_address"),
        Interface::calc_method_id("get_vault_contract_data"),
        Interface::calc_method_id("get_lp_minter_address"),
        Interface::calc_method_id("get_vault_whitelisted_addresses"),
        Interface::calc_method_id("get_vault_data"),
        Interface::calc_method_id("get_vault_type")
    }},

    {"storm_position_manager", {
        Interface::calc_method_id("get_position_manager_contract_data")
    }},

    {"subscription_v1", {
        Interface::calc_method_id("get_subscription_data")
    }},

    {"subscription_v2", {
        Interface::calc_method_id("get_subscription_info"),
        Interface::calc_method_id("get_payment_info"),
        Interface::calc_method_id("get_cron_info")
    }},

    {"coffee_staking_master", {
        Interface::calc_method_id("get_stored_data"),
        Interface::calc_method_id("get_collection_data"),
        Interface::calc_method_id("get_nft_address_by_index"),
        Interface::calc_method_id("get_nft_content")
    }},

    {"coffee_staking_vault", {
        Interface::calc_method_id("get_stored_data"),
        Interface::calc_method_id("get_master_address")
    }},

    {"coffee_staking_item", {
        Interface::calc_method_id("get_stored_data"),
        Interface::calc_method_id("get_nft_data")
    }},

    {"coffee_factory", {
        Interface::calc_method_id("get_vault_address"),
        Interface::calc_method_id("get_pool_address"),
        Interface::calc_method_id("get_pool_address_no_settings"),
        Interface::calc_method_id("get_pool_creator_address"),
        Interface::calc_method_id("get_pool_creator_address_no_settings"),
        Interface::calc_method_id("get_liquidity_depository_address"),
        Interface::calc_method_id("get_liquidity_depository_address_no_settings"),
        Interface::calc_method_id("get_admin_address"),
        Interface::calc_method_id("get_code")
    }},

    {"coffee_vault", {
        Interface::calc_method_id("get_asset"),
        Interface::calc_method_id("is_active")
    }},

    {"coffee_pool", {
        Interface::calc_method_id("get_jetton_data"),
        Interface::calc_method_id("get_wallet_address"),
        Interface::calc_method_id("get_pool_data"),
        Interface::calc_method_id("estimate_swap_amount"),
        Interface::calc_method_id("estimate_liquidity_withdraw_amount"),
        Interface::calc_method_id("estimate_liquidity_deposit_amount")
    }},

    {"teleitem", {
        Interface::calc_method_id("get_telemint_auction_state"),
        Interface::calc_method_id("get_telemint_auction_config"),
        Interface::calc_method_id("get_telemint_token_name")
    }},

    {"tonco_pool", {
        Interface::calc_method_id("get_collection_data"),
        Interface::calc_method_id("getIsActive"),
        Interface::calc_method_id("getPoolStateAndConfiguration"),
        Interface::calc_method_id("getChildContracts"),
        Interface::calc_method_id("getAllTickInfos")
    }},

    {"tonco_router", {
        Interface::calc_method_id("getRouterState"),
        Interface::calc_method_id("getPoolAddress"),
        Interface::calc_method_id("getChildContracts")
    }},

    {"tonkeeper_2fa", {
        Interface::calc_method_id("get_wallet_addr"),
        Interface::calc_method_id("get_root_pubkey"),
        Interface::calc_method_id("get_seed_pubkey"),
        Interface::calc_method_id("get_delegation_state"),
        Interface::calc_method_id("get_estimated_attached_value")
    }},

    {"tv_pool", {
        Interface::calc_method_id("get_pool_data"),
        Interface::calc_method_id("get_nominator_data"),
        Interface::calc_method_id("list_nominators"),
        Interface::calc_method_id("list_votes")
    }},

    {"wallet_v1r2", {
        Interface::calc_method_id("seqno")
    }},

    {"wallet_v1r3", {
        Interface::calc_method_id("seqno"),
        Interface::calc_method_id("get_public_key")
    }},

    {"wallet_v2r1", {
        Interface::calc_method_id("seqno")
    }},

    {"wallet_v2r2", {
        Interface::calc_method_id("seqno"),
        Interface::calc_method_id("get_public_key")
    }},

    {"wallet_v3r1", {
        Interface::calc_method_id("seqno")
    }},

    {"wallet_v3r2", {
        Interface::calc_method_id("get_public_key"),
        Interface::calc_method_id("seqno")
    }},

    {"wallet_v4r1", {
        Interface::calc_method_id("get_plugin_list"),
        Interface::calc_method_id("is_plugin_installed"),
        Interface::calc_method_id("get_public_key"),
        Interface::calc_method_id("seqno"),
        Interface::calc_method_id("get_subwallet_id")
    }},

    {"wallet_v4r2", {
        Interface::calc_method_id("get_plugin_list"),
        Interface::calc_method_id("is_plugin_installed"),
        Interface::calc_method_id("get_public_key"),
        Interface::calc_method_id("seqno"),
        Interface::calc_method_id("get_subwallet_id")
    }},

    {"wallet_v5_beta", {
        Interface::calc_method_id("seqno")
    }},

    {"wallet_v5r1", {
        Interface::calc_method_id("seqno"),
        Interface::calc_method_id("get_public_key"),
        Interface::calc_method_id("get_subwallet_id"),
        Interface::calc_method_id("get_extensions"),
        Interface::calc_method_id("is_signature_allowed")
    }},

    {"wallet_highload_v2", {
        Interface::calc_method_id("get_public_key")
    }},

    {"wallet_highload_v3r1", {
        Interface::calc_method_id("get_public_key"),
        Interface::calc_method_id("get_subwallet_id"),
        Interface::calc_method_id("get_timeout")
    }},

    {"wallet_vesting", {
        Interface::calc_method_id("seqno"),
        Interface::calc_method_id("get_public_key"),
        Interface::calc_method_id("get_vesting_data")
    }},

    {"whales_pool", {
        Interface::calc_method_id("get_staking_status"),
        Interface::calc_method_id("get_pool_status"),
        Interface::calc_method_id("get_member"),
        Interface::calc_method_id("get_members_raw"),
        Interface::calc_method_id("get_params")
    }},
};