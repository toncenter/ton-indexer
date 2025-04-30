package index

var BasicActions = [...]string{
	"call_contract",
	"contract_deploy",
	"tick_tock",
	"ton_transfer",
}

var ActionTypeShortcuts = map[string][]string{
	"v1": {
		"call_contract", "contract_deploy", "ton_transfer", "stake_deposit",
		"stake_withdrawal", "stake_withdrawal_request", "dex_deposit_liquidity",
		"jetton_transfer", "nft_transfer", "nft_mint", "jetton_burn", "jetton_mint",
		"jetton_swap", "change_dns", "delete_dns", "renew_dns", "subscribe",
		"dex_withdraw_liquidity", "unsubscribe", "election_deposit", "election_recover",
		"auction_bid", "tick_tock",
	},
	"v2": {
		"call_contract", "contract_deploy", "ton_transfer", "stake_deposit",
		"stake_withdrawal", "stake_withdrawal_request", "dex_deposit_liquidity",
		"jetton_transfer", "nft_transfer", "nft_mint", "jetton_burn", "jetton_mint",
		"jetton_swap", "change_dns", "delete_dns", "renew_dns", "subscribe",
		"dex_withdraw_liquidity", "unsubscribe", "election_deposit", "election_recover",
		"auction_bid", "tick_tock",
		// New action types
		"multisig_create_order", "multisig_approve", "multisig_execute",
		"vesting_send_message", "vesting_add_whitelist",
		"evaa_supply", "evaa_withdraw", "evaa_liquidate",
		"jvault_stake", "jvault_unstake", "jvault_claim",
		"unknown",
	},
	"staking": {
		"stake_deposit", "stake_withdrawal", "stake_withdrawal_request",
	},
	"jettons": {
		"jetton_transfer", "jetton_burn", "jetton_mint",
	},
	"nft": {
		"nft_transfer", "nft_mint", "auction_bid",
	},
	"dns": {
		"change_dns", "delete_dns", "renew_dns",
	},
	"multisig": {
		"multisig_create_order", "multisig_approve", "multisig_execute",
	},
	"vesting": {
		"vesting_send_message", "vesting_add_whitelist",
	},
	"evaa": {
		"evaa_supply", "evaa_withdraw", "evaa_liquidate",
	},
	"jvault": {
		"jvault_stake", "jvault_unstake", "jvault_claim",
	},
}

func ExpandActionTypeShortcuts(shortcuts []string) []string {
	var expandedTypes []string
	shortcuts = append(shortcuts, "v1")
	typesMap := make(map[string]bool)
	typesMap["v1"] = true // ensure v1 types always present

	for _, shortcut := range shortcuts {
		if types, ok := ActionTypeShortcuts[shortcut]; ok {
			for _, t := range types {
				typesMap[t] = true
			}
		} else {
			typesMap[shortcut] = true
		}
	}

	// Convert map keys to slice
	for t := range typesMap {
		expandedTypes = append(expandedTypes, t)
	}

	return expandedTypes
}
