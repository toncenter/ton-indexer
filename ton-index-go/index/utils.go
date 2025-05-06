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
		"nft_discovery",
		"unknown",
	},
	"latest": {"v2"},
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
	typesMap := make(map[string]bool)
	seen := make(map[string]bool) // Track processed shortcuts to avoid cycles

	for _, shortcut := range shortcuts {
		expandShortcutRecursive(shortcut, typesMap, seen, 0)
	}
	// Always ensure v1 types are included
	expandShortcutRecursive("v1", typesMap, seen, 0)

	// Convert map keys to slice
	var expandedTypes []string
	for t := range typesMap {
		// Only include actual action types, not shortcut names
		if _, isShortcut := ActionTypeShortcuts[t]; !isShortcut {
			expandedTypes = append(expandedTypes, t)
			println(t)
		}
	}
	return expandedTypes
}

func expandShortcutRecursive(shortcut string, typesMap map[string]bool, seen map[string]bool, depth int) {
	if depth > 10 {
		return
	}

	// Check if we've already processed this shortcut in current branch
	if seen[shortcut] {
		return
	}

	// Mark this shortcut as seen in current branch
	seen[shortcut] = true

	if types, ok := ActionTypeShortcuts[shortcut]; ok {
		for _, t := range types {
			if _, isShortcut := ActionTypeShortcuts[t]; isShortcut {
				expandShortcutRecursive(t, typesMap, seen, depth+1)
			} else {
				typesMap[t] = true
			}
		}
	} else {
		// This is not a shortcut, it's an actual action type
		typesMap[shortcut] = true
	}
	// Unmark the shortcut when leaving this branch
	seen[shortcut] = false
}
