// Action-row construction shared by the production actor and dump harness.
// Rendering and stdout output remain harness concerns.
#pragma once

#include "BlockTree.h"
#include "Value.h"

#include <cstdint>
#include <string>
#include <vector>

namespace mch {

// Single source for the struct, serializer table, scrub, and renderer.
#define MCH_ACTION_VALUE_FIELDS(X) \
  X(source) \
  X(source_secondary) \
  X(destination) \
  X(destination_secondary) \
  X(asset) \
  X(asset_secondary) \
  X(asset2) \
  X(asset2_secondary) \
  X(amount) \
  X(value) \
  X(opcode) \
  X(ton_transfer_data) \
  X(jetton_transfer_data) \
  X(jetton_swap_data) \
  X(nft_transfer_data) \
  X(nft_listing_data) \
  X(nft_mint_data) \
  X(dex_deposit_liquidity_data) \
  X(dex_withdraw_liquidity_data) \
  X(staking_data) \
  X(evaa_supply_data) \
  X(evaa_withdraw_data) \
  X(evaa_liquidate_data) \
  X(vesting_send_message_data) \
  X(vesting_add_whitelist_data) \
  X(tonco_deploy_pool_data) \
  X(multisig_create_order_data) \
  X(multisig_approve_data) \
  X(multisig_execute_data) \
  X(cocoon_worker_payout_data) \
  X(cocoon_proxy_payout_data) \
  X(cocoon_proxy_charge_data) \
  X(cocoon_client_top_up_data) \
  X(cocoon_register_proxy_data) \
  X(cocoon_unregister_proxy_data) \
  X(cocoon_client_register_data) \
  X(cocoon_client_change_secret_hash_data) \
  X(cocoon_client_request_refund_data) \
  X(cocoon_grant_refund_data) \
  X(cocoon_client_increase_stake_data) \
  X(cocoon_client_withdraw_data) \
  X(layerzero_packet_data) \
  X(layerzero_send_data) \
  X(layerzero_dvn_verify_data) \
  X(jvault_stake_data) \
  X(jvault_claim_data) \
  X(change_dns_record_data) \
  X(coffee_create_pool_data) \
  X(coffee_staking_deposit_data) \
  X(coffee_staking_withdraw_data) \
  X(extra)

struct Action {
  std::string action_id;
  std::string type;
  bool success{true};
  std::int64_t start_lt{0}, end_lt{0}, start_utime{0}, end_utime{0}, mc_seqno_end{0};
  std::vector<std::string> tx_hashes;
  std::vector<std::string> accounts;  // role-less action_accounts set
#define MCH_DECL(name) Value name;
  MCH_ACTION_VALUE_FIELDS(MCH_DECL)
#undef MCH_DECL
  // Empty on a top-level row; on a child row, the action_id of the block that
  // consumed it and the btypes of every ancestor up the consumption chain.
  std::string parent_action_id;
  std::vector<std::string> ancestor_type;
};

// One serialized row: the block plus the two fields that are not a property
// of the block alone but of where it sits in the consumption tree.
struct ActionRow {
  Block *block{nullptr};
  std::string parent_action_id;
  std::vector<std::string> ancestor_type;
  // action_id of the gasless_request marker this row is an immediate result
  // of (empty otherwise); rendered as extra.parent_gasless_action.
  std::string parent_gasless_action;
};

// The block's lowest-lt event node (nullptr for an empty block).
const EventNode *root_event_node(const Block *b);

// base64(sha256(root_event_node's msg_hash|tx_hash + btype)). A pure function
// of the block, so the serializer can link a child to its parent before either
// is built.
std::string calc_action_id(const Block *b);

// Dispatch over a produced block. Returns false for a btype outside the fill
// set (skip-table entry).
bool build_action(Block *b, Action &a);

bool build_action(const ActionRow &row, Action &a);

// Dispatched like the other fills; also driven directly by the serialize test.
void fill_evaa_liquidate(const Value &data, Action &action);

// Synthetic row a trace gets when classification succeeds but produces
// nothing. Emitted when ClassifyResult::unknown_trace is set.
//
// Carries the only action_id in the system not derived from a hash: the
// trace id, which on the emulated path is also the Redis trace key. The
// lt/utime/mc_seqno_end range comes from the Trace, so every loader must
// populate those fields; see TraceLoader.h.
Action create_unknown_action(const Trace &trace);

}  // namespace mch
