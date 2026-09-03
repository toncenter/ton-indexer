#include "EmuInterfaces.h"

namespace mch {

schema::JettonWalletDataV2 to_v2(const JettonWalletDetectorR::Result &r) {
  schema::JettonWalletDataV2 v{};
  v.balance = r.balance;
  v.address = r.address;
  v.owner = r.owner;
  v.jetton = r.jetton;
  v.mintless_is_claimed = r.mintless_is_claimed;
  return v;
}

schema::NFTItemDataV2 to_v2(const NftItemDetectorR::Result &r) {
  schema::NFTItemDataV2 v{};
  v.address = r.address;
  v.init = r.init;
  v.index = r.index;
  v.collection_address = r.collection_address;
  v.owner_address = r.owner_address;
  // get_nft_data requires content to remain an optional dictionary.
  v.content = r.content;
  // dns_entry has no compatible type and is not read by lookup handlers.
  return v;
}

schema::GetGemsNftFixPriceSaleData to_v2(const GetGemsNftFixPriceSale::Result &r) {
  schema::GetGemsNftFixPriceSaleData v{};
  v.address = r.address;
  v.is_complete = r.is_complete;
  v.created_at = r.created_at;
  v.marketplace_address = r.marketplace_address;
  v.nft_address = r.nft_address;
  v.nft_owner_address = r.nft_owner_address;
  v.full_price = r.full_price;
  v.marketplace_fee_address = r.marketplace_fee_address;
  v.marketplace_fee = r.marketplace_fee;
  v.royalty_address = r.royalty_address;
  v.royalty_amount = r.royalty_amount;
  return v;
}

schema::GetGemsNftAuctionData to_v2(const GetGemsNftAuction::Result &r) {
  schema::GetGemsNftAuctionData v{};
  v.address = r.address;
  v.end = r.end;
  v.end_time = r.end_time;
  v.mp_addr = r.mp_addr;
  v.nft_addr = r.nft_addr;
  v.nft_owner = r.nft_owner;
  v.last_bid = r.last_bid;
  v.last_member = r.last_member;
  v.min_step = r.min_step;
  v.mp_fee_addr = r.mp_fee_addr;
  v.mp_fee_factor = r.mp_fee_factor;
  v.mp_fee_base = r.mp_fee_base;
  v.royalty_fee_addr = r.royalty_fee_addr;
  v.royalty_fee_factor = r.royalty_fee_factor;
  v.royalty_fee_base = r.royalty_fee_base;
  v.max_bid = r.max_bid;
  v.min_bid = r.min_bid;
  v.created_at = r.created_at;
  v.last_bid_at = r.last_bid_at;
  v.is_canceled = r.is_canceled;
  v.activated = r.activated;
  v.step_time = r.step_time;
  v.last_query_id = r.last_query_id;
  v.jetton_wallet = r.jetton_wallet;
  v.jetton_master = r.jetton_master;
  v.is_broken_state = r.is_broken_state;
  v.public_key = r.public_key;
  return v;
}

schema::DedustPoolData to_v2(const DedustPoolDetector::Result &r) {
  schema::DedustPoolData v{};
  v.address = r.address;
  v.asset_1 = r.asset_1;
  v.asset_2 = r.asset_2;
  v.reserve_1 = r.reserve_1;
  v.reserve_2 = r.reserve_2;
  v.is_stable = r.is_stable;
  v.fee = r.fee;
  // asset_*_slice stays behind: it is the factory-verification input, not part
  // of the V2 struct, and nothing downstream of the lookup reads it.
  return v;
}

schema::NominatorPoolData to_v2(const NominatorPoolContract::Result &r) {
  schema::NominatorPoolData v{};
  v.address = r.address;
  v.state = r.state;
  v.nominators_count = r.nominators_count;
  v.stake_amount_sent = r.stake_amount_sent;
  v.validator_amount = r.validator_amount;
  v.validator_address = r.validator_address;
  v.validator_reward_share = r.validator_reward_share;
  v.max_nominators_count = r.max_nominators_count;
  v.min_validator_stake = r.min_validator_stake;
  v.min_nominator_stake = r.min_nominator_stake;
  for (const auto &n : r.nominators) {
    v.nominators.push_back({
        .address = n.address,
        .balance = n.amount,
        .pending_balance = n.pending_deposit_amount,
    });
  }
  return v;
}

schema::GetGemsNftFixPriceSaleV4Data to_v2(const GetGemsNftFixPriceSaleV4::Result &r) {
  schema::GetGemsNftFixPriceSaleV4Data v{};
  v.address = r.address;
  v.is_complete = r.is_complete;
  v.created_at = r.created_at;
  v.marketplace_address = r.marketplace_address;
  v.nft_address = r.nft_address;
  v.nft_owner_address = r.nft_owner_address;
  v.full_price = r.full_price;
  v.marketplace_fee_address = r.marketplace_fee_address;
  v.marketplace_fee = r.marketplace_fee;
  v.royalty_address = r.royalty_address;
  v.royalty_amount = r.royalty_amount;
  v.sold_at = r.sold_at;
  v.sold_query_id = r.sold_query_id;
  v.jetton_price_dict = r.jetton_price_dict;
  return v;
}

schema::MultisigOrderData to_v2(const MultisigOrder::Result &r) {
  schema::MultisigOrderData v{};
  v.address = r.address;
  v.multisig_address = r.multisig_address;
  v.order_seqno = r.order_seqno;
  v.threshold = r.threshold;
  v.sent_for_execution = r.sent_for_execution;
  v.approvals_mask = r.approvals_mask;
  v.approvals_num = r.approvals_num;
  v.expiration_date = r.expiration_date;
  // `order` is deliberately not copied (lazy-cell lifetime; see mch-docs engine/architecture.md).
  v.signers = r.signers;
  return v;
}

}  // namespace mch
