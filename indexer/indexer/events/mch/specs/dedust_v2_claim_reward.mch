# DeDust CPMM V2 liquidity-mining reward claim corresponding to
# DedustV2ClaimRewardBlockMatcher (blocks/dedust_v2/
# claims.py). The three message bodies are parsed through the ton-abi generated
# structs on the C++ side (bridge rows ClaimRewardMessage / PayoutRewardMessage /
# PayoutMessage) and hand parsers on the Python twin (builders/dedust_v2.py). The
# `parse ... as <Name>` names are the BARE ABI decl names, matching both.
#
# Chain shape:
#   ClaimReward 0x909fdb65 (user -> Pool)
#     ClaimPositionReward 0x2e0cccba (Pool -> Position)   # matched by opcode only
#       PayoutReward 0x9e17fbbe (Position -> Pool)        # exact amount + owner
#         payout leg  (PayoutMessage 0x3216ca09 TON | jetton_transfer)   # optional
#         excess 0xd53276db                               # auto-absorbed (default)
#
# Anchored on the payout-back message:
# the request alone doesn't carry the amount. Action mirrors the reference:
# type dedust_v2_claim_reward reusing the dex_withdraw_liquidity_data shape,
# amount1 = PayoutReward.amount, asset1/wallets from the payout leg, side-2 null.
#
# `out` keys are the REFERENCE matcher's data keys (blocks/dedust_v2/claims.py
# DedustV2ClaimRewardBlockMatcher), so the SAME serializer fill
# (_fill_dedust_v2_claim_reward) serves both the legacy block and this one.

opcode ClaimRewardMessage   = 0x909fdb65
opcode ClaimPositionReward  = 0x2e0cccba
opcode PayoutRewardMessage  = 0x9e17fbbe
opcode PayoutMessage        = 0x3216ca09

matcher dedust_v2_claim_reward {
    produces dedust_v2_claim_reward
    entry    @claim

    op ClaimRewardMessage @claim
      -> op ClaimPositionReward @cpr
      -> op PayoutRewardMessage @payout_reward
      -> maybe ( op PayoutMessage @ton | btype jetton_transfer @jt )

    parse claim         as ClaimRewardMessage
    parse payout_reward as PayoutRewardMessage
    # Bound only on the TON-payout arm, and none of its fields are read below —
    # the leg contributes ton_asset() + the message source, not body data. Kept
    # so the arm is parsed (and the parser exercised) on both sides; its only
    # gate today is the mch_abi_bridge unit (no TON-payout fixture in the corpus
# yet).
    parse ton           as PayoutMessage
    out {
        dex:               "dedust_v2"
        sender:            account(claim.msg.source)
        pool:              account(claim.msg.destination)
        position:          account(payout_reward.msg.source)
        owner:             payout_reward.body.ownerAddress
        reward_index:      claim.body.rewardIndex
        amount:            amount(payout_reward.body.amount)
        asset:             jt.data.asset if jt != null else (ton_asset() if ton != null else null)
        user_wallet:       jt.data.receiver_wallet if jt != null else null
        dex_jetton_wallet: jt.data.sender_wallet if jt != null else null
        dex_wallet:        jt.data.sender if jt != null else (account(ton.msg.source) if ton != null else null)
    }
}
