# Stonfi v2 withdraw liquidity, remade from blocks/liquidity.py
# StonfiV2WithdrawLiquidityMatcher. FULLY DECLARATIVE build (no host builder).
#
# The legacy matcher anchors on ONE pool pay-to (op 0x657b54f5) and then does an
# imperative sibling scan (block.previous_block.next_blocks) to pick up the
# second pay-to — a workaround because a single op appears twice under the
# router. The remake drops the scan: the two pay-tos are a children set under
# their common parent, bound positionally (@payout_1/@payout_2), exactly like
# dedust_withdraw. First-match-wins over candidates in canonical order
# reproduces the legacy [anchor]+siblings order.
#
# Legacy's OrMatcher parent (normal burn vs refund) remains split into two
# matchers because they have different structural heads and consumed shapes:
# normal is fronted by a JettonBurn, while refund is fronted by op 0x132b9a2c.
# The anchors are mutually exclusive, so on any one trace at most one fires, and
# each produces the same single dex_withdraw_liquidity block as legacy.
#
# Normal: burn-anchored (btype jetton_burn, the JettonBurnBlockMatcher-produced
# block; stonfi_v2 runs after it in event_processing) -> op 0x297437cf router ->
# {two pay-tos}. Same shape family as dedust_withdraw.
# Refund: anchored on op 0x0f98e2b8, its parent (op 0x132b9a2c) carried on a
# `<-` edge, following the wrapped-children-set Tonco precedent, so the
# core can read the sender off refund.previous_block, and the parent is consumed.
#
# Each pay-to has a jetton_transfer child; a TON payout carries a PTonTransfer
# grandchild under that transfer (`maybe op PTonTransfer`) — the leg's ton_amount
# is read off it and the leg is marked is_ton. All captures bind positionally
# (no recursion), so the build reads each pay-to's jetton_transfer `.data` and
# the optional PTonTransfer body directly; the legacy build core's manual merge
# of the pTON blocks is subsumed by the engine merging the captured pton_1/pton_2.

opcode StonfiV2PayTo                 = 0x657b54f5
opcode StonfiV2WithdrawFromPool      = 0x297437cf
opcode StonfiV2WithdrawRefund        = 0x0f98e2b8
opcode StonfiV2WithdrawRefundParent  = 0x132b9a2c
opcode PTonTransfer                  = 0x01f3835d

matcher stonfi_v2_withdraw {
    produces dex_withdraw_liquidity
    entry    @burn

    btype jetton_burn @burn
      -> op StonfiV2WithdrawFromPool @router
           -> { op StonfiV2PayTo @payout_1 -> btype jetton_transfer @jt_1 -> maybe op PTonTransfer @pton_1
              ; op StonfiV2PayTo @payout_2 -> btype jetton_transfer @jt_2 -> maybe op PTonTransfer @pton_2 }

    parse pton_1 as PTonTransfer
    parse pton_2 as PTonTransfer
    out {
        dex:                 "stonfi_v2"
        sender:              burn.data.owner
        sender_wallet:       burn.data.jetton_wallet
        pool:                account(payout_1.msg.source)
        asset:               burn.data.asset
        lp_tokens_burnt:     burn.data.amount
        is_refund:           false
        amount1_out:         amount(pton_1.body.ton_amount ?? 0) if pton_1 != null else jt_1.data.amount
        asset1_out:          ton_asset() if pton_1 != null else jt_1.data.asset
        dex_wallet_1:        jt_1.data.sender
        dex_jetton_wallet_1: jt_1.data.sender_wallet
        wallet1:             jt_1.data.receiver_wallet
        amount2_out:         amount(pton_2.body.ton_amount ?? 0) if pton_2 != null else jt_2.data.amount
        asset2_out:          ton_asset() if pton_2 != null else jt_2.data.asset
        dex_wallet_2:        jt_2.data.sender
        dex_jetton_wallet_2: jt_2.data.sender_wallet
        wallet2:             jt_2.data.receiver_wallet
    }
}

matcher stonfi_v2_withdraw_refund {
    produces dex_withdraw_liquidity
    entry    @refund

    op StonfiV2WithdrawRefund @refund
      -> { op StonfiV2PayTo @payout_1 -> btype jetton_transfer @jt_1 -> maybe op PTonTransfer @pton_1
         ; op StonfiV2PayTo @payout_2 -> btype jetton_transfer @jt_2 -> maybe op PTonTransfer @pton_2 }
      <- op StonfiV2WithdrawRefundParent @refund_parent

    parse pton_1 as PTonTransfer
    parse pton_2 as PTonTransfer
    out {
        dex:                 "stonfi_v2"
        sender:              account(refund_parent.msg.source)
        sender_wallet:       null
        pool:                account(payout_1.msg.source)
        asset:               null
        lp_tokens_burnt:     null
        is_refund:           true
        amount1_out:         amount(pton_1.body.ton_amount ?? 0) if pton_1 != null else jt_1.data.amount
        asset1_out:          ton_asset() if pton_1 != null else jt_1.data.asset
        dex_wallet_1:        jt_1.data.sender
        dex_jetton_wallet_1: jt_1.data.sender_wallet
        wallet1:             jt_1.data.receiver_wallet
        amount2_out:         amount(pton_2.body.ton_amount ?? 0) if pton_2 != null else jt_2.data.amount
        asset2_out:          ton_asset() if pton_2 != null else jt_2.data.asset
        dex_wallet_2:        jt_2.data.sender
        dex_jetton_wallet_2: jt_2.data.sender_wallet
        wallet2:             jt_2.data.receiver_wallet
    }
}
