# Dedust withdraw liquidity matcher with a declarative build and no host builder.
#
# Shape of interest: a children set with two IDENTICAL-head items (both
# `op DedustPayoutFromPool`). The engine's children-set algorithm
# is first-match-wins over candidates in canonical order, so item 1 consumes
# the first matching payout chain and item 2 the next. This is deterministic and
# matches the legacy labeled('payout_1')/labeled('payout_2') assignment
# exactly. The @req_*/@ton_*/@jt_* captures bind positionally (no recursion),
# so the build reads them directly.
#
# Each payout terminal is either a TON payout (`op DedustPayout`) or a jetton
# payout (`btype jetton_transfer`, produced by the prerequisite
# JettonTransferBlockMatcher/PTonTransferMatcher, with the same registration-order
# dependency as legacy). Splitting the OR into distinct capture names per arm
# (@ton_N | @jt_N) lets the build branch on which arm bound (exactly one is
# non-null); the payout-request call above each terminal is captured (@req_N)
# so its coin amount is read via a declarative `parse`, replacing the legacy
# `payout.previous_block` navigation.

opcode JettonBurn             = 0x595f07bc
opcode JettonBurnNotification = 0x7bdd97de
opcode DedustPayoutFromPool   = 0xad4eb6f5
opcode DedustPayout           = 0x474f86cf

matcher dedust_withdraw {
    produces dex_withdraw_liquidity
    entry    @burn

    op JettonBurn @burn
      -> op JettonBurnNotification @notification
      -> { op DedustPayoutFromPool @req_1 -> ( op DedustPayout @ton_1 | btype jetton_transfer @jt_1 )
         ; op DedustPayoutFromPool @req_2 -> ( op DedustPayout @ton_2 | btype jetton_transfer @jt_2 ) }

    parse burn  as JettonBurn
    parse req_1 as DedustPayoutFromPool
    parse req_2 as DedustPayoutFromPool
    let lp_wallet = lookup jetton_wallet(burn.msg.destination)
    reject when lp_wallet == null
    out {
        dex:                 "dedust"
        sender:              account(burn.msg.source)
        sender_wallet:       account(burn.msg.destination)
        pool:                account(notification.msg.destination)
        asset:               asset(lp_wallet.jetton)
        lp_tokens_burnt:     amount(burn.body.amount)
        is_refund:           false
        amount1_out:         amount(req_1.body.amount)
        asset1_out:          ton_asset() if jt_1 == null else jt_1.data.asset
        dex_wallet_1:        account(ton_1.msg.source) if jt_1 == null else jt_1.data.sender
        dex_jetton_wallet_1: null if jt_1 == null else jt_1.data.sender_wallet
        wallet1:             null if jt_1 == null else jt_1.data.receiver_wallet
        amount2_out:         amount(req_2.body.amount)
        asset2_out:          ton_asset() if jt_2 == null else jt_2.data.asset
        dex_wallet_2:        account(ton_2.msg.source) if jt_2 == null else jt_2.data.sender
        dex_jetton_wallet_2: null if jt_2 == null else jt_2.data.sender_wallet
        wallet2:             null if jt_2 == null else jt_2.data.receiver_wallet
    }
}
