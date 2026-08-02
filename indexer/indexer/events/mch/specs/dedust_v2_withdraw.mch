# DeDust CPMM V2 withdraw liquidity — port of DedustV2WithdrawBlockMatcher
# (blocks/dedust_v2/liquidity.py:43). Fully declarative; no host builder.
#
# Chain:
#   Withdraw 0x20b5ef89 (user -> Pool)                          <- anchor
#     WithdrawLiquidity 0xbc1531a9 (Pool -> Position)   # burns the LP share
#       ExitLiquidity 0xf6f6a3aa (Position -> Pool)     # realized amounts
#         Withdrawal event 0xc0d77b54 (Pool ext-out)    # optional
#         payout leg 1 (PayoutMessage 0x3216ca09 | jetton_transfer)  # optional
#         payout leg 2 (same)                                        # optional
#
# WithdrawLiquidity and ExitLiquidity are matched by `op` only — the reference
# never parses either body, so neither gets a registered ABI row.
#
# LP IS NOT A JETTON. The share lives in the Position contract and there is no
# LP-jetton burn, so `asset` and `sender_wallet` are hard nulls (the reference
# writes None into both) and the burnt amount comes off the message bodies
# rather than off a jetton_burn block.
#
# SUCCESS = the Withdrawal event exists. No event means the withdraw aborted
# (slippage), which drives `failed`, `is_refund`, and which of the two
# `liquidity` fields is the burnt amount: the event carries the EXACT burned
# share, the entry message only the requested one.
#
# autoClaimFees is REJECTED, exactly as the reference bails: with the flag set
# the pool mixes fee payouts into the same transaction and the two payout slots
# can no longer be told apart from the withdrawn amounts. The flag uses the ABI
# declaration's `bool` leaf; `bits 1` would only skip it.
#
# PARSER PATHS, mixed in one spec (legal; the sibling dedust_v2_claim_fees.mch
# does the same): DedustV2Withdraw, DedustV2WithdrawalEvent, and PayoutMessage
# all ride protocol ABI bridge rows, with their authored field spellings. All
# sit behind an `op`
# guard, which the ABI path requires and the legacy-class twins want anyway
# (they `assert op == opcode`).
#
# Per-leg OR order (`op PayoutMessage | btype jetton_transfer`) is the reverse
# of the reference's payout_leg_matcher, and inert: the branches are bare
# concrete nodes with distinct heads, so the compiler upgrades the `|` to an
# exclusive-or and at most one can ever bind. Spelled this way to match
# the claim_fees/claim_reward siblings.
#
# Leg -> slot assignment is POSITIONAL, unlike claim_fees (which matches legs to
# slots by exact amount). The reference reads `get_labeled_sorted("payout", ...)`
# = ascending min_lt, and the children-set algorithm consumes candidates in
# exactly that canonical order, so item 1 binds the earlier leg.
#
# DEVIATION FROM THE REFERENCE (one, engine-level, documented not fixed). The
# reference passes include_excess=False on every NESTED matcher of the chain,
# so the pool's 0xd53276db excess stays a standalone call_contract action. Both
# MCH engines instead absorb it: nested absorption is UNCONDITIONAL for a
# matched non-root node (mch_ir/engine.py `_absorb_aux`, mirrored in C++), and
# the matcher-level `include_excess false` above only suppresses the ANCHOR's
# own aux siblings. Verified against the real legacy classifier over all nine
# corpus withdraw fixtures: every semantic field of the produced row is
# byte-identical, and the whole delta is that the excess transaction moves into
# this row's `tx_hashes` (and, on one fixture, extends `end_lt`) instead of
# standing as its own row. Five of the nine show it — the successful ones; the
# three aborted withdraws have no payout tx and so no excess, and the fourth is
# a truncated chain that neither engine classifies. Making absorption honour
# the flag is an engine change that would move the consumed set of every other
# include_excess=false spec, so it stays out of this port.
#
# ORDERING. The 0x20b5ef89 anchor is claimed by no other spec and neither are
# the three inner opcodes, so this file's position in the built-in source list
# carries no tie-break of its own; it is listed right after
# specs/dedust_v2_swap.mch, which mirrors dedust_v2.matchers() (swap,
# failed swap, withdraw). What IS load-bearing is that it stays after the
# priority-90 jetton specs, since the jetton payout legs are `btype
# jetton_transfer` blocks those must have built first — the default priority 100
# gives that for free.

opcode DedustV2Withdraw          = 0x20B5EF89
opcode DedustV2WithdrawLiquidity = 0xBC1531A9
opcode DedustV2ExitLiquidity     = 0xF6F6A3AA
opcode DedustV2WithdrawalEvent   = 0xC0D77B54
opcode PayoutMessage             = 0x3216CA09

matcher dedust_v2_withdraw {
    produces        dex_withdraw_liquidity
    entry           @withdraw
    include_excess  false

    op DedustV2Withdraw @withdraw
      -> op DedustV2WithdrawLiquidity
        -> op DedustV2ExitLiquidity
          -> { maybe op DedustV2WithdrawalEvent @event
             ; maybe ( op PayoutMessage @ton_1 | btype jetton_transfer @jt_1 )
             ; maybe ( op PayoutMessage @ton_2 | btype jetton_transfer @jt_2 ) }

    parse withdraw as DedustV2Withdraw
    parse event    as DedustV2WithdrawalEvent
    parse ton_1    as PayoutMessage
    parse ton_2    as PayoutMessage

    reject when withdraw.body.auto_claim_fees

    let success = event != null

    # Per-leg projection (parse_payout_leg, dedust_v2/common.py:28): a jetton leg
    # reads the prebuilt block's data, a TON leg is (amount, TON, no user wallet,
    # the payer, no dex jetton wallet). An unbound leg is all-null (_EMPTY_LEG).
    let a_1   = jt_1.data.amount ?? ton_1.body.amount
    let a_2   = jt_2.data.amount ?? ton_2.body.amount

    failed when not success

    out {
        dex:                 "dedust_v2"
        sender:              account(withdraw.msg.source)
        sender_wallet:       null
        pool:                account(withdraw.msg.destination)
        asset:               null
        lp_tokens_burnt:     amount(event.body.liquidity) if success else amount(withdraw.body.liquidity)
        is_refund:           not success
        amount1_out:         amount(a_1)
        asset1_out:          jt_1.data.asset if jt_1 != null else (ton_asset() if ton_1 != null else null)
        dex_wallet_1:        jt_1.data.sender if jt_1 != null else (account(ton_1.msg.source) if ton_1 != null else null)
        dex_jetton_wallet_1: jt_1.data.sender_wallet if jt_1 != null else null
        wallet1:             jt_1.data.receiver_wallet if jt_1 != null else null
        amount2_out:         amount(a_2)
        asset2_out:          jt_2.data.asset if jt_2 != null else (ton_asset() if ton_2 != null else null)
        dex_wallet_2:        jt_2.data.sender if jt_2 != null else (account(ton_2.msg.source) if ton_2 != null else null)
        dex_jetton_wallet_2: jt_2.data.sender_wallet if jt_2 != null else null
        wallet2:             jt_2.data.receiver_wallet if jt_2 != null else null
    }
}
