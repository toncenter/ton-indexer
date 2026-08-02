# DeDust CPMM V2 deposit-liquidity matchers corresponding to the completed,
# rejected, and partial deposit matchers. One host function parses the deposit
# payload; everything else is declarative.
#
# Chain (two carriers, one Deposit escrow, one Pool):
#
#   carrier_X (jetton_transfer | PayNative 0xa5a7cbf8)
#     Pool -> Deposit  CreditAsset 0xdc5ddba1 (is_x)     # leg X credited
#   carrier_Y (same two shapes)
#     Pool -> Deposit  CreditAsset 0xdc5ddba1 (is_y)     # leg Y credited
#       Deposit -> Pool  JoinLiquidity 0x25251ee0        <- completion anchor
#         Pool -> ext     Deposit event 0x35df2e12       # optional = SUCCESS
#         Pool -> Position CreditLiquidity 0x855afcbd    # optional, op only
#         Pool -> user    jetton_transfer / Payout 0x3216ca09   # dust refunds
#
# Only one of the two legs reaches JoinLiquidity through the parent
# chain — the last one credited, since the join hangs off its CreditAsset. That
# leg is folded into the completing block; the other leg is matched on its own
# by the partial matcher and merged in afterwards by the host pass
# post_process_dedust_v2_liquidity (blocks/dedust_v2/liquidity.py:465), keyed on
# the Deposit escrow address. Like the v1 post_process_dedust_liquidity pass, it
# dispatches on the btypes this file
# produces, so the legacy classes and these generic blocks pair identically.
# Matcher order inside this file matters for the same reason it does in
# the reference's matchers() list: final and rejected must claim their
# CreditAsset parent before the partial matcher, which would otherwise eat it.
#
# The reference writes `data[f"asset_{slot}"]` with slot =
# 1 if is_x else 2 — a dynamic key the DSL has no form for, so each of the six
# slot-bearing keys is spelled as the `is_x` conditional instead. Same values,
# and the unwritten slot stays the hard null the reference leaves it as (it is
# the OTHER leg's, filled by the post-process pass).
#
# SUCCESS = the Deposit event exists. The two matchers split on it rather than
# branching one build, exactly as the reference does, because their children
# sets genuinely differ (the completing one binds the event and the
# CreditLiquidity, the rejected one binds an excess). `dedust_v2_deposit_final`
# is listed first and returns no match when the event is absent; the rejected
# matcher rejects when it is present, so the pair is total and disjoint on any
# JoinLiquidity anchor.
#
# AMOUNTS. Success reads the realized amount_x_in/amount_y_in off the event;
# rejection has no event and falls back to the ATTEMPTED amount_x/amount_y off
# the JoinLiquidity body. Both write BOTH slots, so the post-process pass's
# "fill amount_N when null" arm never fires for this family. Its guard keeps a
# partial deposit safe.
#
# REFUNDS -> vault_excesses. A deposit that consumes less than it was given
# refunds dust to the initiator. A refund addressed elsewhere rejects the match,
# implemented by one `reject when … != initiator` per leg
# here, which is the same total check. Elements are 2-element LISTS where the
# reference builds tuples; the expression language has no tuple literal, the
# serializer indexes them identically, and the parity comparison treats the two
# as one value. The rejected matcher additionally requires at least
# one refund leg (`len(...) == 0` reject), mirroring its "no refund legs found"
# bail.
#
# TARGET AMOUNTS. The desired amount_x/amount_y live in the deposit payload,
# which the carrier carries two different ways: behind PayNative's
# payment_payload ref (TON leg) or inside the jetton carrier's b64
# forward_payload, wrapped in a PayJetton 0xcbc33949. Neither descent is
# expressible, and the payload is only ours when its own opcode is 0xc9a015da
# (PayNative also carries swaps and fund rewards), so both shapes go to the
# `dedust_v2_deposit_payload` host fn — the exact structure
# specs/dedust_v2_swap.mch uses for its swap payload. A payload that is
# absent or not a deposit yields null, and both target amounts become null,
# reproducing the reference's `payload is None` arm. The PARTIAL matcher does
# not read the payload: the partial build sets no target_* keys at
# all, so neither does this one.
#
# Nested auxiliary absorption does not widen this match's consumed set. The only
# excess is the escrow gas return, which is captured explicitly. Adding another
# nested matcher can change this invariant if its auxiliary siblings are
# absorbed.
#
# ORDERING. Both anchors (0x25251ee0, 0xdc5ddba1) are claimed by no other spec,
# and no other spec binds a block this one produces, so this file's slot in
# the built-in source list carries no tie-break of its own; it is listed right
# after specs/dedust_v2_withdraw.mch, mirroring dedust_v2.matchers() (swap,
# failed swap, withdraw, deposit, rejected deposit, partial deposit). What IS
# load-bearing is that it stays after the priority-90 jetton specs — the jetton
# carriers and the jetton refund legs are `btype jetton_transfer` blocks those
# must have built first — which the default priority 100 gives for free. The
# two claim specs sit BEFORE this one in the manifest while the reference lists
# them after; their anchors (0x5652f1df / 0x909fdb65) are disjoint from this
# family's and neither binds a jetton_transfer the deposit carriers could be, so
# the inversion is unobservable.

opcode DedustV2PayNative       = 0xa5a7cbf8
opcode DedustV2CreditAsset     = 0xdc5ddba1
opcode DedustV2JoinLiquidity   = 0x25251ee0
opcode DedustV2DepositEvent    = 0x35df2e12
opcode DedustV2CreditLiquidity = 0x855afcbd
opcode PayoutMessage           = 0x3216ca09
opcode ExcessMessage           = 0xd53276db

# One deposit leg's carrier: the TON-side PayNative or a prebuilt jetton
# transfer. Bare concrete nodes with distinct heads, so `^` (the compiler would
# upgrade `|` to it anyway) and at most one arm can ever bind.
#
# ARM ORDER IS LOAD-BEARING, though not semantically: it is the reverse of the
# reference's _deposit_carrier_matcher and matches specs/dedust_v2_swap.mch
# instead, because `parse carrier as DedustV2PayNative` below resolves against
# the FIRST arm's head — a `btype` head there trips R023 (parsing a composed
# block's body does not round-trip). The PayNative parse is harmless on the
# jetton arm: every value it feeds is read only under `jetton_in == false`.
rule dv2_deposit_carrier = (
    op DedustV2PayNative @carrier
  ^ btype jetton_transfer @carrier
)

# The CreditAsset hop plus its carrier, i.e. the parent chain both JoinLiquidity
# matchers hang off (the reference's _deposit_credit_parent).
rule dv2_credit_parent =
    op DedustV2CreditAsset @credit <- $dv2_deposit_carrier

# The completing leg. The Deposit event is bound `maybe` and then REQUIRED: an
# absent event is the rejected matcher's case, and the reference returns a bare
# no-match (not a bail) for it.
matcher dedust_v2_deposit_final {
    produces        dedust_v2_deposit_liquidity
    entry           @join
    include_excess  false

    op DedustV2JoinLiquidity @join
      -> { maybe op DedustV2DepositEvent @event
         ; maybe op DedustV2CreditLiquidity
         ; maybe btype jetton_transfer @refund_jt_1
         ; maybe btype jetton_transfer @refund_jt_2
         ; maybe op PayoutMessage @refund_ton }
      <- $dv2_credit_parent

    parse join       as DedustV2JoinLiquidity
    parse event      as DedustV2DepositEvent
    parse credit     as DedustV2CreditAsset
    parse carrier    as DedustV2PayNative
    parse refund_ton as PayoutMessage

    reject when event == null

    let initiator = account(join.body.initiator)
    let is_x      = credit.body.is_x
    let jetton_in = carrier.btype == "jetton_transfer"

    # parse_payout_leg's deposit twin (_deposit_leg): a jetton carrier gives the
    # leg's asset and the USER's wallet off the prebuilt block, a TON carrier is
    # plain TON with no wallet.
    let leg_asset  = carrier.data.asset if jetton_in else ton_asset()
    let leg_wallet = carrier.data.sender_wallet if jetton_in else null

    let payload = dedust_v2_deposit_payload(carrier.data.forward_payload) if jetton_in else dedust_v2_deposit_payload(carrier.body.payment_payload)

    # Every refund leg must be addressed to the initiator, jetton and TON alike.
    # The two jetton slots are separate captures (one name cannot bind twice in
    # one children set, R005) and fill in canonical min_lt order — the same
    # order the reference's get_labeled_sorted("refund_jetton", …) produces.
    reject when refund_jt_1 != null and refund_jt_1.data.receiver != initiator
    reject when refund_jt_2 != null and refund_jt_2.data.receiver != initiator
    reject when refund_ton != null and account(refund_ton.msg.destination) != initiator

    let jetton_excesses = concat(
        [[refund_jt_1.data.asset, refund_jt_1.data.amount]] if refund_jt_1 != null else [],
        [[refund_jt_2.data.asset, refund_jt_2.data.amount]] if refund_jt_2 != null else [])
    let ton_excesses    = [[ton_asset(), amount(refund_ton.body.amount)]] if refund_ton != null else []

    out {
        dex:              "dedust_v2"
        sender:           initiator
        pool:             account(join.msg.destination)
        deposit_contract: account(join.msg.source)
        amount_1:         amount(event.body.amount_x_in)
        asset_1:          leg_asset if is_x else null
        sender_wallet_1:  leg_wallet if is_x else null
        amount_2:         amount(event.body.amount_y_in)
        asset_2:          null if is_x else leg_asset
        sender_wallet_2:  null if is_x else leg_wallet
        target_amount_1:  amount(payload.amount_x)
        target_amount_2:  amount(payload.amount_y)
        lp_tokens_minted: amount(event.body.liquidity)
        vault_excesses:   concat(jetton_excesses, ton_excesses)
    }
}

# The join REJECTED at the pool (no Deposit event): every credited leg is
# refunded to the initiator. Same anchor and same parent chain as the completing
# matcher, which runs first, so this one only ever sees anchors that one gave
# up on.
matcher dedust_v2_deposit_rejected {
    produces        dedust_v2_deposit_liquidity
    entry           @join
    include_excess  false

    op DedustV2JoinLiquidity @join
      -> { maybe btype jetton_transfer @refund_jt_1
         ; maybe btype jetton_transfer @refund_jt_2
         ; maybe op PayoutMessage @refund_ton
         ; maybe op DedustV2DepositEvent @event
         ; maybe op ExcessMessage }
      <- $dv2_credit_parent

    parse join       as DedustV2JoinLiquidity
    parse credit     as DedustV2CreditAsset
    parse carrier    as DedustV2PayNative
    parse refund_ton as PayoutMessage

    # A Deposit event means the join succeeded — the completing matcher's case
    # (the reference scans the anchor's next_blocks for the same opcode).
    reject when event != null

    let initiator = account(join.body.initiator)
    let is_x      = credit.body.is_x
    let jetton_in = carrier.btype == "jetton_transfer"

    let leg_asset  = carrier.data.asset if jetton_in else ton_asset()
    let leg_wallet = carrier.data.sender_wallet if jetton_in else null

    let payload = dedust_v2_deposit_payload(carrier.data.forward_payload) if jetton_in else dedust_v2_deposit_payload(carrier.body.payment_payload)

    reject when refund_jt_1 != null and refund_jt_1.data.receiver != initiator
    reject when refund_jt_2 != null and refund_jt_2.data.receiver != initiator
    reject when refund_ton != null and account(refund_ton.msg.destination) != initiator

    let jetton_excesses = concat(
        [[refund_jt_1.data.asset, refund_jt_1.data.amount]] if refund_jt_1 != null else [],
        [[refund_jt_2.data.asset, refund_jt_2.data.amount]] if refund_jt_2 != null else [])
    let ton_excesses    = [[ton_asset(), amount(refund_ton.body.amount)]] if refund_ton != null else []
    let excesses        = concat(jetton_excesses, ton_excesses)
    # A rejected join with nothing refunded is not this shape.
    reject when len(excesses) == 0

    failed when true

    out {
        dex:              "dedust_v2"
        sender:           initiator
        pool:             account(join.msg.destination)
        deposit_contract: account(join.msg.source)
        amount_1:         amount(join.body.amount_x)
        asset_1:          leg_asset if is_x else null
        sender_wallet_1:  leg_wallet if is_x else null
        amount_2:         amount(join.body.amount_y)
        asset_2:          null if is_x else leg_asset
        sender_wallet_2:  null if is_x else leg_wallet
        target_amount_1:  amount(payload.amount_x)
        target_amount_2:  amount(payload.amount_y)
        lp_tokens_minted: null
        vault_excesses:   excesses
    }
}

# One credited leg on its own — whatever the two matchers above did not claim.
# The escrow's gas-return excess is bound explicitly because the matcher-level
# `include_excess false` suppresses exactly that absorption on the anchor
# (the reference spells the same thing as child_matcher=excess_matcher()).
# `pool` and `deposit_contract` are the CreditAsset's own source/destination:
# it is the Pool that credits the escrow.
matcher dedust_v2_deposit_partial {
    produces        dedust_v2_deposit_liquidity_partial
    entry           @credit
    include_excess  false

    op DedustV2CreditAsset @credit
      -> { maybe op ExcessMessage }
      <- $dv2_deposit_carrier

    parse credit  as DedustV2CreditAsset
    parse carrier as DedustV2PayNative

    let is_x      = credit.body.is_x
    let jetton_in = carrier.btype == "jetton_transfer"

    let leg_asset  = carrier.data.asset if jetton_in else ton_asset()
    let leg_wallet = carrier.data.sender_wallet if jetton_in else null
    let leg_amount = amount(credit.body.amount)

    out {
        dex:              "dedust_v2"
        sender:           carrier.data.sender if jetton_in else account(carrier.msg.source)
        pool:             account(credit.msg.source)
        deposit_contract: account(credit.msg.destination)
        amount_1:         leg_amount if is_x else null
        asset_1:          leg_asset if is_x else null
        sender_wallet_1:  leg_wallet if is_x else null
        amount_2:         null if is_x else leg_amount
        asset_2:          null if is_x else leg_asset
        sender_wallet_2:  null if is_x else leg_wallet
        lp_tokens_minted: null
    }
}
