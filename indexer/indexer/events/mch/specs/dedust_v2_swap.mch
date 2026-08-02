# DeDust CPMM V2 swap corresponding to DedustV2SwapBlockMatcher and
# DedustV2FailedSwapBlockMatcher (blocks/dedust_v2/swaps.py).
#
# Route shape (one unrolling of $dv2_hop per pool hop):
#
#   carrier (PayNative 0xa5a7cbf8 | jetton_transfer into the pool)
#     Swap event 0x78e79ba4                # the pool's ext-out log: the ONLY
#     continuation                         # exact source of amount_in/amount_out
#       = next carrier (another hop)  |  out leg (jetton_transfer | Payout 0x3216ca09)
#     excess 0xd53276db
#
# The reference matcher takes both carrier kinds in test_self; here that is one
# anchor XOR mixes an `op` and a `btype` head under one capture, so a
# single matcher covers TON-in and jetton-in.
#
# Recursion notes (the shape is forced by the frontier walk):
# - the out leg appears TWICE: as the rule's exit arms (routes WITH hops, where
#   it is reached on the post-step frontier) and again at the invocation site
#   (single-hop routes, where the walk never steps; the frontier entry gate is
#   the STEP head, so an exit-only recursion can never fire);
# - the `where (.payload_opcode …)` clauses are what keep the exit-tried-first
#   order from mistaking an intermediate jetton carrier for the out leg (the
#   premature exit: a pool-to-pool carrier rides a PayJetton
#   (0xcbc33949) forward payload, the user-facing out leg does not;
# - `maybe $dv2_hop` hangs off the Swap EVENT, not off the carrier's children
#   set, and that position is load-bearing. An in-step self-reference lowers to
#   an `any` node that CONSUMES a candidate, so under the carrier it would eat
#   the next hop's carrier before the frontier reaches it. A hop head that
#   is already consumed contributes no next_blocks to the following frontier,
#   which truncated every 3-hop route to its last two hops. The event is a
#   childless ext-out log, so the placeholder finds no candidate and the walk
#   is driven purely by the frontier. R006 only asks that a maybe-guarded self
#   reference exist.
#
# Per-hop amounts come from the Swap events and per-hop assets from the
# carriers, so the peer-swap list is expressible without a host fn: hop i swaps
# in_assets[i] -> leg_assets[i], where leg_assets is [hop carriers…, out leg]
# and in_assets is that sequence shifted by the entry carrier. The reference
# builds the same pairing by scanning the consumed set and sorting on min_lt;
# on a linear route (a pool forwards to exactly one continuation) the two agree.
#
# The one host fn is `dedust_v2_swap_payload`: the swap payload sits behind a
# ref (PayNative.payment_payload) or inside the jetton carrier's b64
# forward_payload (PayJetton wrapper), and its `next` SwapStep chain has to be
# walked to reach the FINAL minimal_amount_out. Neither the cell descent nor the
# walk is expressible in the language — see builders/dedust_v2.py.
#
# `out` keys are the reference matcher's JettonSwapBlock data keys, so the same
# serializer fill (_fill_jetton_swap_action) serves both builds.

opcode DedustV2PayNative = 0xa5a7cbf8
opcode DedustV2SwapEvent = 0x78e79ba4
opcode DedustV2Payout    = 0x3216ca09
opcode ExcessMessage     = 0xd53276db

# One pool hop per unrolling (step arms), then the terminal out leg (exit arms).
rule dv2_hop = (
    op DedustV2PayNative @hop
      -> { op DedustV2SwapEvent @ev -> maybe $dv2_hop }
  ^ btype jetton_transfer @hop where (.payload_opcode == "0xcbc33949")
      -> { op DedustV2SwapEvent @ev -> maybe $dv2_hop }
  ^ btype jetton_transfer @exit_leg where (.payload_opcode != "0xcbc33949")
  ^ op DedustV2Payout @exit_leg
)

matcher dedust_v2_swap {
    produces JettonSwapBlock
    entry    @carrier
    include_excess false          # the reference consumes the carrier's excess
                                  # INTO the swap block (explicit child below),
                                  # not as a sibling of the result

    ( op DedustV2PayNative @carrier ^ btype jetton_transfer @carrier )
      -> { op DedustV2SwapEvent @ev0
           ( $dv2_hop
           ^ btype jetton_transfer @final_leg where (.payload_opcode != "0xcbc33949")
           ^ op DedustV2Payout @final_leg )
           maybe op ExcessMessage @excess }

    # A jetton carrier is a composed block: its `.msg`/`.body` are not reliable
    # (merge_blocks shuffles the event-node order), so every jetton-arm value
    # comes from `.data`. The PayNative parse is harmless on that arm — it is
    # only read under `jetton_in == false`.
    parse carrier as DedustV2PayNative
    parse ev0      as DedustV2SwapEvent
    parse ev       as DedustV2SwapEvent

    let jetton_in    = carrier.btype == "jetton_transfer"
    let source_asset = carrier.data.asset if jetton_in else ton_asset()
    let payload      = dedust_v2_swap_payload(carrier.data.forward_payload) if jetton_in else dedust_v2_swap_payload(carrier.body.payment_payload)
    # PayNative also carries deposits and fund rewards; only a swap payload is ours.
    reject when not jetton_in and payload == null

    let out_leg = first(exit_leg) ?? final_leg
    reject when out_leg == null
    let out_jetton = out_leg.btype == "jetton_transfer"
    let out_asset  = out_leg.data.asset if out_jetton else ton_asset()
    # An out leg still carrying a SwapPayload means the route failed mid-hop.
    reject when out_jetton and dedust_v2_swap_payload(out_leg.data.forward_payload) != null

    let amounts_in  = concat([ev0.body.amount_in],  map(ev.body, "amount_in"))
    let amounts_out = concat([ev0.body.amount_out], map(ev.body, "amount_out"))
    let leg_assets  = concat(map(hop as h => h.data.asset if h.btype == "jetton_transfer" else ton_asset()), [out_asset])
    let in_assets   = concat([source_asset], leg_assets)
    let hops        = zip(zip(amounts_in, amounts_out), zip(in_assets, leg_assets))

    failed when (ev0.msg.transaction.aborted ?? false) or (ev0.msg.exit_code ?? 0) != 0
    failed when any(ev as e => (e.msg.transaction.aborted ?? false) or (e.msg.exit_code ?? 0) != 0)

    out {
        dex:               "dedust_v2"
        sender:            carrier.data.sender if jetton_in else account(carrier.msg.source)
        source_asset:      source_asset
        destination_asset: out_asset
        dex_incoming_transfer: {
            asset:                     source_asset
            amount:                    amount(first(amounts_in))
            source:                    carrier.data.sender if jetton_in else account(carrier.msg.source)
            source_jetton_wallet:      carrier.data.sender_wallet if jetton_in else null
            destination:               carrier.data.receiver if jetton_in else account(carrier.msg.destination)
            destination_jetton_wallet: carrier.data.receiver_wallet if jetton_in else null
        }
        dex_outgoing_transfer: {
            asset:                     out_asset
            amount:                    amount(last(amounts_out))
            source:                    out_leg.data.sender if out_jetton else account(out_leg.msg.source)
            source_jetton_wallet:      out_leg.data.sender_wallet if out_jetton else null
            destination:               out_leg.data.receiver if out_jetton else account(out_leg.msg.destination)
            destination_jetton_wallet: out_leg.data.receiver_wallet if out_jetton else null
        }
        peer_swaps: map(hops as h => {
                        in:  { amount: amount(first(first(h))), asset: first(last(h)) }
                        out: { amount: amount(last(first(h))),  asset: last(last(h)) }
                    }) if len(amounts_in) > 1 else []
        referral_amount:   null
        referral_address:  null
        min_out_amount:    amount(payload.min_out)
    }
}

# Slippage / deadline failure: the pool refunds instead of swapping, so there is
# no Swap event under the carrier.
#
#   TON-in:    PayNative -> Payout 0x3216ca09 (exit_code != 0, back to the sender)
#   jetton-in: jetton_transfer -> jetton_transfer (same asset, back to the sender)
#
# The refunded route's destination asset is not on the wire at all — it is the
# pool's OTHER asset, read from the pool account's interface data.
matcher dedust_v2_failed_swap {
    produces JettonSwapBlock
    entry    @carrier
    include_excess false

    ( op DedustV2PayNative @carrier ^ btype jetton_transfer @carrier )
      -> { ( op DedustV2Payout @refund_ton | btype jetton_transfer @refund_jt )
           maybe op DedustV2SwapEvent @swapped
           maybe op ExcessMessage @excess }

    parse carrier    as DedustV2PayNative
    parse refund_ton as PayoutMessage

    # A Swap event under the carrier means the swap executed — the success
    # matcher's case (it runs first, so this only fires when IT bailed).
    reject when swapped != null

    let jetton_in    = carrier.btype == "jetton_transfer"
    let source_asset = carrier.data.asset if jetton_in else ton_asset()
    let payload      = dedust_v2_swap_payload(carrier.data.forward_payload) if jetton_in else dedust_v2_swap_payload(carrier.body.payment_payload)
    reject when payload == null

    # The refund arm must match the carrier arm, and must go back to the
    # carrier's OWN sender: a failed multi-hop refunds the route initiator, so a
    # pool->pool carrier's payout lands elsewhere and is not this swap's refund.
    reject when jetton_in and refund_jt == null
    reject when not jetton_in and refund_ton == null
    reject when not jetton_in and refund_ton.body.exitCode == 0
    reject when not jetton_in and refund_ton.msg.destination != carrier.msg.source
    reject when jetton_in and refund_jt.data.asset != source_asset
    reject when jetton_in and refund_jt.data.receiver != carrier.data.sender

    let sender = carrier.data.sender if jetton_in else account(carrier.msg.source)
    let pool   = carrier.data.receiver if jetton_in else account(carrier.msg.destination)
    let assets = lookup dedust_pool(pool).assets
    let a0     = first(assets)
    let a1     = last(assets)
    let asset0 = null if a0 == null else (ton_asset() if a0.is_ton else asset(a0.address))
    let asset1 = null if a1 == null else (ton_asset() if a1.is_ton else asset(a1.address))
    # The reference returns the FIRST pool asset that is not the source asset.
    let other  = asset0 if (asset0 != null and asset0 != source_asset) else (asset1 if (asset1 != null and asset1 != source_asset) else null)

    failed when true

    out {
        dex:               "dedust_v2"
        sender:            sender
        source_asset:      source_asset
        destination_asset: other
        dex_incoming_transfer: {
            asset:                     source_asset
            amount:                    carrier.data.amount if jetton_in else amount(carrier.body.amount)
            source:                    sender
            source_jetton_wallet:      carrier.data.sender_wallet if jetton_in else null
            destination:               pool
            destination_jetton_wallet: carrier.data.receiver_wallet if jetton_in else null
        }
        dex_outgoing_transfer: {
            asset:                     other
            amount:                    amount(0)
            source:                    pool
            source_jetton_wallet:      null
            destination:               sender
            destination_jetton_wallet: null
        }
        peer_swaps:        []
        referral_amount:   null
        referral_address:  null
        min_out_amount:    amount(payload.min_out)
    }
}
