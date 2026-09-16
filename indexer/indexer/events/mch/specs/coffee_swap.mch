# Coffee swap — ported from blocks/swaps.py CoffeeSwapBlockMatcher.
#
# New shapes vs earlier ports (jettons/dns/dedust_swap):
# - recursion step is a two-node CHAIN (`op CoffeeSwapInternal -> op
#   CoffeeSwapSuccessfulEvent`): each unrolling consumes the swap AND its
#   success event, mirroring legacy RecursiveMatcher(repeating=ContractMatcher(
#   SwapInternal, child=ContractMatcher(SuccessEvent)));
# - same-name capture across exclusive parent branches (@in_transfer);
# - three-way mixed op/btype alternative under the payout exit.
#
# The anchor children set mirrors legacy children_matchers=[event,
# Or([recursion, payout_chain])]: the payout chain appears BOTH as the rule's
# exit branch (multi-hop) and as the direct Or branch (zero intermediate
# hops). Exit-branch and post-recursion captures bind. The builder deliberately
# works from Match._mch_consumed because the shared legacy core needs the full
# consumed set for its call-contract scan; this is a builder design choice, not
# a capture limitation.

opcode CoffeeSwapNative          = 0xc0ffee00
opcode CoffeeSwapInternal        = 0xc0ffee20
opcode CoffeeSwapSuccessfulEvent = 0xc0ffee30
opcode CoffeePayoutInternal      = 0xc0ffee21
opcode CoffeePayout              = 0xc0ffee32
opcode CoffeeNotification        = 0xc0ffee36

# One pool hop per unrolling: a swap request with its success event; exit =
# the payout chain to the user.
rule swap_chain = (
    op CoffeeSwapInternal @peer -> op CoffeeSwapSuccessfulEvent @peer_event -> maybe $swap_chain
  ^ op CoffeePayoutInternal -> ( btype jetton_transfer | op CoffeePayout | op CoffeeNotification )
)

# HYBRID declarative build. The pattern, the reject/failed wiring and the
# out-field mapping are declarative; the imperative residue stays in one named
# host fn (a gap, like dedust_swap_legs):
#   coffee_swap_data — reconstructs the in-transfer / payout / out-transfer
#     roles from the consumed set, then delegates to the shared
#     build_coffee_swap_core (per-hop success-event scan, peer-leg list
#     assembly, in/out leg extraction). Returns null to reject (core returns
#     None on a missing role or no hops); otherwise a namespace mirroring the
#     core's data dict, with `.failed` driving the failed flag.
matcher coffee_swap {
    produces JettonSwapBlock
    entry    @swap

    op CoffeeSwapInternal @swap
      -> { op CoffeeSwapSuccessfulEvent @event
         ; ( $swap_chain
           | op CoffeePayoutInternal @payout
               -> ( btype jetton_transfer @out ^ op CoffeePayout @out ^ op CoffeeNotification @out ) ) }
      <- ( btype jetton_transfer @in_transfer ^ op CoffeeSwapNative @in_transfer )

    let d = coffee_swap_data(consumed)
    reject when d == null
    failed when d.failed
    out {
        dex:                   d.dex
        sender:                d.sender
        source_asset:          d.source_asset
        destination_asset:     d.destination_asset
        dex_incoming_transfer: d.dex_incoming_transfer
        dex_outgoing_transfer: d.dex_outgoing_transfer
        peer_swaps:            d.peer_swaps
        referral_amount:       d.referral_amount
        referral_address:      d.referral_address
    }
}
