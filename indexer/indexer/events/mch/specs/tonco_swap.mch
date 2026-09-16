# Tonco swap matcher corresponding to ToncoSwapBlockMatcher.
#
# This matcher uses cyclic recursion (`rule NAME cyclic = …`). The source
# matcher is a hand-built cyclic child-matcher graph:
# hop continuation is MANDATORY inside the hop branches (jetton_transfer ->
# [PTonTransfer -> JettonNotify ->] next swap), and the final-transfer
# branches share the `swap -> payout -> jetton_transfer` prefix with them.
# The frontier strategy tries the exit first for each candidate. On a multi-hop
# chain the shared prefix would therefore terminate at hop 2. Cyclic descent
# lowers self-references at
# chain ends to true back-edges in the matcher graph and disambiguates
# hop-vs-final by looking down the actual chain.
#
# Deviations from the legacy graph (same class as the stonfi_v2 port):
# - anchor duplication: the first hop's payout/out-alternative lives in the
#   matcher pattern (`entry` cannot live inside a rule), hops 2..n repeat via
#   $tonco_hop;
# - the anchor's child chain is wrapped in a one-item children set so the
#   parent edge stays attached to the anchor (a bare `-> … <-` chain is C009);
#   the children set consumes ONE payout child per anchor where legacy
#   child_matcher consumes every matching child — identical while a pool
#   answers a swap message with exactly one ROUTERV3_PAY_TO (protocol
#   behavior, holds across the corpus);
# - legacy's final branches carry optional=True INSIDE the OrMatcher; the
#   engine ignores branch-level optionality there. The
#   port simply drops the inert flag;
# - the final-jetton branch carries `{ maybe op PTonTransfer }` so the pattern
#   consumes the trailing pton call that the legacy build pulls in via
#   additional_blocks (the mch builder ignores the core's additional list);
# - legacy also pulls in a pton call hanging off an INTERMEDIATE transfer via
#   additional_blocks; in the only hop shape that carries one (the pton hop
#   branch) the pattern already consumes that exact call, and a jetton->jetton
#   hop has no pton sibling.
#
# C012 forbids captures inside cyclic rule bodies because they never bind.
# the host fn derives every legacy label role (peer_swap/payout/out_transfer/
# intermediate_transfer/intermediate_hop_trigger/in_transfer) structurally
# from `consumed` and delegates to the shared build_tonco_swap_core
# (blocks/swaps.py).
#
# HYBRID declarative build. The cyclic match, multi-hop pool walk, per-hop BOC
# parsing and jetton-wallet interface lookups are all irreducible (cyclic
# bodies bind no captures at all), so a single host fn returns the full
# jetton_swap field record + the failed flag; the `out` block assembles the
# produced Block(btype jetton_swap) from it and `failed when r.failed` sets the
# flag. The fn returns null to reject.

opcode ToncoPoolV3Swap    = 0xa7fb58f8
opcode ToncoRouterV3PayTo = 0xa1daa96d
opcode JettonNotify       = 0x7362d09c
opcode JettonTransfer     = 0x0f8a7ea5
opcode PTonTransfer       = 0x01f3835d

# One router hop per descent level: swap request + payment from the pool, then
# either a transfer that provably continues to the next swap (back-edge,
# mandatory continuation, this is what the frontier strategy cannot express)
# OR a final transfer to the user. Branch order mirrors the legacy OrMatcher:
# hop-continuation branches first, then finals.
rule tonco_hop cyclic = (
    op ToncoPoolV3Swap
      -> op ToncoRouterV3PayTo
      -> ( btype jetton_transfer -> op PTonTransfer -> op JettonNotify -> $tonco_hop
         | btype jetton_transfer -> $tonco_hop
         | op JettonTransfer -> op PTonTransfer
         | btype jetton_transfer -> { maybe op PTonTransfer } )
)

matcher tonco_swap {
    produces jetton_swap
    entry    @swap

    op ToncoPoolV3Swap @swap
      -> { op ToncoRouterV3PayTo
             -> ( btype jetton_transfer -> op PTonTransfer -> op JettonNotify -> $tonco_hop
                | btype jetton_transfer -> $tonco_hop
                | op JettonTransfer -> op PTonTransfer
                | btype jetton_transfer -> { maybe op PTonTransfer } ) }
      <- ( btype jetton_transfer @in_transfer
         ^ op JettonNotify <- op PTonTransfer @in_transfer )

    let r = tonco_swap_data(consumed)
    reject when r == null
    failed when r.failed
    out {
        dex:                   r.dex
        source_asset:          r.source_asset
        destination_asset:     r.destination_asset
        sender:                r.sender
        dex_incoming_transfer: r.dex_incoming_transfer
        dex_outgoing_transfer: r.dex_outgoing_transfer
        referral_amount:       r.referral_amount
        referral_address:      r.referral_address
        peer_swaps:            r.peer_swaps
        min_out_amount:        r.min_out_amount
    }
}
