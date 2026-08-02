# Stonfi V2 swap matcher corresponding to StonfiV2SwapBlockMatcher.
#
# Implementation-specific shape details:
# - anchor moved out of the recursive rule (entry cannot be a list binding);
#   the first hop's swap/payout/out-alternative live in the matcher pattern,
#   hops 2..n repeat via $swap_hop;
# - the anchor's child chain is wrapped in a one-item children set so the
#   parent edge stays attached to the anchor; a bare
#   `-> ... <-` chain is C009;
# - the idealized `op JettonInternalTransfer` out-arm is implemented as
#   `btype jetton_transfer` + an inline `where (not .has_internal_transfer)`
#   (the former `no_internal_transfer` host predicate, folded into the language
#   reads the jetton_transfer block's own `has_internal_transfer` data
#   flag) plus the payload-validating `pton_self_transfer` on a bare `pred` node
#   (legacy GenericMatcher has no opcode constraint);
# - `^` is used where legacy has ordered OrMatcher because same-name captures
#   require declared exclusivity (R005); hop branches actually overlap by
#   prefix — behavior stays first-match-in-order (recursion destructures the
#   alternative; ExclusiveOr elsewhere is try_build-inert);
# - terminal-pton branch carries `{ maybe op PTonTransfer }` so the pattern
#   consumes the pton call the legacy build added via
#   additional_blocks_to_include.
#
# Captures are kept for pattern fidelity but the builder works off the
# consumed set (Match._mch_consumed). It stays consumed-set-driven because the
# legacy scan logic needs the full set
# anyway (same situation as dedust_swap.mch).

opcode StonfiV2Swap  = 0x6664de2a
opcode StonfiV2PayTo = 0x657b54f5
opcode JettonNotify  = 0x7362d09c
opcode PTonTransfer  = 0x01f3835d

predicate pton_self_transfer

# One router hop per unrolling: swap request + payment, then continuation via
# a validated intermediate pton chain, a plain jetton transfer, or directly.
rule swap_hop = (
    op StonfiV2Swap @peer_swap
      -> op StonfiV2PayTo @peer_payout
      -> btype jetton_transfer @peer_out where (not .has_internal_transfer)
      -> pred pton_self_transfer
      -> op JettonNotify
      -> maybe $swap_hop
  ^ op StonfiV2Swap @peer_swap
      -> op StonfiV2PayTo @peer_payout
      -> btype jetton_transfer @peer_out
      -> { maybe op PTonTransfer }
      -> maybe $swap_hop
  ^ op StonfiV2Swap @peer_swap
      -> op StonfiV2PayTo @peer_payout
      -> maybe $swap_hop
)

# HYBRID declarative build. The pattern, the failed flag and the out-field
# mapping are declarative; the imperative residue stays in one named host fn
# (a gap, like dedust_swap_legs):
#   stonfi_v2_swap_data — reconstructs the (swap, pay_to) leg pairs and the
#     in/out transfers from the consumed set, then delegates to the shared
#     build_stonfi_v2_swap_core (async get_pool_accounts_recursive walk,
#     per-hop get_jetton_wallet lookups, peer-leg list assembly, the broken-
#     loop wallet-miss guard, and the failed-swap destination_asset fallback).
#     Returns a namespace mirroring the core's data dict; `.failed` drives the
#     failed flag.
matcher stonfi_v2_swap {
    produces JettonSwapBlock
    entry    @swap

    op StonfiV2Swap @swap
      -> { op StonfiV2PayTo @payout
             -> ( btype jetton_transfer @out_transfer where (not .has_internal_transfer)
                    -> pred pton_self_transfer
                    -> op JettonNotify
                    -> maybe $swap_hop
                ^ btype jetton_transfer @out_transfer
                    -> { maybe op PTonTransfer }
                    -> maybe $swap_hop
                ^ $swap_hop ) }
      <- ( btype jetton_transfer @in_transfer
         ^ op JettonNotify <- op PTonTransfer @in_transfer )

    let d = stonfi_v2_swap_data(consumed)
    failed when d.failed
    out {
        dex:                   d.dex
        source_asset:          d.source_asset
        destination_asset:     d.destination_asset
        sender:                d.sender
        dex_incoming_transfer: d.dex_incoming_transfer
        dex_outgoing_transfer: d.dex_outgoing_transfer
        referral_amount:       d.referral_amount
        referral_address:      d.referral_address
        peer_swaps:            d.peer_swaps
    }
}
