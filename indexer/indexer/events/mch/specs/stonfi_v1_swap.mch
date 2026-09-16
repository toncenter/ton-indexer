# Stonfi V1 swap matcher corresponding to StonfiSwapBlockMatcher.
#
# The legacy hack: the chain matches EVERY payment request (any exit code)
# that carries a jetton transfer, then IN BUILD classifies them by exit code
# and bfs-REMOVES the ok_ref referral-payment subtree from the consumed set
# (build-side consumption surgery). The remake moves the disambiguation to
# match time: the payment-request node is gated on the sender-related exit
# codes (ok 0xc64370e5 / no_liq 0x5ffe1295 / reserve_err 0x38976e9b), so the
# ok_ref (0x45078540) referral subtree is simply never consumed — it then
# classifies on its own as a jetton_transfer action, which is exactly the
# state the legacy surgery produced. The builder still reads the referral
# amount/address from the UNCONSUMED sibling through the tree (the swap
# call's next_blocks) without consuming it.
#
# Exit-code parsing needs the message body, so the gate is a host predicate:
# Inline `.field` predicates see only block.data, and a CallContractBlock
# carries none. The body-parsing check therefore stays in a host predicate.
#
# The payment-request edge accumulates multiple matches: when the referral is
# paid through a second SENDER-RELATED payment request (legacy's out_amt<amount
# branch) both requests match and stay consumed — no surgery in that shape,
# and none here.

opcode StonfiSwap           = 0x25938561
opcode StonfiPaymentRequest = 0xf93bb43f

predicate stonfi_v1_sender_payment

# HYBRID declarative build. The pattern, the failed flag and the out-field
# mapping are declarative; the imperative residue stays in one named host fn
# (a gap, like dedust_swap_legs):
#   stonfi_v1_swap_data — reconstructs the payment-request list from the swap
#     call's children (tree order == legacy consumption order) and delegates to
#     the shared build_stonfi_v1_swap_core (payment-request classification by
#     exit code, referral extraction from the UNCONSUMED ok_ref sibling, and
#     the get_jetton_wallet lookups). Returns a namespace mirroring the core's
#     data dict; `.success` drives the failed flag.
matcher stonfi_v1_swap {
    produces JettonSwapBlock
    entry    @in_transfer

    btype jetton_transfer @in_transfer
      -> op StonfiSwap @swap
      -> op StonfiPaymentRequest @req where stonfi_v1_sender_payment
      -> btype jetton_transfer @out

    let d = stonfi_v1_swap_data(in_transfer, swap)
    failed when not d.success
    out {
        dex:                   d.dex
        sender:                d.sender
        receiver:              d.receiver
        dex_incoming_transfer: d.dex_incoming_transfer
        dex_outgoing_transfer: d.dex_outgoing_transfer
        destination_asset:     d.destination_asset
        destination_wallet:    d.destination_wallet
        referral_amount:       d.referral_amount
        referral_address:      d.referral_address
        peer_swaps:            d.peer_swaps
        success:               d.success
    }
}
