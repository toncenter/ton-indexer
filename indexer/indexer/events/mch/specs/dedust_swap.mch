# Dedust swap matcher corresponding to DedustSwapBlockMatcher.
#
# Production reality mirrored here: DedustPeerBlockMatcher is defined in
# swaps.py but never registered in event_processing.matchers, so the recursive
# chain sees raw DedustSwapPeer/DedustSwapExternal opcodes (never
# SingleLevelWrapper blocks).
#
# Captures in $peer_chain are list bindings. Exit-branch captures bind once on
# termination, and post-recursion captures (@notification,
#   @in_transfer/@swap_request) bind on multi-hop too. The exit payout chain
#   is left uncaptured because the builder works from
#   the consumed set (Match._mch_consumed), like the legacy build_block works
#   off other_blocks. This is the simplest faithful input for its scan logic.

opcode DedustSwapExternal     = 0x61ee542d
opcode DedustSwapPeer         = 0x72aca8aa
opcode DedustSwap             = 0xea06185d
opcode DedustSwapNotification = 0x9c610de3
opcode DedustPayoutFromPool   = 0xad4eb6f5
opcode DedustPayout           = 0x474f86cf

# One pool hop per unrolling: a peer (or nested external) swap request with an
# optional notification child; exit = the terminal payout chain.
rule peer_chain = (
    op DedustSwapPeer @peer
      -> { maybe op DedustSwapNotification @peer_notification }
      -> maybe $peer_chain
  ^ op DedustSwapExternal @peer
      -> { maybe op DedustSwapNotification @peer_notification }
      -> maybe $peer_chain
  ^ op DedustPayoutFromPool -> ( op DedustPayout | btype jetton_transfer )
)

# HYBRID declarative build. The reject/failed wiring, `dex`, and the TON-in
# sender/source_asset are declarative; the imperative residue stays in two host
# functions:
#   dedust_swap_legs: peer-swap list assembly (element-wise list-to-record
#     transform is inexpressible), payout resolution, and destination_asset
#     (a bounded async pool-hop walk over parsed BOC swap-steps — inexpressible);
#   dedust_swap_failed: the notification-coverage scan over the consumed set.
# Data access, record literals, and first/last over a notification-list capture
# could pull the in-leg, payout, and single-hop leg records into `out`,
# shrinking dedust_swap_legs to just peer_swaps + destination_asset.
matcher dedust_swap {
    produces JettonSwapBlock
    entry    @ext

    op DedustSwapExternal @ext
      -> {
           ( op DedustPayoutFromPool @payout
               -> ( btype jetton_transfer @out_jetton | op DedustPayout @out_ton )
           | $peer_chain )
           maybe op DedustSwapNotification @notification
         }
      <- ( btype jetton_transfer @in_transfer | op DedustSwap @swap_request )

    let legs = dedust_swap_legs(consumed, in_transfer, swap_request)
    reject when legs == null
    failed when dedust_swap_failed(consumed)
    out {
        dex:                   "dedust"
        sender:                account(swap_request.msg.source) if in_transfer == null else legs.sender
        source_asset:          ton_asset() if in_transfer == null else legs.asset_in
        destination_asset:     legs.destination_asset
        dex_incoming_transfer: legs.dex_incoming_transfer
        dex_outgoing_transfer: legs.dex_outgoing_transfer
        in:                    legs.in_leg
        out:                   legs.out_leg
        peer_swaps:            legs.peer_swaps
    }
}
