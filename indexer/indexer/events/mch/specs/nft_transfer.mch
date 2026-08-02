# Base NFT transfer matcher corresponding to NftTransferBlockMatcher.
#
# This is the base matcher that produces the `nft_transfer` btype consumed
# downstream by specs/getgems.mch (the getgems purchase anchors on it).
# nft_transfer must
# register before getgems so getgems sees the produced block; that ordering is
# wired in event_processing.py.
#
# Matcher shape
# Anchor: the NftTransfer call. Children: the optional NftOwnershipAssigned
# notification (source of `prev_owner`) and the optional excess. The legacy
# child_matcher is an OrMatcher of {NftOwnershipAssigned?, Excess?} that merges
# whichever children match into the nft_transfer. `include_excess` at the root
# only *appends* the anchor's excess to the result list and never merges it,
# so it would not reproduce the legacy topology where the excess is a child of
# the nft_transfer. The excess is therefore matched explicitly in a children set
# (consumed => merged) and `include_excess` is turned off to avoid a duplicate
# standalone append. include_bounces stays default true (legacy appends bounces
# standalone too; a bounce is never merged into the nft_transfer on either path).
#
# Purchase-parent consumption (getgems)
# -------------------------------------
# When the interface lookups (nft_sale / nft_auction) confirm a getgems
# purchase, the legacy build sets `is_purchase` and marketplace fields and absorbs
# the funding parent (buyer payment / sale-contract call) into the nft_transfer
# except when the parent is a finish/stop sale-contract ton_transfer. That parent
# is upstream of the anchor, so it cannot ride in the pattern-consumed set. The
# builder returns it; the shaper `nft_transfer_parent_absorb` merges it as the
# last build step, after the wrapper merge has placed the produced block
# at the anchor's position (so produced.previous_block is the funding parent).
# On a plain transfer is_purchase is false and the shaper is a no-op.
#
# Hybrid shell (C++-runnable): the data build needs the interface repository
# (`_get_nft_data`, `get_nft_sale`, `get_nft_auction`) and fragment-URI parsing
# the DSL expression language cannot express. The wrapper (btype, reject, out
# mapping) remains declarative. The data
# + purchase detection stay in build_nft_transfer_core (blocks/nft.py), the same
# core the legacy thin wrapper calls, wrapped by the `nft_transfer_data` host fn.
#
# Capture -> legacy role map:
#   @transfer   the NftTransfer anchor (test_self target)
#   @assigned   the NftOwnershipAssigned notification (prev_owner), when present

opcode NftTransfer          = 0x5FCC3D14
opcode NftOwnershipAssigned = 0x05138D91
opcode ExcessMessage        = 0xD53276DB

matcher nft_transfer {
    produces        NftTransferBlock
    shape           nft_transfer_parent_absorb
    entry           @transfer
    include_excess  false

    op NftTransfer @transfer
      -> { maybe op NftOwnershipAssigned @assigned
         ; maybe op ExcessMessage @excess }

    # HYBRID declarative build (C++-runnable). The core (NftTransfer/ownership
    # parse, nft-item lookup, nft_sale/nft_auction purchase detection) is the one
    # host fn; fields map from its namespace. The purchase-only marketplace/price/
    # real_prev_owner keys are `?:` (omit-when-null) to match the legacy variable-
    # key dict; the nested `nft` dict passes through as a field value.
    let d = nft_transfer_data(transfer, assigned)
    reject when d == null
    out {
        is_purchase:          d.is_purchase
        prev_owner:           d.prev_owner
        new_owner:            d.new_owner
        query_id:             d.query_id
        forward_amount:       d.forward_amount
        response_destination: d.response_destination
        custom_payload:       d.custom_payload
        forward_payload:      d.forward_payload
        nft:                  d.nft
        marketplace?:         d.marketplace
        marketplace_address?: d.marketplace_address
        price?:               d.price
        real_prev_owner?:     d.real_prev_owner
    }
}
