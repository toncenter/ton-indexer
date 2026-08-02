# Telegram (Fragment) NFT purchase matcher corresponding to
# TelegramNftPurchaseBlockMatcher.
#
# The legacy matcher is a workaround: it anchors on the NftOwnershipAssigned
# call, then imperatively walks `previous_block` and scans that parent's
# next_blocks for AuctionFillUp payouts inside build_block (no declarative
# structure). The internal walk may differ as long as the output is identical;
# here the whole shape is expressed declaratively:
#
#   the funding payment (a ton_transfer to the marketplace, or the external
#   message that drove the NFT item directly) is the anchor's PARENT; the two
#   AuctionFillUp payouts (royalty then payout, by created_lt) are that parent's
#   children. In the `A <- B -> {C; D}` shape, the parent edge is
#   anchor-headed, and children after a parent edge attach to the parent matcher
#   rather than to the anchor.
#
# The AuctionFillUp children set is what makes the parent edge fire only on
# real fragment purchases (an ordinary orphan NftOwnershipAssigned has no such
# siblings), so no data predicate is needed to gate the parent consumption.
# The builder reuses the legacy field logic via build_telegram_nft_purchase_core;
# is_purchase / mint / len(payouts) branching all live there, driven by the
# captured @assigned / @payment / @payout_* blocks.
#
# Two matchers, split on the legacy CONSUMED SET
# ----------------------------------------------
# Legacy `build_block` always produces a block, but only absorbs the parent and
# the payouts inside one branch — `payload is TeleitemBidInfo` AND the parent is
# a ton_transfer or an EXTERNAL-driven call (`get_message().source is None`).
# Every other assignment (telemint mint parent, orphan assignment) merges the
# ANCHOR ALONE and leaves the parent standing, which is what keeps the separate
# `nft_mint` action alive next to the `nft_transfer` one. A single mch pattern
# cannot express both consumed sets, so the split is by matcher:
#
#   telegram_nft_purchase   the consuming branch. Parent XOR-alternative over
#                           the two legacy parent kinds, payouts attached inside
#                           each branch (a children block hung off the `or` node
#                           itself is silently dropped by the engines).
#   telegram_nft_assigned   the non-consuming branch: bare anchor, no edges, so
#                           the consumed set is exactly `[block]`. Runs second,
#                           over whatever the purchase matcher did not claim.
#
# The parent still reaches the shared core in the second matcher: the host fn
# falls back to `assigned.previous_block` when @payment is unbound, which is
# where legacy reads it from anyway. That is what makes the telemint mint parent
# (call_contract 0x299a3e15, or the `nft_mint` block the priority-80 matcher
# already built from it) flip `is_purchase` to false.
#
# Residual gap (documented, not in the corpus): a teleitem-bid assignment with a
# ton_transfer/external parent and ZERO AuctionFillUp payouts. Legacy absorbs the
# parent; here the purchase matcher misses (payout_1 is required, which is the
# gate that keeps it off ordinary transfers) and the assigned matcher produces
# the same row without absorbing the parent. Row fields are identical; only the
# leftover parent block differs.
#
# Capture -> legacy role map:
#   @assigned   the NftOwnershipAssigned anchor
#   @payment    block.previous_block (the funding ton_transfer, or the external
#               call that drove the item directly — Fragment signer shape)
#   @payout_1   payouts[0] after created_lt sort (royalty / sending fee)
#   @payout_2   payouts[1] after created_lt sort (seller payout), absent for 1-payout traces

opcode NftOwnershipAssigned = 0x05138D91
opcode AuctionFillUp        = 0x370FEC51

matcher telegram_nft_purchase {
    produces NftTransferBlock
    entry    @assigned

    # The two legacy parent kinds, XOR because `ton_transfer` and `call_contract`
    # are disjoint btypes. The payout children hang off EACH branch, not off the
    # alternative: `_match_or` walks branches only, so children recorded on the
    # `or` node itself would never match and never consume.
    op NftOwnershipAssigned @assigned
      <- ( btype ton_transfer @payment
             -> { op AuctionFillUp @payout_1 ; maybe op AuctionFillUp @payout_2 }
         ^ btype call_contract @payment where (.source == null)
             -> { op AuctionFillUp @payout_1 ; maybe op AuctionFillUp @payout_2 } )

    # HYBRID declarative build (C++-runnable). The core (nft-data lookup, teleitem
    # payload dispatch, payout ordering) is the one host fn; fields map from its
    # namespace. Pattern requires a ton_transfer / external-call parent + >=1
    # AuctionFillUp payout => always the fragment purchase shape (is_purchase true,
    # real_prev_owner present-null, one/two payouts) — so all but the royalty pair
    # are plain; the royalty pair (2-payout only) is `?:` to match the legacy
    # conditional keys.
    let d = telegram_nft_purchase_data(assigned, payment, payout_1, payout_2)
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
        real_prev_owner:      d.real_prev_owner
        price:                d.price
        marketplace:          d.marketplace
        payout_amount:        d.payout_amount
        payout_address:       d.payout_address
        royalty_amount?:      d.royalty_amount
        royalty_address?:     d.royalty_address
    }
}

matcher telegram_nft_assigned {
    produces NftTransferBlock
    entry    @assigned

    # No edges at all: the consumed set is the anchor alone, which is the legacy
    # merge for every assignment outside the purchase branch. The parent is read,
    # not matched — the host fn falls back to `assigned.previous_block` when
    # @payment is unbound, exactly where legacy's `build_block` reads it.
    op NftOwnershipAssigned @assigned

    let d = telegram_nft_purchase_data(assigned, null, null, null)
    reject when d == null

    # A `produces switch`, not `?:` keys, because the legacy key set here is
    # BRANCH-shaped, not null-shaped. `payload is TeleitemBidInfo` sets all three
    # of price / marketplace / real_prev_owner — and sets real_prev_owner to None
    # explicitly, a PRESENT key holding null. `?:` is omit-when-null, so it can
    # emit either the present-null key or nothing, never one per branch. The
    # switch condition is `d.marketplace != null`, which is exactly that branch
    # (the core sets marketplace nowhere else). No payout/royalty keys in either
    # branch: this matcher's shapes never reach the parent-absorbing code path —
    # a ton_transfer / external parent belongs to telegram_nft_purchase.
    produces switch {
        when d.marketplace != null => NftTransferBlock out {
            is_purchase:          d.is_purchase
            prev_owner:           d.prev_owner
            new_owner:            d.new_owner
            query_id:             d.query_id
            forward_amount:       d.forward_amount
            response_destination: d.response_destination
            custom_payload:       d.custom_payload
            forward_payload:      d.forward_payload
            nft:                  d.nft
            real_prev_owner:      d.real_prev_owner
            price:                d.price
            marketplace:          d.marketplace
        }
        else => NftTransferBlock out {
            is_purchase:          d.is_purchase
            prev_owner:           d.prev_owner
            new_owner:            d.new_owner
            query_id:             d.query_id
            forward_amount:       d.forward_amount
            response_destination: d.response_destination
            custom_payload:       d.custom_payload
            forward_payload:      d.forward_payload
            nft:                  d.nft
        }
    }
}
