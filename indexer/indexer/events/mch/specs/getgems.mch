# Getgems NFT purchase matcher corresponding to GetgemsNftPurchaseBlockMatcher.
#
# The legacy matcher anchors on the already-produced getgems `nft_transfer`
# block, then imperatively finds the ton_transfer whose destination is the real
# previous owner (the seller payout). It has two branches:
#   * normal: the payout is a direct CHILD of the nft_transfer;
#   * finish/stop sale contract: the nft_transfer's `previous_block` is a
#     ton_transfer with comment "finish"/"stop", and the payout is that parent's
#     CHILD (a SIBLING of the nft_transfer). In that branch legacy inserts an
#     EmptyBlock proxy between the parent and [nft_transfer, payout]
#     (nft.py `insert_between`), leaving the parent itself UNCONSUMED.
#
# The internal walk may differ as long as the output is
# identical. All three nft.yaml getgems fixtures (direct purchase + two auction
# purchases) are the normal shape, expressed here as a plain child edge; the
# seller is picked by a host predicate reusing the exact legacy comparison
# (`payout.destination == nft.real_prev_owner`), so a non-seller sibling
# ton_transfer (marketplace fee, royalty) is skipped and the right child binds
# regardless of order. The child edge is `maybe` because the finish/stop branch
# has NO payout child (the payout is a sibling, reachable only through the
# UNCONSUMED parent — a block the mch tree grammar cannot capture without
# consuming it, which would break the consumed-set / topology parity). That
# branch therefore matches with `@payout` unbound; the builder finds the sibling
# payout for the data fields and the shaper consumes it + inserts the proxy.
#
# The builder reuses the legacy field logic via build_getgems_nft_purchase_core
# (blocks/nft.py) — the same function the legacy matcher's thin wrapper calls.
#
# `shape getgems_proxy_insert` carries the finish/stop branch: it
# lifts the verbatim legacy EmptyBlock `insert_between` surgery and, because the
# sibling payout was not pattern-consumed, absorbs it into the produced block at
# the same point legacy's single merge did. It is guarded by the same finish/
# stop-parent condition the legacy build uses, so on the normal corpus shape it
# is a no-op and topology parity holds by construction.
#
# Capture -> legacy role map:
#   @nft      the getgems nft_transfer block (anchor; legacy test_self target)
#   @payout   the seller ton_transfer, when it is a child (normal shape only)

predicate getgems_purchase
predicate getgems_seller_payout

matcher getgems_nft_purchase {
    produces NftPurchaseBlock
    shape    getgems_proxy_insert
    entry    @nft

    btype nft_transfer @nft where getgems_purchase
      -> maybe btype ton_transfer @payout where getgems_seller_payout

    # HYBRID declarative build (C++-runnable). The imperative residue (seller
    # payout scan + core field build) is the one host fn; every produced field
    # is mapped declaratively from its namespace.
    let d = getgems_nft_purchase_data(nft)
    reject when d == null
    out {
        nft_address:              d.nft_address
        collection_address:       d.collection_address
        nft_index:                d.nft_index
        prev_owner:               d.prev_owner
        new_owner:                d.new_owner
        query_id:                 d.query_id
        forward_amount:           d.forward_amount
        response_destination:     d.response_destination
        custom_payload:           d.custom_payload
        forward_payload:          d.forward_payload
        payout_amount:            d.payout_amount
        payout_comment_encrypted: d.payout_comment_encrypted
        payout_comment_encoded:   d.payout_comment_encoded
        payout_comment:           d.payout_comment
        price:                    d.price
        real_prev_owner:          d.real_prev_owner
        marketplace:              d.marketplace
        marketplace_address:      d.marketplace_address
    }
}
