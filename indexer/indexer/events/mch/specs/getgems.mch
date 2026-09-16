# GetGems purchases consume the NFT and its direct seller payout. Finish/stop
# purchases peek the sale parent and sibling payout, which the shaper absorbs.

# The two arms are independent; a mixed tree can bind both, with the sibling
# selected for output while the matching child remains consumed.

matcher getgems_nft_purchase {
    produces NftPurchaseBlock
    shape    getgems_proxy_insert
    entry    @nft

    btype nft_transfer @nft where (.is_purchase and .marketplace == "getgems")
      -> { maybe btype ton_transfer @payout where (.destination == nft.data.real_prev_owner) }
      <- maybe peek btype ton_transfer where (.comment == "finish" or .comment == "stop")
           -> { maybe peek btype ton_transfer @sib_payout where (.destination == nft.data.real_prev_owner) }

    let seller_payout = sib_payout ?? payout
    reject when seller_payout == null
    out {
        nft_address:              nft.data.nft.address
        collection_address:       nft.data.nft.collection.address
        nft_index:                nft.data.nft.index
        prev_owner:               nft.data.prev_owner
        new_owner:                nft.data.new_owner
        query_id:                 nft.data.query_id
        forward_amount:           nft.data.forward_amount
        response_destination:     nft.data.response_destination
        custom_payload:           nft.data.custom_payload
        forward_payload:          nft.data.forward_payload
        payout_amount:            seller_payout.data.value
        payout_comment_encrypted: seller_payout.data.encrypted
        payout_comment_encoded:   seller_payout.data.comment_encoded
        payout_comment:           seller_payout.data.comment
        price:                    nft.data.price
        real_prev_owner:          nft.data.real_prev_owner
        marketplace:              nft.data.marketplace
        marketplace_address:      nft.data.marketplace_address
    }
}
