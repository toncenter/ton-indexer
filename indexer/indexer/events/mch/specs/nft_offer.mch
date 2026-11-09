# An empty-body TON transfer deploys and funds an NFT offer.

opcode NftOfferCancelByBuyer = 0x69CC5721
opcode NftOfferCancel        = 0x474D0200

matcher nft_offer_create {
    produces nft_offer_create
    entry    @deploy

    btype ton_transfer @deploy

    let status = deploy.msg.transaction.orig_status
    reject when deploy.msg.opcode != null or (status != "nonexist" and status != "uninit")
    let offer = nft_offer_init(deploy)
    let nft   = lookup nft_item(offer.nft)
    reject when offer == null or account(deploy.msg.source) != offer.buyer
    failed when (deploy.msg.transaction.aborted ?? false)
    out {
        source:          offer.buyer
        destination:     account(deploy.msg.destination)
        amount:          amount(deploy.msg.value)
        nft_address:     offer.nft
        nft_collection:  account(nft.collection_address)
        nft_index:       nft.index
        full_price:      amount(offer.price)
        royalty_amount:  amount(offer.royalty_amount)
        royalty_address: offer.royalty_address
    }
}

matcher nft_offer_cancel {
    produces nft_offer_cancel
    entry    @cancel

    # The fine precedes the refund even when both go to the buyer.
    op NftOfferCancelByBuyer @cancel
      -> { peek btype ton_transfer @fine
         ; btype ton_transfer @refund where (.destination == cancel.data.source) }

    let offer = lookup nft_offer(cancel.msg.destination)
    let nft   = lookup nft_item(offer.nft)
    reject when offer == null or account(cancel.msg.source) != offer.buyer
    out {
        source:          offer.buyer
        destination:     account(cancel.msg.destination)
        amount:          amount(refund.msg.value)
        nft_address:     offer.nft
        nft_collection:  account(nft.collection_address)
        nft_index:       nft.index
        royalty_amount:  amount(fine.msg.value)
        royalty_address: account(fine.msg.destination)
    }
}

matcher nft_offer_cancel_without_fine {
    produces nft_offer_cancel
    entry    @cancel

    op NftOfferCancelByBuyer @cancel
      -> maybe btype ton_transfer @refund where (.destination == cancel.data.source)

    let offer = lookup nft_offer(cancel.msg.destination)
    let nft   = lookup nft_item(offer.nft)
    reject when offer == null or account(cancel.msg.source) != offer.buyer
    # An unpaid fine aborts the action phase and emits no messages.
    failed when (cancel.msg.transaction.aborted ?? false)
    out {
        source:         offer.buyer
        destination:    account(cancel.msg.destination)
        amount:         amount(refund.msg.value)
        nft_address:    offer.nft
        nft_collection: account(nft.collection_address)
        nft_index:      nft.index
    }
}

matcher nft_offer_external_cancel {
    produces nft_offer_expire | nft_offer_signed_cancel
    entry    @cancel

    op NftOfferCancel @cancel where (.source == null)
      -> btype ton_transfer @refund

    let offer = lookup nft_offer(cancel.msg.destination)
    let nft   = lookup nft_item(offer.nft)
    reject when offer == null or account(refund.msg.destination) != offer.buyer
    produces switch {
        # Before expiry the offer accepts the external only with its key's signature.
        when cancel.msg.transaction.now >= offer.expires_at => nft_offer_expire out {
            source:         offer.buyer
            destination:    account(cancel.msg.destination)
            amount:         amount(refund.msg.value)
            nft_address:    offer.nft
            nft_collection: account(nft.collection_address)
            nft_index:      nft.index
        }
        else => nft_offer_signed_cancel out {
            source:         offer.buyer
            destination:    account(cancel.msg.destination)
            amount:         amount(refund.msg.value)
            nft_address:    offer.nft
            nft_collection: account(nft.collection_address)
            nft_index:      nft.index
        }
    }
}

matcher nft_offer_accept {
    produces nft_offer_accept
    entry    @accept

    # The offer pays the seller first and sends the rest of its balance to the royalty address last.
    btype nft_transfer @accept
      -> { btype ton_transfer @payout where (.destination == accept.data.prev_owner)
         ; btype nft_transfer @onward
         ; peek btype ton_transfer @rest }

    let offer = lookup nft_offer(accept.data.new_owner)
    reject when offer == null or account(accept.data.nft.address) != offer.nft
    # A fake NFT named in the offer can report any previous owner.
    reject when account(accept.data.prev_owner) != offer.seller
    reject when account(onward.data.new_owner) != offer.recipient
    reject when account(rest.msg.destination) != offer.royalty_address
    # An NFT with its own code can imitate these outgoing messages.
    let offer_address = account(accept.data.new_owner)
    reject when account(payout.msg.source) != offer_address or account(rest.msg.source) != offer_address
    reject when account(onward.data.prev_owner) != offer_address or account(onward.data.nft.address) != offer.nft
    # The NFT goes on non-bounceable, so a rejected transfer leaves it with the offer.
    failed when onward.failed
    out {
        source:          offer.seller
        destination:     offer.recipient
        offer:           account(accept.data.new_owner)
        amount:          amount(payout.msg.value)
        nft_address:     offer.nft
        nft_collection:  account(accept.data.nft.collection.address)
        nft_index:       accept.data.nft.index
        query_id:        accept.data.query_id
        price:           amount(offer.price + offer.royalty_amount)
        royalty_amount:  amount(rest.msg.value)
        royalty_address: offer.royalty_address
    }
}
