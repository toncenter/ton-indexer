# A fixed-price sale of one NFT. The address of the sale's initial state proves its code and terms;
# they come from a deploy in the same trace or from the account data.

opcode FixedSaleBuy         = 0x474D0001
opcode FixedSaleChangePrice = 0x474D0002
opcode FixedSaleCancel      = 0x474D0003

predicate state_init_deploy

matcher nft_fixed_sale_create {
    produces nft_fixed_sale_create
    entry    @deposit

    # A listing starts when the sale accepts the NFT; the sale may be deployed in this trace or earlier.
    btype nft_transfer @deposit
      <- maybe peek any @wallet
        -> { maybe peek pred state_init_deploy @deploy where (.destination == deposit.data.new_owner) }

    # A pending trace cannot read a sale deployed in the same trace from the database.
    let stored = lookup nft_fixed_sale(deposit.data.new_owner)
    let sale   = nft_fixed_sale_init(deploy) ?? stored
    # A refused deposit sends the NFT back; an unpaid or invalid one aborts.
    reject when sale == null or not nft_ownership_accepted(deposit)
    reject when account(deposit.data.nft.address) != sale.nft
    out {
        source:          account(deposit.data.prev_owner)
        destination:     account(deposit.data.new_owner)
        seller:          sale.seller
        amount:          null
        nft_address:     sale.nft
        nft_collection:  account(deposit.data.nft.collection.address)
        nft_index:       deposit.data.nft.index
        full_price:      amount(sale.initial_price)
        fee_amount:      amount(nft_fixed_sale_fee(sale, sale.initial_price))
        fee_address:     sale.fee_address
    }
}

matcher nft_fixed_sale_update {
    produces nft_fixed_sale_update
    entry    @update

    op FixedSaleChangePrice @update where (.source != null)

    let sale  = lookup nft_fixed_sale(update.msg.destination)
    let nft   = lookup nft_item(sale.nft)
    let price = nft_fixed_sale_new_price(update)
    reject when sale == null or price == null or account(update.msg.source) != sale.seller
    failed when (update.msg.transaction.aborted ?? false)
    out {
        source:          sale.seller
        destination:     account(update.msg.destination)
        amount:          null
        nft_address:     sale.nft
        nft_collection:  account(nft.collection_address)
        nft_index:       nft.index
        full_price:      amount(price)
        fee_amount:      amount(nft_fixed_sale_fee(sale, price))
        fee_address:     sale.fee_address
    }
}

matcher nft_fixed_sale_purchase {
    produces nft_fixed_sale_purchase
    entry    @buy

    # The sale pays the seller before it sweeps the rest to the fee address.
    op FixedSaleBuy @buy where (.source != null)
      -> { btype ton_transfer @payout
         ; btype nft_transfer @onward
         ; peek btype ton_transfer @fee }

    let sale = lookup nft_fixed_sale(buy.msg.destination)
    reject when sale == null
    let sale_address = account(buy.msg.destination)
    reject when account(payout.msg.destination) != sale.seller or account(fee.msg.destination) != sale.fee_address
    reject when account(onward.data.prev_owner) != sale_address or account(onward.data.nft.address) != sale.nft
    # The NFT goes on non-bounceable, so a rejected transfer leaves it with the sale.
    failed when onward.failed
    out {
        source:          sale.seller
        destination:     account(onward.data.new_owner)
        buyer:           account(buy.msg.source)
        escrow:          sale_address
        amount:          amount(payout.msg.value)
        nft_address:     sale.nft
        nft_collection:  account(onward.data.nft.collection.address)
        nft_index:       onward.data.nft.index
        query_id:        onward.data.query_id
        price:           amount(payout.msg.value)
        fee_amount:      amount(nft_fixed_sale_fee(sale, payout.msg.value))
        fee_address:     sale.fee_address
    }
}

matcher nft_fixed_sale_cancel {
    produces nft_fixed_sale_cancel
    entry    @cancel

    op FixedSaleCancel @cancel where (.source != null)
      -> maybe btype nft_transfer @back where (.prev_owner == cancel.data.destination and .new_owner == cancel.data.source)

    let sale = lookup nft_fixed_sale(cancel.msg.destination)
    let nft  = lookup nft_item(sale.nft)
    reject when sale == null or account(cancel.msg.source) != sale.seller
    failed when (cancel.msg.transaction.aborted ?? false) or (back.failed ?? false)
    out {
        source:          sale.seller
        destination:     account(cancel.msg.destination)
        amount:          null
        nft_address:     sale.nft
        nft_collection:  account(nft.collection_address)
        nft_index:       nft.index
    }
}
