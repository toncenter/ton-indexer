# Teleitem (Fragment) auction start / cancel — port of blocks/auction.py
# TeleitemStartAuctionMatcher (:460) and TeleitemCancelAuctionMatcher (:506).
#
# Both legacy matchers are already declarative in shape — a single anchor opcode
# plus one optional child (`labeled('teleitem_ok', ContractMatcher(0xa37a0983,
# optional=True))`, include_excess=False) — so the pattern is a literal
# transcription. The build bodies are field mapping over one interface lookup,
# one body parse and one content predicate, which is what `lookup` / `parse` /
# `reject when` cover; nothing here needs a build core.
#
# The teleitem gate
# -----------------
# Both matchers reject unless the destination resolves to an NftItem whose
# metadata `uri` contains "https://nft.fragment.com" (blocks/auction.py
# `_is_teleitem`). That is a SUBSTRING test, and the expression language has no
# string manipulation by design, so it is the single host `fn`
# `is_teleitem(nft)` — registered in builders/teleitem_auction.py, where the
# Python leg calls the legacy `_is_teleitem` verbatim.
#
# Ordering
# --------
# Neither anchor (0x487a8e81 / 0x371638ae) is claimed by any other spec, so this
# file's position in the built-in source list carries no tie-break; it is listed
# where the file-name sort puts it, which also happens to mirror the legacy
# matchers list (both Teleitem matchers sit right after the telegram/getgems nft
# purchase matchers).
#
# Deviations from legacy, both unobservable on the corpus and documented rather
# than fixed:
#  - `parse` failure REJECTS, while the legacy `TeleitemStartAuction`
#    parser wraps its auction-config read in try/except and falls back to
#    beneficiary=None + zero bids. A truncated auction-config ref is therefore a
#    dropped row here and a zeroed row there.
#  - `failed` reads the raw `transaction.aborted`, exactly like the legacy
#    `new_block.failed = block.get_message().transaction.aborted` — NOT the
#    anchor block's own `.failed`, which CallContractBlock un-sets for an aborted
#    zero-value call (basic_blocks.py:86).
#
# Capture -> legacy role map:
#   @auction / @cancel   the anchor call (`test_self` target, `block` in build_block)
#   @ok                  the optional 0xa37a0983 reply (legacy `other_blocks`)

opcode TeleitemStartAuction  = 0x487A8E81
opcode TeleitemCancelAuction = 0x371638AE
opcode TeleitemOk            = 0xA37A0983

matcher teleitem_start_auction {
    produces        teleitem_start_auction
    entry           @auction
    include_excess  false

    op TeleitemStartAuction @auction -> maybe op TeleitemOk @ok

    parse auction as TeleitemStartAuction
    let nft = lookup nft_item(auction.msg.destination)
    reject when nft == null
    reject when not is_teleitem(nft)
    # `AccountId(...)` is unconditional in the legacy build, so a missing address
    # becomes addr_none (present-but-empty), not null.
    let item  = account(auction.msg.destination) ?? addr_none()
    let owner = account(auction.msg.source) ?? addr_none()
    failed when (auction.msg.transaction.aborted ?? false)
    out {
        nft_address:         item
        nft_index:           nft.index
        nft_collection:      account(nft.collection_address)
        owner:               owner
        listing_address:     null
        auction_address:     item
        marketplace_address: null
        marketplace:         "fragment"
        mp_fee_address:      account(auction.body.config.ref.beneficiary_address)
        mp_fee_factor:       amount(0)
        mp_fee_base:         amount(0)
        royalty_fee_addr:    null
        royalty_fee_base:    amount(0)
        max_bid:             amount(auction.body.config.ref.max_bid)
        min_bid:             amount(auction.body.config.ref.initial_min_bid)
    }
}

matcher teleitem_cancel_auction {
    produces        teleitem_cancel_auction
    entry           @cancel
    include_excess  false

    op TeleitemCancelAuction @cancel -> maybe op TeleitemOk @ok

    let nft = lookup nft_item(cancel.msg.destination)
    reject when nft == null
    reject when not is_teleitem(nft)
    let item = account(cancel.msg.destination) ?? addr_none()
    failed when (cancel.msg.transaction.aborted ?? false)
    out {
        nft_address:         item
        # Unconditional AccountId in the legacy build (unlike the start
        # matcher's guarded one) — a collection-less item yields addr_none.
        nft_collection:      account(nft.collection_address) ?? addr_none()
        owner:               account(cancel.msg.source) ?? addr_none()
        trade_contract:      item
        marketplace_address: null
    }
}
