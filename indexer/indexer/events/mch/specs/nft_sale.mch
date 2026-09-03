# GetGems and Fragment NFT sale and auction matchers from blocks/auction.py:
# UpdateSaleMatcher (:642), NftPutOnSaleBlockMatcher (:235), NftCancelSaleMatcher
# (:359), NftFinishAuctionMatcher (:419), NftCancelAuctionMatcher (:388),
# AuctionBidMatcher (:53), AuctionOutbidMatcher (:133), plus blocks/nft.py
# NftDiscoveryBlockMatcher (:298).
#
# One file because the eight matchers share a lookup surface (nft_sale /
# nft_auction / nft_item), two serializer fills, and, for three of them, an
# anchor. Matcher order in this file IS registration order, and it reproduces the
# legacy relative order verbatim; see "Ordering" below for what actually depends
# on it.
#
# Ordering
# --------
# Position in the built-in source list: AFTER specs/getgems.mch and
# specs/nft_transfer.mch, listed next to specs/teleitem_auction.mch.
#
#  - `nft_put_on_sale` anchors `btype nft_transfer`, the same anchor
#    specs/getgems.mch uses. Legacy runs GetgemsNftPurchaseBlockMatcher first, so
#    getgems.mch must stay earlier in the manifest. This is a cross-file
#    ordering constraint.
#  - `nft_cancel_sale` / `nft_cancel_auction` peek an `nft_transfer` and
#    `nft_finish_auction` peeks an `nft_purchase`, so both producer specs must
#    register earlier. They do.
#  - `nft_finish_auction` is listed BEFORE `nft_cancel_auction` here because
#    legacy does (event_processing.py: NftFinishAuctionMatcher then
#    NftCancelAuctionMatcher). Their anchors OVERLAP on the stop/finish opcodes
#    and on a finish/stop-comment ton_transfer; the two are separated only by
#    the block they look for (nft_purchase vs nft_transfer), and on a trace
#    carrying both the earlier registration wins. Do not reorder.
#  - `auction_outbid` anchors on the outbid LEG but reads the `auction_bid`
#    through `.previous_block`, so it still must register after `auction_bid` —
#    two matchers earlier in this same file.
#  - `nft_update_sale` and `nft_discovery` own their anchor opcodes outright
#    (0x6c6c2080 / 0x2fcb26a2), so their position carries no tie-break at all,
#    even though legacy registers `nft_update_sale` much earlier (ahead of
#    NftTransferBlockMatcher).
#
# `code_hash`
# -----------
# `nft_item.code_hash`, `nft_sale.code_hash` and `nft_auction.code_hash` did not
# are required on the C++ lookup shapes. They are supplied by
# ParsedBlockLookupSource.cpp, fixtures/FixtureLookupSource.cpp, and
# emu/EmuClassifierBridge.cpp. No detector Result carries a code hash, so the emulator
# leg computes it from the account state cell the detector ran on). Zero renders
# as null, so an unknown code hash reads as a MISS, never as a wrong version
# string. Consumers: the DNS-item test inside `auction_bid_data`, and the GetGems
# StateInit version dispatch in `getgems_sale_init` / `getgems_auction_init`.
#
# `init_state`
# ------------
# NftPutOnSale reads the sale parameters out of the DEPLOY MESSAGE's StateInit
# (`sale_init.get_message().init_state.body`), not out of the account's current
# interface snapshot. The two are NOT interchangeable: a later UpdateSale rewrites
# full_price on the live contract while the StateInit keeps the price the listing
# was created at, so reading the interface instead would silently produce a
# different number than Python on exactly the traces that matter. `init_state` is
# therefore plumbed onto the mch `Message` (TraceLoader.h) and read by two host
# fns — no DSL surface change. The data was already present on
# all three loader paths (fixture .lz4, schema/prod, emulator); only the readers
# were missing.
#
# Host bindings and why each one exists (the expression language cannot do it):
#   fn  getgems_sale_init(block, code_hash)     StateInit parse, per-code-hash
#   fn  getgems_auction_init(block, code_hash)  version dispatch (getgems.py
#                                               get_sale_data / get_auction_data)
#   fn  auction_bid_data(block)                 CHAINED lookups — the auction's
#                                               nft_addr feeds an nft_item lookup,
#                                               which the DSL forbids
#                                               (B004_LOOKUP_DEPENDS_ON_LOOKUP)
#   pred sale_contract_deploy                   btype in {ton_transfer,
#                                               call_contract} AND
#                                               len(contract_deployments) > 0
#   pred nft_trade_cancel_comment               comment in {cancel,finish,stop}
#   pred nft_trade_finish_comment               comment in {finish,stop}
#   pred auction_bid_candidate                  cheap pre-lookup gate (no
#                                               deployments, not a cancel comment)
#   pred auction_outbid_leg                     requires exactly one qualifying
#                                               child of the peeked auction_bid
#
# Differences from the runtime matchers:
#  - `NftCancelSaleMatcher` / `NftCancelAuctionMatcher` reach the collection via
#    `NftTransferBlock.get_nft_collection()` (blocks/nft.py:30), which indexes
#    `data['nft']['collection']['address']` after testing only that the
#    'collection' KEY exists. The key always exists and is None for a
#    collection-less item, so legacy raises TypeError there and `try_build`
#    swallows it into a non-match. The peeked field read null-propagates instead,
#    so such a trace yields a row with a null collection where legacy yielded nothing.
#  - Legacy's `assert` guards inside NftPutOnSale (nft/owner cross-checks against
#    the interface) raise and are swallowed the same way; they are plain
#    `reject when`s here. Same outcome, different mechanism.
#  - `amount(null)` is null, while an unconditional legacy `Amount(x)` with x
#    None is Amount(None). Both serialize to a null column (`_value`), so the
#    difference stops at the block data.
#
# Capture -> legacy role map:
#   @upd        the SaleUpdate call            | @listing   transfer to the listing
#   @sale_init  the deploying message          | @to_sale   transfer to the sale
#   @disc/@report  discovery call / its report | @cancel    the cancel/finish anchor
#   @bid        the bid ton_transfer, or the peeked auction_bid parent
#   @leg        the outbid leg (getgems ton_transfer OR fragment call)
#   @ret        the returned nft_transfer / nft_purchase, peeked without consuming

opcode SaleUpdate          = 0x6C6C2080
opcode NftDiscovery        = 0x2FCB26A2
opcode NftReportStaticData = 0x8B771735
opcode NftSaleCancel       = 0x3
opcode NftAuctionCancel    = 0x5616C572
opcode NftAuctionFinish    = 0x20C9EB18
opcode NftAuctionStop      = 0xB95616B6
opcode TeleitemOutbid      = 0x557CEA20

predicate sale_contract_deploy
predicate nft_trade_cancel_comment
predicate nft_trade_finish_comment
predicate auction_bid_candidate
predicate auction_outbid_leg

matcher nft_update_sale {
    produces nft_update_sale
    entry    @upd

    op SaleUpdate @upd

    parse upd as SaleUpdateMessage
    let sale = lookup nft_sale(upd.msg.destination)
    reject when sale == null
    failed when (upd.msg.transaction.aborted ?? false)
    out {
        sender:              account(upd.msg.source) ?? addr_none()
        sale_contract:       account(upd.msg.destination) ?? addr_none()
        nft_address:         account(sale.nft_address) ?? addr_none()
        new_full_price:      amount(upd.body.new_full_price)
        new_marketplace_fee: amount(upd.body.new_mp_fee)
        new_royalty_amount:  amount(upd.body.new_royalty_amount)
        marketplace_address: account(sale.marketplace_address) ?? addr_none()
    }
}

# The listing shape: the owner sends the NFT to a listing/marketplace address,
# which deploys the sale (or auction) contract and forwards the item to it. Both
# children are mandatory, exactly like the legacy `children_matchers` pair.
matcher nft_put_on_sale {
    produces        nft_put_on_sale | nft_put_on_auction
    entry           @listing
    include_excess  false

    btype nft_transfer @listing
      -> { pred sale_contract_deploy @sale_init
         ; btype nft_transfer @to_sale }

    reject when account(to_sale.data.nft.address) != account(listing.data.nft.address)
    reject when account(to_sale.data.new_owner) != account(sale_init.msg.destination)

    # Both independent lookups run in the same batch; chaining is forbidden.
    # The switch below picks the sale branch first — legacy's
    # `if get_nft_sale(...) is not
    # None: ... else: get_nft_auction(...)`.
    let sale    = lookup nft_sale(sale_init.msg.destination)
    let auction = lookup nft_auction(sale_init.msg.destination)
    reject when sale == null and auction == null

    # Legacy's asserts, per branch.
    reject when sale != null and account(to_sale.data.nft.address) != account(sale.nft_address)
    reject when sale != null and account(listing.data.prev_owner) != account(sale.nft_owner_address)
    reject when sale == null and account(to_sale.data.nft.address) != account(auction.nft_addr)
    reject when sale == null and account(listing.data.prev_owner) != account(auction.nft_owner)

    # A null code_hash means "unknown version", which both host fns resolve the
    # same way Python's `MAPPING.get(code_hash, "latest")` does.
    let sd = getgems_sale_init(sale_init, sale.code_hash)
    let ad = getgems_auction_init(sale_init, auction.code_hash)
    reject when sale != null and sd == null
    reject when sale == null and ad == null

    produces switch {
        when sale != null => nft_put_on_sale out {
            nft_address:             account(to_sale.data.nft.address) ?? addr_none()
            nft_index:               to_sale.data.nft.index
            nft_collection:          account(to_sale.data.nft.collection.address) ?? addr_none()
            owner:                   account(sale.nft_owner_address) ?? addr_none()
            listing_address:         account(listing.data.new_owner) ?? addr_none()
            sale_address:            account(sale_init.msg.destination) ?? addr_none()
            full_price:              amount(sd.full_price)
            marketplace_address:     account(sd.marketplace_address) ?? addr_none()
            marketplace:             "getgems"
            marketplace_fee_address: account(sd.marketplace_fee_address) ?? addr_none()
            marketplace_fee:         amount(sd.marketplace_fee)
            royalty_address:         account(sd.royalty_address) ?? addr_none()
            royalty_amount:          amount(sd.royalty_amount)
        }
        # mp_fee_address / royalty_fee_addr are the two CONDITIONAL AccountId
        # calls in the legacy auction build (`if ad.mp_fee_addr else None`), so
        # they stay bare `account(...)` — null, not addr_none.
        else => nft_put_on_auction out {
            nft_address:         account(to_sale.data.nft.address) ?? addr_none()
            nft_index:           to_sale.data.nft.index
            nft_collection:      account(to_sale.data.nft.collection.address) ?? addr_none()
            owner:               account(auction.nft_owner) ?? addr_none()
            listing_address:     account(listing.data.new_owner) ?? addr_none()
            auction_address:     account(sale_init.msg.destination) ?? addr_none()
            marketplace_address: account(auction.mp_addr) ?? addr_none()
            marketplace:         "getgems"
            mp_fee_address:      account(ad.mp_fee_addr)
            mp_fee_factor:       amount(ad.mp_fee_factor)
            mp_fee_base:         amount(ad.mp_fee_base)
            royalty_fee_addr:    account(ad.royalty_fee_addr)
            royalty_fee_base:    amount(ad.royalty_fee_base)
            max_bid:             amount(ad.max_bid)
            min_bid:             amount(ad.min_bid)
        }
    }
}

# Zero lookups, zero host bindings — every field comes off the two message
# bodies. The anchor body carries only a query_id the legacy build never reads
# (it takes the report's), so the anchor is not parsed at all.
matcher nft_discovery {
    produces nft_discovery
    entry    @disc

    op NftDiscovery @disc -> op NftReportStaticData @report

    parse report as NftReportStaticData
    out {
        sender:            account(disc.msg.source) ?? addr_none()
        nft:               account(disc.msg.destination) ?? addr_none()
        result_collection: account(report.body.collection) ?? addr_none()
        result_index:      report.body.index
        query_id:          report.body.query_id
    }
}

# A bare `btype ton_transfer` anchor, like legacy's `isinstance(block,
# TonTransferBlock)`. `auction_bid_candidate` carries the two cheap legacy
# guards (no contract deployment, comment not a cancel/finish/stop) so the
# interface lookups inside the fn only run on plausible bids.
matcher auction_bid {
    produces        auction_bid
    entry           @bid
    include_excess  false

    btype ton_transfer @bid where auction_bid_candidate

    let d = auction_bid_data(bid)
    reject when d == null
    # Legacy reads the BLOCK's `failed`, not transaction.aborted.
    failed when bid.failed
    out {
        amount:         d.amount
        bidder:         d.bidder
        auction:        d.auction
        nft_address:    d.nft_address
        nft_item_index: d.nft_item_index
        nft_collection: d.nft_collection
        auction_type:   d.auction_type
    }
}

# The outbid leg remains the consumed anchor; its auction_bid parent is peeked.
# The residual predicate preserves the exactly-one qualifying-child requirement.
matcher auction_outbid {
    produces auction_outbid
    entry    @leg

    ( btype ton_transfer @leg
    ^ op TeleitemOutbid @leg )
      <- peek btype auction_bid @bid where auction_outbid_leg

    let getgems = leg.btype == "ton_transfer"
    reject when account(leg.data.source) != account(bid.data.auction)
    reject when getgems and (bid.data.auction_type != "getgems" or not contains(leg.data.comment ?? "", "Your bid has been outbid by another user"))
    reject when not getgems and bid.data.auction_type != "fragment"
    out {
        auction_address: bid.data.auction
        nft:             bid.data.nft_address
        nft_collection:  bid.data.nft_collection
        bidder:          account(leg.msg.destination) ?? addr_none()
        new_bidder:      bid.data.bidder
        amount:          amount(leg.msg.value)
        comment:         leg.data.comment if getgems else null
        auction_type:    bid.data.auction_type
    }
}

# The returned NFT is required but remains unconsumed so its action stays top-level.
matcher nft_cancel_sale {
    produces nft_cancel_sale
    entry    @cancel

    op NftSaleCancel @cancel
      -> peek btype nft_transfer @ret

    let sale = lookup nft_sale(cancel.msg.destination)
    reject when sale == null
    reject when account(ret.data.nft.address) != account(sale.nft_address)
    out {
        nft_address:         ret.data.nft.address
        nft_collection:      ret.data.nft.collection.address if ret.data.nft.collection != null else null
        owner:               ret.data.new_owner
        trade_contract:      account(cancel.msg.destination) ?? addr_none()
        marketplace_address: account(sale.marketplace_address) ?? addr_none()
    }
}

# Registered BEFORE nft_cancel_auction (see Ordering). Same anchor set minus the
# cancel opcode, and an nft_purchase child instead of an nft_transfer one: this
# is the auction that ended in a SALE, so the item left through a purchase.
# `marketplace_address` is absent from the legacy NftCancelTradeData constructor
# here (it keeps the dataclass default None), unlike the cancel matcher below.
matcher nft_finish_auction {
    produces nft_finish_auction
    entry    @cancel

    ( btype ton_transfer @cancel where nft_trade_finish_comment
    ^ op NftAuctionStop @cancel
    ^ op NftAuctionFinish @cancel )
      -> peek btype nft_purchase @ret

    let auction = lookup nft_auction(cancel.msg.destination)
    reject when auction == null
    reject when account(ret.data.nft_address) != account(auction.nft_addr)
    out {
        nft_address:         ret.data.nft_address
        nft_collection:      ret.data.collection_address
        owner:               ret.data.new_owner
        trade_contract:      account(cancel.msg.destination) ?? addr_none()
        marketplace_address: null
    }
}

matcher nft_cancel_auction {
    produces nft_cancel_auction | nft_finish_auction
    entry    @cancel

    ( btype ton_transfer @cancel where nft_trade_cancel_comment
    ^ op NftAuctionCancel @cancel
    ^ op NftAuctionFinish @cancel
    ^ op NftAuctionStop @cancel )
      -> peek btype nft_transfer @ret

    let auction = lookup nft_auction(cancel.msg.destination)
    reject when auction == null
    reject when account(ret.data.nft.address) != account(auction.nft_addr)

    # Same data either way; only the produced btype differs based on the
    # anchor's opcode or comment.
    produces switch {
        when (cancel.btype == "ton_transfer" and (cancel.data.comment == "finish" or cancel.data.comment == "stop")) or (cancel.btype == "call_contract" and (cancel.data.opcode == 0xB95616B6 or cancel.data.opcode == 0x20C9EB18)) => nft_finish_auction out {
            nft_address:         ret.data.nft.address
            nft_collection:      ret.data.nft.collection.address if ret.data.nft.collection != null else null
            owner:               ret.data.new_owner
            trade_contract:      account(cancel.msg.destination) ?? addr_none()
            marketplace_address: account(auction.mp_addr) ?? addr_none()
        }
        else => nft_cancel_auction out {
            nft_address:         ret.data.nft.address
            nft_collection:      ret.data.nft.collection.address if ret.data.nft.collection != null else null
            owner:               ret.data.new_owner
            trade_contract:      account(cancel.msg.destination) ?? addr_none()
            marketplace_address: account(auction.mp_addr) ?? addr_none()
        }
    }
}
