# DNS record-change matcher corresponding to blocks/dns.py.
# using `produces switch`. One anchor opcode (ChangeDnsRecord), three
# content-dispatched outcomes: change_dns / renew_dns / delete_dns. The optional
# trailing bounce (op 0xffffffff) is consumed as an explicit child;
# include_excess/include_bounces stay at their defaults (true) to mirror the
# legacy consumed set.
#
# The switch dispatches exactly like build_change_dns_core: a present value ->
# change_dns; else a zero key -> renew_dns; else -> delete_dns. `collection_address`
# is an OPTIONAL key (`name?: expr`) — emitted only when the destination resolves
# to an NftItem, mirroring the legacy conditional `data['collection_address'] = …`.

opcode ChangeDnsRecord = 0x4eb1f0f9

matcher change_dns {
    produces change_dns | renew_dns | delete_dns
    entry    @dns

    op ChangeDnsRecord @dns -> maybe op 0xffffffff

    parse dns as ChangeDnsRecordMessage
    let nft = lookup nft_item(dns.msg.destination)
    failed when dns.failed
    produces switch {
        when dns.body.has_value => change_dns out {
            source:               account(dns.msg.source)
            destination:          account(dns.msg.destination)
            key:                  dns.body.key
            value:                dns.body.value
            collection_address?:  account(nft.collection_address)
        }
        # zero key (b64 of 32 zero bytes) with no value -> renew.
        when b64(dns.body.key) == "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=" => renew_dns out {
            source:               account(dns.msg.source)
            destination:          account(dns.msg.destination)
            collection_address?:  account(nft.collection_address)
        }
        else => delete_dns out {
            source:               account(dns.msg.source)
            destination:          account(dns.msg.destination)
            key:                  dns.body.key
            collection_address?:  account(nft.collection_address)
        }
    }
}

opcode AuctionFillUp      = 0x370fec51
opcode DnsReleaseBalance = 0x4ed14b65

matcher dns_purchase {
    produces dns_purchase
    entry    @anchor

    op AuctionFillUp @anchor

    parse anchor as AuctionFillUpMessage
    let buyer = dns_purchase_buyer(anchor)
    reject when buyer == null
    let nft = lookup nft_item(anchor.msg.source)
    reject when nft == null or (nft.code_hash != "i1/8nr/TkGTY1fVuRlnIJrt1k5I/XKSHKL5NYK9vUfk=" and nft.collection_address != "0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF")
    failed when anchor.failed
    out {
        nft_address:              account(anchor.msg.source)
        collection_address:       account(nft.collection_address)
        nft_index:                nft.index
        prev_owner:               null
        new_owner:                buyer
        query_id:                 anchor.body.query_id
        forward_amount:           null
        response_destination:     null
        custom_payload:           null
        forward_payload:          null
        payout_amount:            amount(anchor.msg.value)
        payout_comment_encrypted: null
        payout_comment_encoded:   null
        payout_comment:           null
        price:                    amount(anchor.msg.value)
        real_prev_owner:          null
        marketplace:              null
        marketplace_address:      null
    }
}

matcher dns_release {
    produces dns_release
    entry    @anchor

    op DnsReleaseBalance @anchor

    parse anchor as DnsReleaseBalanceMessage
    let nft = lookup nft_item(anchor.msg.destination)
    reject when nft == null or (nft.code_hash != "i1/8nr/TkGTY1fVuRlnIJrt1k5I/XKSHKL5NYK9vUfk=" and nft.collection_address != "0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF")
    failed when anchor.failed
    out {
        query_id:      anchor.body.query_id
        nft_address:   account(anchor.msg.destination)
        nft_collection: account(nft.collection_address)
        nft_index:     nft.index
        source:        account(anchor.msg.source)
        value:         amount(anchor.msg.value)
    }
}
