tolk 1.0

struct (0x370fec51) AuctionFillUpWithQuery {
    query_id: uint64;
}

struct (0x370fec51) AuctionFillUpNoQuery {}

struct (0x4ed14b65) DnsReleaseBalanceWithQuery {
    query_id: uint64;
}

struct (0x4ed14b65) DnsReleaseBalanceNoQuery {}

type AllowedMessage = AuctionFillUpWithQuery | DnsReleaseBalanceWithQuery

// AuctionFillUpNoQuery and DnsReleaseBalanceNoQuery can't join AllowedMessage
// (they share opcodes with their WithQuery twins). Storage exists only to keep
// them visible to the C++ generator.
struct Storage {
    auction_fill_up_no_query: AuctionFillUpNoQuery;
    dns_release_balance_no_query: DnsReleaseBalanceNoQuery;
}

contract DnsMessages {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        AuctionFillUpWithQuery => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        DnsReleaseBalanceWithQuery => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
