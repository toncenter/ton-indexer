tolk 1.4

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
// (they share opcodes with their WithQuery twins); forceAbiExport keeps them
// visible to the C++ generator.

contract DnsMessages {
    incomingMessages: AllowedMessage
    forceAbiExport: AuctionFillUpNoQuery | DnsReleaseBalanceNoQuery
}
