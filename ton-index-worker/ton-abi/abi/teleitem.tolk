tolk 1.4

struct TeleitemAuctionConfig {
    beneficiary_address: any_address;
    initial_min_bid: coins;
    max_bid: coins;
    _min_bid_step: bits8;
    _min_extend_time: bits32;
    _duration: bits32;
}

@overflow1023_policy("suppress")
struct (0x487a8e81) TeleitemStartAuction {
    _query_id: bits64;
    config: Cell<TeleitemAuctionConfig>;
}

type AllowedMessage = TeleitemStartAuction

contract TeleitemMessages {
    incomingMessages: AllowedMessage
}
