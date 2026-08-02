tolk 1.0

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

struct Storage {
    dummy: uint1
}

contract TeleitemMessages {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        TeleitemStartAuction => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
