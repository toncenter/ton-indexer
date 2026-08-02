tolk 1.0

@overflow1023_policy("suppress")
struct (0xed58b0b2) TONStakersNftBurnNotification {
    query_id: uint64;
    amount: coins;
    owner: any_address;
}

type AllowedMessage = TONStakersNftBurnNotification

struct Storage {
    dummy: uint1
}

contract TonstakersMessages {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        TONStakersNftBurnNotification => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
