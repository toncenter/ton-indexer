tolk 1.0

@overflow1023_policy("suppress")
struct (0x706c7567) SubscriptionPaymentRequest {
    query_id: uint64;
    grams: coins;
}

type AllowedMessage = SubscriptionPaymentRequest

struct Storage {
    dummy: uint1
}

contract SubscriptionMessages {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        SubscriptionPaymentRequest => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
