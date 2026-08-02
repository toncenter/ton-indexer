tolk 1.0

@overflow1023_policy("suppress")
struct (0xcb03bfaf) CoffeeStakingPositionWithdraw3 {
    _query_id: bits64;
    jetton_wallet: any_address;
    jetton_amount: coins;
}

type AllowedMessage = CoffeeStakingPositionWithdraw3

struct Storage {
    dummy: uint1
}

contract CoffeeStakingWithdraw3Messages {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        CoffeeStakingPositionWithdraw3 => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
