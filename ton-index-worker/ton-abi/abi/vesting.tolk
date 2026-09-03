tolk 1.0

struct Node {
    addr: any_address;
    next: Cell<Node> | void;
}

struct (0x7258a69b) VestingAddWhiteList {
    query_id: uint64;
    head: Node;
}

type AllowedMessage = VestingAddWhiteList

struct Storage {
    dummy: uint1
}

contract VestingMessages {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        VestingAddWhiteList => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
