tolk 1.0

// Copy this template to abi/<contract>.tolk, add authoritative declarations,
// list their names in AllowedMessage, and add the stem to PROD_DECLS.
struct (0x00000001) ExampleMessage {
    queryId: uint64;
    amount: coins;
    someAddress: address;
}

type AllowedMessage = ExampleMessage

struct Storage {
    dummy: uint1
}

contract TemplateContract {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        ExampleMessage => {
            assert (msg.amount >= 0) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
