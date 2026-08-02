tolk 1.0

struct (0b1) JettonForwardPayloadRef {
    value: cell;
}

struct (0b0) JettonForwardPayloadInline {
    value: RemainingBitsAndRefs;
}

type JettonForwardTail = JettonForwardPayloadRef | JettonForwardPayloadInline

@overflow1023_policy("suppress")
struct (0x178d4519) JettonInternalTransfer {
    query_id: uint64;
    amount: coins;
    from_address: any_address;
    response_address: any_address;
    forward_ton_amount: coins;
}

@overflow1023_policy("suppress")
struct (0xb2583ed5) JettonInternalTransferEthena {
    query_id: uint64;
    amount: coins;
    from_address: any_address;
    response_address: any_address;
    forward_ton_amount: coins;
}

@overflow1023_policy("suppress")
struct (0x595f07bc) JettonBurn {
    query_id: uint64;
    amount: coins;
    response_destination: any_address;
}

@overflow1023_policy("suppress")
struct (0x7362d09c) JettonNotify {
    query_id: uint64;
    jetton_amount: coins;
    from_user: any_address;
    forward_payload_cell: JettonForwardTail;
}

@overflow1023_policy("suppress")
struct (0x642b7d07) JettonMint {
    query_id: uint64;
    to_address: any_address;
    ton_amount: coins;
}

type AllowedMessage = JettonInternalTransfer | JettonInternalTransferEthena | JettonBurn | JettonNotify | JettonMint

struct Storage {
    dummy: uint1
}

contract JettonMessages {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        JettonInternalTransfer => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        JettonInternalTransferEthena => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        JettonBurn => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        JettonNotify => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        JettonMint => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
