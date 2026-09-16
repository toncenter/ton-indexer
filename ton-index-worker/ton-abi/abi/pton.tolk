tolk 1.4

struct (0b1) PTonForwardPayloadRef {
    value: cell;
}

struct (0b0) PTonForwardPayloadInline {
    value: RemainingBitsAndRefs;
}

type PTonForwardTail = PTonForwardPayloadRef | PTonForwardPayloadInline

@overflow1023_policy("suppress")
struct (0x01f3835d) PTonTransfer {
    query_id: uint64;
    ton_amount: coins;
    refund_address: any_address;
    forward_payload: PTonForwardTail;
}

type AllowedMessage = PTonTransfer

contract PTonMessages {
    incomingMessages: AllowedMessage
}
