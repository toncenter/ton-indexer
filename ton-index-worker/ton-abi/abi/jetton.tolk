tolk 1.4

struct (0b1) JettonForwardPayloadRef {
    value: cell;
}

struct (0b0) JettonForwardPayloadInline {
    value: RemainingBitsAndRefs;
}

type JettonForwardTail = JettonForwardPayloadRef | JettonForwardPayloadInline

struct MinterJettonMintMasterMsg {
    _op: uint32;
    query_id: uint64;
    jetton_amount: coins;
}

@overflow1023_policy("suppress")
struct (0x0f8a7ea5) JettonTransfer {
    query_id: uint64;
    amount: coins;
    destination: any_address;
    response_destination: any_address;
    custom_payload: cell?;
    forward_ton_amount: coins;
}

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

@overflow1023_policy("suppress")
struct (0x00000015) MinterJettonMint {
    query_id: uint64;
    to_address: any_address;
    ton_amount: coins;
    master_msg: Cell<MinterJettonMintMasterMsg>;
}

type AllowedMessage = JettonTransfer | JettonInternalTransfer | JettonInternalTransferEthena | JettonBurn | JettonNotify | JettonMint | MinterJettonMint

contract JettonMessages {
    incomingMessages: AllowedMessage
}
