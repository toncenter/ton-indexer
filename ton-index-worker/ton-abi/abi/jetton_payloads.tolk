tolk 1.4

struct (0x00000000) TextCommentPayload {
    comment: RemainingBitsAndRefs;
}

struct (0x2167da4b) EncryptedCommentPayload {
    comment: RemainingBitsAndRefs;
}

struct (0x25938561) StonfiSwapPayload {
    jetton_wallet: any_address;
    min_amount: coins;
    user_address: any_address;
}

// Payload fragments, not messages: exported to the C++ generator via
// forceAbiExport instead of incomingMessages.

contract JettonPayloads {
    forceAbiExport: TextCommentPayload | EncryptedCommentPayload | StonfiSwapPayload
}
