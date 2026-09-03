tolk 1.0

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

// TextCommentPayload, EncryptedCommentPayload, and StonfiSwapPayload can't join
// AllowedMessage (they are payload fragments, not messages). Storage exists only
// to keep them visible to the C++ generator.
struct (0xffffffff) JettonPayloadsCodegenAnchor {}

type AllowedMessage = JettonPayloadsCodegenAnchor

struct Storage {
    text_comment_payload: TextCommentPayload;
    encrypted_comment_payload: EncryptedCommentPayload;
    stonfi_swap_payload: StonfiSwapPayload;
}

contract JettonPayloads {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        JettonPayloadsCodegenAnchor => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
