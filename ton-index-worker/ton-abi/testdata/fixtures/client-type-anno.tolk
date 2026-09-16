import "@stdlib/strings"

contract ClientTypeAnno {
    incomingMessages: ClientPayload | NotificationForNewOwner
    storage: ClientTypeStorage
}

struct ClientTypeStorage {
    dummy: uint32
}

/// Payload with a client-only field type.
struct (0x12345678) ClientPayload {
    /// Human-readable payload.
    @abi.clientType(string)
    note: RemainingBitsAndRefs
}

struct (0b0) PayloadInline {
    value: RemainingBitsAndRefs
}

struct (0b1) PayloadInRef {
    value: Cell<RemainingBitsAndRefs>
}

struct (0x05138d91) NotificationForNewOwner {
    queryId: uint64
    oldOwnerAddress: address
    @abi.clientType(PayloadInline | PayloadInRef)
    payload: RemainingBitsAndRefs
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy ClientPayload.fromSlice(in.body);
    assert (!msg.note.isEmpty()) throw 100;
}

get fun clientPayload(): ClientPayload {
    return {
        note: "CAFE".hexToSlice(),
    };
}

get fun echoPayload(payload: ClientPayload): ClientPayload {
    return payload;
}

get fun notificationForNewOwner(): NotificationForNewOwner {
    return {
        queryId: 123,
        oldOwnerAddress: address("EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c"),
        payload: "BEEF".hexToSlice(),
    };
}

get fun echoNotificationForNewOwner(notification: NotificationForNewOwner) {
    return notification;
}
