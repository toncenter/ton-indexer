tolk 1.4

@overflow1023_policy("suppress")
struct (0x706c7567) SubscriptionPaymentRequest {
    query_id: uint64;
    grams: coins;
}

type AllowedMessage = SubscriptionPaymentRequest

contract SubscriptionMessages {
    incomingMessages: AllowedMessage
}
