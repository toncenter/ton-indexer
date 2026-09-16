tolk 1.4

@overflow1023_policy("suppress")
struct (0xed58b0b2) TONStakersNftBurnNotification {
    query_id: uint64;
    amount: coins;
    owner: any_address;
}

type AllowedMessage = TONStakersNftBurnNotification

contract TonstakersMessages {
    incomingMessages: AllowedMessage
}
