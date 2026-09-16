tolk 1.4

@overflow1023_policy("suppress")
struct (0x00000001) EvaaSupplyJettonForward {
    _include_user_code: bits2;
    recipient_address: any_address;
}

type AllowedMessage = EvaaSupplyJettonForward

contract EvaaSupplyForwardMessages {
    incomingMessages: AllowedMessage
}
