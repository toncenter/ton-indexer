tolk 1.4

@overflow1023_policy("suppress")
struct (0xcb03bfaf) CoffeeStakingPositionWithdraw3 {
    _query_id: bits64;
    jetton_wallet: any_address;
    jetton_amount: coins;
}

type AllowedMessage = CoffeeStakingPositionWithdraw3

contract CoffeeStakingWithdraw3Messages {
    incomingMessages: AllowedMessage
}
