tolk 1.4

@overflow1023_policy("suppress")
struct (0xa040ad28) CocoonPayoutPayload {
    query_id: uint64;
    new_tokens: uint64;
    expected_my_address: any_address;
}

@overflow1023_policy("suppress")
struct (0xf5f26a36) CocoonLastPayoutPayload {
    query_id: uint64;
    new_tokens: uint64;
    expected_my_address: any_address;
}

@overflow1023_policy("suppress")
struct (0x4d725d2c) CocoonWorkerProxyRequest {
    query_id: uint64;
    owner_address: any_address;
    state: uint2;
    tokens: uint64;
    payload: cell?;
}

@overflow1023_policy("suppress")
struct (0x65448ff4) CocoonClientProxyRequest {
    query_id: uint64;
    owner_address: any_address;
    state_data: cell;
    payload: cell?;
}

@overflow1023_policy("suppress")
struct (0xc68ebc7b) CocoonClientProxyRefundGranted {
    coins: coins;
    send_excesses_to: any_address;
}

@overflow1023_policy("suppress")
struct (0x7610e6eb) CocoonExtProxyPayoutRequest {
    query_id: uint64;
    send_excesses_to: any_address;
}

@overflow1023_policy("suppress")
struct (0xbb63ff93) CocoonChargePayload {
    query_id: uint64;
    new_tokens_used: uint64;
    expected_my_address: any_address;
}

@overflow1023_policy("suppress")
struct (0xefd711e1) CocoonGrantRefundPayload {
    query_id: uint64;
    new_tokens_used: uint64;
    expected_my_address: any_address;
}

@overflow1023_policy("suppress")
struct (0xf172e6c2) CocoonExtClientTopUp {
    query_id: uint64;
    top_up_amount: coins;
    send_excesses_to: any_address;
}

@overflow1023_policy("suppress")
struct (0x927c7cb5) CocoonRegisterProxy {
    query_id: uint64;
}

@overflow1023_policy("suppress")
struct (0x6d49eaf2) CocoonUnregisterProxy {
    query_id: uint64;
    seqno: uint32;
}

@overflow1023_policy("suppress")
struct (0xc45f9f3b) CocoonOwnerClientRegister {
    query_id: uint64;
    nonce: uint64;
    send_excesses_to: any_address;
}

@overflow1023_policy("suppress")
struct (0xa9357034) CocoonOwnerClientChangeSecretHash {
    query_id: uint64;
    new_secret_hash: uint256;
    send_excesses_to: any_address;
}

@overflow1023_policy("suppress")
struct (0xfafa6cc1) CocoonOwnerClientRequestRefund {
    query_id: uint64;
    send_excesses_to: any_address;
}

@overflow1023_policy("suppress")
struct (0x6a1f6a60) CocoonOwnerClientIncreaseStake {
    query_id: uint64;
    new_stake: coins;
    send_excesses_to: any_address;
}

@overflow1023_policy("suppress")
struct (0xda068e78) CocoonOwnerClientWithdraw {
    query_id: uint64;
    send_excesses_to: any_address;
}

type AllowedMessage = CocoonPayoutPayload | CocoonLastPayoutPayload | CocoonWorkerProxyRequest | CocoonClientProxyRequest | CocoonClientProxyRefundGranted | CocoonExtProxyPayoutRequest | CocoonChargePayload | CocoonGrantRefundPayload | CocoonExtClientTopUp | CocoonRegisterProxy | CocoonUnregisterProxy | CocoonOwnerClientRegister | CocoonOwnerClientChangeSecretHash | CocoonOwnerClientRequestRefund | CocoonOwnerClientIncreaseStake | CocoonOwnerClientWithdraw

contract CocoonMessages {
    incomingMessages: AllowedMessage
}
