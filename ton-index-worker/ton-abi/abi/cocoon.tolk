tolk 1.0

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

type AllowedMessage = CocoonPayoutPayload | CocoonLastPayoutPayload | CocoonWorkerProxyRequest | CocoonExtProxyPayoutRequest | CocoonChargePayload | CocoonGrantRefundPayload | CocoonExtClientTopUp | CocoonRegisterProxy | CocoonUnregisterProxy | CocoonOwnerClientRegister | CocoonOwnerClientChangeSecretHash | CocoonOwnerClientRequestRefund | CocoonOwnerClientIncreaseStake | CocoonOwnerClientWithdraw

struct Storage {
    dummy: uint1
}

contract CocoonMessages {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        CocoonPayoutPayload => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        CocoonLastPayoutPayload => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        CocoonWorkerProxyRequest => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        CocoonExtProxyPayoutRequest => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        CocoonChargePayload => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        CocoonGrantRefundPayload => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        CocoonExtClientTopUp => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        CocoonRegisterProxy => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        CocoonUnregisterProxy => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        CocoonOwnerClientRegister => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        CocoonOwnerClientChangeSecretHash => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        CocoonOwnerClientRequestRefund => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        CocoonOwnerClientIncreaseStake => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        CocoonOwnerClientWithdraw => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
