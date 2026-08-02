tolk 1.0

struct StonfiPaymentRequestInfo {
    amount0_out: coins;
    token0: any_address;
    amount1_out: coins;
    token1: any_address;
}

struct StonfiSwapInfo {
    from_real_user: any_address;
}

struct StonfiV2PayToInfo {
    _fwd_ton: coins;
    amount0_out: coins;
    token0: any_address;
    amount1_out: coins;
    token1: any_address;
}

@overflow1023_policy("suppress")
struct (0x37c096df) StonfiV2ProvideLiquidity {
    query_id: uint64;
    from_user: any_address;
    amount1: coins;
    amount2: coins;
}

@overflow1023_policy("suppress")
struct (0xf93bb43f) StonfiPaymentRequest {
    _query_id: bits64;
    _owner: any_address;
    exit_code: uint32;
    info: Cell<StonfiPaymentRequestInfo>;
}

@overflow1023_policy("suppress")
struct (0x25938561) StonfiSwapMessage {
    _query_id: bits64;
    from_user_address: any_address;
    token_wallet: any_address;
    amount: coins;
    _min_out: coins;
    _has_ref: bits1;
    info: Cell<StonfiSwapInfo>;
}

@overflow1023_policy("suppress")
struct (0x657b54f5) StonfiV2PayTo {
    _query_id: bits64;
    _to: any_address;
    _exc: any_address;
    _orig: any_address;
    exit_code: uint32;
    _cp: cell?;
    info: Cell<StonfiV2PayToInfo>;
}

type AllowedMessage = StonfiV2ProvideLiquidity | StonfiPaymentRequest | StonfiSwapMessage | StonfiV2PayTo

struct Storage {
    dummy: uint1
}

contract StonfiMessages {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        StonfiV2ProvideLiquidity => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        StonfiPaymentRequest => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        StonfiSwapMessage => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        StonfiV2PayTo => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
