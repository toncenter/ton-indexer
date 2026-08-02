tolk 1.0

// Subset of DeDust CPMM V2 declarations used by the claim_reward spec.

struct (0x909fdb65) ClaimRewardMessage {
    queryId: uint64;
    rewardIndex: uint2;
    excessesTo: address;
}

struct (0x9e17fbbe) PayoutRewardMessage {
    queryId: uint64;
    amount: coins;
    rewardIndex: uint2;
    ownerAddress: address;
    excessesTo: address;
}

struct (0x3216ca09) PayoutMessage {
    queryId: uint64;
    amount: coins;
    exitCode: int32;
    payload: cell?;
}

// Faithful schema-migrated declarations use the schema's snake_case field
// spellings. The three established declarations above intentionally retain
// their authoritative camelCase names.
@overflow1023_policy("suppress")
struct (0xa5a7cbf8) DedustV2PayNative {
    _query_id: bits64;
    amount: coins;
    payment_payload: cell;
}

@overflow1023_policy("suppress")
struct (0x78e79ba4) DedustV2SwapEvent {
    _x_to_y: bits1;
    amount_in: coins;
    amount_out: coins;
}

@overflow1023_policy("suppress")
struct (0x29ff1bcf) DedustV2PayoutPositionFees {
    query_id: uint64;
    x_fees: coins;
    y_fees: coins;
    owner: any_address;
}

@overflow1023_policy("suppress")
struct (0x20b5ef89) DedustV2Withdraw {
    _query_id: bits64;
    liquidity: coins;
    _minimal_x_out: coins;
    _minimal_y_out: coins;
    auto_claim_fees: bool;
    _payout_config: cell;
}

@overflow1023_policy("suppress")
struct (0xc0d77b54) DedustV2WithdrawalEvent {
    liquidity: coins;
    _amount_x_out: coins;
    _amount_y_out: coins;
    _initiator: any_address;
    _recipient: any_address;
    _next_state: cell;
}

@overflow1023_policy("suppress")
struct (0xdc5ddba1) DedustV2CreditAsset {
    _query_id: bits64;
    is_x: bool;
    amount: coins;
}

@overflow1023_policy("suppress")
struct (0x25251ee0) DedustV2JoinLiquidity {
    _query_id: bits64;
    _config: cell;
    amount_x: coins;
    amount_y: coins;
    initiator: any_address;
}

@overflow1023_policy("suppress")
struct (0x35df2e12) DedustV2DepositEvent {
    amount_x_in: coins;
    amount_y_in: coins;
    liquidity: coins;
    _initiator: any_address;
    _recipient: any_address;
    _next_state: cell;
}

type AllowedMessage = ClaimRewardMessage | PayoutRewardMessage | PayoutMessage
    | DedustV2PayNative | DedustV2SwapEvent | DedustV2PayoutPositionFees
    | DedustV2Withdraw | DedustV2WithdrawalEvent | DedustV2CreditAsset
    | DedustV2JoinLiquidity | DedustV2DepositEvent

struct Storage {
    dummy: uint1
}

contract DedustV2 {
    incomingMessages: AllowedMessage
    storage: Storage
}

fun onInternalMessage(in: InMessage) {
    val msg = lazy AllowedMessage.fromSlice(in.body);
    match (msg) {
        ClaimRewardMessage => {
            assert (msg.rewardIndex >= 0) throw 0xFFFF
        }
        PayoutRewardMessage => {
            assert (msg.amount >= 0) throw 0xFFFF
        }
        PayoutMessage => {
            assert (msg.exitCode >= 0) throw 0xFFFF
        }
        DedustV2PayNative => {
            assert (msg.amount >= 0) throw 0xFFFF
        }
        DedustV2SwapEvent => {
            assert (msg.amount_in >= 0) throw 0xFFFF
        }
        DedustV2PayoutPositionFees => {
            assert (msg.x_fees >= 0) throw 0xFFFF
        }
        DedustV2Withdraw => {
            assert (msg.liquidity >= 0) throw 0xFFFF
        }
        DedustV2WithdrawalEvent => {
            assert (msg.liquidity >= 0) throw 0xFFFF
        }
        DedustV2CreditAsset => {
            assert (msg.amount >= 0) throw 0xFFFF
        }
        DedustV2JoinLiquidity => {
            assert (msg.amount_x >= 0) throw 0xFFFF
        }
        DedustV2DepositEvent => {
            assert (msg.liquidity >= 0) throw 0xFFFF
        }
        else => {
            assert (in.body.isEmpty()) throw 0xFFFF
        }
    }
}
