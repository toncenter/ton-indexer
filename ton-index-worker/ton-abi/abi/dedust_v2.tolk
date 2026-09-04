tolk 1.4

// Subset of DeDust CPMM V2 declarations.

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

// Jetton carrier: rides as the forward payload of the TEP-74 transfer the pool
// receives; payment_payload holds the swap / deposit payload.
@overflow1023_policy("suppress")
struct (0xcbc33949) DedustV2PayJetton {
    payment_payload: cell;
    _payout_config: cell;
}

@overflow1023_policy("suppress")
struct (0x78e79ba4) DedustV2SwapEvent {
    _x_to_y: bits1;
    amount_in: coins;
    amount_out: coins;
}

@overflow1023_policy("suppress")
struct (0xc442500f) DedustV2SwapPayload {
    minimal_amount_out: coins;
    _deadline: bits40;
    next: cell?;
    _partner_config: bits272?;
    _referrer_config: bits272?;
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

@overflow1023_policy("suppress")
struct (0xc9a015da) DedustV2DepositPayload {
    amount_x: coins;
    amount_y: coins;
    _minimal_liquidity: coins;
    _locked_liquidity_share: bits16;
}

type AllowedMessage = ClaimRewardMessage | PayoutRewardMessage | PayoutMessage
    | DedustV2PayNative | DedustV2PayJetton | DedustV2SwapEvent | DedustV2SwapPayload
    | DedustV2PayoutPositionFees
    | DedustV2Withdraw | DedustV2WithdrawalEvent | DedustV2CreditAsset
    | DedustV2JoinLiquidity | DedustV2DepositEvent | DedustV2DepositPayload

contract DedustV2 {
    incomingMessages: AllowedMessage
}
