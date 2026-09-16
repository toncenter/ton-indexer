tolk 1.4

struct (0b00) AssetTon {}

struct (0b01) AssetJetton {
    workchain: uint8;
    hash: bits256;
}

type Asset = AssetTon | AssetJetton

struct CoffeeDepositParams {
    _recipient: any_address;
    _use_recipient_on_failure: bits1;
}

struct CoffeePoolParams {
    pool_first: Asset;
    pool_second: Asset;
    _amm: bits3;
    _amm_settings: cell?;
}

struct CoffeePoolCreationParams {
    recipient: any_address;
    _use_recipient_on_failure: bits1;
    _notification_data: cell?;
    _is_active: bits1;
    _extra_settings: cell?;
}

@overflow1023_policy("suppress")
struct (0xc0ffee06) CoffeeCreateVault {
    query_id: uint64;
    asset: Asset;
}

@overflow1023_policy("suppress")
struct (0xc0ffee30) CoffeeSwapEvent {
    _query_id: bits64;
    asset: Asset;
    amount_in: coins;
    amount_out: coins;
}

@overflow1023_policy("suppress")
struct (0xc0ffee27) CoffeeCreateLiquidityDepositoryRequest {
    _query_id: bits64;
    amount: coins;
    _params: Cell<CoffeeDepositParams>;
    pool_params: Cell<CoffeePoolParams>;
    sender: any_address;
    _proof: cell;
}

@overflow1023_policy("suppress")
struct (0xc0ffee33) CoffeeDepositLiquiditySuccessfulEvent {
    _query_id: bits64;
    _amount1: coins;
    _amount2: coins;
    lp_amount: coins;
    _total_supply_in: coins;
    _total_supply_out: coins;
    _reserves_in: coins;
    _reserves_out: coins;
}

@overflow1023_policy("suppress")
struct (0xc0ffee35) CoffeeLiquidityWithdrawalEvent {
    _query_id: bits64;
    amount1: coins;
    amount2: coins;
    _lp_amount: coins;
    _total_supply_in: coins;
    _total_supply_out: coins;
    _reserves_in: coins;
    _reserves_out: coins;
}

@overflow1023_policy("suppress")
struct (0xc0ffee23) CoffeeCreatePoolCreatorRequest {
    _query_id: bits64;
    amount: coins;
    pool_first: Asset;
    pool_second: Asset;
    _amm: bits3;
    _amm_settings: cell?;
    creation_params: Cell<CoffeePoolCreationParams>;
    sender: any_address;
    _proof: cell;
}

@overflow1023_policy("suppress")
struct (0xc0ffee25) CoffeeCreatePoolRequest {
    _query_id: bits64;
    amount1: coins;
    amount2: coins;
    tx_initiator: any_address;
}

@overflow1023_policy("suppress")
struct (0xee51ce51) CoffeeMevProtectFailedSwap {
    query_id: uint64;
    recipient: any_address;
}

@overflow1023_policy("suppress")
struct (0xb30c7310) CoffeeStakingClaimRewards {
    _query_id: bits64;
    jetton_wallet: any_address;
    jetton_amount: coins;
    receiver: any_address;
}

@overflow1023_policy("suppress")
struct (0xcb03bfaf) CoffeeStakingPositionWithdraw2 {
    _query_id: bits64;
    nft_id: uint64;
    owner: any_address;
    points: coins;
}

type AllowedMessage = CoffeeCreateVault | CoffeeSwapEvent | CoffeeCreateLiquidityDepositoryRequest | CoffeeDepositLiquiditySuccessfulEvent | CoffeeLiquidityWithdrawalEvent | CoffeeCreatePoolCreatorRequest | CoffeeCreatePoolRequest | CoffeeMevProtectFailedSwap | CoffeeStakingClaimRewards | CoffeeStakingPositionWithdraw2

contract CoffeeMessages {
    incomingMessages: AllowedMessage
}
