tolk 1.4

@overflow1023_policy("suppress")
struct (0x00000001) EvaaSupplyMaster {
    _query_id: bits64;
    _include_user_code: bits2;
    supply_amount: uint64;
    recipient_address: any_address;
}

@overflow1023_policy("suppress")
struct (0x0000011a) EvaaSupplySuccess {
    _query_id: bits64;
    _owner_address: any_address;
    _asset_id: uint256;
    amount_supplied: uint64;
}

@overflow1023_policy("suppress")
struct (0x00000002) EvaaWithdrawMaster {
    query_id: uint64;
    asset_id: uint256;
    amount: uint64;
    recipient_address: any_address;
}

@overflow1023_policy("suppress")
struct (0x00000211) EvaaWithdrawCollateralized {
    query_id: uint64;
    owner_address: any_address;
    asset_id: uint256;
    withdraw_amount_current: uint64;
}

struct (0x000021e6) EvaaWithdrawLockedExcess {}

struct (0x000021e7) EvaaWithdrawNotCollateralizedExcess {}

struct (0x000021e8) EvaaWithdrawMissingPricesExcess {}

struct (0x000021ec) EvaaWithdrawExecutionCrashed {}

type AllowedMessage = EvaaSupplyMaster | EvaaSupplySuccess | EvaaWithdrawMaster | EvaaWithdrawCollateralized | EvaaWithdrawLockedExcess | EvaaWithdrawNotCollateralizedExcess | EvaaWithdrawMissingPricesExcess | EvaaWithdrawExecutionCrashed

contract EvaaMessages {
    incomingMessages: AllowedMessage
}
