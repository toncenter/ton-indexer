tolk 1.4

struct ToncoPositionOldFee {
    _fee_growth_inside0_last_x128: uint256;
    _fee_growth_inside1_last_x128: uint256;
}

struct ToncoPoolOldFee {
    _fee_growth_inside_0_last_x128: uint256;
    _fee_growth_inside_1_last_x128: uint256;
}

struct ToncoPoolNewFee {
    _fee_growth_inside_0_current_x128: uint256;
    _fee_growth_inside_1_current_x128: uint256;
}

struct ToncoPoolMinters {
    jetton0_minter: any_address;
    jetton1_minter: any_address;
    _controller_addr: any_address;
}

@overflow1023_policy("suppress")
struct (0xa7fb58f8) ToncoRouterV3SwapSourceWallet {
    _query_id: bits64;
    _owner: any_address;
    source_wallet: any_address;
}

@overflow1023_policy("suppress")
struct (0x530b5f2c) ToncoPoolV3StartBurn {
    _query_id: bits64;
    burned_index: uint64;
    liquidity_to_burn: uint128;
    tick_lower: int24;
    tick_upper: int24;
}

@overflow1023_policy("suppress")
struct (0x46ca335a) ToncoPositionNftV3PositionBurn {
    _query_id: bits64;
    nft_owner: any_address;
    _liquidity_to_burn: uint128;
    _tick_lower: int24;
    _tick_upper: int24;
    _old_fee: Cell<ToncoPositionOldFee>;
}

@overflow1023_policy("suppress")
struct (0xd73ac09d) ToncoPoolV3Burn {
    _query_id: bits64;
    _recipient: any_address;
    burned_index: uint64;
    _liquidity: uint128;
    _tick_lower: int24;
    _tick_upper: int24;
    _liquidity_to_burn: uint128;
    _old_fee: Cell<ToncoPoolOldFee>;
    _new_fee: Cell<ToncoPoolNewFee>;
}

@overflow1023_policy("suppress")
struct (0x2e3034ef) ToncoRouterV3CreatePool {
    _query_id: bits64;
    jetton_wallet0: any_address;
    jetton_wallet1: any_address;
    tick_spacing: int24;
    initial_price_x96: uint160;
    protocol_fee: uint16;
    lp_fee_base: uint16;
    lp_fee_current: uint16;
    _nftv3_content: cell;
    _nftv3item_content: cell;
    minters: Cell<ToncoPoolMinters>;
}

@overflow1023_policy("suppress")
struct (0x441c39ed) ToncoPoolV3Init {
    _query_id: bits64;
    _from_admin: bits1;
    _has_admin: bits1;
    _admin_addr: any_address;
    _has_controller: bits1;
    _controller_addr: any_address;
    _set_spacing: bits1;
    _tick_spacing: int24;
    _set_price: bits1;
    _initial_price_x96: uint160;
    _set_active: bits1;
    pool_active: bool;
}

type AllowedMessage = ToncoRouterV3SwapSourceWallet | ToncoPoolV3StartBurn | ToncoPositionNftV3PositionBurn | ToncoPoolV3Burn | ToncoRouterV3CreatePool | ToncoPoolV3Init

contract ToncoMessages {
    incomingMessages: AllowedMessage
}
