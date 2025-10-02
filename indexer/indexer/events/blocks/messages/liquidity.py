from pytoniq_core import Address, Slice

from indexer.events.blocks.messages.swaps import load_asset
from indexer.events.blocks.utils import Asset


def load_asset(slice: Slice) -> Asset:
    kind = slice.load_uint(4)
    if kind == 0:
        return Asset(True)
    else:
        wc = slice.load_uint(8)
        account_id = slice.load_bytes(32)
        return Asset(False, Address((wc, account_id)))


class DedustDepositTONToVault:
    opcode = 0xD55E4686

    # _ min_lp_amount:Coins asset0_target_balance:Coins asset1_target_balance:Coins = DedustDepositLiquidityParams;
    # dedust_deposit_liquidity#d55e4686 query_id:uint64 amount:Coins pool_params:DedustPoolParams
    #         deposit_params:^DedustDepositLiquidityParams
    #         fulfill_payload:(Maybe ^Cell)
    #         reject_payload:(Maybe ^Cell) = InternalMsgBody0;

    def __init__(self, slice: Slice):
        assert slice.load_uint(32) == self.opcode
        self.query_id = slice.load_uint(64)
        self.amount = slice.load_coins()
        self.pool_type = "volatile" if slice.load_bit() == 0 else "stable"
        self.asset0 = load_asset(slice)
        self.asset1 = load_asset(slice)
        params = slice.load_ref().begin_parse()
        min_lp_amount = params.load_coins()
        self.asset0_target_balance = params.load_coins()
        self.asset1_target_balance = params.load_coins()
        # others are not needed for now


class DedustDepositLiquidityJettonForwardPayload:
    opcode = 0x40E108D6

    def __init__(self, slice: Slice):
        # deposit_liquidity#40e108d6 pool_params:DedustPoolParams min_lp_amount:Coins
        # asset0_target_balance:Coins asset1_target_balance:Coins
        # fulfill_payload:(Maybe ^Cell)
        # reject_payload:(Maybe ^Cell) = ForwardPayload;
        assert slice.load_uint(32) == self.opcode
        self.pool_type = "volatile" if slice.load_bit() == 0 else "stable"
        self.asset0 = load_asset(slice)
        self.asset1 = load_asset(slice)
        min_lp_amount = slice.load_coins()
        self.asset0_target_balance = slice.load_coins()
        self.asset1_target_balance = slice.load_coins()


class DedustDeployLiquidityFactory:
    opcode = 0x9B3AA3FA


class DedustAskLiquidityFactory:
    opcode = 0xF04EC526


class DedustDeployDepositContract:
    opcode = 0x9B3AA3FA


class DedustTopUpLiquidityDepositContract:
    opcode = 0x54240FE5


class DedustDepositLiquidityToPool:
    opcode = 0xB56B9598

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.proof = slice.load_ref()
        self.owner_addr = slice.load_address()
        self.min_lp_amount = slice.load_coins()
        field4 = slice.load_ref().begin_parse()
        self.asset0 = load_asset(field4)
        self.asset0_amount = field4.load_coins()
        self.asset1 = load_asset(field4)
        self.asset1_amount = field4.load_coins()
        self.fulfill_payload = slice.load_maybe_ref()
        self.reject_payload = slice.load_maybe_ref()


class DedustDestroyLiquidityDepositContract:
    opcode = 0xAAE79256


class DedustReturnExcessFromVault:
    opcode = 0x6B0B787F


class StonfiV2ProvideLiquidity:
    opcode = 0x37c096df
    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.from_user = body.load_address()
        self.amount1 = body.load_coins()
        self.amount2 = body.load_coins()


class ToncoPoolV3Mint:
    """
    POOLV3_MINT#b2c1b6e3
        query_id:uint64
        owner_addr:MsgAddress
        amount0:(VarUInteger 16)
        amount1:(VarUInteger 16)
        enough0:(VarUInteger 16)
        enough1:(VarUInteger 16)
        liquidity:uint128
        tick_lower:int24
        tick_upper:int24
    = ContractMessages;
    """

    opcode = 0xB2C1B6E3

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.owner_addr = body.load_address()
        self.amount0 = body.load_coins()
        self.amount1 = body.load_coins()
        self.enough0 = body.load_coins()
        self.enough1 = body.load_coins()
        self.liquidity = body.load_uint(128)
        self.tick_lower = body.load_int(24)
        self.tick_upper = body.load_int(24)


class ToncoPoolV3MinAndRefund:
    """
    POOLV3_MINT#81702ef8
        query_id:uint64
        amount0_funded:(VarUInteger 16)
        amount1_funded:(VarUInteger 16)
        recipient:MsgAddress
        liquidity:uint128
        tickLower:int24
        tickUpper:int24
    = ContractMessages;
    """

    opcode = 0x81702EF8

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.amount0_funded = body.load_coins()
        self.amount1_funded = body.load_coins()
        self.recipient = body.load_address()
        self.liquidity = body.load_uint(128)
        self.tick_lower = body.load_int(24)
        self.tick_upper = body.load_int(24)


class ToncoPoolV3StartBurn:
    """
    POOLV3_START_BURN#530b5f2c
        query_id:uint64
        burned_index:uint64
        liquidity_to_burn:uint128
        tick_lower:int24
        tick_upper:int24
    = ContractMessages;
    """

    opcode = 0x530B5F2C

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.burned_index = body.load_uint(64)
        self.liquidity_to_burn = body.load_uint(128)
        self.tick_lower = body.load_int(24)
        self.tick_upper = body.load_int(24)


class ToncoPoolV3Burn:
    """
    POOLV3_BURN#d73ac09d
        query_id:uint64
        recipient:MsgAddress
        burned_index:uint64
        liquidity:uint128
        tick_lower:int24
        tick_upper:int24
        liquidity_to_burn:uint128
        old_fee_cell:^[
            fee_growth_inside_0_last_x128:uint256
            fee_growth_inside_1_last_x128:uint256
        ]
        new_fee_cell:^[
            fee_growth_inside_0_current_x128:uint256
            fee_growth_inside_1_current_x128:uint256
        ]
    = ContractMessages;
    """

    opcode = 0xD73AC09D

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.recipient = body.load_address()
        self.burned_index = body.load_uint(64)
        self.liquidity = body.load_uint(128)
        self.tick_lower = body.load_int(24)
        self.tick_upper = body.load_int(24)
        self.liquidity_to_burn = body.load_uint(128)
        old_fee_cell = body.load_ref().to_slice()
        self.fee_growth_inside_0_last_x128 = old_fee_cell.load_uint(256)
        self.fee_growth_inside_1_last_x128 = old_fee_cell.load_uint(256)
        new_fee_cell = body.load_ref().to_slice()
        self.fee_growth_inside_0_current_x128 = new_fee_cell.load_uint(256)
        self.fee_growth_inside_1_current_x128 = new_fee_cell.load_uint(256)


class ToncoPoolV3FundAccount:
    """
    POOLV3_FUND_ACCOUNT#4468de77
        query_id:uint64
        owner_addr:MsgAddress
        amount0:(VarUInteger 16)
        amount1:(VarUInteger 16)
        enough0:(VarUInteger 16)
        enough1:(VarUInteger 16)
        liquidity:uint128
        tick_lower:int24
        tick_upper:int24
    = ContractMessages;
    """

    opcode = 0x4468DE77

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.owner_addr = body.load_address()
        self.amount0 = body.load_coins()
        self.amount1 = body.load_coins()
        self.enough0 = body.load_coins()
        self.enough1 = body.load_coins()
        self.liquidity = body.load_uint(128)
        self.tick_lower = body.load_int(24)
        self.tick_upper = body.load_int(24)


class ToncoPoolV3FundAccountPayload:
    """
    Payload for jetton notification during liquidity provision.
    Used inside jetton transfers to router for mint operations.
    TL-B structure from SDK:
    POOLV3_FUND_ACCOUNT#4468de77
        other_jetton_wallet:MsgAddress    // jetton wallet of the other token
        amount0:(VarUInteger 16)          // amount of first token
        amount1:(VarUInteger 16)          // amount of second token
        liquidity:uint128                 // liquidity to provide
        tick_lower:int24                  // lower tick
        tick_upper:int24                  // upper tick
    = ContractMessages;
    """

    payload_opcode = 0x4468DE77

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.other_jetton_wallet = body.load_address()
        self.amount0 = body.load_coins() or 0
        self.amount1 = body.load_coins() or 0
        # self.liquidity = body.load_uint(128)
        # self.tick_lower = body.load_int(24)
        # self.tick_upper = body.load_int(24)

    def get_other_jetton_wallet(self) -> str:
        if not isinstance(self.other_jetton_wallet, Address):
            raise ValueError("other_jetton_wallet is not an Address")
        return self.other_jetton_wallet.to_str(False).upper()


class ToncoPoolV3Init:
    """
    POOLV3_INIT#441c39ed
        query_id:uint64
        from_admin:bool
        has_admin:bool
        admin_addr:MsgAddress
        has_controller:bool
        controller_addr:MsgAddress
        set_spacing:bool
        tick_spacing:int24
        set_price:bool
        initial_price_x96:uint160
        set_active:bool
        pool_active:bool
        protocol_fee:uint16
        lp_fee_base:uint16
        lp_fee_current:uint16
        nftv3_content:^Cell
        nftv3item_content:^Cell
        minter_cell:(Maybe ^[
            jetton0_minter:MsgAddress
            jetton1_minter:MsgAddress
        ])
    = ContractMessages;
    """

    opcode = 0x441C39ED

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.from_admin = body.load_bit()
        self.has_admin = body.load_bit()
        self.admin_addr = body.load_address()
        self.has_controller = body.load_bit()
        self.controller_addr = body.load_address()
        self.set_spacing = body.load_bit()
        self.tick_spacing = body.load_int(24)
        self.set_price = body.load_bit()
        self.initial_price_x96 = body.load_uint(160)
        self.set_active = body.load_bit()
        self.pool_active = body.load_bit()
        self.protocol_fee = body.load_uint(16)
        self.lp_fee_base = body.load_uint(16)
        self.lp_fee_current = body.load_uint(16)
        self.nftv3_content = body.load_ref()
        self.nftv3item_content = body.load_ref()
        minter_cell_ref = body.load_maybe_ref()
        self.jetton0_minter = None
        self.jetton1_minter = None
        if minter_cell_ref:
            minter_slice = minter_cell_ref.to_slice()
            self.jetton0_minter = minter_slice.load_address()
            self.jetton1_minter = minter_slice.load_address()


class ToncoRouterV3CreatePool:
    """
    Opcode: 0x2e3034ef
    TL-B:
    ROUTERV3_CREATE_POOL#2e3034ef
        query_id:uint64
        jetton_wallet0:MsgAddress
        jetton_wallet1:MsgAddress
        tick_spacing:int24
        initial_price_x96:uint160
        protocol_fee:uint16
        lp_fee_base:uint16
        lp_fee_current:uint16
        nftv3_content:^Cell
        nftv3item_content:^Cell
        minter_cell:^[
            jetton0_minter:MsgAddress
            jetton1_minter:MsgAddress
            controller_addr:MsgAddress
        ]
    = ContractMessages;
    """

    opcode = 0x2E3034EF

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.jetton_wallet0 = body.load_address()
        self.jetton_wallet1 = body.load_address()
        self.tick_spacing = body.load_int(24)
        self.initial_price_x96 = body.load_uint(160)
        self.protocol_fee = body.load_uint(16)
        self.lp_fee_base = body.load_uint(16)
        self.lp_fee_current = body.load_uint(16)
        self.nftv3_content = body.load_ref()
        self.nftv3item_content = body.load_ref()

        minter_cell_ref = body.load_ref()
        minter_slice = minter_cell_ref.to_slice()
        self.jetton0_minter = minter_slice.load_address()
        self.jetton1_minter = minter_slice.load_address()
        self.controller_addr = minter_slice.load_address()


class ToncoPositionNftV3PositionInit:
    """
    Opcode: 0xd5ecca2a
    TL-B:
    POSITIONNFTV3_POSITION_INIT#d5ecca2a
        query_id:uint64
        user_address:MsgAddress
        liquidity:uint128
        tick_lower:int24
        tick_upper:int24
        old_fee_cell:^[
            fee_growth_inside_0_last_x128:uint256
            fee_growth_inside_1_last_x128:uint256
            nft_index:uint64
            jetton0_amount:(VarUInteger 16)
            jetton1_amount:(VarUInteger 16)
            tick:int24
        ]
    = ContractMessages;
    """

    opcode = 0xD5ECCA2A

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.user_address = body.load_address()
        self.liquidity = body.load_uint(128)
        self.tick_lower = body.load_int(24)
        self.tick_upper = body.load_int(24)

        old_fee_cell_ref = body.load_ref()
        old_fee_slice = old_fee_cell_ref.to_slice()
        self.fee_growth_inside0_last_x128 = old_fee_slice.load_uint(256)
        self.fee_growth_inside1_last_x128 = old_fee_slice.load_uint(256)
        self.nft_index = old_fee_slice.load_uint(64)
        self.jetton0_amount = old_fee_slice.load_coins()
        self.jetton1_amount = old_fee_slice.load_coins()
        self.tick = old_fee_slice.load_int(24)


class ToncoPositionNftV3PositionBurn:
    """
    Opcode: 0x46ca335a
    TL-B:
    POSITIONNFTV3_POSITION_BURN#46ca335a
        query_id:uint64
        nft_owner:MsgAddress
        liquidity_to_burn:uint128
        tick_lower:int24
        tick_upper:int24
        old_fee_cell:^[
            fee_growth_inside_0_last_x128:uint256
            fee_growth_inside_1_last_x128:uint256
        ]
    = ContractMessages;
    """

    opcode = 0x46CA335A

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.nft_owner = body.load_address()
        self.liquidity_to_burn = body.load_uint(128)
        self.tick_lower = body.load_int(24)
        self.tick_upper = body.load_int(24)

        old_fee_cell_ref = body.load_ref()
        old_fee_slice = old_fee_cell_ref.to_slice()
        self.fee_growth_inside0_last_x128 = old_fee_slice.load_uint(256)
        self.fee_growth_inside1_last_x128 = old_fee_slice.load_uint(256)


class ToncoAccountV3AddLiquidity:
    """
    Opcode: 0x3ebe5431
    TL-B:
    ACCOUNTV3_ADD_LIQUIDITY#3ebe5431
        query_id:uint64
        new_amount0:(VarUInteger 16)
        new_amount1:(VarUInteger 16)
        new_enough0:(VarUInteger 16)
        new_enough1:(VarUInteger 16)
        liquidity:uint128
        tick_lower:int24
        tick_upper:int24
    = ContractMessages;
    """

    opcode = 0x3EBE5431

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.new_amount0 = body.load_coins() or 0
        self.new_amount1 = body.load_coins() or 0
        self.new_enough0 = body.load_coins() or 0
        self.new_enough1 = body.load_coins() or 0
        self.liquidity = body.load_uint(128)
        self.tick_lower = body.load_int(24)
        self.tick_upper = body.load_int(24)


class ToncoPoolV3Lock:
    """
    POOLV3_LOCK#b1b0b7e2
        query_id:uint64
    = ContractMessages;
    """

    opcode = 0xB1B0B7E2

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)


class ToncoPoolV3Unlock:
    """
    POOLV3_UNLOCK#4e737e4d
        query_id:uint64
    = ContractMessages;
    """

    opcode = 0x4E737E4D

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)


class ToncoPoolV3SetFee:
    """
    POOLV3_SET_FEE#6bdcbeb8
        query_id:uint64
        protocol_fee:uint16
        lp_fee_base:uint16
        lp_fee_current:uint16
    = ContractMessages;
    """

    opcode = 0x6BDCBEB8

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.protocol_fee = body.load_uint(16)
        self.lp_fee_base = body.load_uint(16)
        self.lp_fee_current = body.load_uint(16)


class ToncoAccountV3RefundMe:
    """
    Opcode: 0xbf3f447
    TL-B:
    ACCOUNTV3_REFUND_ME#bf3f447
        query_id:uint64
    = ContractMessages;
    """

    opcode = 0xBF3F447

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)


class ToncoResetGas:
    """
    Opcode: 0x42a0fb43
    TL-B:
    ROUTERV3_RESET_GAS#42a0fb43
        query_id:uint64
    = ContractMessages;
    """

    opcode = 0x42A0FB43

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
