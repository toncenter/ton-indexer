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

    # pool_params#_ pool_type:DedustPoolType asset0:DedustAsset asset1:DedustAsset = DedustPoolParams;
    # deposit_liquidity#d55e4686 query_id:uint64 amount:Coins pool_params:DedustPoolParams
    # params:^[ min_lp_amount:Coins
    # asset0_target_balance:Coins asset1_target_balance:Coins ]
    # fulfill_payload:(Maybe ^Cell)
    # reject_payload:(Maybe ^Cell) = InMsgBody;

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


class DestroyLiquidityDepositContract:
    opcode = 0xAAE79256


class DedustReturnExcessFromVault:
    opcode = 0x6B0B787F
