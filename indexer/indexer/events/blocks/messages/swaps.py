from pytoniq_core import Slice, Address

from indexer.events.blocks.utils import Asset


class StonfiSwapMessage:
    opcode = 0x25938561

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.from_user_address = body.load_address()
        self.token_wallet = body.load_address()
        self.amount = body.load_coins()
        self.min_out = body.load_coins()
        self.has_ref = body.load_bit()
        ref = body.load_ref().to_slice()
        self.from_real_user = ref.load_address()
        self.ref_address = None
        if self.has_ref:
            self.ref_address = ref.load_address()


class StonfiPaymentRequest:
    opcode = 0xf93bb43f

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.query_id = body.load_uint(64)
        self.owner = body.load_address()
        self.exit_code = body.load_uint(32)
        ref = body.load_ref().to_slice()
        self.amount0_out = ref.load_coins()
        self.token0_out = ref.load_address()
        self.amount1_out = ref.load_coins()
        self.token1_out = ref.load_address()


class DedustSwapNotification:
    opcode = 0x9c610de3

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.asset_in = self.load_asset(body)
        self.asset_out = self.load_asset(body)
        self.amount_in = body.load_coins()
        self.amount_out = body.load_coins()
        ref = body.load_ref().to_slice()
        self.sender_address = ref.load_address()
        self.ref_address = ref.load_address()
        self.reserve_0 = ref.load_coins()
        self.reserve_1 = ref.load_coins()

    def load_asset(self, slice: Slice) -> Asset:
        kind = slice.load_uint(4)
        if kind == 0:
            return Asset(True)
        else:
            wc = slice.load_uint(8)
            account_id = slice.load_bytes(32)
            return Asset(False, Address((wc, account_id)))


class DedustPayout:
    opcode = 0x474f86cf


class DedustPayoutFromPool:
    opcode = 0xad4eb6f5


class DedustSwapPeer:
    opcode = 0x72aca8aa


class DedustSwapExternal:
    opcode = 0x61ee542d


class DedustSwap:
    opcode = 0xea06185d
