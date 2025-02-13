from __future__ import annotations

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


def load_asset(slice: Slice) -> Asset:
    kind = slice.load_uint(4)
    if kind == 0:
        return Asset(True)
    else:
        wc = slice.load_uint(8)
        account_id = slice.load_bytes(32)
        return Asset(False, Address((wc, account_id)))

class PTonTransfer:
    opcode = 0x01f3835d
    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.ton_amount = body.load_coins()
        self.refund_address = body.load_address()


class StonfiV2PayTo:
    def __init__(self, body: Slice):
        body.load_uint(32) # opcode
        self.query_id = body.load_uint(64)
        self.to_address = body.load_address()
        self.excesses_address = body.load_address()
        self.original_caller = body.load_address()
        self.exit_code = body.load_uint(32)
        self.custom_payload = body.load_maybe_ref()
        additional_info = body.load_ref().to_slice()
        self.fwd_ton_amount = additional_info.load_coins()
        self.amount0_out = additional_info.load_coins()
        self.token0_address = additional_info.load_address()
        self.amount1_out = additional_info.load_coins()
        self.token1_address = additional_info.load_address()


class DedustSwapNotification:
    opcode = 0x9c610de3

    def __init__(self, body: Slice):
        body.load_uint(32)  # opcode
        self.asset_in = load_asset(body)
        self.asset_out = load_asset(body)
        self.amount_in = body.load_coins()
        self.amount_out = body.load_coins()
        ref = body.load_ref().to_slice()
        self.sender_address = ref.load_address()
        self.ref_address = ref.load_address()
        self.reserve_0 = ref.load_coins()
        self.reserve_1 = ref.load_coins()


class DedustPayout:
    opcode = 0x474f86cf

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.proof = body.load_ref()
        self.amount = body.load_coins()


class DedustPayoutFromPool:
    opcode = 0xad4eb6f5

    def __init__(self, body: Slice):
        body.load_uint(32)
        self.query_id = body.load_uint(64)
        self.proof = body.load_ref()
        self.amount = body.load_coins()

class DedustSwapPeer:
    opcode = 0x72aca8aa


class DedustSwapExternal:
    opcode = 0x61ee542d

class DedustSwap:
    opcode = 0xea06185d

class DedustSwapPayload:
    opcode = 0xe3a0d482

class StonfiSwapV2:
    opcode = 0x657b54f5
    query_id: int
    from_user: Address
    left_amount: int
    right_amount: int
    transferred_op: int
    token_wallet1: Address
    refund_address: Address
    excesses_address: str
    tx_deadline: int
    min_out: int
    receiver: str
    fwd_gas: int
    custom_payload: bytes | None
    refund_fwd_gas: int
    refund_payload: bytes | None
    ref_fee: int
    ref_address: str

    def __init__(self, boc: Slice):
        boc.skip_bits(32)  # Skip opcode
        self.query_id = boc.load_uint(64)
        self.from_user = boc.load_address()
        self.left_amount = boc.load_coins()
        self.right_amount = boc.load_coins()
        dex_payload_slice = boc.load_ref().to_slice()
        self.transferred_op = dex_payload_slice.load_uint(32)
        self.token_wallet1 = dex_payload_slice.load_address()
        self.refund_address = dex_payload_slice.load_address()
        self.excesses_address = dex_payload_slice.load_address()
        self.tx_deadline = dex_payload_slice.load_uint(64)
        swap_body_slice = dex_payload_slice.load_ref().to_slice()
        self.min_out = swap_body_slice.load_coins()
        self.receiver = swap_body_slice.load_address()
        self.fwd_gas = swap_body_slice.load_coins()
        custom_payload = swap_body_slice.load_maybe_ref()
        self.custom_payload = None
        if custom_payload:
            self.custom_payload = custom_payload.to_boc(hash_crc32=True)
        self.refund_fwd_gas = swap_body_slice.load_coins()
        self.refund_payload = None
        refund_payload = swap_body_slice.load_maybe_ref()
        if refund_payload:
            self.refund_payload = refund_payload.to_boc(hash_crc32=True)
        self.ref_fee = swap_body_slice.load_uint(16)
        self.ref_address = swap_body_slice.load_address()

    def get_pool_accounts_recursive(self) -> list[str]:
        accounts = [self.token_wallet1.to_str(is_user_friendly=False).upper()]
        if self.custom_payload is None:
            return accounts
        current_slice = Slice.one_from_boc(self.custom_payload)
        while True:
            sum_type = current_slice.load_uint(32)
            if sum_type in (0x6664de2a, 0x69cf1a5b):
                accounts.append(current_slice.load_address().to_str(is_user_friendly=False).upper())
                if current_slice.remaining_refs > 0:
                    cross_swap = current_slice.load_ref().to_slice()
                else:
                    break
                cross_swap.load_coins() # min_out
                cross_swap.load_coins()
                if cross_swap.remaining_refs > 0:
                    custom_payload = cross_swap.load_maybe_ref()
                    if custom_payload:
                        current_slice = custom_payload.to_slice()
                    else:
                        break
                else:
                    break
            else:
                break
        return accounts