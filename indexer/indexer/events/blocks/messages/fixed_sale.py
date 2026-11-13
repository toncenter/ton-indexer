from __future__ import annotations

import base64

from pytoniq_core import Address, Cell, Slice, begin_cell

from indexer.events.blocks.messages.state_init import is_state_init_address, state_init_data

FIXED_SALE_CODE = (base64.b64decode('8k0Y8Bj2bsonmo84WeXCgQ2xUkCsKnQpJf/2jWVCtJA='), 4)


class FixedSaleBuy:
    opcode = 0x474D0001


class FixedSaleChangePrice:
    opcode = 0x474D0002

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.price = slice.load_coins()


class FixedSaleCancel:
    opcode = 0x474D0003


class FixedSaleData:
    seller: Address
    nft: Address
    initial_price: int
    fee_address: Address
    fee_bps: int

    def __init__(self, slice: Slice):
        config_cell = slice.load_ref()
        slice.load_coins()  # price
        slice.skip_bits(8)  # phase
        if slice.remaining_bits != 0 or slice.remaining_refs != 0:
            raise ValueError("not a FixedSale data cell")
        config = config_cell.begin_parse()
        self.seller = config.load_address()
        self.nft = config.load_address()
        self.initial_price = config.load_coins()
        config.skip_bits(64)  # salt
        fee = config.load_ref().begin_parse()
        self.fee_address = fee.load_address()
        self.fee_bps = fee.load_uint(16)
        if self.seller is None or self.nft is None or self.fee_address is None:
            raise ValueError("addr_none in a FixedSale config")
        self._config = config_cell

    def fee_amount(self, price: int) -> int:
        return price * self.fee_bps // 10000

    def is_at(self, address: Address) -> bool:
        """Only price and phase change after deploy; a crafted initial phase would skip the deposit."""
        initial = begin_cell().store_ref(self._config).store_coins(self.initial_price).store_uint(0, 8).end_cell()
        return is_state_init_address(address, *FIXED_SALE_CODE, initial)


def parse_fixed_sale(address: str, data: Cell) -> FixedSaleData | None:
    try:
        sale = FixedSaleData(data.begin_parse())
        return sale if sale.is_at(Address(address)) else None
    except Exception:
        return None


def parse_fixed_sale_state_init(address: str, state_init_boc: str) -> FixedSaleData | None:
    data = state_init_data(address, state_init_boc)
    return parse_fixed_sale(address, data) if data is not None else None
