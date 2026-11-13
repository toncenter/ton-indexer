from __future__ import annotations

import base64

from pytoniq_core import Address, Cell, Slice, begin_cell

from indexer.events.blocks.messages.state_init import is_state_init_address, state_init_data

NFT_OFFER_CODES = [(base64.b64decode('jUjSF20IDx2xXmHCFnnQ9wEHUgOA1TMa55WqvIm7ztw='), 5)]


class NftOfferCancelByBuyer:
    opcode = 0x69CC5721


class NftOfferCancel:
    opcode = 0x474D0200


class NftOfferData:
    nft: Address
    price: int
    expires_at: int
    buyer: Address
    recipient: Address
    seller: Address
    fee_address: Address
    fee_bps: int

    def __init__(self, slice: Slice):
        config = slice.load_ref().to_slice()
        slice.skip_bits(256 + 8)
        if slice.remaining_bits != 0 or slice.remaining_refs != 0:
            raise ValueError("not an NftOffer data cell")
        self.nft = config.load_address()
        self.price = config.load_coins()
        config.skip_bits(64)  # salt
        self.expires_at = config.load_uint(32)
        parties = config.load_ref().to_slice()
        self.buyer = parties.load_address()
        self.recipient = parties.load_address()
        self.seller = parties.load_address()
        fee = config.load_ref().to_slice()
        self.fee_address = fee.load_address()
        self.fee_bps = fee.load_uint(16)

    @property
    def fee_amount(self) -> int:
        return self.price * self.fee_bps // 10000


def is_nft_offer(address: Address, data: Cell) -> bool:
    """Only phase changes after deploy; a crafted initial phase can bypass the buyer's funding."""
    s = data.begin_parse()
    initial = begin_cell().store_ref(s.load_ref()).store_uint(s.load_uint(256), 256).store_uint(0, 8).end_cell()
    return any(is_state_init_address(address, code_hash, code_depth, initial)
               for code_hash, code_depth in NFT_OFFER_CODES)


def parse_nft_offer(address: str, data: Cell) -> NftOfferData | None:
    try:
        offer = NftOfferData(data.begin_parse())
        return offer if is_nft_offer(Address(address), data) else None
    except Exception:
        return None


def parse_nft_offer_state_init(address: str, state_init_boc: str) -> NftOfferData | None:
    data = state_init_data(address, state_init_boc)
    return parse_nft_offer(address, data) if data is not None else None
