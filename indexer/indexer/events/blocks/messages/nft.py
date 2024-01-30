from __future__ import annotations

from pytoniq_core import Slice, Address


class TeleitemBidInfo:
    bid: int
    bit_ts: int

    def __init__(self, slice: Slice):
        self.bid = slice.load_coins()
        self.bit_ts = slice.load_uint(32)


class NftPayload:
    op: int | None
    value: TeleitemBidInfo | None

    def __init__(self, slice: Slice):
        self.value = None
        self.op = None
        if slice.remaining_bits == 0 and slice.remaining_refs == 0:
            return
        tmp_cell = slice.copy()
        try:
            self.op = tmp_cell.load_uint(32) & 0xFFFFFFFF
        except:
            return
        if self.op == 0x38127de1:
            self.value = TeleitemBidInfo(tmp_cell)


class NftTransfer:
    opcode = 0x5fcc3d14

    def __init__(self, slice: Slice):
        slice.load_uint(32)  # opcode
        self.query_id = slice.load_uint(64)
        self.new_owner = slice.load_address()
        self.response_destination = slice.load_address()
        self.custom_payload = slice.load_maybe_ref()
        self.forward_amount = slice.load_coins()


class NftOwnershipAssigned:
    opcode = 0x05138d91

    query_id: int
    prev_owner: Address
    nft_payload: NftPayload | None

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.prev_owner = slice.load_address()
        try:
            if slice.load_bit():
                self.nft_payload = NftPayload(slice.load_ref().to_slice())
            else:
                self.nft_payload = NftPayload(slice)
        except:
            self.nft_payload = None
