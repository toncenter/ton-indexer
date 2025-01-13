from __future__ import annotations

from pytoniq_core import Address, ExternalAddress, Slice
from pytoniq_core.boc.address import typing


class TeleitemBidInfo:
    bid: int
    bit_ts: int

    def __init__(self, slice: Slice):
        self.bid = slice.load_coins()
        self.bit_ts = slice.load_uint(32)


class NftPayload:
    op: int | None
    value: TeleitemBidInfo | None
    raw: bytes

    def __init__(self, slice: Slice):
        self.value = None
        self.op = None
        self.raw = slice.to_cell().to_boc(hash_crc32=True)
        if slice.remaining_bits == 0 and slice.remaining_refs == 0:
            return
        tmp_cell = slice.copy()
        try:
            self.op = tmp_cell.load_uint(32) & 0xFFFFFFFF
        except:
            return
        if self.op == 0x38127DE1:
            self.value = TeleitemBidInfo(tmp_cell)


class NftTransfer:
    opcode = 0x5FCC3D14

    def __init__(self, slice: Slice):
        slice.load_uint(32)  # opcode
        self.query_id = slice.load_uint(64)
        self.new_owner = slice.load_address()
        self.response_destination = slice.load_address()
        custom_payload = slice.load_maybe_ref()
        if custom_payload:
            self.custom_payload = custom_payload.to_boc(hash_crc32=True)
        else:
            self.custom_payload = None
        self.forward_amount = slice.load_coins()
        self.forward_payload = None
        if slice.remaining_bits > 0:
            is_right = slice.load_bool()
            forward_payload = slice.load_ref() if is_right else slice.copy().to_cell()
            self.forward_payload = forward_payload.to_boc(hash_crc32=True)


class NftOwnershipAssigned:
    opcode = 0x05138D91

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


# get_static_data#2fcb26a2 query_id:uint64 = InternalMsgBody;
class NftDiscovery:
    opcode = 0x2FCB26A2
    query_id: int

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)


# report_static_data#8b771735
#        query_id:uint64 index:uint256
#        collection:MsgAddress
#        = InternalMsgBody;
class NftReportStaticData:
    opcode = 0x8B771735
    query_id: int
    index: int
    collection: Address

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        self.query_id = slice.load_uint(64)
        self.index = slice.load_uint(256)
        self.collection = slice.load_address()


class AuctionFillUp:
    opcode = 0x370FEC51
