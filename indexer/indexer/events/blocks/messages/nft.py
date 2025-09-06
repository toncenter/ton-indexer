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


class TeleitemStartAuction:
    opcode = 0x487a8e81
    query_id: int
    beneficiary_address: Address | None
    initial_min_bid: int
    max_bid: int
    min_bid_step: int
    min_extend_time: int
    duration: int
    
    def __init__(self, slice: Slice):
        slice.load_uint(32)  # opcode
        self.query_id = slice.load_uint(64)
        auction_config_cell = slice.load_ref()
        self._parse_auction_config(auction_config_cell.to_slice())
    
    def _parse_auction_config(self, config_slice: Slice):
        try:
            self.beneficiary_address = config_slice.load_address()
            self.initial_min_bid = config_slice.load_coins()
            self.max_bid = config_slice.load_coins()
            self.min_bid_step = config_slice.load_uint(8)  # uint8 according to common.fc:381
            self.min_extend_time = config_slice.load_uint(32)
            self.duration = config_slice.load_uint(32)
        except:
            self.beneficiary_address = None
            self.initial_min_bid = 0
            self.max_bid = 0
            self.min_bid_step = 0
            self.min_extend_time = 0
            self.duration = 0

class AuctionFillUp:
    opcode = 0x370FEC51

    query_id: int | None
    def __init__(self, slice: Slice):
        slice.load_uint(32)
        if slice.remaining_bits >= 64:
            self.query_id = slice.load_uint(64)
        else:
            self.query_id = None

class DnsReleaseBalance:
    opcode = 0x4ed14b65

    query_id: int | None

    def __init__(self, slice: Slice):
        slice.load_uint(32)
        if slice.remaining_bits >= 64:
            self.query_id = slice.load_uint(64)
        else:
            self.query_id = None

