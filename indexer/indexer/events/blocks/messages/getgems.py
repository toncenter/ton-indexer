import base64
import logging
from dataclasses import dataclass
from typing import Optional, Dict

from pytoniq_core import Slice, StateInit

from indexer.core.database import Message
from indexer.events.blocks.basic_blocks import TonTransferBlock
from indexer.events.blocks.core import Block

AUCTION_VERSION_MAPPING = {
    # "zlp4U06qps7tja/UhtB262CpsNbb+1Nnb2YmScBomVY=": "v4r1", # TODO
    "ZmiHL6eXBUQ//UdSPo6eqfdquZ+aC1nSfej4GhwnudQ=": "v2",
    "G9nFo5v/t6DzQViLXdkrgTqEK/Ze8UEJOCIAzq+Pct8=": "v3r2",
    "u29ireD+stefqzuK6/CTCvmFU99gCTsgJ/Covxab/Ow=": "v3r3",
    "/ACindAgW83MDT/7nKOMw8jBWexg2KpUMkCpLxBZLUA=": "v1"
}

SALE_VERSION_MAPPING = {
    "2pufziLofEllctIDZSWVebzO+RpyA1fMvowFLvyb4I8=": "v1",
    "gnj0xSM95vvtyWmvUZNEp6m//FRIVtuphqlcC8+Fcck=": "v2",
    "MgUN+sRPZIZrzIbyzZ4TBf6dyts5WcACI3z7CQLUQyM=": "v3",
    "3rU7bFdlwebNI4v0e8XoO6WWvcwEsLhM1Qqx5HSgjzE=": "v3r2",
    "JCIfpXHlQuBVx3vt/b9SfHr0YM/cfzRMRQeHtM+h600=": "v3r3",
    "a5WmQYucnSNZBF0edVm41UmuDlBvJMqrWPowyPsf64Y=": "v4r1",
}
loggger = logging.getLogger(__name__)
@dataclass
class NftSaleData:
    """Data class for NFT Fixed Price Sale contracts."""
    marketplace_address: Optional[str] = None
    marketplace: Optional[str] = None
    full_price: Optional[int] = None
    marketplace_fee_address: Optional[str] = None
    marketplace_fee: Optional[int] = None
    royalty_address: Optional[str] = None
    royalty_amount: Optional[int] = None
    fee_percent: Optional[int] = None
    royalty_percent: Optional[int] = None
    # TODO: Jetton sale support
    jetton_price_dict: Optional[dict] = None


@dataclass
class NftAuctionData:
    """Data class for NFT Auction contracts."""
    mp_addr: Optional[str] = None
    mp_fee_addr: Optional[str] = None
    mp_fee_factor: Optional[int] = None
    mp_fee_base: Optional[int] = None
    marketplace: Optional[str] = None
    royalty_fee_addr: Optional[str] = None
    royalty_fee_factor: Optional[int] = None
    royalty_fee_base: Optional[int] = None
    min_bid: Optional[int] = None
    max_bid: Optional[int] = None
    min_step: Optional[int] = None
    end_time: Optional[int] = None
    step_time: Optional[int] = None
    try_step_time: Optional[int] = None


def get_sale_data(boc: str, code_hash_b64: str) -> Optional[NftSaleData]:

    version = SALE_VERSION_MAPPING.get(code_hash_b64, "latest")
    """Parses NFT sale contract data from a BOC string."""
    if version == "latest":
        version = "v4r1"

    try:
        state_init = StateInit.deserialize(Slice.one_from_boc(boc))
        data = NftSaleData()
        cs = state_init.data.to_slice()
        if version == "v4r1":
            cs.load_uint(1)  # is_complete
            data.marketplace_address = cs.load_address()
            cs.load_address()  # nft_owner_address
            data.full_price = cs.load_coins()
            cs.load_uint(32)  # sold_at
            cs.load_uint(64)  # query_id

            static_data_cell = cs.load_ref()
            static_slice = static_data_cell.to_slice()
            data.marketplace_fee_address = static_slice.load_address()
            data.royalty_address = static_slice.load_address()
            data.fee_percent = static_slice.load_uint(17)
            data.royalty_percent = static_slice.load_uint(17)

            return data

        if version in ["v2", "v3", "v3r2", "v3r3"]:
            cs.load_uint(1)  # is_complete
            cs.load_uint(32)  # created_at

        data.marketplace_address = cs.load_address()
        cs.load_address()  # nft_address
        cs.load_address()  # nft_owner_address
        data.full_price = cs.load_coins()

        fees_cell = cs.load_ref()
        fees_slice = fees_cell.to_slice()

        if version == "v1":
            data.marketplace_fee = fees_slice.load_coins()
            data.marketplace_fee_address = fees_slice.load_address()
            data.royalty_address = fees_slice.load_address()
            data.royalty_amount = fees_slice.load_coins()
        else:  # v2, v3, v3r3
            data.marketplace_fee_address = fees_slice.load_address()
            data.marketplace_fee = fees_slice.load_coins()
            data.royalty_address = fees_slice.load_address()
            data.royalty_amount = fees_slice.load_coins()

        return data

    except Exception as e:
        loggger.exception(e, exc_info=True)
        return None


def get_auction_data(boc: str, code_hash_b64: str) -> Optional[NftAuctionData]:
    """Parses NFT auction contract data from a BOC string."""
    version = AUCTION_VERSION_MAPPING.get(code_hash_b64, "latest")
    if version == "latest":
        version = "v3r3"

    try:
        state_init = StateInit.deserialize(Slice.one_from_boc(boc))
        cs = state_init.data.to_slice()
        if version == "v1":
            return parse_auction_v1(cs)
        elif version == "v3r2":
            return parse_auction_v3r2(cs)
        elif version == "v3r3":
            return parse_auction_v3r3(cs)
        return None

    except Exception:
        return None

def parse_auction_v1(cs: Slice) -> NftAuctionData:
    data = NftAuctionData()
    fees_cell = cs.load_ref().to_slice()
    bids_cell = cs.load_ref().to_slice()

    data.mp_fee_addr = fees_cell.load_address()
    data.mp_fee_factor = fees_cell.load_uint(32)
    data.mp_fee_base = fees_cell.load_uint(32)
    data.royalty_fee_addr = fees_cell.load_address()
    data.royalty_fee_factor = fees_cell.load_uint(32)
    data.royalty_fee_base = fees_cell.load_uint(32)

    data.min_bid = bids_cell.load_coins()
    data.max_bid = bids_cell.load_coins()
    data.min_step = bids_cell.load_coins()
    data.end_time = bids_cell.load_uint(32)
    data.step_time = bids_cell.load_uint(32)
    data.try_step_time = bids_cell.load_uint(32)
    return data

def parse_auction_v3r2(cs: Slice) -> NftAuctionData:
    data = NftAuctionData()
    cs.load_uint(1 + 1 + 1)  # end?, activated?, is_canceled?
    cs.load_address()  # last_member
    cs.load_coins()  # last_bid
    cs.load_uint(32)  # last_bid_at
    data.end_time = cs.load_uint(32)
    fees_cell = cs.load_ref().to_slice()
    constant_cell = cs.load_ref().to_slice()

    constant_cell.load_uint(32) # sub_gas_price_from_bid
    data.mp_addr = constant_cell.load_address()
    data.min_bid = constant_cell.load_coins()
    data.max_bid = constant_cell.load_coins()
    data.min_step = constant_cell.load_coins()
    data.step_time = constant_cell.load_uint(32)

    data.mp_fee_addr = fees_cell.load_address()
    data.mp_fee_factor = fees_cell.load_uint(32)
    data.mp_fee_base = fees_cell.load_uint(32)
    data.royalty_fee_addr = fees_cell.load_address()
    data.royalty_fee_factor = fees_cell.load_uint(32)
    data.royalty_fee_base = fees_cell.load_uint(32)

    return data

def parse_auction_v3r3(cs: Slice) -> NftAuctionData:
    data = NftAuctionData()
    cs.load_uint(1 + 1)  # end?, is_canceled?
    cs.load_address()  # last_member
    cs.load_coins()  # last_bid
    cs.load_uint(32)  # last_bid_at

    data.end_time = cs.load_uint(32)

    cs.load_address() # nft owner
    cs.load_uint(64) # last_query_id

    data.mp_fee_factor = cs.load_uint(32)
    data.mp_fee_base = cs.load_uint(32)
    data.royalty_fee_factor = cs.load_uint(32)
    data.royalty_fee_base = cs.load_uint(32)

    fees_cell = cs.load_ref().to_slice()
    constant_cell = cs.load_ref().to_slice()

    data.mp_fee_addr = fees_cell.load_address()
    data.royalty_fee_addr = fees_cell.load_address()

    data.mp_addr = constant_cell.load_address()
    data.min_bid = constant_cell.load_coins()
    data.max_bid = constant_cell.load_coins()
    data.min_step = constant_cell.load_coins()
    data.step_time = constant_cell.load_uint(32)

    return data