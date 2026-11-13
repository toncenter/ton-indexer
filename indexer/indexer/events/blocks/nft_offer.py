from __future__ import annotations

from typing import Callable, TypeVar

from pytoniq_core import Address, Cell

from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.basic_matchers import BlockMatcher
from indexer.events.blocks.core import Block
from indexer.events.blocks.nft import NftTransferBlock
from indexer.events.blocks.messages.nft_offer import NftOfferCancel, NftOfferCancelByBuyer, \
    parse_nft_offer, parse_nft_offer_state_init
from indexer.events.blocks.utils import AccountId, Amount

T = TypeVar('T')


class NftEscrowBlock(Block):
    def __init__(self, btype: str, data: dict):
        super().__init__(btype, [], data)

    def __repr__(self):
        return f"{self.btype} {self.data}"


async def get_escrow(address: str, parse: Callable[[str, Cell], T | None]) -> T | None:
    extra = await context.interface_repository.get().get_extra_data(address, 'data_boc')
    if extra is None:
        return None
    try:
        data = Cell.one_from_boc(extra['data_boc'])
    except Exception:
        return None
    return parse(address, data)


async def nft_escrow_data(owner: Address, escrow_address: str, nft_address: Address,
                          transfer: TonTransferBlock | None = None) -> dict:
    nft = await context.interface_repository.get().get_nft_item(nft_address.to_str(is_user_friendly=False).upper())
    return {
        'source': AccountId(owner),
        'destination': AccountId(escrow_address),
        'amount': Amount(transfer.value) if transfer is not None else None,
        'nft_address': AccountId(nft_address),
        'nft_collection': AccountId(nft.collection_address) if nft is not None and nft.collection_address else None,
        'nft_index': nft.index if nft is not None else None,
    }


def _transfers(block: Block) -> list[TonTransferBlock]:
    return sorted((b for b in block.next_blocks if isinstance(b, TonTransferBlock)), key=lambda b: b.min_lt)


class NftOfferCancelMatcher(BlockMatcher):
    def test_self(self, block: Block):
        return (isinstance(block, CallContractBlock) and not block.is_external
                and block.opcode == NftOfferCancelByBuyer.opcode)

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        offer = await get_escrow(msg.destination, parse_nft_offer)
        if offer is None or AccountId(msg.source) != AccountId(offer.buyer):
            return []
        transfers = _transfers(block)
        # When the fee address is the buyer, only creation order distinguishes fee from refund.
        fee = transfers[0] if len(transfers) > 1 else None
        refund = transfers[-1] if transfers and transfers[-1].data['destination'] == AccountId(offer.buyer) else None
        data = await nft_escrow_data(offer.buyer, msg.destination, offer.nft, refund)
        data['fee_amount'] = Amount(fee.value) if fee is not None else None
        data['fee_address'] = fee.data['destination'] if fee is not None else None
        new_block = NftEscrowBlock('nft_offer_cancel', data)
        # An unpaid fee aborts the action phase and emits no messages.
        new_block.failed = block.event_nodes[0].get_tx().aborted
        new_block.merge_blocks([block] + other_blocks + ([refund] if refund is not None else []))
        return [new_block]


class NftOfferExternalCancelMatcher(BlockMatcher):
    """Before expiry, the contract accepts an external cancel only with its key's signature."""

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.is_external and block.opcode == NftOfferCancel.opcode

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        offer = await get_escrow(msg.destination, parse_nft_offer)
        if offer is None:
            return []
        transfers = _transfers(block)
        refund = transfers[0] if transfers else None
        if refund is None or refund.data['destination'] != AccountId(offer.buyer):
            return []
        expired = block.event_nodes[0].get_tx().now >= offer.expires_at
        btype = 'nft_offer_expire' if expired else 'nft_offer_signed_cancel'
        new_block = NftEscrowBlock(btype, await nft_escrow_data(offer.buyer, msg.destination, offer.nft, refund))
        new_block.merge_blocks([block] + other_blocks + [refund])
        return [new_block]


class NftOfferCreateMatcher(BlockMatcher):
    """An expired or underfunded deploy aborts but leaves the account deployed in Funding."""

    def test_self(self, block: Block):
        if not isinstance(block, TonTransferBlock):
            return False
        msg = block.get_message()
        return (msg.opcode is None and msg.init_state is not None
                and msg.transaction.orig_status in ('nonexist', 'uninit'))

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        offer = parse_nft_offer_state_init(msg.destination, msg.init_state.body)
        if offer is None or AccountId(msg.source) != AccountId(offer.buyer):
            return []
        data = await nft_escrow_data(offer.buyer, msg.destination, offer.nft, block)
        data['full_price'] = Amount(offer.price)
        data['fee_amount'] = Amount(offer.fee_amount)
        data['fee_address'] = AccountId(offer.fee_address)
        new_block = NftEscrowBlock('nft_offer_create', data)
        new_block.failed = msg.transaction.aborted
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


class NftOfferAcceptMatcher(BlockMatcher):
    def test_self(self, block: Block):
        return isinstance(block, NftTransferBlock)

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        # Checking the shape avoids unrelated kvrocks reads on ordinary NFT transfers.
        transfers = _transfers(block)
        onwards = [b for b in block.next_blocks if isinstance(b, NftTransferBlock)]
        # A refused accept returns the NFT and pays nothing.
        if len(transfers) != 2 or len(onwards) != 1:
            return []
        offer_address = block.data['new_owner']
        offer = await get_escrow(offer_address.as_str(), parse_nft_offer)
        # A fake NFT named in the offer can report any previous owner.
        seller = block.data['prev_owner']
        if offer is None or block.data['nft']['address'] != AccountId(offer.nft) or seller != AccountId(offer.seller):
            return []
        # An NFT with its own code can imitate these outgoing messages.
        onward = onwards[0]
        if (onward.data['prev_owner'] != offer_address or onward.data['nft']['address'] != AccountId(offer.nft)
                or onward.data['new_owner'] != AccountId(offer.recipient)):
            return []
        payout, rest = transfers
        if payout.data['source'] != offer_address or rest.data['source'] != offer_address:
            return []
        if payout.data['destination'] != seller or rest.data['destination'] != AccountId(offer.fee_address):
            return []
        nft = block.data['nft']
        new_block = NftEscrowBlock('nft_offer_accept', {
            'source': seller,
            'destination': AccountId(offer.recipient),
            'buyer': AccountId(offer.buyer),
            'escrow': offer_address,
            'amount': Amount(payout.value),
            'nft_address': nft['address'],
            'nft_collection': nft['collection']['address'] if nft['collection'] else None,
            'nft_index': nft['index'],
            'query_id': block.data['query_id'],
            'price': Amount(offer.price),
            'fee_amount': Amount(offer.fee_amount),
            'fee_address': AccountId(offer.fee_address),
        })
        # The onward transfer is non-bounceable, so rejection leaves the NFT with the offer.
        new_block.failed = onward.failed
        new_block.merge_blocks([block, payout, onward] + other_blocks)
        return [new_block]
