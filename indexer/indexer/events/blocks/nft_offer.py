from __future__ import annotations

from pytoniq_core import Cell

from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.basic_matchers import BlockMatcher
from indexer.events.blocks.core import Block
from indexer.events.blocks.nft import NftTransferBlock
from indexer.events.blocks.messages.nft_offer import NftOfferCancel, NftOfferCancelByBuyer, NftOfferData, \
    parse_nft_offer, parse_nft_offer_state_init
from indexer.events.blocks.utils import AccountId, Amount


class NftOfferBlock(Block):
    def __init__(self, btype: str, data: dict):
        super().__init__(btype, [], data)

    def __repr__(self):
        return f"{self.btype} {self.data}"


async def get_nft_offer(address: str) -> NftOfferData | None:
    extra = await context.interface_repository.get().get_extra_data(address, 'data_boc')
    if extra is None:
        return None
    try:
        data = Cell.one_from_boc(extra['data_boc'])
    except Exception:
        return None
    return parse_nft_offer(address, data)


async def _nft_offer_data(offer_address: str, offer: NftOfferData, transfer: TonTransferBlock | None,
                          fine: TonTransferBlock | None) -> dict:
    nft = await context.interface_repository.get().get_nft_item(offer.nft.to_str(is_user_friendly=False).upper())
    return {
        'source': AccountId(offer.buyer),
        'destination': AccountId(offer_address),
        'amount': Amount(transfer.value) if transfer is not None else None,
        'nft_address': AccountId(offer.nft),
        'nft_collection': AccountId(nft.collection_address) if nft is not None and nft.collection_address else None,
        'nft_index': nft.index if nft is not None else None,
        'royalty_amount': Amount(fine.value) if fine is not None else None,
        'royalty_address': fine.data['destination'] if fine is not None else None,
    }


def _transfers(block: Block) -> list[TonTransferBlock]:
    return sorted((b for b in block.next_blocks if isinstance(b, TonTransferBlock)), key=lambda b: b.min_lt)


class NftOfferCancelMatcher(BlockMatcher):
    def test_self(self, block: Block):
        return (isinstance(block, CallContractBlock) and not block.is_external
                and block.opcode == NftOfferCancelByBuyer.opcode)

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        offer = await get_nft_offer(msg.destination)
        if offer is None or AccountId(msg.source) != AccountId(offer.buyer):
            return []
        transfers = _transfers(block)
        # When the royalty address is the buyer, only creation order distinguishes fine from refund.
        fine = transfers[0] if len(transfers) > 1 else None
        refund = transfers[-1] if transfers and transfers[-1].data['destination'] == AccountId(offer.buyer) else None
        new_block = NftOfferBlock('nft_offer_cancel', await _nft_offer_data(msg.destination, offer, refund, fine))
        # An unpaid fine aborts the action phase and emits no messages.
        new_block.failed = block.event_nodes[0].get_tx().aborted
        new_block.merge_blocks([block] + other_blocks + ([refund] if refund is not None else []))
        return [new_block]


class NftOfferExternalCancelMatcher(BlockMatcher):
    """Before expiry, the contract accepts an external cancel only with its key's signature."""

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.is_external and block.opcode == NftOfferCancel.opcode

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        offer = await get_nft_offer(msg.destination)
        if offer is None:
            return []
        transfers = _transfers(block)
        refund = transfers[0] if transfers else None
        if refund is None or refund.data['destination'] != AccountId(offer.buyer):
            return []
        expired = block.event_nodes[0].get_tx().now >= offer.expires_at
        btype = 'nft_offer_expire' if expired else 'nft_offer_signed_cancel'
        new_block = NftOfferBlock(btype, await _nft_offer_data(msg.destination, offer, refund, None))
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
        data = await _nft_offer_data(msg.destination, offer, block, None)
        data['full_price'] = Amount(offer.price)
        data['royalty_amount'] = Amount(offer.price * offer.royalty_bps // 10000)
        data['royalty_address'] = AccountId(offer.royalty_address)
        new_block = NftOfferBlock('nft_offer_create', data)
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
        offer = await get_nft_offer(offer_address.as_str())
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
        if payout.data['destination'] != seller or rest.data['destination'] != AccountId(offer.royalty_address):
            return []
        nft = block.data['nft']
        new_block = NftOfferBlock('nft_offer_accept', {
            'source': seller,
            'destination': AccountId(offer.recipient),
            'offer': offer_address,
            'amount': Amount(payout.value),
            'nft_address': nft['address'],
            'nft_collection': nft['collection']['address'] if nft['collection'] else None,
            'nft_index': nft['index'],
            'query_id': block.data['query_id'],
            'price': Amount(offer.price + offer.price * offer.royalty_bps // 10000),
            'royalty_amount': Amount(rest.value),
            'royalty_address': AccountId(offer.royalty_address),
        })
        # The onward transfer is non-bounceable, so rejection leaves the NFT with the offer.
        new_block.failed = onward.failed
        new_block.merge_blocks([block, payout, onward] + other_blocks)
        return [new_block]
