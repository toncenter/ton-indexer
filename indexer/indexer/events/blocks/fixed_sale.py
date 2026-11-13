from __future__ import annotations

from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, BlockTypeMatcher, GenericMatcher, TonTransferMatcher
from indexer.events.blocks.core import Block
from indexer.events.blocks.messages import NftOwnershipAssigned
from indexer.events.blocks.messages.fixed_sale import FixedSaleBuy, FixedSaleCancel, FixedSaleChangePrice, \
    parse_fixed_sale, parse_fixed_sale_state_init
from indexer.events.blocks.nft import NftTransferBlock
from indexer.events.blocks.nft_offer import NftEscrowBlock, get_escrow, nft_escrow_data
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.utils import AccountId, Amount
from indexer.events.blocks.utils.block_utils import get_labeled, get_labeled_sorted


def _call(block: Block, opcode: int) -> bool:
    return isinstance(block, CallContractBlock) and not block.is_external and block.opcode == opcode


def _deploy(block: Block) -> bool:
    """A top-up carrying the same StateInit can reach a sale deployed earlier."""
    if not isinstance(block, TonTransferBlock):
        return False
    msg = block.get_message()
    return msg.init_state is not None and msg.transaction.orig_status in ('nonexist', 'uninit')


class FixedSaleCreateMatcher(BlockMatcher):
    """A listing starts when the sale accepts the NFT; the sale may be deployed in this trace or earlier."""

    def __init__(self):
        super().__init__(parent_matcher=GenericMatcher(
            lambda b: b is not None, optional=True,
            child_matcher=labeled('deploy', GenericMatcher(_deploy, optional=True))))

    def test_self(self, block: Block):
        return isinstance(block, NftTransferBlock)

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        sale_address = block.data['new_owner']
        # A refused deposit sends the NFT back; an unpaid or invalid one aborts.
        notice = next((n for n in block.event_nodes if n.get_opcode() == NftOwnershipAssigned.opcode
                       and AccountId(n.message.destination) == sale_address), None)
        if notice is None or notice.children:
            return []
        tx = notice.get_tx()
        if tx.aborted or tx.compute_exit_code != 0:
            return []
        # A pending trace cannot read a sale deployed in the same trace from the database.
        deploy = next((b for b in get_labeled_sorted('deploy', other_blocks, TonTransferBlock)
                       if b.data['destination'] == sale_address), None)
        if deploy is not None:
            sale = parse_fixed_sale_state_init(sale_address.as_str(), deploy.get_message().init_state.body)
        else:
            sale = await get_escrow(sale_address.as_str(), parse_fixed_sale)
        if sale is None or block.data['nft']['address'] != AccountId(sale.nft):
            return []
        nft = block.data['nft']
        new_block = NftEscrowBlock('nft_fixed_sale_create', {
            'source': block.data['prev_owner'],
            'destination': sale_address,
            'seller': AccountId(sale.seller),
            'amount': None,
            'nft_address': nft['address'],
            'nft_collection': nft['collection']['address'] if nft['collection'] else None,
            'nft_index': nft['index'],
            'full_price': Amount(sale.initial_price),
            'fee_amount': Amount(sale.fee_amount(sale.initial_price)),
            'fee_address': AccountId(sale.fee_address),
        })
        new_block.merge_blocks([block])
        return [new_block]


class FixedSaleUpdateMatcher(BlockMatcher):
    def test_self(self, block: Block):
        return _call(block, FixedSaleChangePrice.opcode)

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        sale = await get_escrow(msg.destination, parse_fixed_sale)
        if sale is None or AccountId(msg.source) != AccountId(sale.seller):
            return []
        price = FixedSaleChangePrice(block.get_body()).price
        data = await nft_escrow_data(sale.seller, msg.destination, sale.nft)
        data['full_price'] = Amount(price)
        data['fee_amount'] = Amount(sale.fee_amount(price))
        data['fee_address'] = AccountId(sale.fee_address)
        new_block = NftEscrowBlock('nft_fixed_sale_update', data)
        new_block.failed = block.event_nodes[0].get_tx().aborted
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


class FixedSalePurchaseMatcher(BlockMatcher):
    # A refused buy bounces and pays nothing.
    def __init__(self):
        super().__init__(children_matchers=[labeled('payout', TonTransferMatcher()),
                                            labeled('onward', BlockTypeMatcher('nft_transfer')),
                                            labeled('fee', TonTransferMatcher())])

    def test_self(self, block: Block):
        return _call(block, FixedSaleBuy.opcode)

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        sale = await get_escrow(msg.destination, parse_fixed_sale)
        if sale is None:
            return []
        # The sale pays the seller before it sweeps the rest to the fee address.
        payout, fee = sorted([get_labeled('payout', other_blocks), get_labeled('fee', other_blocks)],
                             key=lambda b: b.min_lt)
        onward = get_labeled('onward', other_blocks, NftTransferBlock)
        sale_address = AccountId(msg.destination)
        if (payout.data['destination'] != AccountId(sale.seller) or fee.data['destination'] != AccountId(sale.fee_address)
                or onward.data['prev_owner'] != sale_address or onward.data['nft']['address'] != AccountId(sale.nft)):
            return []
        nft = onward.data['nft']
        new_block = NftEscrowBlock('nft_fixed_sale_purchase', {
            'source': AccountId(sale.seller),
            'destination': onward.data['new_owner'],
            'buyer': AccountId(msg.source),
            'escrow': sale_address,
            'amount': Amount(payout.value),
            'nft_address': nft['address'],
            'nft_collection': nft['collection']['address'] if nft['collection'] else None,
            'nft_index': nft['index'],
            'query_id': onward.data['query_id'],
            'price': Amount(payout.value),
            'fee_amount': Amount(sale.fee_amount(payout.value)),
            'fee_address': AccountId(sale.fee_address),
        })
        # The NFT goes on non-bounceable, so a rejected transfer leaves it with the sale.
        new_block.failed = onward.failed
        new_block.merge_blocks([block, payout, onward])
        return [new_block]


class FixedSaleCancelMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=labeled('back', BlockTypeMatcher('nft_transfer', optional=True)))

    def test_self(self, block: Block):
        return _call(block, FixedSaleCancel.opcode)

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        sale = await get_escrow(msg.destination, parse_fixed_sale)
        if sale is None or AccountId(msg.source) != AccountId(sale.seller):
            return []
        sale_address = AccountId(msg.destination)
        back = next((b for b in get_labeled_sorted('back', other_blocks, NftTransferBlock)
                     if b.data['prev_owner'] == sale_address and b.data['new_owner'] == AccountId(sale.seller)), None)
        data = await nft_escrow_data(sale.seller, msg.destination, sale.nft)
        new_block = NftEscrowBlock('nft_fixed_sale_cancel', data)
        new_block.failed = block.event_nodes[0].get_tx().aborted or (back is not None and back.failed)
        new_block.merge_blocks([block] + ([back] if back is not None else []))
        return [new_block]
