from __future__ import annotations

from pytoniq_core import Slice

from events.blocks.utils.block_utils import find_messages
from indexer.core.database import SyncSessionMaker, NFTItem
from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, OrMatcher, ContractMatcher
from indexer.events.blocks.core import Block
from indexer.events.blocks.messages import NftOwnershipAssigned, ExcessMessage
from indexer.events.blocks.messages.nft import NftTransfer, TeleitemBidInfo, AuctionFillUp
from indexer.events.blocks.utils import AccountId, Amount, block_utils
from indexer.events.blocks.utils.block_utils import find_call_contracts


class NftTransferBlock(Block):
    def __init__(self):
        super().__init__('nft_transfer', [],  None)


def _get_nft_data(nft_address: AccountId):
    data = {
        "address": nft_address
    }
    with SyncSessionMaker() as session:
        nft = session.query(NFTItem).filter(NFTItem.address == nft_address.as_str()).join(NFTItem.collection).first()
        if nft is not None:
            if "uri" in nft.content and "https://nft.fragment.com" in nft.content["uri"]:
                tokens = nft.content["uri"].split("/")
                data["name"] = tokens[-1][:-5]
                data["type"] = tokens[-2]
            else:
                data['meta'] = nft.content
            data['collection'] = {
                'address': AccountId(nft.collection.address),
                'meta': nft.collection.collection_content
            }
    return data


def _try_get_nft_purchase_data(block: Block, owner: str) -> tuple[list[Block], int] | None:
    prev_block = block.previous_block
    event_node = block.previous_block.event_nodes[0]
    if not isinstance(prev_block, TonTransferBlock) or event_node.message.message.source.upper() != owner.upper():
        return None
    price = 0
    block_to_include = [block.previous_block]
    for sibling in block.previous_block.next_blocks:
        if isinstance(sibling, TonTransferBlock) and sibling != block:
            price += sibling.value
            block_to_include.append(sibling)
    tx = event_node.message.transaction
    flow = block_utils.merge_flows(block_to_include + [block])
    nft_flow = flow.flow[AccountId(tx.account)]
    price += nft_flow.ton - nft_flow.fees

    return block_to_include, price


class NftTransferBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=OrMatcher([
            ContractMatcher(opcode=NftOwnershipAssigned.opcode, optional=True),
            ContractMatcher(opcode=ExcessMessage.opcode, optional=True)
        ], optional=True), parent_matcher=None)

    def test_self(self, block: Block):
        if isinstance(block, CallContractBlock) and block.opcode == NftTransfer.opcode:
            return True

    async def build_block(self, block: Block, other_blocks: list['Block']):
        new_block = NftTransferBlock()
        include = [block]
        data = dict()
        data['is_purchase'] = False
        nft_transfer_message = NftTransfer(
            Slice.one_from_boc(block.event_nodes[0].message.message.message_content.body))
        ownership_assigned_message = find_messages(other_blocks, NftOwnershipAssigned)
        if len(ownership_assigned_message) > 0:
            nft_ownership_message = ownership_assigned_message[0][1]
            data['prev_owner'] = AccountId(nft_ownership_message.prev_owner)
        else:
            data['prev_owner'] = AccountId(block.event_nodes[0].message.message.source)
        data['query_id'] = nft_transfer_message.query_id
        data['new_owner'] = AccountId(nft_transfer_message.new_owner)
        data['nft'] = _get_nft_data(AccountId(block.event_nodes[0].message.transaction.account))
        if block.previous_block is not None and isinstance(block.previous_block, TonTransferBlock):
            nft_purchase_data = _try_get_nft_purchase_data(block, nft_transfer_message.new_owner.to_str(False))
            if nft_purchase_data is not None:
                block_to_include, price = nft_purchase_data
                data['is_purchase'] = True
                data['price'] = Amount(price)
                if isinstance(block.previous_block, TonTransferBlock):
                    include.append(block.previous_block)

        include.extend(other_blocks)
        new_block.merge_blocks(include)
        new_block.data = data
        return [new_block]


class TelegramNftPurchaseBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=None,
                         parent_matcher=None)

    def test_self(self, block: Block):
        if isinstance(block, CallContractBlock) and block.opcode == NftOwnershipAssigned.opcode:
            return True

    async def build_block(self, block: Block, other_blocks: list['Block']):
        assert isinstance(block, CallContractBlock)
        new_block = NftTransferBlock()
        include = [block]
        data = dict()
        data['is_purchase'] = False
        message = block.get_message()
        nft_ownership_message = NftOwnershipAssigned(Slice.one_from_boc(message.message.message_content.body))
        data['new_owner'] = AccountId(message.message.destination)
        data['query_id'] = nft_ownership_message.query_id
        data['nft'] = _get_nft_data(AccountId(block.get_message().message.destination))
        payload = nft_ownership_message.nft_payload
        if payload is not None and isinstance(payload.value, TeleitemBidInfo):
            data['is_purchase'] = True
            data['price'] = Amount(payload.value.bid)
            prev_block = block.previous_block
            if (isinstance(prev_block, TonTransferBlock) or
                    (isinstance(prev_block, CallContractBlock) and prev_block.get_message().message.source is None)):
                include.extend(find_call_contracts(prev_block.next_blocks, AuctionFillUp.opcode))
                include.append(prev_block)

        include.extend(other_blocks)
        new_block.merge_blocks(include)
        new_block.data = data
        return [new_block]
