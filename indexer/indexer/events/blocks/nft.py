from __future__ import annotations

import base64
from dataclasses import dataclass

from pytoniq_core import Slice

from indexer.events import context

from indexer.events.blocks.labels import labeled
from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, OrMatcher, ContractMatcher
from indexer.events.blocks.core import Block
from indexer.events.blocks.messages import NftOwnershipAssigned, ExcessMessage
from indexer.events.blocks.messages.nft import NftDiscovery, NftReportStaticData, NftTransfer, TeleitemBidInfo, AuctionFillUp
from indexer.events.blocks.utils import AccountId, Amount
from indexer.events.blocks.utils.block_utils import find_call_contracts, get_labeled
from indexer.events.blocks.utils.block_utils import find_messages


class NftMintBlock(Block):
    def __init__(self, data: dict):
        super().__init__('nft_mint', [], data)


class NftTransferBlock(Block):
    def __init__(self):
        super().__init__('nft_transfer', [],  None)


@dataclass
class NftDiscoveryBlockData:
    sender: AccountId
    nft: AccountId
    result_collection: AccountId
    result_index: int
    query_id: int

class NftDiscoveryBlock(Block):
    data: NftDiscoveryBlockData

    def __init__(self, data: NftDiscoveryBlockData):
        super().__init__("nft_discovery", [], data)

    def __repr__(self):
        return f"nft_discovery {self.data}"


async def _get_nft_data(nft_address: AccountId):
    data = {
        "address": nft_address,
        "index": None,
        "collection": None,
        "exists": False
    }
    nft = await context.interface_repository.get().get_nft_item(nft_address.as_str())
    if nft is not None:
        data['index'] = nft.index
        data['exists'] = True
        if "uri" in nft.content and "https://nft.fragment.com" in nft.content["uri"]:
            tokens = nft.content["uri"].split("/")
            data["name"] = tokens[-1][:-5]
            data["type"] = tokens[-2]
        else:
            data['meta'] = nft.content
        if nft.collection_address is not None:
            data['collection'] = {
                'address': AccountId(nft.collection_address),
            }
    return data


async def _try_get_nft_purchase_data(block: Block, owner: str) -> dict | None:
    prev_block = block.previous_block
    event_node = block.previous_block.event_nodes[0]
    if isinstance(prev_block, TonTransferBlock) and event_node.message.source.upper() == owner.upper():
        nft_sale = await context.interface_repository.get().get_nft_sale(event_node.message.transaction.account)
        if nft_sale is not None:
            return {
                'marketplace_address': nft_sale.marketplace_address,
                'nft_address': nft_sale.nft_address,
                'block': block.previous_block,
                'price': nft_sale.full_price,
                'real_prev_owner': nft_sale.nft_owner_address,
            }

    nft_auction = await context.interface_repository.get().get_nft_auction(event_node.message.transaction.account)
    if nft_auction is not None:
        return {
            'marketplace_address': nft_auction.mp_addr,
            'nft_address': nft_auction.nft_addr,
            'block': block.previous_block,
            'price': nft_auction.last_bid,
            'real_prev_owner': nft_auction.nft_owner,
        }

    return None


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
            Slice.one_from_boc(block.event_nodes[0].message.message_content.body))
        ownership_assigned_message = find_messages(other_blocks, NftOwnershipAssigned)
        if len(ownership_assigned_message) > 0:
            nft_ownership_message = ownership_assigned_message[0][1]
            data['prev_owner'] = AccountId(nft_ownership_message.prev_owner)
        else:
            data['prev_owner'] = AccountId(block.event_nodes[0].message.source)
        data['query_id'] = nft_transfer_message.query_id
        data['forward_amount'] = Amount(nft_transfer_message.forward_amount)
        if nft_transfer_message.response_destination:
            data['response_destination'] = AccountId(nft_transfer_message.response_destination)
        else:
            data['response_destination'] = None
        data['custom_payload'] = base64.b64encode(nft_transfer_message.custom_payload).decode('utf-8') if (
                nft_transfer_message.custom_payload is not None) else None
        data['forward_payload'] = base64.b64encode(nft_transfer_message.forward_payload).decode('utf-8') if (
                nft_transfer_message.forward_payload is not None) else None
        data['new_owner'] = AccountId(nft_transfer_message.new_owner)
        data['nft'] = await _get_nft_data(AccountId(block.event_nodes[0].message.transaction.account))
        if block.previous_block is not None:
            nft_purchase_data = await _try_get_nft_purchase_data(block, nft_transfer_message.new_owner.to_str(False))
            if nft_purchase_data is not None and AccountId(nft_purchase_data['nft_address']) == data['nft']['address']:
                real_owner = AccountId(nft_purchase_data['real_prev_owner'])
                if real_owner != data['new_owner']:
                    data['is_purchase'] = True
                    data['marketplace'] = 'getgems'
                    data['marketplace_address'] = AccountId(nft_purchase_data['marketplace_address'])
                    data['price'] = Amount(nft_purchase_data['price'])
                    data['real_prev_owner'] = AccountId(nft_purchase_data['real_prev_owner'])
                    if isinstance(block.previous_block, TonTransferBlock):
                        include.append(block.previous_block)
                    elif isinstance(block.previous_block, CallContractBlock) and block.previous_block.get_message().source is None:
                        include.append(block.previous_block)

                    siblings = block.previous_block.next_blocks
                    for sibling in siblings:
                        if sibling is not block:
                            if isinstance(sibling, TonTransferBlock) and sibling.get_message().destination == real_owner:
                                data['payout_amount'] = Amount(sibling.get_message().value)
                                data['payout_comment_encrypted'] = sibling.encrypted
                                data['payout_comment_encoded'] = sibling.comment_encoded
                                data['payout_comment'] = sibling.comment
                                include.append(sibling)
                                break

        include.extend(other_blocks)
        new_block.merge_blocks(include)
        new_block.data = data
        if not data['nft']['exists']:
            new_block.broken = True
        new_block.failed = block.failed
        return [new_block]

class NftDiscoveryBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(
            optional=False,
            child_matcher=labeled("report",
                            ContractMatcher(
                                opcode=NftReportStaticData.opcode,
                                optional=False,
                            )
                    ),
            )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == NftDiscovery.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        include = [block]

        sender = block.get_message().source
        nft = block.get_message().destination

        report_block = get_labeled("report", other_blocks, CallContractBlock)
        if not report_block:
            return []
        report_data = NftReportStaticData(report_block.get_body())

        data = NftDiscoveryBlockData(
            sender=AccountId(sender),
            nft=AccountId(nft),
            result_collection=AccountId(report_data.collection),
            result_index=report_data.index,
            query_id=report_data.query_id,
        )
        include.append(report_block)

        new_block = NftDiscoveryBlock(data)
        new_block.merge_blocks(include)

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
        nft_ownership_message = NftOwnershipAssigned(Slice.one_from_boc(message.message_content.body))
        data['new_owner'] = AccountId(message.destination)
        data['prev_owner'] = AccountId(nft_ownership_message.prev_owner) if nft_ownership_message.prev_owner is not None else None
        data['query_id'] = nft_ownership_message.query_id
        data['forward_amount'] = None
        data['response_destination'] = None
        data['custom_payload'] = None
        data['forward_payload'] = None
        data['nft'] = await _get_nft_data(AccountId(block.get_message().source))
        payload = nft_ownership_message.nft_payload
        if payload is not None:
            data['forward_payload'] = base64.b64encode(payload.raw).decode('utf-8')
        if payload is not None and isinstance(payload.value, TeleitemBidInfo):
            data['is_purchase'] = True
            data['price'] = Amount(payload.value.bid)
            data['marketplace'] = 'fragment'
            data['real_prev_owner'] = None
            prev_block = block.previous_block
            if (isinstance(prev_block, TonTransferBlock) or
                    (isinstance(prev_block, CallContractBlock) and prev_block.get_message().source is None)):
                include.extend(find_call_contracts(prev_block.next_blocks, AuctionFillUp.opcode))
                include.append(prev_block)

        include.extend(other_blocks)
        new_block.merge_blocks(include)
        new_block.data = data
        if not data['nft']['exists']:
            new_block.broken = True
        return [new_block]


class NftMintBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=None,
                         parent_matcher=None)

    def test_self(self, block: Block):
        return len(block.contract_deployments) == 1

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        address = next(iter(block.contract_deployments)).as_str()
        nft_item = await context.interface_repository.get().get_nft_item(address)
        if nft_item is None:
            return []
        source = block.event_nodes[0].message.source
        data = {
            "source": AccountId(source) if source else None,
            "address": AccountId(address),
            "index": nft_item.index,
            "opcode": block.event_nodes[0].get_opcode(),
            "collection": AccountId(nft_item.collection_address) if nft_item.collection_address else None,
        }
        new_block = NftMintBlock(data)
        new_block.merge_blocks([block])
        return [new_block]
