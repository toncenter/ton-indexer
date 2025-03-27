from __future__ import annotations

from pytoniq_core import Slice

from indexer.events.blocks.messages.dns import ChangeDnsRecordMessage
from indexer.events.blocks.utils import AccountId
from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, ContractMatcher
from indexer.events.blocks.core import Block
from indexer.events import context

zero_key = b'\x00' * 32

class DeleteDnsRecordBlock(Block):
    def __init__(self, data):
        super().__init__('delete_dns', [], data)

    def __repr__(self):
        return f"DELETE_DNS {self.event_nodes[0].message.transaction.hash}"

class DnsRenewBlock(Block):
    def __init__(self, data):
        super().__init__('renew_dns', [], data)

    def __repr__(self):
        return f"DNS_RENEW {self.event_nodes[0].message.transaction.hash}"

class ChangeDnsRecordBlock(Block):
    def __init__(self, data):
        super().__init__('change_dns', [], data)

    def __repr__(self):
        return f"CHANGE_DNS {self.event_nodes[0].message.transaction.hash}"


class ChangeDnsRecordMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=ContractMatcher(opcode=0xffffffff,
                                                       optional=True,
                                                       include_excess=False))

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == ChangeDnsRecordMessage.opcode

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        change_dns_message = ChangeDnsRecordMessage(Slice.one_from_boc(block.event_nodes[0].message.message_content.body))
        new_block = None
        sender = block.event_nodes[0].message.source
        nft_item = await context.interface_repository.get().get_nft_item(block.event_nodes[0].message.destination)

        if change_dns_message.has_value:
            new_block = ChangeDnsRecordBlock({
                'source': AccountId(sender) if sender is not None else None,
                'destination': AccountId(block.event_nodes[0].message.destination),
                'key': change_dns_message.key,
                'value': change_dns_message.value,
            })
        else:
            if change_dns_message.key == zero_key:
                new_block = DnsRenewBlock({
                    'source': AccountId(sender) if sender is not None else None,
                    'destination': AccountId(block.event_nodes[0].message.destination),
                })
            else:
                new_block = DeleteDnsRecordBlock({
                    'source': AccountId(sender) if sender is not None else None,
                    'destination': AccountId(block.event_nodes[0].message.destination),
                    'key': change_dns_message.key,
                })
        if nft_item is not None:
            new_block.data['collection_address'] = AccountId(nft_item.collection_address)
        new_block.failed = block.failed
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]
