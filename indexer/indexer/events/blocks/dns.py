from __future__ import annotations

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


async def build_change_dns_core(
    dns_block: Block | CallContractBlock,
) -> ChangeDnsRecordBlock | DnsRenewBlock | DeleteDnsRecordBlock:
    """Shared build core for the DNS record-change family. Does NOT merge consumed
    blocks — the caller (legacy build_block or the mch synthesized wrapper) is
    responsible for merge_blocks.

    One anchor opcode, three semantic outcomes dispatched on parsed content:
    has_value -> change_dns, zero key -> renew_dns, else -> delete_dns.
    """
    change_dns_message = ChangeDnsRecordMessage(dns_block.get_body())
    sender = dns_block.event_nodes[0].message.source
    destination = dns_block.event_nodes[0].message.destination
    nft_item = await context.interface_repository.get().get_nft_item(destination)

    if change_dns_message.has_value:
        new_block = ChangeDnsRecordBlock({
            'source': AccountId(sender) if sender is not None else None,
            'destination': AccountId(destination),
            'key': change_dns_message.key,
            'value': change_dns_message.value,
        })
    elif change_dns_message.key == zero_key:
        new_block = DnsRenewBlock({
            'source': AccountId(sender) if sender is not None else None,
            'destination': AccountId(destination),
        })
    else:
        new_block = DeleteDnsRecordBlock({
            'source': AccountId(sender) if sender is not None else None,
            'destination': AccountId(destination),
            'key': change_dns_message.key,
        })
    if nft_item is not None:
        new_block.data['collection_address'] = AccountId(nft_item.collection_address)
    new_block.failed = dns_block.failed
    return new_block


class ChangeDnsRecordMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=ContractMatcher(opcode=0xffffffff,
                                                       optional=True,
                                                       include_excess=False))

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == ChangeDnsRecordMessage.opcode

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        new_block = await build_change_dns_core(block)
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]
