from __future__ import annotations

from events.blocks.messages import ExcessMessage
from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, BlockTypeMatcher, ContractMatcher
from indexer.events.blocks.core import Block
from indexer.events.blocks.messages import JettonInternalTransfer, JettonNotify
from indexer.events.blocks.utils import Amount, AccountId
from indexer.events.blocks.utils.block_utils import find_messages


class WtonMintBlock(Block):
    def __init__(self, data):
        super().__init__('wton_mint', [], data)

    def __repr__(self):
        return f"WTON_MINT {self.event_nodes[0].message.transaction.hash}"


class WtonMintBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(children_matchers=[
            ContractMatcher(opcode=JettonInternalTransfer.opcode,
                            include_excess=True,
                            child_matcher=ContractMatcher(opcode=JettonNotify.opcode,
                                                          optional=True,
                                                          include_excess=True)),
            ContractMatcher(opcode=ExcessMessage.opcode,
                            optional=True)]
        ),

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == 0x77a33521

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        new_block = WtonMintBlock({})
        new_block.merge_blocks([block] + other_blocks)
        jetton_internal_transfers = find_messages(other_blocks, JettonInternalTransfer)
        jetton_internal_transfer = jetton_internal_transfers[0][1]
        new_block.data = {
            'amount': Amount(jetton_internal_transfer.amount),
            'receiver': AccountId(jetton_internal_transfers[0][0].event_nodes[0].message.message.destination),
        }
        return [new_block]
