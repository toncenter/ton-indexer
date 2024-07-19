from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, ContractMatcher, OrMatcher
from indexer.events.blocks.core import Block
from indexer.events.blocks.utils import Amount, AccountId
from indexer.events.blocks.utils.block_utils import find_call_contract


class ElectionDepositStakeBlock(Block):
    def __init__(self, data):
        super().__init__('election_deposit', [], data)

    def __repr__(self):
        return f"ELECTION_DEPOSIT {self.event_nodes[0].message.transaction.hash}"


class ElectionRecoverStakeBlock(Block):
    def __init__(self, data):
        super().__init__('election_recover', [], data)

    def __repr__(self):
        return f"ELECTION_RECOVER {self.event_nodes[0].message.transaction.hash}"


class ElectionDepositStakeBlockMatcher(BlockMatcher):
    def __init__(self):
        confirmation_matcher = ContractMatcher(opcode=0xf374484c, optional=True, include_excess=False)
        super().__init__(child_matcher=OrMatcher([
            ContractMatcher(opcode=0x4e73744b, optional=True, include_excess=False, child_matcher=confirmation_matcher),
            confirmation_matcher]), include_excess=False)

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == 0x4e73744b

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        data = {
            'amount': Amount(block.event_nodes[0].message.value),
            'stake_holder': AccountId(block.event_nodes[0].message.source),
        }
        new_block = ElectionDepositStakeBlock(data)
        new_block.failed = find_call_contract(other_blocks, 0xf374484c) is None
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


class ElectionRecoverStakeBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=ContractMatcher(opcode=0xf96f7324,
                                                       optional=True,
                                                       include_excess=False),
                         include_excess=False)

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == 0x47657424

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        data = {
            'stake_holder': AccountId(block.event_nodes[0].message.source)
        }
        response = find_call_contract(other_blocks, 0xf96f7324)
        failed = False
        if response is not None:
            data['amount'] = Amount(response.event_nodes[0].message.value)
        else:
            failed = True
        new_block = ElectionRecoverStakeBlock(data)
        new_block.failed = failed
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]
