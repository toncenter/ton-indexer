from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, ContractMatcher
from indexer.events.blocks.core import Block
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.messages import ElectorRecoverStakeConfirmation, ElectorRecoverStakeRequest, \
    ElectorDepositStakeRequest, ElectorDepositStakeConfirmation
from indexer.events.blocks.utils import Amount, AccountId
from indexer.events.blocks.utils.block_utils import get_labeled

elector_address = '-1:3333333333333333333333333333333333333333333333333333333333333333'

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
        confirmation_matcher = labeled('confirmation', ContractMatcher(
            opcode=ElectorDepositStakeConfirmation.opcode, optional=True, include_excess=False))
        super().__init__(child_matcher=confirmation_matcher, include_excess=False)

    def test_self(self, block: Block):
        return (isinstance(block, CallContractBlock) and block.opcode == ElectorDepositStakeRequest.opcode
                and block.get_message().destination == elector_address)

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        data = {
            'amount': Amount(block.event_nodes[0].message.value),
            'stake_holder': AccountId(block.event_nodes[0].message.source),
        }
        confirmation = get_labeled('confirmation', other_blocks, CallContractBlock)
        new_block = ElectionDepositStakeBlock(data)
        new_block.failed = confirmation is None
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


class ElectionRecoverStakeBlockMatcher(BlockMatcher):
    def __init__(self):
        confirmation_matcher = labeled('confirmation', ContractMatcher(
            opcode=ElectorRecoverStakeConfirmation.opcode,
            optional=True,
            include_excess=False))
        super().__init__(child_matcher=confirmation_matcher, include_excess=False)

    def test_self(self, block: Block):
        return (isinstance(block, CallContractBlock) and block.opcode == ElectorRecoverStakeRequest.opcode
                and block.get_message().destination == elector_address)

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        data = {
            'stake_holder': AccountId(block.event_nodes[0].message.source)
        }
        response = get_labeled('confirmation', other_blocks, CallContractBlock)
        failed = False
        if response is not None:
            data['amount'] = Amount(response.event_nodes[0].message.value)
        else:
            failed = True
        new_block = ElectionRecoverStakeBlock(data)
        new_block.failed = failed
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]
