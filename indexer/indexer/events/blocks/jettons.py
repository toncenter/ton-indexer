from __future__ import annotations

from events.blocks.messages import JettonNotify, JettonInternalTransfer, ExcessMessage, JettonBurnNotification
from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, OrMatcher, ContractMatcher, child_sequence_matcher
from indexer.events.blocks.core import Block
from indexer.events.blocks.messages import JettonTransfer, JettonBurn
from indexer.events.blocks.utils import AccountId, Asset, Amount


class JettonTransferBlock(Block):
    def __init__(self, data):
        super().__init__('jetton_transfer', [], data)

    def __repr__(self):
        return f"JETTON TRANSFER {self.event_nodes[0].message.transaction.hash}"


class JettonBurnBlock(Block):
    def __init__(self):
        super().__init__('jetton_burn', [], {})

    def __repr__(self):
        return f"JETTON BURN {self.data}"


class JettonTransferBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=OrMatcher([
            ContractMatcher(opcode=JettonNotify.opcode, optional=True),
            ContractMatcher(opcode=JettonInternalTransfer.opcode, optional=True,
                            child_matcher=ContractMatcher(opcode=JettonNotify.opcode, optional=True))
        ], optional=True), parent_matcher=None)

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == JettonTransfer.opcode

    async def build_block(self, block: Block | CallContractBlock, other_blocks: list[Block]) -> list[Block]:
        new_block = JettonTransferBlock({})
        include = [block]
        include.extend(other_blocks)
        jetton_transfer_message = JettonTransfer(block.get_body())
        receiver: AccountId = AccountId(jetton_transfer_message.destination)

        data = {
            'sender': None,
            'sender_wallet': AccountId(block.event_nodes[0].message.message.destination),
            'receiver': receiver,
            'asset': None,
            'amount': Amount(jetton_transfer_message.amount),
        }
        if len(block.next_blocks) > 0:
            data['receiver_wallet'] = AccountId(block.next_blocks[0].event_nodes[0].message.message.destination)
        sender = await context.extra_data_repository.get().get_jetton_wallet(
            block.event_nodes[0].message.message.destination)
        if sender is not None:
            data['sender'] = AccountId(sender.owner) if sender is not None else None
            data['asset'] = Asset(is_ton=False, jetton_address=sender.jetton if sender is not None else None)
            new_block.value_flow.add_jetton(AccountId(sender.owner), AccountId(sender.jetton),
                                            -jetton_transfer_message.amount)
            new_block.value_flow.add_jetton(receiver, AccountId(sender.jetton),
                                            jetton_transfer_message.amount)

        new_block.data = data
        new_block.merge_blocks(include)
        new_block.failed = block.failed
        return [new_block]


async def _get_jetton_burn_data(new_block: Block, block: Block | CallContractBlock) -> dict:
    jetton_burn_message = JettonBurn(block.get_body())
    wallet = await context.extra_data_repository.get().get_jetton_wallet(block.get_message().message.destination)
    new_block.value_flow.add_jetton(AccountId(wallet.owner), AccountId(wallet.jetton), -jetton_burn_message.amount)
    data = {
        'owner': wallet.owner if wallet is not None else None,
        'jetton_wallet': block.get_message().message.destination,
        'amount': Amount(jetton_burn_message.amount),
        'asset': Asset(is_ton=False, jetton_address=wallet.jetton if wallet is not None else None)
    }
    return data


class JettonBurnBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=child_sequence_matcher([
            ContractMatcher(opcode=JettonBurnNotification.opcode),
            ContractMatcher(opcode=ExcessMessage.opcode)
        ]))

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == JettonBurn.opcode

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        new_block = JettonBurnBlock()
        include = [block]
        include.extend(other_blocks)
        new_block.merge_blocks(include)
        new_block.data = await _get_jetton_burn_data(new_block, block)
        return [new_block]
