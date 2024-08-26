from __future__ import annotations

import base64

from indexer.events.blocks.utils.block_utils import find_call_contract
from indexer.events.blocks.messages import JettonNotify, JettonInternalTransfer, ExcessMessage, JettonBurnNotification
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
        has_internal_transfer = find_call_contract(other_blocks, JettonInternalTransfer.opcode) is not None
        data = {
            'has_internal_transfer': has_internal_transfer,
            'sender': None,
            'sender_wallet': AccountId(block.event_nodes[0].message.destination),
            'receiver': receiver,
            'response_address': AccountId(jetton_transfer_message.response),
            'forward_amount': Amount(jetton_transfer_message.forward_amount),
            'query_id': jetton_transfer_message.query_id,
            'asset': None,
            'amount': Amount(jetton_transfer_message.amount),
            'forward_payload': base64.b64encode(jetton_transfer_message.forward_payload).decode(
                'utf-8') if jetton_transfer_message.forward_payload is not None else None,
            'custom_payload': base64.b64encode(jetton_transfer_message.custom_payload).decode(
                'utf-8') if jetton_transfer_message.custom_payload is not None else None,
            'comment': jetton_transfer_message.comment,
            'encrypted_comment': jetton_transfer_message.encrypted_comment,
            'payload_opcode': jetton_transfer_message.payload_sum_type,
            'stonfi_swap_body': jetton_transfer_message.stonfi_swap_body
        }
        if len(block.next_blocks) > 0:
            data['receiver_wallet'] = AccountId(block.next_blocks[0].event_nodes[0].message.destination)
        sender = await context.interface_repository.get().get_jetton_wallet(
            block.event_nodes[0].message.destination)
        if sender is not None:
            data['sender'] = AccountId(sender.owner) if sender is not None else None
            data['asset'] = Asset(is_ton=False, jetton_address=sender.jetton if sender is not None else None)
            new_block.value_flow.add_jetton(AccountId(sender.owner), AccountId(sender.jetton),
                                            -jetton_transfer_message.amount)
            new_block.value_flow.add_jetton(receiver, AccountId(sender.jetton),
                                            jetton_transfer_message.amount)
        else:
            return []

        new_block.data = data
        new_block.merge_blocks(include)
        new_block.failed = block.failed
        return [new_block]


async def _get_jetton_burn_data(new_block: Block, block: Block | CallContractBlock) -> dict:
    jetton_burn_message = JettonBurn(block.get_body())
    wallet = await context.interface_repository.get().get_jetton_wallet(block.get_message().destination)
    assert wallet is not None
    new_block.value_flow.add_jetton(AccountId(wallet.owner), AccountId(wallet.jetton), -jetton_burn_message.amount)
    data = {
        'owner': AccountId(wallet.owner) if wallet is not None else None,
        'jetton_wallet': AccountId(block.get_message().destination),
        'amount': Amount(jetton_burn_message.amount),
        'asset': Asset(is_ton=False, jetton_address=wallet.jetton if wallet is not None else None)
    }
    return data


class JettonBurnBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=ContractMatcher(opcode=JettonBurnNotification.opcode, optional=True,
                                                       include_excess=True))

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == JettonBurn.opcode

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        new_block = JettonBurnBlock()
        include = [block]
        include.extend(other_blocks)
        new_block.data = await _get_jetton_burn_data(new_block, block)
        new_block.merge_blocks(include)
        return [new_block]
