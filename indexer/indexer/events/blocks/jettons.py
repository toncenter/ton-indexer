from __future__ import annotations

import base64

from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, OrMatcher, ContractMatcher
from indexer.events.blocks.core import Block
from indexer.events.blocks.messages import JettonNotify, JettonInternalTransfer, JettonBurnNotification, JettonMint
from indexer.events.blocks.messages import JettonTransfer, JettonBurn
from indexer.events.blocks.utils import AccountId, Asset, Amount
from indexer.events.blocks.utils.block_utils import find_call_contract


class JettonTransferBlock(Block):
    def __init__(self, data, jetton_transfer_message: JettonTransfer):
        super().__init__('jetton_transfer', [], data)
        self.jetton_transfer_message = jetton_transfer_message

    def __repr__(self):
        return f"JETTON TRANSFER {self.event_nodes[0].message.transaction.hash}"


class JettonBurnBlock(Block):
    def __init__(self):
        super().__init__('jetton_burn', [], {})

    def __repr__(self):
        return f"JETTON BURN {self.data}"


class JettonMintBlock(Block):
    def __init__(self):
        super().__init__("jetton_mint", [], {})

    def __repr__(self):
        return f"JETTON MINT {self.data}"


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
        include = [block]
        include.extend(other_blocks)
        jetton_transfer_message = JettonTransfer(block.get_body())
        new_block = JettonTransferBlock({}, jetton_transfer_message)

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

async def _get_jetton_mint_data(
    new_block: Block, block: Block | CallContractBlock
) -> dict:
    jetton_mint_info = JettonMint(block.get_body())
    if not block.failed:
        internal_transfer_info = JettonInternalTransfer(block.next_blocks[0].get_body())
        # receiver_address = (
        #     block.next_blocks[0].next_blocks[0].event_nodes[0].message.destination
        # )

        receiver_jwallet_addr = block.next_blocks[0].event_nodes[0].message.destination
        receiver_jwallet = await context.interface_repository.get().get_jetton_wallet(
            receiver_jwallet_addr
        )
        assert receiver_jwallet is not None

        receiver_account = AccountId(receiver_jwallet.owner)

        new_block.value_flow.add_jetton(
            # owner, jetton master, amount to add
            receiver_account,
            AccountId(receiver_jwallet.jetton),
            internal_transfer_info.amount,
        )

        data = {
            "to": AccountId(receiver_jwallet.owner),
            "to_jetton_wallet": AccountId(block.get_message().destination),
            "amount": Amount(internal_transfer_info.amount),
            "ton_amount": Amount(jetton_mint_info.ton_amount),
            "asset": Asset(
                is_ton=False,
                jetton_address=(receiver_jwallet.jetton),
            ),
        }
    else:
        data = {
            "to": AccountId(jetton_mint_info.to_address),
            "to_jetton_wallet": None,
            "asset": Asset(is_ton=False, jetton_address=block.get_message().destination),
            "amount": None,
            "ton_amount": Amount(jetton_mint_info.ton_amount),
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
        new_block.failed = block.failed
        return [new_block]


class JettonMintBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(
            child_matcher=OrMatcher(
                [
                    ContractMatcher(opcode=JettonNotify.opcode, optional=True),
                    ContractMatcher(
                        opcode=JettonInternalTransfer.opcode,
                        optional=True,
                        child_matcher=ContractMatcher(
                            opcode=JettonNotify.opcode, optional=True
                        ),
                    ),
                ],
                optional=True,
            ),
            parent_matcher=None,
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock) and block.opcode == JettonMint.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        new_block = JettonMintBlock()
        include = [block]
        include.extend(other_blocks)
        new_block.data = await _get_jetton_mint_data(new_block, block)
        new_block.merge_blocks(include)
        new_block.failed = block.failed
        return [new_block]
