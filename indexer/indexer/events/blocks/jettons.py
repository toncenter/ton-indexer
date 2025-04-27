from __future__ import annotations

import base64

from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, OrMatcher, ContractMatcher
from indexer.events.blocks.core import Block
from indexer.events.blocks.messages import JettonNotify, JettonInternalTransfer, JettonBurnNotification, JettonMint
from indexer.events.blocks.messages import JettonTransfer, JettonBurn
from indexer.events.blocks.messages.jettons import MinterJettonMint
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
        super().__init__(child_matcher=ContractMatcher(opcode=JettonInternalTransfer.opcode,
                            child_matcher=ContractMatcher(opcode=JettonNotify.opcode, optional=True)), parent_matcher=None)

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == JettonTransfer.opcode

    async def build_block(self, block: Block | CallContractBlock, other_blocks: list[Block]) -> list[Block]:
        include = [block]
        include.extend(other_blocks)
        jetton_transfer_message = JettonTransfer(block.get_body())
        new_block = JettonTransferBlock({}, jetton_transfer_message)

        internal_transfer = find_call_contract(other_blocks, JettonInternalTransfer.opcode)
        has_internal_transfer = internal_transfer is not None
        amount = jetton_transfer_message.amount
        forward_ton_amount = jetton_transfer_message.forward_amount
        if has_internal_transfer:
            try:
                internal_transfer_msg = JettonInternalTransfer(internal_transfer.get_body())
                amount = internal_transfer_msg.amount
                forward_ton_amount = internal_transfer_msg.forward_ton_amount
            except Exception:
                pass
        sender = block.get_message().source
        sender_jetton_wallet = block.get_message().destination
        receiver_wallet = internal_transfer.get_message().destination
        receiver = jetton_transfer_message.destination

        receiver_wallet_info = await context.interface_repository.get().get_jetton_wallet(receiver_wallet)
        if receiver_wallet_info is None:
            return []

        if receiver_wallet_info.owner != receiver.to_str(False):
            block.broken = True
            receiver = receiver_wallet_info.owner

        asset = Asset(is_ton=False, jetton_address=receiver_wallet_info.jetton)

        data = {
            'has_internal_transfer': has_internal_transfer,
            'sender': AccountId(sender),
            'sender_wallet': AccountId(sender_jetton_wallet),
            'receiver': AccountId(receiver),
            'receiver_wallet': AccountId(receiver_wallet),
            'response_address': AccountId(jetton_transfer_message.response),
            'forward_amount': Amount(forward_ton_amount),
            'desired_forward_amount': Amount(jetton_transfer_message.forward_amount),
            'query_id': jetton_transfer_message.query_id,
            'asset': asset,
            'amount': Amount(amount),
            'desired_amount': Amount(jetton_transfer_message.amount),
            'forward_payload': base64.b64encode(jetton_transfer_message.forward_payload).decode(
                'utf-8') if jetton_transfer_message.forward_payload is not None else None,
            'custom_payload': base64.b64encode(jetton_transfer_message.custom_payload).decode(
                'utf-8') if jetton_transfer_message.custom_payload is not None else None,
            'comment': jetton_transfer_message.comment,
            'encrypted_comment': jetton_transfer_message.encrypted_comment,
            'payload_opcode': jetton_transfer_message.payload_sum_type,
            'stonfi_swap_body': jetton_transfer_message.stonfi_swap_body
        }

        new_block.data = data
        new_block.merge_blocks(include)
        new_block.failed = block.failed or internal_transfer.failed

        return [new_block]

class PTonTransferMatcher(BlockMatcher):

    pton_masters = [
        "0:8CDC1D7640AD5EE326527FC1AD0514F468B30DC84B0173F0E155F451B4E11F7C",
        "0:671963027F7F85659AB55B821671688601CDCF1EE674FC7FBBB1A776A18D34A3"
    ]

    def __init__(self):
        super().__init__(child_matcher=ContractMatcher(opcode=JettonNotify.opcode, optional=True), parent_matcher=None)

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == JettonTransfer.opcode

    async def build_block(self, block: Block | CallContractBlock, other_blocks: list[Block]) -> list[Block]:
        include = [block]
        include.extend(other_blocks)

        jetton_transfer_message = JettonTransfer(block.get_body())
        new_block = JettonTransferBlock({}, jetton_transfer_message)

        wallet = block.get_message().destination
        wallet_info = await context.interface_repository.get().get_jetton_wallet(wallet)
        if wallet_info is None or wallet_info.jetton not in PTonTransferMatcher.pton_masters:
            return []
        receiver = jetton_transfer_message.destination
        sender = block.get_message().source

        asset = Asset(is_ton=False, jetton_address=wallet_info.jetton)

        data = {
            'has_internal_transfer': False,
            'sender': AccountId(sender),
            'sender_wallet': None,
            'receiver': AccountId(receiver),
            'receiver_wallet': None,
            'response_address': AccountId(jetton_transfer_message.response),
            'forward_amount': Amount(jetton_transfer_message.forward_amount),
            'query_id': jetton_transfer_message.query_id,
            'asset': asset,
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
    new_block: Block, block: Block | CallContractBlock,
    blocks: list[Block]
) -> tuple[dict, bool]:
    if block.opcode == MinterJettonMint.opcode:
        jetton_mint_info = MinterJettonMint(block.get_body())
    else:
        jetton_mint_info = JettonMint(block.get_body())
    internal_transfer_block = find_call_contract(blocks, JettonInternalTransfer.opcode)

    if not block.failed and internal_transfer_block is not None:
        internal_transfer_info = JettonInternalTransfer(internal_transfer_block.get_body())
        failed = block.failed or internal_transfer_block.failed

        receiver_jwallet_addr = internal_transfer_block.event_nodes[0].message.destination
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
            "to_jetton_wallet": AccountId(receiver_jwallet.address),
            "amount": Amount(internal_transfer_info.amount),
            "ton_amount": Amount(jetton_mint_info.ton_amount),
            "asset": Asset(
                is_ton=False,
                jetton_address=(receiver_jwallet.jetton),
            ),
        }
        return data, failed
    else:
        data = {
            "to": AccountId(jetton_mint_info.to_address),
            "to_jetton_wallet": None,
            "asset": Asset(is_ton=False, jetton_address=block.get_message().destination),
            "amount": None,
            "ton_amount": Amount(jetton_mint_info.ton_amount),
        }
        if block.opcode == MinterJettonMint.opcode:
            data['amount'] = Amount(jetton_mint_info.master_msg_jetton_amount)
    return data, True

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
            isinstance(block, CallContractBlock) and block.opcode in [JettonMint.opcode, MinterJettonMint.opcode]
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        new_block = JettonMintBlock()
        include = [block]
        include.extend(other_blocks)
        new_block.data, new_block.failed = await _get_jetton_mint_data(new_block, block, other_blocks)
        new_block.merge_blocks(include)
        return [new_block]

class FallbackJettonTransferBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__()

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == JettonTransfer.opcode

    async def build_block(self, block: Block | CallContractBlock, other_blocks: list[Block]) -> list[Block]:
        include = [block]
        include.extend(other_blocks)
        jetton_transfer_message = JettonTransfer(block.get_body())
        new_block = JettonTransferBlock({}, jetton_transfer_message)

        sender = block.get_message().source
        sender_jetton_wallet = block.get_message().destination
        receiver = jetton_transfer_message.destination

        receiver_wallet_info = await context.interface_repository.get().get_jetton_wallet(sender_jetton_wallet)
        if receiver_wallet_info is not None:
            asset = Asset(is_ton=False, jetton_address=receiver_wallet_info.jetton)
        else:
            asset = None

        data = {
            'has_internal_transfer': False,
            'sender': AccountId(sender),
            'sender_wallet': AccountId(sender_jetton_wallet),
            'receiver': AccountId(receiver),
            'receiver_wallet': None,
            'response_address': AccountId(jetton_transfer_message.response),
            'forward_amount': Amount(jetton_transfer_message.forward_amount),
            'query_id': jetton_transfer_message.query_id,
            'asset': asset,
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

        new_block.data = data
        new_block.merge_blocks(include)
        new_block.failed = block.failed

        return [new_block]

