from __future__ import annotations

from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import BlockMatcher
from indexer.events.blocks.core import Block
from indexer.events.blocks.messages.externals import (
    TG_WALLET_CHANGE_PUBLIC_KEY_EXTERNAL,
    TG_WALLET_CHANGE_PUBLIC_KEY_INTERNAL,
)
from indexer.events.blocks.utils import AccountId

TG_WALLET_CHANGE_PUBLIC_KEY_OPCODES = frozenset({
    TG_WALLET_CHANGE_PUBLIC_KEY_INTERNAL,
    TG_WALLET_CHANGE_PUBLIC_KEY_EXTERNAL,
})


class ChangeWalletKeyBlock(Block):
    """
    A wallet rotated its signing key. Carries no custom data, only the generic action fields.
    """

    def __init__(self, data):
        super().__init__('change_wallet_key', [], data)

    def __repr__(self):
        return f"CHANGE_WALLET_KEY {self.data}"


class ChangeWalletKeyMatcher(BlockMatcher):
    """
    Telegram wallet (https://github.com/ton-blockchain/tg-wallet-contract) key rotation.

    The request comes either as an external (0xFBBA99C8) or, when someone else pays the gas, as an
    internal message (0xFBBA99C7). Both bodies start with a 512 bit signature, so the opcode stored
    on the message row is a slice of that signature - CallContractBlock.opcode already holds the
    real request opcode (see basic_blocks.get_call_contract_opcode).
    """

    def __init__(self):
        super().__init__()

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode in TG_WALLET_CHANGE_PUBLIC_KEY_OPCODES
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        new_block = ChangeWalletKeyBlock({
            # externals have no source
            'source': AccountId(msg.source) if msg.source is not None else None,
            'destination': AccountId(msg.destination) if msg.destination is not None else None,
            'value': block.data['value'],
        })
        new_block.failed = block.failed
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]
