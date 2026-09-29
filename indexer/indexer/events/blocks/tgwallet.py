from __future__ import annotations

from pytoniq_core import Slice

from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import BlockMatcher, ContractMatcher
from indexer.events.blocks.core import Block
from indexer.events.blocks.messages.externals import (
    TG_WALLET_CHANGE_PUBLIC_KEY_EXTERNAL,
    TG_WALLET_CHANGE_PUBLIC_KEY_INTERNAL,
    TgWalletChangePublicKeyRequest,
)
from indexer.events.blocks.utils import AccountId

# a successful rotation is logged by the wallet: an external out message with this opcode and encryptedOldPrivateKey
TG_WALLET_KEY_CHANGED_LOG = 0xEBA19948


class ChangeWalletKeyBlock(Block):
    """
    A wallet rotated its signing key. Keys and signatures are kept as hex strings.
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

    Only successful rotations of Telegram wallets are recognized
    """

    def __init__(self):
        super().__init__(child_matcher=ContractMatcher(opcode=TG_WALLET_KEY_CHANGED_LOG))

    def test_self(self, block: Block):
        if not isinstance(block, CallContractBlock):
            return False
        expected = TG_WALLET_CHANGE_PUBLIC_KEY_EXTERNAL if block.is_external else TG_WALLET_CHANGE_PUBLIC_KEY_INTERNAL
        return block.opcode == expected

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        tx = block.event_nodes[0].get_tx()
        if tx is None or tx.aborted:
            return []
        log_block = next(b for b in other_blocks if isinstance(b, CallContractBlock) and b.opcode == TG_WALLET_KEY_CHANGED_LOG)
        log = Slice.one_from_boc(log_block.get_message().message_content.body)
        if log.remaining_bits != 32 + 256 or log.load_uint(32) != TG_WALLET_KEY_CHANGED_LOG:
            return []
        encrypted_old_private_key = log.load_bytes(32)
        msg = block.get_message()
        if 'TgWallet' not in await context.interface_repository.get().get_interfaces(msg.destination):
            return []
        try:
            request = TgWalletChangePublicKeyRequest(Slice.one_from_boc(msg.message_content.body))
        except Exception:
            return []
        new_block = ChangeWalletKeyBlock({
            # externals have no source
            'source': AccountId(msg.source) if msg.source is not None else None,
            'destination': AccountId(msg.destination) if msg.destination is not None else None,
            'value': block.data['value'],
            'new_public_key': request.new_public_key.hex(),
            'rotation_signature': request.rotation_signature.hex() if request.rotation_signature else None,
            'encrypted_old_private_key': encrypted_old_private_key.hex(),
        })
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]
