from __future__ import annotations

import logging

from pytoniq_core import Slice

from indexer.core.database import Transaction
from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import BlockMatcher
from indexer.events.blocks.core import Block
from indexer.events.blocks.messages.externals import (
    TG_WALLET_CHANGE_PUBLIC_KEY_EXTERNAL,
    TG_WALLET_CHANGE_PUBLIC_KEY_INTERNAL,
    TgWalletChangePublicKeyRequest,
)
from indexer.events.blocks.utils import AccountId

logger = logging.getLogger(__name__)


async def _storage_changed(tx: Transaction) -> bool:
    extra = await context.interface_repository.get().get_extra_data(tx.account, 'account_states')
    states = (extra or {}).get('states', {})
    before = states.get(tx.account_state_hash_before)
    after = states.get(tx.account_state_hash_after)
    if before is not None and after is not None:
        return before['data_hash'] != after['data_hash']
    logger.info(f"tg-wallet key rotation {tx.hash} accepted without the storage check")
    return True


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

    Only successful rotations of Telegram wallets are recognized. The contract accepts the internal
    opcode only in internal messages and the external one only in externals; a failing external never
    lands on chain, a failing internal one may be a silent return, so its storage change is checked.
    """

    def __init__(self):
        super().__init__()

    def test_self(self, block: Block):
        if not isinstance(block, CallContractBlock):
            return False
        expected = TG_WALLET_CHANGE_PUBLIC_KEY_EXTERNAL if block.is_external else TG_WALLET_CHANGE_PUBLIC_KEY_INTERNAL
        return block.opcode == expected

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        tx = block.event_nodes[0].get_tx()
        # failed rotations are not recognized
        if tx is None or tx.aborted:
            return []
        msg = block.get_message()
        if 'TgWallet' not in await context.interface_repository.get().get_interfaces(msg.destination):
            return []
        if not block.is_external and not await _storage_changed(tx):
            return []
        try:
            request = TgWalletChangePublicKeyRequest(Slice.one_from_boc(msg.message_content.body))
        except Exception:
            return []
        encrypted_old_private_key = request.encrypted_old_private_key
        new_block = ChangeWalletKeyBlock({
            # externals have no source
            'source': AccountId(msg.source) if msg.source is not None else None,
            'destination': AccountId(msg.destination) if msg.destination is not None else None,
            'value': block.data['value'],
            'new_public_key': request.new_public_key.hex(),
            'rotation_signature': request.rotation_signature.hex() if request.rotation_signature else None,
            'encrypted_old_private_key': encrypted_old_private_key.hex() if encrypted_old_private_key else None,
        })
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]
