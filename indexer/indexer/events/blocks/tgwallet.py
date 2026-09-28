from __future__ import annotations

from pytoniq_core import Slice

from indexer.core.database import FinalityState, Transaction
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

TG_WALLET_CHANGE_PUBLIC_KEY_OPCODES = frozenset({
    TG_WALLET_CHANGE_PUBLIC_KEY_INTERNAL,
    TG_WALLET_CHANGE_PUBLIC_KEY_EXTERNAL,
})


async def _storage_changed(tx: Transaction) -> bool:
    if tx.emulated or tx.finality != FinalityState.finalized:
        return True  # states of pending traces are not stored; the trace is classified again once finalized
    # requested along with the interfaces, see address_selectors.extract_tg_wallet_key_rotation_states
    extra = await context.interface_repository.get().get_extra_data(tx.account, 'account_states')
    states = (extra or {}).get('states', {})
    before = states.get(tx.account_state_hash_before)
    after = states.get(tx.account_state_hash_after)
    if before is None or after is None:
        return False
    return before['data_hash'] != after['data_hash']


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

    Only successful rotations are recognized. Everything is read from the request body
    """

    def __init__(self):
        super().__init__()

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode in TG_WALLET_CHANGE_PUBLIC_KEY_OPCODES
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        tx = block.event_nodes[0].get_tx()
        # failed rotations are not recognized
        if tx is None or tx.aborted or tx.compute_exit_code not in (None, 0):
            return []
        if block.opcode == TG_WALLET_CHANGE_PUBLIC_KEY_INTERNAL and not await _storage_changed(tx):
            return []
        msg = block.get_message()
        try:
            request = TgWalletChangePublicKeyRequest(Slice.one_from_boc(msg.message_content.body))
        except Exception:
            return []
        if request.rotation_signature is None:
            return []  # the contract checks the rotation proof, so a rotation always has a valid one
        encrypted_old_private_key = request.encrypted_old_private_key
        new_block = ChangeWalletKeyBlock({
            # externals have no source
            'source': AccountId(msg.source) if msg.source is not None else None,
            'destination': AccountId(msg.destination) if msg.destination is not None else None,
            'value': block.data['value'],
            'new_public_key': request.new_public_key.hex(),
            'rotation_signature': request.rotation_signature.hex(),
            'encrypted_old_private_key': encrypted_old_private_key.hex() if encrypted_old_private_key else None,
        })
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]
