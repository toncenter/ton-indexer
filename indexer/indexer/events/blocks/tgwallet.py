from __future__ import annotations

import logging

from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey
from pytoniq_core import Address, Slice, begin_cell

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


# KeyRotationProofPayload tag, the new key signs it together with the wallet address
KEY_ROTATION_PROOF_TAG = 0x4B45595F524F544154494F4E


def _signature_valid(public_key: bytes, signed_hash: bytes, signature: bytes | None) -> bool:
    if signature is None:
        return False
    try:
        VerifyKey(public_key).verify(signed_hash, signature)
        return True
    except BadSignatureError:
        return False


def _latest_state_confirms(tx: Transaction, request: TgWalletChangePublicKeyRequest, data_boc: str) -> bool:
    """
    Checks the request against the latest wallet storage (revision:uint8 seqno:uint32 subwallet:uint32
    publicKey:uint256). For a finalized trace it is the storage after the rotation or later, for a pending
    trace the one before it. A later rotation makes it inconclusive, so only a positive answer counts.
    """
    storage = Slice.one_from_boc(data_boc)
    storage.skip_bits(8)
    seqno, subwallet_id, public_key = storage.load_uint(32), storage.load_uint(32), storage.load_bytes(32)
    address = Address(tx.account)
    proof_payload = begin_cell().store_uint(KEY_ROTATION_PROOF_TAG, 96).store_int(address.wc, 8).store_bytes(address.hash_part).end_cell()
    if request.valid_until <= tx.now or request.subwallet_id != subwallet_id or \
            not _signature_valid(request.new_public_key, proof_payload.hash, request.rotation_signature):
        return False
    if public_key == request.new_public_key:
        return seqno > request.seqno
    # the storage before the rotation: the same checks the contract does
    return seqno == request.seqno and _signature_valid(public_key, request.signed_hash, request.signature)


async def _rotation_confirmed(tx: Transaction, request: TgWalletChangePublicKeyRequest) -> bool:
    # an internal request with a bad signature is silently ignored: the tx succeeds, the storage stays.
    # states are requested along with the interfaces, see address_selectors.extract_tg_wallet_key_rotation_states
    repository = context.interface_repository.get()
    extra = await repository.get_extra_data(tx.account, 'account_states')
    states = (extra or {}).get('states', {})
    before = states.get(tx.account_state_hash_before)
    after = states.get(tx.account_state_hash_after)
    if before is not None and after is not None:
        return before['data_hash'] != after['data_hash']
    # only the end-of-block state is stored and pending traces have none: check the latest storage
    latest = await repository.get_extra_data(tx.account, 'data_boc')
    if not latest or not latest.get('data_boc'):
        return False
    try:
        return _latest_state_confirms(tx, request, latest['data_boc'])
    except Exception:
        return False


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
        try:
            request = TgWalletChangePublicKeyRequest(Slice.one_from_boc(msg.message_content.body))
        except Exception:
            return []
        if not block.is_external and not await _rotation_confirmed(tx, request):
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
