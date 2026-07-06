from __future__ import annotations

import base64
from dataclasses import dataclass

from pytoniq_core import Slice

from indexer.core.database import Transaction
from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
    ContractMatcher,
    OrMatcher,
)
from indexer.events.blocks.core import Block
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.messages.vesting import (
    VestingAddWhiteList,
    VestingSendMessage,
)
from indexer.events.blocks.utils import AccountId
from indexer.events.blocks.utils.block_utils import get_labeled
from indexer.events.blocks.utils.ton_utils import Amount


def _vesting_message_was_sent(
    vesting_tx: Transaction, request: VestingSendMessage
) -> bool:
    """Whether the vesting contract actually emitted the requested message.

    The contract forwards the message with ``send_raw_message(msg, send_mode)``
    where ``send_mode`` includes the "ignore errors" bit (mode 3). If the send
    fails during the action phase (e.g. not enough balance) the message is
    silently skipped while the transaction itself still succeeds, so we must
    not report the action as successful.

    The delivered message can only be compared against the requested one on the
    fields the validator does not rewrite: ``src``, fees, ``created_lt`` and
    ``created_at`` are filled in on send, and the value may change with carry
    modes, so we match on destination together with the body cell hash.
    """
    expected_dest = AccountId(request.message_destination)
    for msg in vesting_tx.messages:
        if msg.direction != "out" or msg.destination is None:
            continue
        if AccountId(msg.destination) != expected_dest:
            continue
        if request.message_body_hash is None:
            # Could not hash the requested body - fall back to value matching.
            if msg.value == request.message_value:
                return True
            continue
        if msg.message_content is None or not msg.message_content.body:
            continue
        try:
            out_body_hash = Slice.one_from_boc(msg.message_content.body).to_cell().hash
        except Exception:
            continue
        if out_body_hash == request.message_body_hash:
            return True
    return False


@dataclass
class VestingSendMessageData:
    query_id: int
    sender: AccountId
    vesting: AccountId
    message_boc_str: str
    message_destination: AccountId
    message_value: Amount
    success: bool


class VestingSendMessageBlock(Block):
    data: VestingSendMessageData

    def __init__(self, data: VestingSendMessageData):
        super().__init__("vesting_send_message", [], data)

    def __repr__(self):
        return f"vesting_send_message {self.data}"


@dataclass
class VestingAddWhiteListData:
    query_id: int
    adder: AccountId
    vesting: AccountId
    accounts_added: list[AccountId]


class VestingAddWhiteListBlock(Block):
    data: VestingAddWhiteListData

    def __init__(self, data: VestingAddWhiteListData):
        super().__init__("vesting_add_whitelist", [], data)

    def __repr__(self):
        return f"vesting_add_whitelist {self.data}"


class VestingSendMessageBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(
            child_matcher=labeled(
                "response",
                ContractMatcher(
                    opcode=VestingSendMessage.response_opcode,
                    optional=False,
                ),
            )
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == VestingSendMessage.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        request_data = VestingSendMessage(block.get_body())
        sender = AccountId(msg.source)
        vesting = AccountId(msg.destination)

        # The contract sends the requested message with mode 3 (ignore errors),
        # so it may be silently dropped even though the vesting transaction
        # itself succeeds. Mark the action successful only if the message was
        # actually emitted as an outgoing message of the vesting transaction.
        success = _vesting_message_was_sent(msg.transaction, request_data)

        include = [block]
        resp = get_labeled("response", other_blocks, CallContractBlock)
        if resp:
            include.append(resp)

        new_block = VestingSendMessageBlock(
            data=VestingSendMessageData(
                sender=sender,
                vesting=vesting,
                query_id=request_data.query_id,
                message_boc_str=base64.b64encode(
                    request_data.message_cell.to_boc()
                ).decode(),
                message_destination=AccountId(request_data.message_destination),
                message_value=Amount(request_data.message_value),
                success=success,
            )
        )
        new_block.merge_blocks(include)
        return [new_block]


class VestingAddWhiteListBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(
            child_matcher=labeled(
                "response",
                ContractMatcher(
                    opcode=VestingAddWhiteList.response_opcode,
                    optional=False,
                ),
            )
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == VestingAddWhiteList.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        request_data = VestingAddWhiteList(block.get_body())
        adder = AccountId(msg.source)
        vesting = AccountId(msg.destination)

        include = [block]
        resp = get_labeled("response", other_blocks, CallContractBlock)
        if resp:
            include.append(resp)

        new_block = VestingAddWhiteListBlock(
            data=VestingAddWhiteListData(
                adder=adder,
                vesting=vesting,
                query_id=request_data.query_id,
                accounts_added=[
                    AccountId(account) for account in request_data.addresses
                ],
            )
        )
        new_block.merge_blocks(include)
        return [new_block]
