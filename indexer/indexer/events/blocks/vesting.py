from __future__ import annotations

import base64
from dataclasses import dataclass

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


@dataclass
class VestingSendMessageData:
    query_id: int
    sender: AccountId
    vesting: AccountId
    message_boc_str: str
    message_destination: AccountId
    message_value: Amount


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
