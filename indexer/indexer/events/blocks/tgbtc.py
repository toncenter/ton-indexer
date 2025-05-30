from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from loguru import logger

from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
    BlockTypeMatcher,
    ContractMatcher,
    GenericMatcher,
)
from indexer.events.blocks.core import Block, EmptyBlock
from indexer.events.blocks.jettons import JettonBurnBlock
from indexer.events.blocks.labels import labeled

from indexer.events.blocks.messages.jettons import JettonBurnNotification
from indexer.events.blocks.utils import AccountId, Amount, Asset
from indexer.events.blocks.utils.block_utils import get_labeled
from indexer.events.blocks.messages.tgbtc import (
    TgBTCMintEvent,
    TgBTCBurnEvent,
    TgBTCNewKeyEvent,
    TgBTCDkgCompletedEvent,
)


@dataclass
class TgBTCMintData:
    sender: AccountId
    recipient: AccountId | None
    amount: int | None
    asset: Asset | None
    bitcoin_txid: int | None
    success: bool


class TgBTCMintBlock(Block):
    data: TgBTCMintData

    def __init__(self, data: TgBTCMintData):
        super().__init__("tgbtc_mint", [], data)

    def __repr__(self):
        return f"tgbtc_mint {self.data.__dict__}"


class TgBTCMintBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(
            # all optional, but marked success is only when sucess log and jetton_mint are found
            child_matcher=ContractMatcher(
                opcode=0x498AF388,
                optional=True,
                child_matcher=ContractMatcher(
                    opcode=0xC98AF388,
                    optional=True,
                    child_matcher=ContractMatcher(
                        opcode=0x642A879B,
                        optional=True,
                        child_matcher=ContractMatcher(
                            opcode=0xE42A879B,
                            optional=True,
                            children_matchers=[
                                labeled(
                                    "success_log",
                                    ContractMatcher(opcode=0x77A80EF3, optional=True),
                                ),
                                labeled(
                                    "jetton_mint",
                                    BlockTypeMatcher("jetton_mint", optional=True),
                                ),
                            ],
                        ),
                    ),
                ),
            )
        )

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == 0x3F781D24

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        sender = block.get_message().source
        recipient = None
        amount = None
        minted_asset = None
        minted_amount = None
        success = False

        success_log = get_labeled("success_log", other_blocks)
        jetton_mint = get_labeled("jetton_mint", other_blocks)
        if success_log and jetton_mint:
            success = True
            log_data = TgBTCMintEvent(success_log.get_body())
            recipient = log_data.recipient_address
            amount = log_data.amount
            bitcoin_txid = log_data.bitcoin_txid
            minted_asset = jetton_mint.data["asset"]
            minted_amount = jetton_mint.data["amount"]
            if minted_amount != amount:
                logger.warning(
                    f"Minted amount {minted_amount} does not match log amount {amount}"
                )
                return []

        new_block = TgBTCMintBlock(
            data=TgBTCMintData(
                sender=AccountId(sender),
                recipient=recipient,
                amount=amount,
                asset=minted_asset,
                bitcoin_txid=bitcoin_txid,
                success=success,
            )
        )
        new_block.merge_blocks(other_blocks + [block])
        return [new_block]


@dataclass
class TgBTCBurnData:
    sender: AccountId
    jetton_wallet: AccountId
    asset: Asset
    amount: Amount
    pegout_address: AccountId


class TgBTCBurnBlock(Block):
    data: TgBTCBurnData

    def __init__(self, data: TgBTCBurnData):
        super().__init__("tgbtc_burn", [], data)

    def __repr__(self):
        return f"tgbtc_burn {self.data.__dict__}"


class TgBTCBurnBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(
            child_matcher=ContractMatcher(
                opcode=JettonBurnNotification.opcode,
                optional=False,
                children_matchers=[
                    labeled(
                        "tgbtc_burn_log",
                        ContractMatcher(opcode=TgBTCBurnEvent.opcode, optional=False),
                    ),
                    ContractMatcher(
                        opcode=0xBE44E7A6,
                        optional=False,
                        children_matchers=[
                            ContractMatcher(opcode=0x1A84C0E0, optional=True),
                            BlockTypeMatcher("ton_transfer", optional=True),
                        ],
                    ),
                ],
            )
        )

    def test_self(self, block: Block):
        return block.btype == "jetton_burn"

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        jetton_burn_block = cast(JettonBurnBlock, block)

        sender = jetton_burn_block.data["owner"]
        jetton_wallet_addr = jetton_burn_block.data["jetton_wallet"]
        asset = jetton_burn_block.data["asset"]

        tgbtc_burn_log_block = get_labeled(
            "tgbtc_burn_log", other_blocks, CallContractBlock
        )

        if not tgbtc_burn_log_block:
            return []

        log_data = TgBTCBurnEvent(tgbtc_burn_log_block.get_body())

        data = TgBTCBurnData(
            sender=sender,
            jetton_wallet=jetton_wallet_addr,
            asset=asset,
            amount=Amount(log_data.amount),
            pegout_address=log_data.pegout_address,
        )

        new_block = TgBTCBurnBlock(data=data)
        new_block.merge_blocks(other_blocks + [block])
        return [new_block]


@dataclass
class TgBTCNewKeyData:
    teleport_contract: AccountId
    coordinator_contract: AccountId
    old_pubkey: int
    new_pubkey: int
    pegout_address: AccountId
    timestamp: int
    amount: int


class TgBTCNewKeyBlock(Block):
    data: TgBTCNewKeyData

    def __init__(self, data: TgBTCNewKeyData):
        super().__init__("tgbtc_new_key", [], data)

    def __repr__(self):
        return f"tgbtc_new_key {self.data.__dict__}"


class TgBTCNewKeyBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(
            parent_matcher=GenericMatcher(
                # go to sibling dkg_completed_log block via parent matcher
                # parent is an external msg with signature,
                # so opcode is random, and we accept any block
                # that has a suitable child
                test_self_func=lambda block: True,
                optional=False,
                child_matcher=labeled(
                    "dkg_completed_log",
                    ContractMatcher(
                        opcode=TgBTCDkgCompletedEvent.opcode, optional=False
                    ),
                ),
            ),
            children_matchers=[
                labeled(
                    "new_key_log",
                    ContractMatcher(opcode=TgBTCNewKeyEvent.opcode, optional=False),
                ),
                ContractMatcher(
                    opcode=0xBE44E7A6,
                    optional=True,
                    children_matchers=[
                        ContractMatcher(opcode=0x1A84C0E0, optional=True),
                        BlockTypeMatcher("ton_transfer", optional=True),
                    ],
                ),
            ],
        )

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == 0x690F357A

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        dkg_completed_log_block = get_labeled("dkg_completed_log", other_blocks)
        new_key_log_block = get_labeled("new_key_log", other_blocks)

        if not dkg_completed_log_block or not new_key_log_block:
            return []

        dkg_completed_log_data = TgBTCDkgCompletedEvent(
            dkg_completed_log_block.get_body()
        )
        new_key_log_data = TgBTCNewKeyEvent(new_key_log_block.get_body())
        dkg_completed_log_msg = dkg_completed_log_block.get_message()
        new_key_log_msg = new_key_log_block.get_message()

        data = TgBTCNewKeyData(
            teleport_contract=AccountId(dkg_completed_log_msg.source),
            coordinator_contract=AccountId(new_key_log_msg.source),
            old_pubkey=dkg_completed_log_data.internal_pubkey,
            new_pubkey=new_key_log_data.new_internal_pubkey,
            pegout_address=new_key_log_data.pegout_address,
            timestamp=dkg_completed_log_data.timestamp,
            amount=new_key_log_data.amount,
        )
        print(data)

        new_block = TgBTCNewKeyBlock(data=data)
        new_block.merge_blocks(other_blocks + [block])
        return [new_block]
