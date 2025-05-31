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
    bitcoin_txid: str | None
    success: bool
    recipient_wallet: AccountId | None
    teleport_contract: AccountId | None


class TgBTCMintBlock(Block):
    data: TgBTCMintData

    def __init__(self, data: TgBTCMintData):
        super().__init__("tgbtc_mint", [], data)

    def __repr__(self):
        return f"tgbtc_mint {self.data.__dict__}"


class TgBTCMintBlockMatcher(BlockMatcher):
    def __init__(self):
        # we will actually include all the parent blocks until the first one (0x3F781D24), but later
        super().__init__(
            children_matchers=[
                labeled(
                    "success_log", ContractMatcher(opcode=0x77A80EF3, optional=False)
                ),
                labeled("jetton_mint", BlockTypeMatcher("jetton_mint", optional=False)),
            ]
        )

    def test_self(self, block: Block):
        # the tail of the mint chain
        return isinstance(block, CallContractBlock) and block.opcode == 0xE42A879B

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        success_log_block = get_labeled("success_log", other_blocks, CallContractBlock)
        jetton_mint_block = get_labeled("jetton_mint", other_blocks)

        if (
            not success_log_block
            or not jetton_mint_block
            or jetton_mint_block.btype != "jetton_mint"
        ):
            return []

        current_block_in_chain = block
        collected_intermediate_blocks = {block}
        collected_intermediate_blocks.add(success_log_block)
        collected_intermediate_blocks.add(jetton_mint_block)
        for ob in other_blocks:
            collected_intermediate_blocks.add(ob)

        head_block = None  # all mints start with 0x3F781D24

        temp_check = current_block_in_chain
        for _ in range(20):
            if temp_check.previous_block:
                collected_intermediate_blocks.add(temp_check.previous_block)
                for child_of_prev in temp_check.previous_block.children_blocks:
                    if child_of_prev != temp_check:
                        collected_intermediate_blocks.add(child_of_prev)

                temp_check = temp_check.previous_block
                if (
                    isinstance(temp_check, CallContractBlock)
                    and temp_check.opcode == 0x3F781D24
                ):
                    head_block = temp_check
                    break
            else:
                break

        if not head_block:
            return []

        collected_intermediate_blocks.add(head_block)

        sender = AccountId(head_block.get_message().source)
        recipient = None
        amount = None
        minted_asset = None
        bitcoin_txid = None
        recipient_wallet = None
        teleport_contract = None
        success = False

        try:
            log_data = TgBTCMintEvent(success_log_block.get_body())
            parsed_amount = log_data.amount
            success = True
            recipient = log_data.recipient_address
            teleport_contract = AccountId(success_log_block.get_message().source)
            amount = parsed_amount
            bitcoin_txid_bytes_big_endian = log_data.bitcoin_txid.to_bytes(
                32, byteorder="little"
            )
            bitcoin_txid = bitcoin_txid_bytes_big_endian.hex()
            minted_asset = jetton_mint_block.data["asset"]
            recipient_wallet = AccountId(jetton_mint_block.data["to_jetton_wallet"])
        except Exception as e:
            logger.warning(
                f"TgBTCMint: Failed to parse TgBTCMintEvent log or process jetton_mint: {e}"
            )

        if not success:
            return []

        mint_data = TgBTCMintData(
            sender=sender,
            recipient=recipient,
            amount=amount,
            asset=minted_asset,
            bitcoin_txid=bitcoin_txid,
            success=success,
            recipient_wallet=recipient_wallet,
            teleport_contract=teleport_contract,
        )

        new_logical_block = TgBTCMintBlock(data=mint_data)
        new_logical_block.merge_blocks(list(collected_intermediate_blocks))
        return [new_logical_block]


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
    pubkey: int
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
            pubkey=new_key_log_data.new_internal_pubkey,
            pegout_address=new_key_log_data.pegout_address,
            timestamp=dkg_completed_log_data.timestamp,
            amount=new_key_log_data.amount,
        )
        print(data)

        new_block = TgBTCNewKeyBlock(data=data)
        new_block.merge_blocks(other_blocks + [block])
        return [new_block]
