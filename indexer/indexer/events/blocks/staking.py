from __future__ import annotations

from dataclasses import dataclass

from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

from indexer.core.database import LatestAccountState, SessionMaker
from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
    BlockTypeMatcher,
    ContractMatcher,
    OrMatcher,
)
from indexer.events.blocks.core import Block
from indexer.events.blocks.jettons import (
    JettonBurnBlockMatcher,
    JettonTransferBlockMatcher,
)
from indexer.events.blocks.messages.jettons import (
    JettonBurn,
    JettonBurnNotification,
    JettonInternalTransfer,
    JettonNotify,
    JettonTransfer,
)
from indexer.events.blocks.messages.nft import NftOwnershipAssigned
from indexer.events.blocks.messages.staking import (
    TONStakersDepositRequest,
    TONStakersInitNFT,
    TONStakersMintJettons,
    TONStakersMintNFT,
    TONStakersWithdrawRequest,
)
from indexer.events.blocks.utils import AccountId, Amount
from indexer.events.blocks.utils.block_utils import find_call_contract


@dataclass
class TONStakersDepositRequestData:
    source: AccountId
    pool: AccountId
    value: Amount


class TONStakersDepositRequestBlock(Block):
    data: TONStakersDepositRequestData

    def __init__(self, data: TONStakersDepositRequestData):
        super().__init__("tonstakers_deposit_request", [], data)

    def __repr__(self):
        return f"tonstakers_deposit_request {self.data}"


@dataclass
class TONStakersWithdrawRequestData:
    source: AccountId
    tsTON_wallet: AccountId
    pool: AccountId
    value: Amount


class TONStakersWithdrawRequestBlock(Block):
    data: TONStakersWithdrawRequestData

    def __init__(self, data):
        super().__init__("tonstakers_withdraw_request", [], data)

    def __repr__(self):
        return f"tonstakers_withdraw_request {self.data}"


@dataclass
class NominatorPoolDepositData:
    source: AccountId
    pool: AccountId
    value: Amount


class NominatorPoolDepositBlock(Block):
    data: NominatorPoolDepositData

    def __init__(self, data):
        super().__init__("nominator_pool_deposit", [], data)

    def __repr__(self):
        return f"nominator_pool_deposit {self.data}"


@dataclass
class NominatorPoolWithdrawRequestData:
    source: AccountId
    pool: AccountId


class NominatorPoolWithdrawRequestBlock(Block):
    data: NominatorPoolWithdrawRequestData

    def __init__(self, data):
        super().__init__("nominator_pool_withdraw_request", [], data)

    def __repr__(self):
        return f"nominator_pool_withdraw_request {self.data}"


class TONStakersDepositRequestMatcher(BlockMatcher):
    # optimistic version
    def __init__(self):
        super().__init__(
            child_matcher=ContractMatcher(
                opcode=TONStakersMintJettons.opcode,
                optional=True,
                child_matcher=ContractMatcher(
                    opcode=JettonInternalTransfer.opcode,
                    include_excess=True,
                    child_matcher=ContractMatcher(
                        opcode=JettonNotify.opcode, optional=True
                    ),
                ),
            )
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == TONStakersDepositRequest.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()

        new_block = TONStakersDepositRequestBlock(
            data=TONStakersDepositRequestData(
                source=AccountId(msg.source),
                pool=AccountId(msg.destination),
                value=Amount(msg.value - 10**9),  # 1 TON deposit fee
            )
        )
        new_block.failed = block.failed
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


class TONStakersWithdrawRequestMatcher(BlockMatcher):
    # optimistic version
    def __init__(self):
        super().__init__(
            child_matcher=ContractMatcher(
                opcode=JettonBurnNotification.opcode,
                child_matcher=ContractMatcher(
                    opcode=TONStakersWithdrawRequest.opcode,
                    child_matcher=ContractMatcher(
                        opcode=TONStakersMintNFT.opcode,
                        optional=True,
                        child_matcher=ContractMatcher(
                            opcode=TONStakersInitNFT.opcode,
                            child_matcher=ContractMatcher(
                                opcode=NftOwnershipAssigned.opcode,
                            ),
                        ),
                    ),
                ),
            )
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock) and block.opcode == JettonBurn.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()

        burn_request_data = JettonBurn(block.get_body())

        new_block = TONStakersWithdrawRequestBlock(
            data=TONStakersWithdrawRequestData(
                source=AccountId(msg.source),
                tsTON_wallet=AccountId(msg.destination),
                pool=AccountId(burn_request_data.response_destination),
                value=Amount(burn_request_data.amount),
            )
        )
        new_block.failed = block.failed
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


async def check_pool_code_hash(pool_addr: str):
    NOMINATOR_POOL_CODE_HASH = "mj7BS8CY9rRAZMMFIiyuooAPF92oXuaoGYpwle3hDc8="

    # FIXME: make it through interfaces. now it doesn't work
    with SessionMaker() as session:
        pool = await session.execute(
            select(LatestAccountState).where(account=pool_addr)
        )
    try:
        pool = pool.scalars().first()
    except Exception:
        return False

    return pool and pool.code_hash == NOMINATOR_POOL_CODE_HASH


class NominatorPoolDepositMatcher(BlockMatcher):
    def __init__(self):
        super().__init__()

    def test_self(self, block: Block):
        return isinstance(block, TonTransferBlock)
        # return isinstance(block, CallContractBlock) and block.opcode == 0

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        pool_addr = msg.destination

        body = block.get_body()
        body.load_uint(32)  # skip op
        letter = body.load_string()
        if letter != "d":
            return []

        if not await check_pool_code_hash(pool_addr):
            return []

        new_block = NominatorPoolDepositBlock(
            data=NominatorPoolDepositData(
                source=AccountId(msg.source),
                pool=AccountId(msg.destination),
                value=Amount(msg.value),
            )
        )
        new_block.failed = block.failed
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


class NominatorPoolWithdrawRequestMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=None)

    def test_self(self, block: Block):
        return isinstance(block, TonTransferBlock)

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        pool_addr = msg.destination

        body = block.get_body()
        body.load_uint(32)  # skip op
        letter = body.load_string()
        if letter != "d":
            return []

        if not await check_pool_code_hash(pool_addr):
            return []

        new_block = NominatorPoolWithdrawRequestBlock(
            data=NominatorPoolWithdrawRequestData(
                source=AccountId(msg.source),
                pool=AccountId(msg.destination),
            )
        )
        new_block.failed = block.failed
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]
