from __future__ import annotations

from dataclasses import dataclass

from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
    ContractMatcher,
)
from indexer.events.blocks.core import Block
from indexer.events.blocks.messages.jettons import (
    JettonBurn,
    JettonBurnNotification,
    JettonInternalTransfer,
    JettonNotify,
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
