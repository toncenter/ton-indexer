from __future__ import annotations

from dataclasses import dataclass

from pytoniq_core import Cell

from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
    BlockTypeMatcher,
    ContractMatcher,
    child_sequence_matcher,
)
from indexer.events.blocks.core import Block
from indexer.events.blocks.jettons import JettonTransferBlock
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.messages.jettons import JettonTransfer
from indexer.events.blocks.messages.jvault import (
    JVaultClaim,
    JVaultReceiveJettons,
    JVaultRequestUpdateReferrer,
    JVaultRequestUpdateRewards,
    JVaultSendClaimedRewards,
    JVaultSetData,
    JVaultUnstakeJettons,
    JVaultUpdateReferrer,
    JVaultUpdateRewards,
)
from indexer.events.blocks.utils import AccountId
from indexer.events.blocks.utils.block_utils import get_labeled


@dataclass
class JVaultStakeData:
    sender: AccountId
    stake_wallet: AccountId
    staking_pool: AccountId
    staked_amount: int
    period: int
    minted_stake_jettons: int


class JVaultStakeBlock(Block):
    data: JVaultStakeData

    def __init__(self, data: JVaultStakeData):
        super().__init__("jvault_stake", [], data)

    def __repr__(self):
        return f"jvault_stake {self.data}"


# it's actually same opcode 3 times...
refs_stuff_snake = child_sequence_matcher(
    [
        ContractMatcher(opcode=JVaultRequestUpdateReferrer.opcode, optional=True),
        ContractMatcher(opcode=JVaultRequestUpdateReferrer.opcode, optional=True),
        ContractMatcher(opcode=JVaultRequestUpdateReferrer.opcode, optional=True),
        ContractMatcher(  # finally
            opcode=JVaultUpdateReferrer.opcode,
            optional=True,
            include_excess=True,  # ends with excesses
        ),
    ]
)

update_with_exceses = labeled(
    "update_rewards_on_stake_wallet",
    ContractMatcher(
        opcode=JVaultUpdateRewards.opcode, optional=False, include_excess=True
    ),
)


class JVaultStakeBlockMatcher(BlockMatcher):
    # https://tonviewer.com/transaction/12a9cfe9803d2d18844d5cf8ac628a9fe8e0103bf23e2d4b2e1a607d221711cd

    def __init__(self):
        request_update = labeled(
            "request_update_rewards_from_pool",
            ContractMatcher(
                opcode=JVaultRequestUpdateRewards.opcode,
                optional=False,
                children_matchers=[refs_stuff_snake, update_with_exceses],
            ),
        )

        staked_jettons_snake = labeled(
            "receive_stake_jettons_on_stake_wallet",
            ContractMatcher(
                opcode=JVaultReceiveJettons.opcode,
                optional=False,
                child_matcher=request_update,
            ),
        )

        super().__init__(
            parent_matcher=None,
            optional=False,
            children_matchers=[
                staked_jettons_snake,
                ContractMatcher(opcode=JVaultSetData.opcode, optional=True),
            ],
        )

    def test_self(self, block: Block):
        return isinstance(block, JettonTransferBlock)

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        if not isinstance(block, JettonTransferBlock):
            return []

        sender = block.data["sender"]
        msg = block.jetton_transfer_message
        staked_amount = msg.amount
        body = Cell.from_boc(msg.forward_payload)[0].begin_parse()
        body.load_uint(32)  # op
        period = body.load_uint(32)

        receive_block = get_labeled(
            "receive_stake_jettons_on_stake_wallet", other_blocks
        )
        request_update_from_pool = get_labeled("request_update_rewards_from_pool", other_blocks)
        if not receive_block or not request_update_from_pool:
            return []

        receive_info = JVaultReceiveJettons(receive_block.get_body())
        minted_stake_jettons = receive_info.received_jettons

        stake_wallet = receive_block.get_message().destination
        staking_pool = request_update_from_pool.get_message().destination

        new_block = JVaultStakeBlock(
            data=JVaultStakeData(
                sender=AccountId(sender),
                stake_wallet=AccountId(stake_wallet),
                staking_pool=AccountId(staking_pool),
                staked_amount=staked_amount,
                period=period,
                minted_stake_jettons=minted_stake_jettons,
            )
        )
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


@dataclass
class JVaultUnstakeData:
    sender: AccountId
    stake_wallet: AccountId
    staking_pool: AccountId
    unstaked_amount: int
    unstake_fee_taken: int


class JVaultUnstakeBlock(Block):
    data: JVaultUnstakeData

    def __init__(self, data: JVaultUnstakeData):
        super().__init__("jvault_unstake", [], data)

    def __repr__(self):
        return f"jvault_unstake {self.data}"


class JVaultUnstakeBlockMatcher(BlockMatcher):
    # https://tonviewer.com/transaction/eb639edae4a3d535bab8837e85fce1484f09a59527e52e6966258521186095d6

    def __init__(self):

        super().__init__(
            parent_matcher=None,
            optional=False,
            child_matcher=labeled(
                "request_update_rewards_from_pool",
                ContractMatcher(
                    opcode=JVaultRequestUpdateRewards.opcode,
                    optional=False,
                    children_matchers=[  # 2-4 blocks
                        # optional
                        refs_stuff_snake,
                        # optional
                        labeled(
                            "unstake_fee",
                            BlockTypeMatcher(block_type="ton_transfer", optional=True),
                        ),
                        # required
                        labeled(
                            "withdraw_unstaked_jettons",
                            BlockTypeMatcher(
                                block_type="jetton_transfer", optional=False
                            ),
                        ),
                        # required
                        update_with_exceses,
                    ],
                ),
            ),
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == JVaultUnstakeJettons.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        info = JVaultUnstakeJettons(block.get_body())
        unstaked_amount = info.jettons_to_unstake
        unstake_fee = 0
        unstake_fee_block = get_labeled("unstake_fee", other_blocks, TonTransferBlock)

        if unstake_fee_block:
            unstake_fee = unstake_fee_block.get_message().value

        request_update_from_pool = get_labeled("request_update_rewards_from_pool", other_blocks)
        if not request_update_from_pool:
            return []

        stake_wallet = msg.destination
        staking_pool = request_update_from_pool.get_message().destination

        new_block = JVaultUnstakeBlock(
            data=JVaultUnstakeData(
                sender=AccountId(msg.source),
                stake_wallet=AccountId(stake_wallet),
                staking_pool=AccountId(staking_pool),
                unstaked_amount=unstaked_amount,
                unstake_fee_taken=unstake_fee,
            )
        )
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


@dataclass
class JVaultClaimData:
    sender: AccountId
    stake_wallet: AccountId
    staking_pool: AccountId
    claimed_jettons: list[AccountId]
    claimed_amounts: list[int]


class JVaultClaimBlock(Block):
    data: JVaultClaimData

    def __init__(self, data: JVaultClaimData):
        super().__init__("jvault_claim", [], data)

    def __repr__(self):
        return f"jvault_claim {self.data}"


class JVaultClaimBlockMatcher(BlockMatcher):
    # https://tonviewer.com/transaction/eb639edae4a3d535bab8837e85fce1484f09a59527e52e6966258521186095d6

    def __init__(self):
        super().__init__(
            parent_matcher=None,
            optional=False,
            child_matcher=labeled(
                "send_claimed_rewards",
                ContractMatcher(
                    opcode=JVaultSendClaimedRewards.opcode,
                    optional=False,
                    children_matchers=[
                        # optional
                        refs_stuff_snake,
                        # required
                        labeled(
                            "withdraw_claimed_jettons",
                            BlockTypeMatcher(
                                block_type="jetton_transfer", optional=False
                            ),
                        ),
                        # required
                        update_with_exceses,
                    ],
                ),
            ),
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock) and block.opcode == JVaultClaim.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        info = JVaultClaim(block.get_body())
        withdrawal = get_labeled(
            "withdraw_claimed_jettons", other_blocks, JettonTransferBlock
        )
        send_to_pool = get_labeled(
            "send_claimed_rewards", other_blocks, CallContractBlock
        )
        if not withdrawal or not send_to_pool:
            return []

        amount = withdrawal.jetton_transfer_message.amount
        sender = msg.source
        stake_wallet = msg.destination
        staking_pool = send_to_pool.get_message().destination

        new_block = JVaultClaimBlock(
            data=JVaultClaimData(
                sender=AccountId(sender),
                stake_wallet=AccountId(stake_wallet),
                staking_pool=AccountId(staking_pool),
                claimed_jettons=list(map(AccountId, info.jettons_to_claim)),
                claimed_amounts=[amount],
            )
        )
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]
