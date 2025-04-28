from __future__ import annotations

from dataclasses import dataclass

from pytoniq_core import Cell, Slice

from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
    BlockTypeMatcher,
    ContractMatcher,
    child_sequence_matcher, OrMatcher, RecursiveMatcher,
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
from indexer.events.blocks.utils import AccountId, Asset
from indexer.events.blocks.utils.block_utils import get_labeled


@dataclass
class JVaultStakeData:
    sender: AccountId
    sender_wallet: AccountId
    asset: Asset
    stake_wallet: AccountId
    staking_pool: AccountId
    staked_amount: int
    period: int


class JVaultStakeBlock(Block):
    data: JVaultStakeData

    def __init__(self, data: JVaultStakeData):
        super().__init__("jvault_stake", [], data)

    def __repr__(self):
        return f"jvault_stake {self.data}"


referral_subchain = RecursiveMatcher(repeating_matcher=ContractMatcher(opcode=JVaultRequestUpdateReferrer.opcode, include_excess=True),
                                        exit_matcher=ContractMatcher(opcode=JVaultUpdateReferrer.opcode, include_excess=True),
                                        optional=True)
referral_chain = RecursiveMatcher(repeating_matcher=referral_subchain, exit_matcher=None, optional=True)

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
                children_matchers=[referral_chain, update_with_exceses],
            ),
        )

        cancellation = labeled('cancellation', ContractMatcher(
            opcode=0x9eada1d9, # TODO add to messages
            optional=False,
            child_matcher=BlockTypeMatcher(block_type="jetton_transfer", optional=True),
        ))

        staked_jettons_snake = labeled(
            "receive_stake_jettons_on_stake_wallet",
            ContractMatcher(
                opcode=JVaultReceiveJettons.opcode,
                optional=False,
                child_matcher=OrMatcher([request_update, cancellation]),
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
        sender_wallet = block.data["sender_wallet"]

        msg = block.jetton_transfer_message
        staked_amount = msg.amount
        body = Cell.from_boc(msg.forward_payload)[0].begin_parse()
        body.load_uint(32)  # op
        period = body.load_uint(32)

        receive_block = get_labeled(
            "receive_stake_jettons_on_stake_wallet", other_blocks
        )
        failed = receive_block.failed
        if not receive_block:
            return []

        cancellation = get_labeled('cancellation', other_blocks, CallContractBlock)
        request_update_from_pool = get_labeled("request_update_rewards_from_pool", other_blocks)
        stake_wallet = receive_block.get_message().destination
        staking_pool = receive_block.get_message().source

        if cancellation:
            failed = True
        elif request_update_from_pool:
            failed = failed or request_update_from_pool.failed
        else:
            return []
        data = JVaultStakeData(sender=AccountId(sender), stake_wallet=AccountId(stake_wallet),
                               sender_wallet=AccountId(sender_wallet), asset=block.data["asset"],
                               staking_pool=AccountId(staking_pool), staked_amount=staked_amount, period=period)
        new_block = JVaultStakeBlock(
            data=data
        )
        new_block.failed = failed
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


@dataclass
class JVaultUnstakeData:
    sender: AccountId
    stake_wallet: AccountId
    staking_pool: AccountId
    unstaked_amount: int
    unstake_fee_taken: int | None
    exit_code: int | None = None


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
                    optional=True,
                    children_matchers=[  # 2-4 blocks
                        referral_chain,
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
        stake_wallet = msg.destination

        request_update_from_pool = get_labeled("request_update_rewards_from_pool", other_blocks)
        if not request_update_from_pool:
            extra = await context.interface_repository.get().get_extra_data(stake_wallet, "data_boc")
            if extra is None:
                return []
            staking_pool = Slice.one_from_boc(extra).load_address()
            new_block = JVaultUnstakeBlock(
                data=JVaultUnstakeData(
                    sender=AccountId(msg.source),
                    stake_wallet=AccountId(stake_wallet),
                    staking_pool=AccountId(staking_pool),
                    unstaked_amount=unstaked_amount,
                    unstake_fee_taken=None,
                    exit_code=block.get_message().transaction.compute_exit_code
                )
            )
            new_block.merge_blocks([block] + other_blocks)
            return [new_block]

        unstake_fee = 0
        unstake_fee_block = get_labeled("unstake_fee", other_blocks, TonTransferBlock)

        if unstake_fee_block:
            unstake_fee = unstake_fee_block.get_message().value



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
