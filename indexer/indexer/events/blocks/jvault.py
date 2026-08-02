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
    JVaultUnstakeRequest,
)
from indexer.events.blocks.utils import AccountId, Asset
from indexer.events.blocks.utils.block_utils import get_labeled
from indexer.events.retryable_errors import raise_if_retryable_data_access_error


async def extract_jvault_assets(stake_wallet: str) -> tuple[AccountId | None, Asset | None, Asset | None]:
    """
    Extract JVault assets from stake wallet data.
    
    Returns:
        Tuple of (staking_pool, asset, jvault_asset)
        - staking_pool: The staking pool address
        - asset: The underlying asset being staked
        - jvault_asset: The JVault token representing the stake
    """
    try:
        extra = await context.interface_repository.get().get_extra_data(stake_wallet, "data_boc")
        if extra is None:
            return None, None, None
        
        pool_data_slice = Slice.one_from_boc(extra['data_boc'])
        staking_pool = pool_data_slice.load_address()
        minter_address = pool_data_slice.load_address()
        
        stake_pool_extra = await context.interface_repository.get().get_extra_data(
            staking_pool.to_str(is_user_friendly=False).upper(), "data_boc"
        )
        if stake_pool_extra is None:
            return AccountId(staking_pool), None, Asset(jetton_address=minter_address, is_ton=False)
        
        lock_wallet_address = stake_pool_extra['lock_wallet_address']
        lock_wallet = await context.interface_repository.get().get_jetton_wallet(lock_wallet_address)
        asset = Asset(jetton_address=lock_wallet.jetton, is_ton=False)
        jvault_asset = Asset(jetton_address=minter_address, is_ton=False)
        
        return AccountId(staking_pool), asset, jvault_asset
    except Exception as e:
        raise_if_retryable_data_access_error(e)
        return None, None, None


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


def build_jvault_stake_core(
    block: JettonTransferBlock,
    receive_block: Block | None,
    request_update_from_pool: Block | None,
    cancellation: Block | None,
) -> JVaultStakeBlock | None:
    """Shared build core for jvault_stake. `block` is the incoming jetton
    transfer; the rest are the outcome blocks of the matched subtree. Legacy
    derives them from LabelBlocks, the mch builder from the consumed set.
    Returns None to reject. Does NOT merge consumed blocks — callers merge."""
    sender = block.data["sender"]
    sender_wallet = block.data["sender_wallet"]

    msg = block.jetton_transfer_message
    staked_amount = msg.amount
    body = Cell.from_boc(msg.forward_payload)[0].begin_parse()
    body.load_uint(32)  # op
    period = body.load_uint(32)

    failed = receive_block.failed
    if not receive_block:
        return None

    stake_wallet = receive_block.get_message().destination
    staking_pool = receive_block.get_message().source

    if cancellation:
        failed = True
    elif request_update_from_pool:
        failed = failed or request_update_from_pool.failed
    else:
        return None
    data = JVaultStakeData(sender=AccountId(sender), stake_wallet=AccountId(stake_wallet),
                           sender_wallet=AccountId(sender_wallet), asset=block.data["asset"],
                           staking_pool=AccountId(staking_pool), staked_amount=staked_amount, period=period)
    new_block = JVaultStakeBlock(data=data)
    new_block.failed = failed
    return new_block


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
        receive_block = get_labeled(
            "receive_stake_jettons_on_stake_wallet", other_blocks
        )
        cancellation = get_labeled('cancellation', other_blocks, CallContractBlock)
        request_update_from_pool = get_labeled("request_update_rewards_from_pool", other_blocks)
        new_block = build_jvault_stake_core(
            block, receive_block, request_update_from_pool, cancellation
        )
        if new_block is None:
            return []
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


@dataclass
class JVaultUnstakeData:
    sender: AccountId
    stake_wallet: AccountId
    staking_pool: AccountId
    unstaked_amount: int
    unstake_fee_taken: int | None
    asset: Asset | None = None
    jvault_asset: Asset | None = None
    exit_code: int | None = None


class JVaultUnstakeBlock(Block):
    data: JVaultUnstakeData

    def __init__(self, data: JVaultUnstakeData):
        super().__init__("jvault_unstake", [], data)

    def __repr__(self):
        return f"jvault_unstake {self.data}"


async def build_jvault_unstake_core(
    block: Block,
    request_update_from_pool: Block | None,
    unstake_transfer: JettonTransferBlock | None,
    unstake_fee_block: TonTransferBlock | None,
) -> JVaultUnstakeBlock | None:
    """Shared build core for jvault_unstake. `block` is the unstake_jettons
    call. Returns None to reject. Does NOT merge — callers merge."""
    msg = block.get_message()
    info = JVaultUnstakeJettons(block.get_body())
    unstaked_amount = info.jettons_to_unstake
    stake_wallet = msg.destination
    staking_pool, asset, jvault_asset = await extract_jvault_assets(stake_wallet)
    if staking_pool is None or asset is None:
        return None
    if not request_update_from_pool or not unstake_transfer:
        return JVaultUnstakeBlock(
            data=JVaultUnstakeData(
                sender=AccountId(msg.source),
                stake_wallet=AccountId(stake_wallet),
                staking_pool=staking_pool,
                unstaked_amount=unstaked_amount,
                unstake_fee_taken=None,
                asset=asset,
                jvault_asset=jvault_asset,
                exit_code=block.get_message().transaction.compute_exit_code
            )
        )
    if asset != unstake_transfer.data["asset"]:
        raise Exception(f"Assets do not match: {asset} != {unstake_transfer.data['asset']}")
    unstake_fee = 0
    if unstake_fee_block:
        unstake_fee = unstake_fee_block.get_message().value

    staking_pool = request_update_from_pool.get_message().destination
    return JVaultUnstakeBlock(
        data=JVaultUnstakeData(
            sender=AccountId(msg.source),
            stake_wallet=AccountId(stake_wallet),
            staking_pool=AccountId(staking_pool),
            unstaked_amount=unstaked_amount,
            unstake_fee_taken=unstake_fee,
            asset=unstake_transfer.data["asset"],
            jvault_asset=jvault_asset,
        )
    )


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
        unstake_transfer = get_labeled('withdraw_unstaked_jettons', other_blocks, JettonTransferBlock)
        request_update_from_pool = get_labeled("request_update_rewards_from_pool", other_blocks)
        unstake_fee_block = get_labeled("unstake_fee", other_blocks, TonTransferBlock)
        new_block = await build_jvault_unstake_core(
            block, request_update_from_pool, unstake_transfer, unstake_fee_block
        )
        if new_block is None:
            return []
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


def build_jvault_claim_core(
    block: Block,
    send_to_pool: CallContractBlock | None,
    withdrawal: JettonTransferBlock | None,
) -> JVaultClaimBlock | None:
    """Shared build core for jvault_claim. `block` is the claim call.
    Returns None to reject. Does NOT merge — callers merge."""
    msg = block.get_message()
    info = JVaultClaim(block.get_body())
    if not withdrawal or not send_to_pool:
        return None

    amount = withdrawal.jetton_transfer_message.amount
    sender = msg.source
    stake_wallet = msg.destination
    staking_pool = send_to_pool.get_message().destination

    return JVaultClaimBlock(
        data=JVaultClaimData(
            sender=AccountId(sender),
            stake_wallet=AccountId(stake_wallet),
            staking_pool=AccountId(staking_pool),
            claimed_jettons=list(map(AccountId, info.jettons_to_claim)),
            claimed_amounts=[amount],
        )
    )


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
        withdrawal = get_labeled(
            "withdraw_claimed_jettons", other_blocks, JettonTransferBlock
        )
        send_to_pool = get_labeled(
            "send_claimed_rewards", other_blocks, CallContractBlock
        )
        new_block = build_jvault_claim_core(block, send_to_pool, withdrawal)
        if new_block is None:
            return []
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]

@dataclass
class JVaultUnstakeRequestData:
    sender: AccountId
    stake_wallet: AccountId
    staking_pool: AccountId
    requested_amount: int
    asset: Asset | None = None
    jvault_asset: Asset | None = None
    exit_code: int | None = None


class JVaultUnstakeRequestBlock(Block):
    data: JVaultUnstakeRequestData

    def __init__(self, data: JVaultUnstakeRequestData):
        super().__init__("jvault_unstake_request", [], data)

    def __repr__(self):
        return f"jvault_unstake_request {self.data}"

async def build_jvault_unstake_request_core(
    block: Block,
    request_update_from_pool: Block | None,
) -> JVaultUnstakeRequestBlock | None:
    """Shared build core for jvault_unstake_request. `block` is the
    unstake_request call. Returns None to reject. Does NOT merge — callers
    merge. Sets `.failed` on the returned block (callers keep it)."""
    msg = block.get_message()
    info = JVaultUnstakeRequest(block.get_body())
    requested_amount = info.jettons_to_unstake
    stake_wallet = msg.destination

    if not request_update_from_pool:
        # If the request failed early, we might not have the update message
        # Try to get staking pool address and assets from stake wallet data
        staking_pool, asset, jvault_asset = await extract_jvault_assets(stake_wallet)
        if staking_pool is None or asset is None:
            return None
        new_block = JVaultUnstakeRequestBlock(
            data=JVaultUnstakeRequestData(
                sender=AccountId(msg.source),
                stake_wallet=AccountId(stake_wallet),
                staking_pool=staking_pool,
                requested_amount=requested_amount,
                asset=asset,
                jvault_asset=jvault_asset,
                exit_code=block.get_message().transaction.compute_exit_code,
            )
        )
        new_block.failed = True  # Mark as failed since update didn't happen
        return new_block

    staking_pool = request_update_from_pool.get_message().destination
    failed = block.failed or request_update_from_pool.failed

    # Extract assets from stake wallet data
    _, asset, jvault_asset = await extract_jvault_assets(stake_wallet)

    new_block = JVaultUnstakeRequestBlock(
        data=JVaultUnstakeRequestData(
            sender=AccountId(msg.source),
            stake_wallet=AccountId(stake_wallet),
            staking_pool=AccountId(staking_pool),
            requested_amount=requested_amount,
            asset=asset,
            jvault_asset=jvault_asset,
        )
    )
    new_block.failed = failed
    return new_block


class JVaultUnstakeRequestBlockMatcher(BlockMatcher):
    # Transaction flow:
    # 1. User sends unstake_request to stake_wallet
    # 2. Stake wallet sends request_update_rewards to staking_pool (with negative tvl_change)
    # 3. Staking pool may update referrer wallets
    # 4. Staking pool sends update_rewards back to stake_wallet
    # 5. Excess messages are sent

    def __init__(self):
        super().__init__(
            parent_matcher=None,
            optional=False,
            child_matcher=labeled(
                "request_update_rewards_from_pool",
                ContractMatcher(
                    opcode=JVaultRequestUpdateRewards.opcode,
                    optional=True,
                    children_matchers=[
                        referral_chain,  # optional referral updates
                        update_with_exceses,  # required update_rewards response
                    ],
                ),
            ),
        )

    def test_self(self, block: Block):
        return (
                isinstance(block, CallContractBlock)
                and block.opcode == JVaultUnstakeRequest.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        request_update_from_pool = get_labeled(
            "request_update_rewards_from_pool", other_blocks
        )
        new_block = await build_jvault_unstake_request_core(block, request_update_from_pool)
        if new_block is None:
            return []
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]
