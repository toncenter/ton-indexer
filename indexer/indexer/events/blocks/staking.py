from __future__ import annotations

from dataclasses import dataclass

from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
    BlockTypeMatcher,
    ContractMatcher, OrMatcher,
)
from indexer.events.blocks.core import Block
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.messages.jettons import (
    JettonBurn,
    JettonBurnNotification,
    JettonInternalTransfer,
    JettonNotify,
)
from indexer.events.blocks.messages.staking import (
    TONStakersDepositRequest,
    TONStakersInitNFT,
    TONStakersMintJettons,
    TONStakersMintNFT,
    TONStakersWithdrawRequest, TONStakersPoolWithdrawal, TONStakersDistributedAsset, TONStakersNftBurnNotification,
    TONStakersNftBurn, NominatorPoolProcessWithdrawRequests,
)
from indexer.events.blocks.nft import NftMintBlock
from indexer.events.blocks.utils import AccountId, Amount
from indexer.events.blocks.utils.block_utils import find_call_contract, get_labeled
from indexer.events.blocks.utils.ton_utils import Asset
from indexer.events.blocks.jettons import JettonTransferBlock
from indexer.events.blocks.messages.coffee import (
    CoffeeStakingDeposit,
    CoffeeStakingLock,
    CoffeeStakingPositionWithdraw1,
    CoffeeStakingPositionWithdraw2,
    CoffeeStakingPositionWithdraw3,
    CoffeeStakingClaimRewards,
)


@dataclass
class TONStakersDepositData:
    source: AccountId
    user_jetton_wallet: AccountId
    pool: AccountId
    value: Amount
    tokens_minted: Amount
    asset: Asset


class TONStakersDepositBlock(Block):
    data: TONStakersDepositData

    def __init__(self, data: TONStakersDepositData):
        super().__init__("tonstakers_deposit", [], data)

    def __repr__(self):
        return f"tonstakers_deposit {self.data}"


@dataclass
class TONStakersWithdrawRequestData:
    source: AccountId
    tsTON_wallet: AccountId
    pool: AccountId
    tokens_burnt: Amount
    minted_nft: AccountId
    asset: Asset

class TONStakersWithdrawRequestBlock(Block):
    data: TONStakersWithdrawRequestData

    def __init__(self, data):
        super().__init__("tonstakers_withdraw_request", [], data)

    def __repr__(self):
        return f"tonstakers_withdraw_request {self.data}"

@dataclass
class TONStakersWithdrawData:
    stake_holder: AccountId
    burnt_nft: AccountId | None
    pool: AccountId | None
    tokens_burnt: Amount | None
    amount: Amount
    asset: Asset

class TONStakersWithdrawBlock(Block):
    data: TONStakersWithdrawData

    def __init__(self, data):
        super().__init__("tonstakers_withdraw", [], data)

    def __repr__(self):
        return f"tonstakers_withdraw {self.data}"

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
    payout_amount: Amount | None


class NominatorPoolWithdrawRequestBlock(Block):
    data: NominatorPoolWithdrawRequestData

    def __init__(self, data):
        super().__init__("nominator_pool_withdraw_request", [], data)

    def __repr__(self):
        return f"nominator_pool_withdraw_request {self.data}"

class TONStakersDepositMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(
            child_matcher=ContractMatcher(
                opcode=TONStakersMintJettons.opcode,
                optional=True,
                child_matcher=labeled('transfer', ContractMatcher(
                    opcode=JettonInternalTransfer.opcode,
                    include_excess=True,
                    child_matcher=ContractMatcher(
                        opcode=JettonNotify.opcode, optional=True
                    ),
                )),
            )
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == TONStakersDepositRequest.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        transfer = get_labeled('transfer', other_blocks, CallContractBlock)
        transfer_message = JettonInternalTransfer(transfer.get_body()) if transfer is not None else None

        failed = block.failed
        if transfer is None:
            failed = True

        new_block = TONStakersDepositBlock(
            data=TONStakersDepositData(
                user_jetton_wallet=AccountId(transfer.get_message().destination) if not failed else None,
                tokens_minted=Amount(transfer_message.amount) if not failed else None,
                source=AccountId(msg.source),
                pool=AccountId(msg.destination),
                value=Amount(msg.value - 10**9),  # 1 TON deposit fee,
                asset=Asset(False, transfer.get_message().source)   
            )
        )
        new_block.failed = failed
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


class TONStakersWithdrawMatcher(BlockMatcher):
    def __init__(self):

        super().__init__(
            child_matcher=ContractMatcher(
                opcode=JettonBurnNotification.opcode,
                child_matcher=labeled('request', ContractMatcher(
                    opcode=TONStakersWithdrawRequest.opcode,
                    child_matcher=OrMatcher([
                        labeled('immediate_withdrawal', ContractMatcher(opcode=TONStakersPoolWithdrawal.opcode)),
                        labeled('delayed_withdrawal', ContractMatcher(opcode=TONStakersMintNFT.opcode))
                    ])
                ))
            )
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock) and block.opcode == JettonBurn.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()

        burn_request_data = JettonBurn(block.get_body())

        immediate_withdrawal = get_labeled('immediate_withdrawal', other_blocks, CallContractBlock)
        delayed_withdrawal = get_labeled('delayed_withdrawal', other_blocks, CallContractBlock)
        request = get_labeled('request', other_blocks, CallContractBlock)
        failed = block.failed
        asset = Asset(False, request.get_message().source)

        if immediate_withdrawal is not None:
            value = immediate_withdrawal.get_message().value - immediate_withdrawal.previous_block.get_message().value
            new_block = TONStakersWithdrawBlock(
                data=TONStakersWithdrawData(
                    stake_holder=AccountId(msg.source),
                    burnt_nft=None,
                    pool=AccountId(request.get_message().destination),
                    tokens_burnt=Amount(burn_request_data.amount),
                    amount=Amount(value),
                    asset=asset
                )
            )
        else:
            nft_mint_block = next((b for b in delayed_withdrawal.next_blocks if isinstance(b, NftMintBlock)), None)
            if nft_mint_block is None:
                nft_mint_block = find_call_contract(delayed_withdrawal.next_blocks, TONStakersInitNFT.opcode)
            minted_nft = None
            if nft_mint_block is not None:
                minted_nft = AccountId(nft_mint_block.event_nodes[0].message.destination)
            else:
                failed = True
            new_block = TONStakersWithdrawRequestBlock(
                data=TONStakersWithdrawRequestData(
                    source=AccountId(msg.source),
                    tsTON_wallet=AccountId(msg.destination),
                    pool=AccountId(request.get_message().destination),
                    tokens_burnt=Amount(burn_request_data.amount),
                    minted_nft=minted_nft,
                    asset=asset
                )
            )
        new_block.failed = failed
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]

class TONStakersDelayedWithdrawalMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(
            parent_matcher=ContractMatcher(
                opcode=TONStakersNftBurnNotification.opcode,
                parent_matcher=ContractMatcher(
                    opcode=TONStakersNftBurn.opcode,
                )
            )
        )

    def test_self(self, block: Block) -> bool:
        return isinstance(block, CallContractBlock) and block.opcode == TONStakersDistributedAsset.opcode

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        notification = block.previous_block
        notification_msg = TONStakersNftBurnNotification(notification.get_body())

        new_block = TONStakersWithdrawBlock(
            data=TONStakersWithdrawData(
                stake_holder=AccountId(notification_msg.owner),
                burnt_nft=AccountId(notification.get_message().source),
                pool=self._try_find_pool_addr(notification),
                amount=Amount(block.get_message().value),
                tokens_burnt=Amount(notification_msg.amount),
                asset=None
            )
        )
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]

    def _try_find_pool_addr(self, block: Block) -> AccountId | None:
        try:
            supported_opcodes = {
                TONStakersNftBurnNotification.opcode,
                TONStakersNftBurn.opcode,
                TONStakersDistributedAsset.opcode
            }

            current_block = block
            while True:
                current_block = current_block.previous_block
                if current_block is None:
                    break
                # if it is start asset distribution call
                if isinstance(current_block, CallContractBlock) and current_block.opcode == 0x1140a64f:
                    return AccountId(current_block.get_message().source)
                if isinstance(current_block, TONStakersWithdrawBlock):
                    return current_block.data.pool
                if isinstance(current_block, CallContractBlock) and current_block.opcode in supported_opcodes:
                    continue
                break
            return None
        except Exception as e:
            return None


class NominatorPoolDepositMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(include_bounces=True, pre_build_auto_append=True)

    def test_self(self, block: Block):
        return isinstance(block, TonTransferBlock) and block.comment == 'd'

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        pool_addr = msg.destination
        interfaces = await context.interface_repository.get().get_interfaces(pool_addr)
        if "NominatorPool" not in interfaces:
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
        super().__init__(child_matcher=None, include_bounces=True, pre_build_auto_append=True)

    def test_self(self, block: Block):
        return isinstance(block, TonTransferBlock) and block.comment == 'w'

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        pool_addr = msg.destination
        extra_blocks = []
        interfaces = await context.interface_repository.get().get_interfaces(pool_addr)
        if "NominatorPool" not in interfaces:
            return []

        ton_transfers = [b for b in block.next_blocks if isinstance(b, TonTransferBlock)]
        new_block = None
        if len(ton_transfers) == 1:
            transfer = ton_transfers[0]
            extra_blocks.append(transfer)
            # immediate withdrawal
            if transfer.value > msg.value:
                new_block = NominatorPoolWithdrawRequestBlock(
                    data=NominatorPoolWithdrawRequestData(
                        source=AccountId(msg.source),
                        pool=AccountId(msg.destination),
                        payout_amount=Amount(transfer.value)
                    )
                )
        elif len(ton_transfers) == 2:
            # immediate withdrawal
            # payout always the first by lt
            payout = min(ton_transfers, key=lambda x: x.event_nodes[0].message.created_lt)
            extra_blocks += ton_transfers
            new_block = NominatorPoolWithdrawRequestBlock(
                data=NominatorPoolWithdrawRequestData(
                    source=AccountId(msg.source),
                    pool=AccountId(msg.destination),
                    payout_amount=Amount(payout.value)

                )
            )
        if new_block is None:
            new_block = NominatorPoolWithdrawRequestBlock(
                data=NominatorPoolWithdrawRequestData(
                    source=AccountId(msg.source),
                    pool=AccountId(msg.destination),
                    payout_amount=None
                )
            )
        new_block.failed = block.failed
        new_block.merge_blocks([block] + other_blocks + extra_blocks)
        return [new_block]

# Withdrawal initiated by owner
class NominatorPoolWithdrawMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(child_matcher=None, include_bounces=True, pre_build_auto_append=True)

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == NominatorPoolProcessWithdrawRequests.opcode

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        pool_addr = msg.destination
        interfaces = await context.interface_repository.get().get_interfaces(pool_addr)
        if "NominatorPool" not in interfaces:
            return []

        new_blocks = []
        for transfer_block in block.next_blocks:
            if isinstance(transfer_block, TonTransferBlock):
                new_block = NominatorPoolWithdrawRequestBlock(
                    data=NominatorPoolWithdrawRequestData(
                        source=AccountId(transfer_block.event_nodes[0].message.destination),
                        pool=AccountId(pool_addr),
                        payout_amount=Amount(transfer_block.value)
                    )
                )
                new_block.merge_blocks([transfer_block])
                new_blocks.append(new_block)
        return new_blocks
    
@dataclass
class CoffeeStakingDepositData:
    source: AccountId
    user_jetton_wallet: AccountId
    pool_jetton_wallet: AccountId
    pool: AccountId  # or nft collection
    value: Amount
    minted_item_address: AccountId
    minted_item_index: int
    asset: Asset


class CoffeeStakingDepositBlock(Block):
    data: CoffeeStakingDepositData

    def __init__(self, data):
        super().__init__("coffee_staking_deposit", [], data)

    def __repr__(self):
        return f"coffee_staking_deposit {self.data}"


class CoffeeStakingDepositMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(
            child_matcher=labeled(
                "pool_call",
                ContractMatcher(
                    opcode=CoffeeStakingDeposit.opcode,
                    children_matchers=[
                        labeled(
                            "log",
                            ContractMatcher(  # just duplicated log msg
                                opcode=CoffeeStakingDeposit.opcode,
                                optional=True,
                            ),
                        ),
                        labeled(
                            "nft_mint",
                            BlockTypeMatcher(block_type="nft_mint", optional=False),
                        ),
                    ],
                ),
            )
        )

    def test_self(self, block: Block):
        return isinstance(block, JettonTransferBlock)

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        jetton_transfer = block
        pool_call = get_labeled("pool_call", other_blocks, CallContractBlock)
        if pool_call is None:
            return []
        log_block = get_labeled("log", other_blocks, CallContractBlock)
        nft_mint_block = get_labeled("nft_mint", other_blocks, NftMintBlock)
        if nft_mint_block is None:
            return []
        nft_mint_data = nft_mint_block.data
        new_block = CoffeeStakingDepositBlock(
            data=CoffeeStakingDepositData(
                source=AccountId(jetton_transfer.data["sender"]),
                user_jetton_wallet=AccountId(jetton_transfer.data["sender_wallet"]),
                pool_jetton_wallet=AccountId(jetton_transfer.data["receiver_wallet"]),
                pool=AccountId(pool_call.get_message().destination),
                value=Amount(jetton_transfer.data["amount"]),
                minted_item_address=AccountId(nft_mint_data["address"]),
                minted_item_index=nft_mint_data["index"],
                asset=jetton_transfer.data["asset"],
            )
        )
        blocks = [jetton_transfer, pool_call, nft_mint_block]
        if log_block is not None:
            blocks.append(log_block)
        new_block.merge_blocks(blocks)
        return [new_block]


@dataclass
class CoffeeStakingWithdrawData:
    source: AccountId
    pool: AccountId
    asset: Asset
    amount: Amount
    user_jetton_wallet: AccountId
    pool_jetton_wallet: AccountId
    nft_address: AccountId
    nft_index: int
    points: int


class CoffeeStakingWithdrawBlock(Block):
    data: CoffeeStakingWithdrawData

    def __init__(self, data):
        super().__init__("coffee_staking_withdraw", [], data)

    def __repr__(self):
        return f"coffee_staking_withdraw {self.data}"


class CoffeeStakingWithdrawMatcher(BlockMatcher):
    def __init__(self):
        # withdraw_1 -> withdraw_2 -> withdraw_3 -> jetton_transfer
        jetton_transfer = labeled(
            "jetton_transfer",
            BlockTypeMatcher(block_type="jetton_transfer", optional=False),
        )

        withdraw_3 = labeled(
            "withdraw_3",
            ContractMatcher(
                opcode=CoffeeStakingPositionWithdraw3.opcode,
                children_matchers=[jetton_transfer],
            ),
        )

        withdraw_2 = labeled(
            "withdraw_2",
            ContractMatcher(
                opcode=CoffeeStakingPositionWithdraw2.opcode,
                children_matchers=[
                    withdraw_3,
                    labeled(
                        "log",
                        ContractMatcher(
                            opcode=CoffeeStakingPositionWithdraw3.opcode, optional=True
                        ),
                    ),
                ],
            ),
        )

        super().__init__(child_matcher=withdraw_2)

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == CoffeeStakingPositionWithdraw1.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        # block is withdraw_1
        withdraw_1_block = block
        withdraw_1_msg = CoffeeStakingPositionWithdraw1(withdraw_1_block.get_body())

        withdraw_2_block = get_labeled("withdraw_2", other_blocks, CallContractBlock)
        if not withdraw_2_block:
            return []
        withdraw_2_msg = CoffeeStakingPositionWithdraw2(withdraw_2_block.get_body())

        withdraw_3_block = get_labeled("withdraw_3", other_blocks, CallContractBlock)
        if not withdraw_3_block:
            return []
        withdraw_3_msg = CoffeeStakingPositionWithdraw3(withdraw_3_block.get_body())

        jetton_transfer_block = get_labeled(
            "jetton_transfer", other_blocks, JettonTransferBlock
        )
        if not jetton_transfer_block:
            return []

        # extract data
        source = AccountId(withdraw_2_msg.owner)
        pool = AccountId(
            withdraw_2_block.get_message().source
        )  # master sends withdraw_2
        nft_address = AccountId(withdraw_1_block.get_message().destination)
        nft_index = withdraw_2_msg.nft_id
        points = withdraw_2_msg.points or 0

        # from withdraw_3
        amount = Amount(withdraw_3_msg.jetton_amount or 0)
        pool_jetton_wallet = AccountId(withdraw_3_msg.jetton_wallet)

        # from jetton transfer
        asset = jetton_transfer_block.data["asset"]
        user_jetton_wallet = jetton_transfer_block.data["receiver_wallet"]

        data = CoffeeStakingWithdrawData(
            source=source,
            pool=pool,
            asset=asset,
            amount=amount,
            user_jetton_wallet=user_jetton_wallet,
            pool_jetton_wallet=pool_jetton_wallet,
            nft_address=nft_address,
            nft_index=nft_index,
            points=points,
        )
        blocks = [
            withdraw_1_block,
            withdraw_2_block,
            withdraw_3_block,
            jetton_transfer_block,
        ]
        log_block = get_labeled("log", other_blocks, CallContractBlock)
        if log_block is not None:
            blocks.append(log_block)
        new_block = CoffeeStakingWithdrawBlock(data)
        new_block.merge_blocks(blocks)
        return [new_block]


@dataclass
class CoffeeStakingClaimRewardsData:
    admin: AccountId
    recipient: AccountId
    pool: AccountId
    asset: Asset
    amount: Amount
    pool_jetton_wallet: AccountId
    recipient_jetton_wallet: AccountId


class CoffeeStakingClaimRewardsBlock(Block):
    data: CoffeeStakingClaimRewardsData

    def __init__(self, data):
        super().__init__("coffee_staking_claim_rewards", [], data)

    def __repr__(self):
        return f"coffee_staking_claim_rewards {self.data}"


class CoffeeStakingClaimRewardsMatcher(BlockMatcher):
    def __init__(self):
        # claim_rewards -> jetton_transfer + log message
        jetton_transfer = labeled(
            "jetton_transfer",
            BlockTypeMatcher(block_type="jetton_transfer", optional=False),
        )

        log_message = labeled(
            "log",
            ContractMatcher(opcode=CoffeeStakingClaimRewards.opcode, optional=True),
        )

        super().__init__(children_matchers=[jetton_transfer, log_message])

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == CoffeeStakingClaimRewards.opcode
            and len(block.next_blocks) > 0
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        claim_rewards_block = block
        claim_rewards_msg = CoffeeStakingClaimRewards(claim_rewards_block.get_body())

        jetton_transfer_block = get_labeled(
            "jetton_transfer", other_blocks, JettonTransferBlock
        )
        if not jetton_transfer_block:
            return []

        # extract data
        recipient = AccountId(claim_rewards_msg.receiver)
        pool = AccountId(claim_rewards_block.get_message().destination)
        pool_jetton_wallet = AccountId(claim_rewards_msg.jetton_wallet)
        amount = Amount(claim_rewards_msg.jetton_amount or 0)

        # from jetton transfer
        asset = jetton_transfer_block.data["asset"]
        recipient_jetton_wallet = jetton_transfer_block.data["receiver_wallet"]

        # admin wallet that initiated the claim
        admin = AccountId(claim_rewards_block.get_message().source)

        data = CoffeeStakingClaimRewardsData(
            admin=admin,
            recipient=recipient,
            pool=pool,
            asset=asset,
            amount=amount,
            pool_jetton_wallet=pool_jetton_wallet,
            recipient_jetton_wallet=recipient_jetton_wallet,
        )

        blocks = [claim_rewards_block, jetton_transfer_block]
        log_block = get_labeled("log", other_blocks, CallContractBlock)
        if log_block is not None:
            blocks.append(log_block)

        new_block = CoffeeStakingClaimRewardsBlock(data)
        new_block.merge_blocks(blocks)
        return [new_block]
