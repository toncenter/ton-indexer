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
    HIPO_PARENT_ADDRESS,
    HIPO_TREASURY_ADDRESS,
    HipoAssignBill,
    HipoBillBurned,
    HipoBurnBill,
    HipoBurnTokens,
    HipoDepositCoins,
    HipoMintBill,
    HipoMintTokens,
    HipoProxyReserveTokens,
    HipoProxyRollbackUnstake,
    HipoProxySaveCoins,
    HipoProxyTokensBurned,
    HipoProxyTokensMinted,
    HipoReserveTokens,
    HipoRollbackUnstake,
    HipoSaveCoins,
    HipoTokensBurned,
    HipoTokensMinted,
    HipoWithdrawalNotification,
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


# ---------------------------------------------------------------------------
# Hipo (hGRAM liquid staking)
#
# Trace shapes (see contracts/schema.tlb and docs/architecture.md of
# https://github.com/HipoFinance/contract):
#
#   stake, instant       deposit_coins -> treasury -> proxy_tokens_minted ->
#                        parent -> tokens_minted -> hGRAM wallet -> transfer_notification
#   stake, deferred      deposit_coins -> treasury -> {proxy_save_coins -> save_coins}
#                        + {mint_bill -> collection -> assign_bill -> bill}
#   stake, by comment    a plain GRAM transfer whose body is the text comment "d" (or "D")
#                        routes into the very same deposit handler with coins = 0, meaning
#                        "stake everything after fees". Hipo documents this for senders that
#                        cannot attach a custom payload - multisigs above all - and real
#                        seven-figure deposits have used it, so it is not a curiosity.
#                        The message carries op-code 0, which means it never becomes a
#                        CallContractBlock at all; it has to be matched on the comment.
#   unstake, instant     unstake_tokens (TEP-74 burn) -> hGRAM wallet ->
#                        proxy_reserve_tokens -> parent -> reserve_tokens -> treasury ->
#                        proxy_tokens_burned -> parent -> tokens_burned -> hGRAM wallet ->
#                        withdrawal_notification
#   unstake, deferred    ... -> reserve_tokens -> treasury -> mint_bill -> collection ->
#                        assign_bill -> bill
#   unstake, rolled back ... -> reserve_tokens -> treasury -> proxy_rollback_unstake ->
#                        parent -> rollback_unstake  (nothing was staked or unstaked)
#   unstake, by comment  the comment "w" on the treasury, or on the hGRAM wallet, unstakes
#                        the whole balance. Both end in the ordinary TEP-74 burn above
#                        (op::unstake_tokens *is* 0x595f07bc), so HipoUnstakeMatcher already
#                        covers them and no extra matcher is needed.
#   round end            burn_bill -> bill -> bill_burned -> collection ->
#                        {mint_tokens | burn_tokens} -> treasury -> ... -> hGRAM wallet
#
# Deferred requests mint a bill SBT that is burned when the round ends. Its address is
# reported as `ts_nft` on both the request action and the completing action, so consumers
# can join the two halves of a deferred deposit/withdrawal (this mirrors how Tonstakers
# uses `ts_nft` to join stake_withdrawal_request with the later stake_withdrawal).
#
# Two halves, one amount. A deferred deposit is reported twice, once when the GRAM arrives
# and once when the hGRAM is finally minted, and both actions serialize to type
# `stake_deposit`. They must not both carry the GRAM: `proxy_save_coins.coins` and the
# later `proxy_tokens_minted.coins` are the same number, so anyone summing
# `stake_deposit.amount` over the treasury - a TVL feed, an inflow dashboard - would count
# every deferred deposit twice, and a wallet history would show the same "Deposit 5 GRAM"
# twice with nothing marking the second as the settlement of the first. The settlement half
# therefore reports `amount = null` and carries only what is new at settlement: the hGRAM in
# `tokens_minted`, joined back to the request through `ts_nft`. The unstake side never had
# this problem because its two halves are different action types in different units
# (`stake_withdrawal_request` in hGRAM, `stake_withdrawal` in GRAM).
#
# Anchoring. Every matcher below names the holder whose hGRAM balance moves, and it takes
# that name out of a message body. The op-codes that carry it are public and some are
# genuinely accepted from strangers: `reserve_tokens` has no access check in treasury.fc at
# all - the treasury answers a sender that is not the parent with `proxy_rollback_unstake`
# straight back to that sender - so an attacker can stage a chain of look-alike messages
# between contracts they own and, if a matcher keys on op-codes alone, have it reported as
# hGRAM arriving in or leaving an address they picked. toncenter never serializes
# `value_flow`, so the worst case here is a misleading action rather than a corrupted
# balance, but a misleading action is still worth refusing. Two rules keep that shut:
#   * an action is only built from a chain that reached the treasury address, and
#   * every leg that carries a balance change must have been *sent by* the treasury
#     (`_hipo_from_treasury`), which is the one thing an attacker cannot forge.
# Today the block tree gives the second rule for free, because these legs are matched as
# children of a block whose destination is the treasury; the checks are written out anyway
# so the guarantee survives being re-expressed in a matcher language that does not.
# ---------------------------------------------------------------------------


@dataclass
class HipoStakeDepositData:
    source: AccountId
    pool: AccountId
    user_jetton_wallet: AccountId | None
    # None on the settlement half of a deferred deposit: the GRAM inflow was already
    # reported when the deposit was made, and repeating it here would double count it.
    value: Amount | None
    tokens_minted: Amount | None
    asset: Asset
    bill: AccountId | None


class HipoStakeDepositBlock(Block):
    data: HipoStakeDepositData

    def __init__(self, data: HipoStakeDepositData):
        super().__init__("hipo_stake_deposit", [], data)

    def __repr__(self):
        return f"hipo_stake_deposit {self.data}"


@dataclass
class HipoStakeWithdrawalRequestData:
    source: AccountId
    pool: AccountId
    user_jetton_wallet: AccountId | None
    tokens_burnt: Amount
    asset: Asset
    bill: AccountId | None
    # Only set when a round end could not fund an unstake and re-minted it against the next
    # round: the bill that has just burned, i.e. the `ts_nft` of the request this one
    # continues. It is not serialized - the `staking_details` composite has a single nft
    # slot and `ts_nft` has to hold the *new* bill so the request still joins to whatever
    # settles it - but the burned bill stays reachable through the action's `accounts`, and
    # keeping it on the block makes the chain explicit for anything reading these blocks.
    previous_bill: AccountId | None = None


class HipoStakeWithdrawalRequestBlock(Block):
    data: HipoStakeWithdrawalRequestData

    def __init__(self, data: HipoStakeWithdrawalRequestData):
        super().__init__("hipo_stake_withdrawal_request", [], data)

    def __repr__(self):
        return f"hipo_stake_withdrawal_request {self.data}"


@dataclass
class HipoStakeWithdrawalData:
    source: AccountId
    pool: AccountId
    user_jetton_wallet: AccountId | None
    # None when the unstake ended without a payout: the round end had no later round to
    # postpone the bill to, so the treasury handed the hGRAM back instead. Such a block is
    # marked failed, which serializes to success = false.
    amount: Amount | None
    tokens_burnt: Amount | None
    asset: Asset
    bill: AccountId | None


class HipoStakeWithdrawalBlock(Block):
    data: HipoStakeWithdrawalData

    def __init__(self, data: HipoStakeWithdrawalData):
        super().__init__("hipo_stake_withdrawal", [], data)

    def __repr__(self):
        return f"hipo_stake_withdrawal {self.data}"


def _hipo_is_treasury(address: str | None) -> bool:
    return address is not None and address.upper() == HIPO_TREASURY_ADDRESS


def _hipo_from_treasury(block: Block | None) -> bool:
    """True when `block` is a message the treasury itself sent.

    See the anchoring note above: this is what stops a look-alike chain assembled by an
    attacker's own contracts from being reported as somebody's hGRAM moving.
    """
    return block is not None and _hipo_is_treasury(block.get_message().source)


def _hipo_find_bill(mint_bill_block: Block | None) -> AccountId | None:
    """Address of the bill SBT deployed by a mint_bill -> assign_bill pair.

    The bill is deployed by `assign_bill`, so the generic NftMintBlockMatcher (which runs
    before us) usually swallows that call into an `nft_mint` block. Handle both shapes.
    """
    if mint_bill_block is None:
        return None
    for next_block in mint_bill_block.next_blocks:
        if isinstance(next_block, NftMintBlock):
            return AccountId(next_block.data["address"])
    assign_bill = find_call_contract(mint_bill_block.next_blocks, HipoAssignBill.opcode)
    if assign_bill is not None:
        return AccountId(assign_bill.get_message().destination)
    return None


def _hipo_deposit_children_matchers() -> list[BlockMatcher]:
    """The fan-out the treasury produces for a deposit, however the deposit was phrased.

    `deposit_coins` and the bare "d" comment route into the same handler, so both matchers
    below wait for the same children.
    """
    instant = labeled(
        "proxy_tokens_minted",
        ContractMatcher(
            opcode=HipoProxyTokensMinted.opcode,
            child_matcher=labeled(
                "tokens_minted",
                ContractMatcher(
                    opcode=HipoTokensMinted.opcode,
                    child_matcher=ContractMatcher(opcode=JettonNotify.opcode, optional=True),
                ),
            ),
        ),
    )
    deferred = labeled(
        "proxy_save_coins",
        ContractMatcher(
            opcode=HipoProxySaveCoins.opcode,
            child_matcher=labeled(
                "save_coins",
                ContractMatcher(opcode=HipoSaveCoins.opcode, optional=True),
            ),
        ),
    )
    return [
        OrMatcher([instant, deferred], optional=True),
        labeled("mint_bill", ContractMatcher(opcode=HipoMintBill.opcode, optional=True)),
    ]


def _hipo_build_deposit(
    block: Block, other_blocks: list[Block], staker: AccountId, fallback_coins: int
) -> list[Block]:
    """Turn a deposit landing on the treasury into a hipo_stake_deposit block."""
    proxy_tokens_minted = get_labeled("proxy_tokens_minted", other_blocks, CallContractBlock)
    proxy_save_coins = get_labeled("proxy_save_coins", other_blocks, CallContractBlock)
    tokens_minted = get_labeled("tokens_minted", other_blocks, CallContractBlock)
    save_coins = get_labeled("save_coins", other_blocks, CallContractBlock)
    mint_bill = get_labeled("mint_bill", other_blocks, CallContractBlock)

    for leg in (proxy_tokens_minted, proxy_save_coins, mint_bill):
        if leg is not None and not _hipo_from_treasury(leg):
            return []

    tokens = None
    parent = None
    if proxy_tokens_minted is not None:
        minted = HipoProxyTokensMinted(proxy_tokens_minted.get_body())
        coins = minted.coins
        tokens = Amount(minted.tokens)
        parent = proxy_tokens_minted.get_message().destination
    elif proxy_save_coins is not None:
        saved = HipoProxySaveCoins(proxy_save_coins.get_body())
        coins = saved.coins
        parent = proxy_save_coins.get_message().destination
    else:
        coins = fallback_coins

    wallet = None
    for leg in (tokens_minted, save_coins):
        if leg is not None:
            wallet = AccountId(leg.get_message().destination)
            break

    new_block = HipoStakeDepositBlock(
        data=HipoStakeDepositData(
            source=staker,
            pool=AccountId(block.get_message().destination),
            user_jetton_wallet=wallet,
            value=Amount(coins),
            tokens_minted=tokens,
            asset=Asset(False, parent if parent is not None else HIPO_PARENT_ADDRESS),
            bill=_hipo_find_bill(mint_bill),
        )
    )
    new_block.failed = block.failed or (proxy_tokens_minted is None and proxy_save_coins is None)
    new_block.merge_blocks([block] + other_blocks)
    return [new_block]


class HipoDepositMatcher(BlockMatcher):
    """deposit_coins -> treasury: instant mint, or a deposit pending until round end."""

    def __init__(self):
        super().__init__(children_matchers=_hipo_deposit_children_matchers())

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == HipoDepositCoins.opcode
            and _hipo_is_treasury(block.get_message().destination)
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        deposit = HipoDepositCoins(block.get_body())
        # `owner` is addr_none when the sender stakes for itself.
        staker = AccountId(deposit.owner) if deposit.owner is not None else AccountId(msg.source)
        # `coins == 0` means "stake everything that is left after fees".
        fallback = deposit.coins if deposit.coins else msg.value
        return _hipo_build_deposit(block, other_blocks, staker, fallback)


class HipoCommentDepositMatcher(BlockMatcher):
    """A GRAM transfer to the treasury whose whole body is the comment "d".

    treasury.fc accepts op-code 0 with a one-byte body and routes "d"/"D" into
    `deposit_coins` with coins = 0, so the trace below this transfer is exactly the one
    `HipoDepositMatcher` handles. What differs is the root: a text comment is op-code 0,
    which `init_block` turns into a TonTransferBlock rather than a CallContractBlock, so a
    matcher keyed on the `deposit_coins` op-code never sees it and the whole deposit is
    reported as a bare ton_transfer plus an unexplained jetton mint.

    NominatorPoolDepositMatcher claims the same comment - Hipo chose "d" and "w" precisely
    so nominator-pool front ends would work against it - but it bails out on anything whose
    destination is not a NominatorPool, and a matcher that returns no blocks leaves the tree
    untouched, so the two do not collide.
    """

    def __init__(self):
        super().__init__(children_matchers=_hipo_deposit_children_matchers())

    def test_self(self, block: Block):
        return (
            isinstance(block, TonTransferBlock)
            # treasury.fc reads exactly one byte and then end_parse()s, and lowercases it,
            # so "d" and "D" are accepted and "deposit" or "d " are not.
            and block.comment in ("d", "D")
            and _hipo_is_treasury(block.get_message().destination)
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        # The comment form has no `owner` field, so the sender always stakes for itself.
        return _hipo_build_deposit(block, other_blocks, AccountId(msg.source), msg.value)


class HipoUnstakeMatcher(BlockMatcher):
    """TEP-74 burn of hGRAM: instant unstake, deferred unstake request, or a rollback.

    Registered before JettonBurnBlockMatcher so that the trace is claimed here instead of
    being reported as a plain jetton burn (same trick as TONStakersWithdrawMatcher).
    """

    def __init__(self):
        instant = labeled(
            "proxy_tokens_burned",
            ContractMatcher(
                opcode=HipoProxyTokensBurned.opcode,
                child_matcher=labeled(
                    "tokens_burned",
                    ContractMatcher(
                        opcode=HipoTokensBurned.opcode,
                        child_matcher=labeled(
                            "withdrawal_notification",
                            ContractMatcher(opcode=HipoWithdrawalNotification.opcode, optional=True),
                        ),
                    ),
                ),
            ),
        )
        deferred = labeled("mint_bill", ContractMatcher(opcode=HipoMintBill.opcode))
        rollback = labeled(
            "rollback",
            ContractMatcher(
                opcode=HipoProxyRollbackUnstake.opcode,
                child_matcher=ContractMatcher(opcode=HipoRollbackUnstake.opcode, optional=True),
            ),
        )
        super().__init__(
            child_matcher=labeled(
                "proxy_reserve_tokens",
                ContractMatcher(
                    opcode=HipoProxyReserveTokens.opcode,
                    child_matcher=labeled(
                        "reserve_tokens",
                        ContractMatcher(
                            opcode=HipoReserveTokens.opcode,
                            child_matcher=OrMatcher([instant, deferred, rollback]),
                        ),
                    ),
                ),
            )
        )

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == JettonBurn.opcode

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        msg = block.get_message()
        reserve_tokens = get_labeled("reserve_tokens", other_blocks, CallContractBlock)
        proxy_reserve_tokens = get_labeled("proxy_reserve_tokens", other_blocks, CallContractBlock)
        if reserve_tokens is None or not _hipo_is_treasury(reserve_tokens.get_message().destination):
            return []

        rollback = get_labeled("rollback", other_blocks, CallContractBlock)
        if rollback is not None:
            # The treasury could not serve the unstake and gave the hGRAM back. Nothing was
            # withdrawn, so this must not become a stake_withdrawal; leave it to the generic
            # jetton classifier. This is also the branch an attacker's forged chain always
            # lands in, because `reserve_tokens` from anyone other than the parent is
            # rolled straight back - so refusing it here is the anchoring check as well.
            return []

        burn = JettonBurn(block.get_body())
        reserve = HipoReserveTokens(reserve_tokens.get_body())
        staker = AccountId(reserve.owner) if reserve.owner is not None else AccountId(msg.source)
        pool = AccountId(reserve_tokens.get_message().destination)
        wallet = AccountId(msg.destination)
        asset = Asset(False, proxy_reserve_tokens.get_message().destination)

        proxy_tokens_burned = get_labeled("proxy_tokens_burned", other_blocks, CallContractBlock)
        mint_bill = get_labeled("mint_bill", other_blocks, CallContractBlock)

        # Only the treasury can answer a reserve_tokens with either of these; see the
        # anchoring note above.
        for leg in (proxy_tokens_burned, mint_bill):
            if leg is not None and not _hipo_from_treasury(leg):
                return []

        if proxy_tokens_burned is not None:
            burned = HipoProxyTokensBurned(proxy_tokens_burned.get_body())
            new_block = HipoStakeWithdrawalBlock(
                data=HipoStakeWithdrawalData(
                    source=staker,
                    pool=pool,
                    user_jetton_wallet=wallet,
                    amount=Amount(burned.coins),
                    tokens_burnt=Amount(burned.tokens),
                    asset=asset,
                    bill=None,
                )
            )
        elif mint_bill is not None:
            bill_request = HipoMintBill(mint_bill.get_body())
            new_block = HipoStakeWithdrawalRequestBlock(
                data=HipoStakeWithdrawalRequestData(
                    source=staker,
                    pool=pool,
                    user_jetton_wallet=wallet,
                    tokens_burnt=Amount(bill_request.amount or burn.amount),
                    asset=asset,
                    bill=_hipo_find_bill(mint_bill),
                )
            )
        else:
            return []

        new_block.failed = block.failed
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


class HipoRoundEndDepositMatcher(BlockMatcher):
    """Round end: a pending deposit is settled and the hGRAM is finally minted.

    This is the settlement half of a deferred deposit, not a new deposit, so it reports no
    GRAM - see the "two halves, one amount" note above. What it does report is the hGRAM
    that has just come into existence, plus the bill that joins it to the request.
    """

    def __init__(self):
        super().__init__(
            parent_matcher=labeled(
                "bill_burned",
                ContractMatcher(
                    opcode=HipoBillBurned.opcode,
                    optional=True,
                    parent_matcher=ContractMatcher(opcode=HipoBurnBill.opcode, optional=True),
                ),
            ),
            child_matcher=labeled(
                "proxy_tokens_minted",
                ContractMatcher(
                    opcode=HipoProxyTokensMinted.opcode,
                    child_matcher=labeled(
                        "tokens_minted",
                        ContractMatcher(
                            opcode=HipoTokensMinted.opcode,
                            child_matcher=ContractMatcher(opcode=JettonNotify.opcode, optional=True),
                        ),
                    ),
                ),
            ),
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == HipoMintTokens.opcode
            and _hipo_is_treasury(block.get_message().destination)
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        mint_tokens = HipoMintTokens(block.get_body())
        proxy_tokens_minted = get_labeled("proxy_tokens_minted", other_blocks, CallContractBlock)
        if proxy_tokens_minted is None or not _hipo_from_treasury(proxy_tokens_minted):
            return []
        minted = HipoProxyTokensMinted(proxy_tokens_minted.get_body())
        tokens_minted = get_labeled("tokens_minted", other_blocks, CallContractBlock)
        bill_burned = get_labeled("bill_burned", other_blocks, CallContractBlock)

        owner = minted.owner if minted.owner is not None else mint_tokens.owner
        new_block = HipoStakeDepositBlock(
            data=HipoStakeDepositData(
                source=AccountId(owner),
                pool=AccountId(block.get_message().destination),
                user_jetton_wallet=(
                    AccountId(tokens_minted.get_message().destination) if tokens_minted is not None else None
                ),
                # Deliberately not minted.coins: that is the same GRAM the deposit half
                # already reported, and both halves serialize to type stake_deposit.
                value=None,
                tokens_minted=Amount(minted.tokens),
                asset=Asset(False, proxy_tokens_minted.get_message().destination),
                bill=AccountId(bill_burned.get_message().source) if bill_burned is not None else None,
            )
        )
        new_block.failed = block.failed
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


class HipoRoundEndWithdrawalMatcher(BlockMatcher):
    """Round end: a pending unstake is settled.

    `burn_tokens` in treasury.fc has three outcomes, not one:

      (a) the treasury can fund the payout -> proxy_tokens_burned -> tokens_burned ->
          withdrawal_notification. The GRAM goes out and the unstake is done.
      (b) it cannot, but a later round still holds bills -> mint_bill against that round.
          Bill A has already burned and bill B now tracks the same unstake.
      (c) it cannot and there is no round left to postpone to -> proxy_rollback_unstake ->
          rollback_unstake. The hGRAM goes back onto the owner's wallet (wallet.fc does
          `tokens += amount`) and the unstake is abandoned.

    Only (a) used to be handled, and the other two were dropped silently. For (b) that is
    worse than a missing action: the user's stake_withdrawal_request names bill A, the
    settlement that eventually arrives names bill B, and with nothing emitted in between the
    two can never be paired on the NFT key the whole deferred design rests on. So (b) emits
    a fresh stake_withdrawal_request carrying bill B, keeping the chain joinable, and (c)
    emits an unsuccessful stake_withdrawal so the request stops dangling for ever and the
    returned hGRAM is at least visible as `tokens_burnt`.
    """

    def __init__(self):
        paid = labeled(
            "proxy_tokens_burned",
            ContractMatcher(
                opcode=HipoProxyTokensBurned.opcode,
                child_matcher=labeled(
                    "tokens_burned",
                    ContractMatcher(
                        opcode=HipoTokensBurned.opcode,
                        child_matcher=labeled(
                            "withdrawal_notification",
                            ContractMatcher(opcode=HipoWithdrawalNotification.opcode, optional=True),
                        ),
                    ),
                ),
            ),
        )
        postponed = labeled("mint_bill", ContractMatcher(opcode=HipoMintBill.opcode))
        rolled_back = labeled(
            "rollback",
            ContractMatcher(
                opcode=HipoProxyRollbackUnstake.opcode,
                child_matcher=labeled(
                    "rollback_unstake",
                    ContractMatcher(opcode=HipoRollbackUnstake.opcode, optional=True),
                ),
            ),
        )
        super().__init__(
            parent_matcher=labeled(
                "bill_burned",
                ContractMatcher(
                    opcode=HipoBillBurned.opcode,
                    optional=True,
                    parent_matcher=ContractMatcher(opcode=HipoBurnBill.opcode, optional=True),
                ),
            ),
            child_matcher=OrMatcher([paid, postponed, rolled_back]),
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == HipoBurnTokens.opcode
            and _hipo_is_treasury(block.get_message().destination)
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        burn_tokens = HipoBurnTokens(block.get_body())
        pool = AccountId(block.get_message().destination)
        bill_burned = get_labeled("bill_burned", other_blocks, CallContractBlock)
        # The bill that has just burned: the `ts_nft` of the request being settled here.
        settled_bill = AccountId(bill_burned.get_message().source) if bill_burned is not None else None

        proxy_tokens_burned = get_labeled("proxy_tokens_burned", other_blocks, CallContractBlock)
        mint_bill = get_labeled("mint_bill", other_blocks, CallContractBlock)
        rollback = get_labeled("rollback", other_blocks, CallContractBlock)

        for leg in (proxy_tokens_burned, mint_bill, rollback):
            if leg is not None and not _hipo_from_treasury(leg):
                return []

        if proxy_tokens_burned is not None:
            # (a) paid out.
            burned = HipoProxyTokensBurned(proxy_tokens_burned.get_body())
            tokens_burned = get_labeled("tokens_burned", other_blocks, CallContractBlock)
            owner = burned.owner if burned.owner is not None else burn_tokens.owner
            new_block = HipoStakeWithdrawalBlock(
                data=HipoStakeWithdrawalData(
                    source=AccountId(owner),
                    pool=pool,
                    user_jetton_wallet=(
                        AccountId(tokens_burned.get_message().destination) if tokens_burned is not None else None
                    ),
                    amount=Amount(burned.coins),
                    tokens_burnt=Amount(burned.tokens),
                    asset=Asset(False, proxy_tokens_burned.get_message().destination),
                    bill=settled_bill,
                )
            )
            new_block.failed = block.failed
        elif mint_bill is not None:
            # (b) postponed to the next round that holds bills. No hGRAM and no GRAM move:
            # this is a request again, now tracked by a new bill.
            request = HipoMintBill(mint_bill.get_body())
            owner = request.owner if request.owner is not None else burn_tokens.owner
            new_block = HipoStakeWithdrawalRequestBlock(
                data=HipoStakeWithdrawalRequestData(
                    source=AccountId(owner),
                    pool=pool,
                    # Nothing reaches the owner's hGRAM wallet on this leg - the tokens were
                    # burned when the first request was made - so there is no wallet to name.
                    user_jetton_wallet=None,
                    tokens_burnt=Amount(request.amount if request.amount is not None else burn_tokens.tokens),
                    # `parent` travels on the bill, so it is the parent the owner's balances
                    # live under even if set_parent has replaced the live one since.
                    asset=Asset(False, request.parent if request.parent is not None else HIPO_PARENT_ADDRESS),
                    bill=_hipo_find_bill(mint_bill),
                    previous_bill=settled_bill,
                )
            )
            new_block.failed = block.failed
        elif rollback is not None:
            # (c) nowhere left to postpone to, so the hGRAM is handed back and the unstake
            # ends with no payout. Reported as an unsuccessful withdrawal rather than
            # silence, so the outstanding request does not dangle for ever.
            returned = HipoProxyRollbackUnstake(rollback.get_body())
            rollback_unstake = get_labeled("rollback_unstake", other_blocks, CallContractBlock)
            owner = returned.owner if returned.owner is not None else burn_tokens.owner
            new_block = HipoStakeWithdrawalBlock(
                data=HipoStakeWithdrawalData(
                    source=AccountId(owner),
                    pool=pool,
                    user_jetton_wallet=(
                        AccountId(rollback_unstake.get_message().destination)
                        if rollback_unstake is not None
                        else None
                    ),
                    amount=None,
                    tokens_burnt=Amount(returned.tokens),
                    asset=Asset(False, rollback.get_message().destination),
                    bill=settled_bill,
                )
            )
            new_block.failed = True
        else:
            return []

        new_block.merge_blocks([block] + other_blocks)
        return [new_block]
