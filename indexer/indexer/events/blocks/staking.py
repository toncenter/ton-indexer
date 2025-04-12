from __future__ import annotations

from dataclasses import dataclass

from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock, TonTransferBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
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
                amount=Amount(notification_msg.amount),
                tokens_burnt=None,
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