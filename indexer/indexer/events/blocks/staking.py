from __future__ import annotations

from dataclasses import dataclass

from indexer.events.blocks.basic_blocks import CallContractBlock
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
    TONStakersNftBurn,
)
from indexer.events.blocks.nft import NftMintBlock
from indexer.events.blocks.utils import AccountId, Amount
from indexer.events.blocks.utils.block_utils import find_call_contract, get_labeled


@dataclass
class TONStakersDepositData:
    source: AccountId
    user_jetton_wallet: AccountId
    pool: AccountId
    value: Amount
    tokens_minted: Amount


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
    amount: Amount


class TONStakersWithdrawBlock(Block):
    data: TONStakersWithdrawData

    def __init__(self, data):
        super().__init__("tonstakers_withdraw", [], data)

    def __repr__(self):
        return f"tonstakers_withdraw {self.data}"


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
                value=Amount(msg.value - 10**9),  # 1 TON deposit fee
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

        if immediate_withdrawal is not None:
            new_block = TONStakersWithdrawBlock(
                data=TONStakersWithdrawData(
                    stake_holder=AccountId(msg.source),
                    burnt_nft=None,
                    pool=AccountId(request.get_message().destination),
                    amount=Amount(burn_request_data.amount),
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
                    minted_nft=minted_nft
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
                pool=None,
                amount=Amount(notification_msg.amount)
            )
        )
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]
