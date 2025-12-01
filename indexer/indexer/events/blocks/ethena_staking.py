from __future__ import annotations

from dataclasses import dataclass

from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
    BlockTypeMatcher,
    ContractMatcher, child_sequence_matcher,
)
from indexer.events.blocks.core import Block
from indexer.events.blocks.labels import labeled
from indexer.events.blocks.messages import JettonTransfer, JettonMint
from indexer.events.blocks.messages.jettons import (
    JettonInternalTransfer,
    JettonNotify,
)
from indexer.events.blocks.utils import AccountId, Amount
from indexer.events.blocks.utils.block_utils import get_labeled
from indexer.events.blocks.utils.ton_utils import Asset
from indexer.events.blocks.jettons import JettonTransferBlock


@dataclass
class EthenaDepositData:
    source: AccountId
    user_jetton_wallet: AccountId
    pool: AccountId
    value: Amount
    tokens_minted: Amount
    asset: Asset
    source_asset: Asset


class EthenaDepositBlock(Block):
    data: EthenaDepositData
    def __init__(self, data):
        super().__init__("ethena_deposit", [], data)


@dataclass
class EthenaWithdrawalRequestData:
    source: AccountId
    source_wallet: AccountId
    receiver_wallet: AccountId
    pool: AccountId
    asset: AccountId
    amount: Amount
    ts_usde_amount: Amount

class EthenaWithdrawalRequestBlock(Block):
    data: EthenaWithdrawalRequestData
    def __init__(self, data):
        super().__init__("ethena_withdrawal_request", [], data)


class EthenaWithdrawalRequestBlockMatcher(BlockMatcher):
    def __init__(self):
        child_matcher = child_sequence_matcher([
            labeled('internal_transfer', ContractMatcher(JettonInternalTransfer.opcode)),
            ContractMatcher(JettonNotify.opcode),
            labeled('mint', ContractMatcher(JettonMint.opcode)),
            labeled('ts_usde_transfer', ContractMatcher(0xb2583ed5))
        ])
        super().__init__(
            child_matcher=child_matcher
        )

    def test_self(self, block: Block) -> bool:
        return isinstance(block, CallContractBlock) and block.opcode == JettonTransfer.opcode


    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        internal_transfer = get_labeled('internal_transfer', other_blocks, CallContractBlock)
        ts_usde_transfer = get_labeled('ts_usde_transfer', other_blocks, CallContractBlock)
        mint = get_labeled('mint', other_blocks, CallContractBlock)
        source = AccountId(block.get_message().source)
        source_wallet = AccountId(block.get_message().destination)
        receiver_wallet = AccountId(ts_usde_transfer.get_message().destination)
        usde_master = AccountId(internal_transfer.get_message().destination)
        source_wallet_info = await context.interface_repository.get().get_jetton_wallet(source_wallet.as_str())
        receiver_wallet_info = await context.interface_repository.get().get_jetton_wallet(receiver_wallet.as_str())
        if (
                source_wallet_info.owner != source.as_str() or
                receiver_wallet_info.owner != source.as_str() or
                source_wallet_info.jetton != usde_master.as_str() or
                receiver_wallet_info.jetton != ts_usde_transfer.get_message().source):
            return []

        timelocked_body = JettonInternalTransfer(ts_usde_transfer.get_body())
        internal_transfer_body = JettonInternalTransfer(internal_transfer.get_body())

        data = EthenaWithdrawalRequestData(
            amount=Amount(internal_transfer_body.amount),
            ts_usde_amount=Amount(timelocked_body.amount),
            source=source,
            source_wallet=source_wallet,
            receiver_wallet=receiver_wallet,
            asset=AccountId(source_wallet_info.jetton),
            pool=AccountId(mint.get_message().source)
        )

        new_block = EthenaWithdrawalRequestBlock(data)
        new_block.merge_blocks([block] + other_blocks)
        return [new_block]


class EthenaDepositBlockMatcher(BlockMatcher):
    def __init__(self):
        super().__init__(
            child_matcher=BlockTypeMatcher(block_type="jetton_mint", optional=False)
        )

    def test_self(self, block: Block) -> bool:
        return isinstance(block, JettonTransferBlock)

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        from indexer.events.blocks.jettons import JettonMintBlock
        
        jetton_transfer = block
        jetton_mint = None
        
        for other_block in other_blocks:
            if isinstance(other_block, JettonMintBlock):
                jetton_mint = other_block
                break
        
        if jetton_mint is None:
            return []
        
        transfer_asset = jetton_transfer.data["asset"]
        mint_asset = jetton_mint.data["asset"]
        
        expected_transfer_asset = "0:086FA2A675F74347B08DD4606A549B8FDB98829CB282BC1949D3B12FBAED9DCC"
        expected_mint_asset = "0:D0E545323C7ACB7102653C073377F7E3C67F122EB94D430A250739F109D4A57D"
        
        if transfer_asset is None or not transfer_asset.jetton_address:
            return []
        if transfer_asset.jetton_address.as_str() != expected_transfer_asset:
            return []
        
        if mint_asset is None or not mint_asset.jetton_address:
            return []
        if mint_asset.jetton_address.as_str() != expected_mint_asset:
            return []
        
        data = EthenaDepositData(
            source=AccountId(jetton_transfer.data["sender"]),
            user_jetton_wallet=AccountId(jetton_transfer.data["sender_wallet"]),
            pool=AccountId(jetton_transfer.data["receiver"]),
            value=Amount(jetton_transfer.data["amount"]),
            tokens_minted=Amount(jetton_mint.data["amount"]),
            asset=mint_asset,
            source_asset=transfer_asset
        )
        
        new_block = EthenaDepositBlock(data)
        new_block.merge_blocks([jetton_transfer, jetton_mint])
        return [new_block]