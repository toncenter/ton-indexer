from __future__ import annotations

from loguru import logger
from pytoniq_core import Cell

from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import (
    AndMatcher,
    BlockMatcher,
    ContractMatcher,
    OrMatcher,
)
from indexer.events.blocks.core import Block
from indexer.events.blocks.jettons import JettonTransferBlockMatcher
from indexer.events.blocks.messages.jettons import (
    JettonInternalTransfer,
    JettonNotify,
    JettonTransfer,
)
from indexer.events.blocks.messages.liquidity import (
    DedustAskLiquidityFactory,
    DedustDeployDepositContract,
    DedustDepositLiquidityJettonForwardPayload,
    DedustDepositLiquidityToPool,
    DedustDepositTONToVault,
    DedustReturnExcessFromVault,
    DedustTopUpLiquidityDepositContract,
)
from indexer.events.blocks.utils import AccountId, Amount
from indexer.events.blocks.utils.block_utils import find_call_contract


class DEXProvideLiquidityBlock(Block):
    def __init__(self, data):
        super().__init__("dex_deposit", [], data)

    def __repr__(self):
        return f"dex_deposit {self.data}"


class DEXDepositFirstAssetBlock(Block):
    def __init__(self, data):
        super().__init__("dex_deposit_first_asset", [], data)

    def __repr__(self):
        return f"dex_deposit_first_asset {self.data}"


async def _get_provision_data(block: Block | CallContractBlock) -> dict:
    deposit_info = DedustDepositLiquidityToPool(block.get_body())
    deposit_contract = block.get_message().source
    internal_transfer_call = find_call_contract(
        block.next_blocks, JettonInternalTransfer.opcode
    )
    if internal_transfer_call is None:
        raise Exception("Internal transfer not found")
    internal_transfer_info = JettonInternalTransfer(internal_transfer_call.get_body())

    sender = AccountId(deposit_info.owner_addr)
    lpool = AccountId(block.get_message().destination)
    lp_tokens = Amount(internal_transfer_info.amount)

    data = {
        "sender": sender,
        "pool_address": lpool,
        "deposit_contract": AccountId(deposit_contract),
        "lp_tokens_minted": lp_tokens,
        "asset_0": deposit_info.asset0,
        "amount_0": Amount(deposit_info.asset0_amount or 0),
        "asset_1": deposit_info.asset1,
        "amount_1": Amount(deposit_info.asset1_amount or 0),
    }
    return data


async def _get_deposit_one_data(
    block: Block | CallContractBlock, all_blocks: list[Block | CallContractBlock]
) -> dict:
    # its either TON deposit to vault or jetton transfer request to wallet
    # block:
    # user -> vault -> factory *-> deposit
    # or
    # user -> wallet -> wallet -> vault -> factory *-> deposit
    vault_call = block.previous_block.previous_block  # :
    # user *-> vault -> factory -> deposit
    # or
    # user -> wallet -> wallet -*> vault -> factory -> deposit

    # deposit_send_call = find_call_contract(all_blocks, DedustDepositTONToVault.opcode)
    # is_ton_deposit = deposit_send_call is not None
    deposit_contract_address = vault_call.get_message().destination
    try:
        sender = vault_call.get_message().source
        body = vault_call.get_body()
        msg_data = DedustDepositTONToVault(body)
        deposit_contract_address = block.get_message().destination
        asset0 = msg_data.asset0
        asset0_amount = msg_data.asset0_target_balance
        asset1 = msg_data.asset1
        asset1_amount = msg_data.asset1_target_balance
    except:
        body = vault_call.get_body()
        # transfer_notification query_id:uint64 amount:(VarUInteger 16)
        #            sender:MsgAddress forward_payload:(Either Cell ^Cell)
        #            = InternalMsgBody;
        assert body.load_uint(32) == JettonNotify.opcode
        body.load_uint(64)
        body.load_coins()
        sender = body.load_address()
        forward_payload_slice = body.load_ref().begin_parse()
        forward_payload_data = DedustDepositLiquidityJettonForwardPayload(
            forward_payload_slice
        )
        asset0 = forward_payload_data.asset0
        asset0_amount = forward_payload_data.asset0_target_balance
        asset1 = forward_payload_data.asset1
        asset1_amount = forward_payload_data.asset1_target_balance

    data = {
        "sender": AccountId(sender),
        "deposit_contract": AccountId(deposit_contract_address),
        "asset_0": asset0,
        "amount_0": Amount(asset0_amount or 0),
        "asset_1": asset1,
        "amount_1": Amount(asset1_amount or 0),
    }
    return data


class DedustDepositBlockMatcher(BlockMatcher):
    # Deposit requires 2 assets to be sent to a contract in parallel .
    # One can do the first in one trace and the second in the another.
    # This matcher includes only the last asset deposit
    # which actually triggered the msg to pool.
    # The other asset deposit actions is handled by
    # DedustDepositFirstAssetBlockMatcher.
    def __init__(self):
        super().__init__(
            optional=False,
            parent_matcher=ContractMatcher(
                parent_matcher=ContractMatcher(
                    # in parent (from left) we include the first asset deposit
                    # (may be TON or jetton - thus there's OrMatcher)
                    optional=False,
                    opcode=DedustAskLiquidityFactory.opcode,
                    parent_matcher=OrMatcher(
                        [
                            ContractMatcher(
                                optional=False,
                                opcode=DedustDepositTONToVault.opcode,
                            ),
                            ContractMatcher(
                                # recursive matcher for jetton transfer
                                # starting from notification
                                optional=False,
                                opcode=JettonNotify.opcode,
                                parent_matcher=ContractMatcher(
                                    optional=False,
                                    opcode=JettonInternalTransfer.opcode,
                                    include_excess=True,
                                    parent_matcher=ContractMatcher(
                                        optional=False,
                                        opcode=JettonTransfer.opcode,
                                    ),
                                ),
                            ),
                        ]
                    ),
                    child_matcher=ContractMatcher(
                        # just a msg to be included, nevermind
                        optional=True,
                        opcode=DedustDeployDepositContract.opcode,
                    ),
                ),
                optional=False,
                opcode=DedustTopUpLiquidityDepositContract.opcode,
            ),
            child_matcher=AndMatcher(
                # to the right, after deposit msg to liquidity pool
                [
                    ContractMatcher(
                        # mint lp tokens
                        optional=False,
                        opcode=JettonInternalTransfer.opcode,
                        child_matcher=ContractMatcher(
                            opcode=JettonNotify.opcode, optional=True
                        ),
                    ),
                    ContractMatcher(
                        # calling excesses from deposit contract
                        # and destroing it
                        optional=True,
                        opcode=DedustDeployDepositContract.opcode,
                        child_matcher=OrMatcher(
                            [
                                ContractMatcher(
                                    # it's probably a TON payout
                                    # it doesn't use vault, just from deposit
                                    optional=True,
                                    opcode=None,
                                ),
                                ContractMatcher(
                                    # it's a jetton payout from vault
                                    optional=True,
                                    opcode=DedustReturnExcessFromVault.opcode,
                                    child_matcher=JettonTransferBlockMatcher(),
                                ),
                            ]
                        ),
                    ),
                ]
            ),
        )

    def test_self(self, block: Block):
        print("in matcher for dedust deposit")
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == DedustDepositLiquidityToPool.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        new_block = DEXProvideLiquidityBlock({})
        include = [block]
        include.extend(other_blocks)
        new_block.data = await _get_provision_data(block)
        new_block.data["dex"] = "dedust"
        new_block.merge_blocks(include)
        return [new_block]


class DedustDepositFirstAssetBlockMatcher(BlockMatcher):

    @logger.catch()
    def __init__(self):
        super().__init__(
            optional=False,
            child_matcher=None,
            parent_matcher=ContractMatcher(
                optional=False,
                # this opcode is executed on Factory,
                # called by a Vault.
                # and Factory deploys a Deposit Contract
                opcode=DedustAskLiquidityFactory.opcode,
                child_matcher=ContractMatcher(
                    # the deploy
                    optional=False,
                    opcode=DedustDeployDepositContract.opcode,
                ),
                parent_matcher=OrMatcher(
                    [
                        ContractMatcher(
                            optional=False,
                            opcode=DedustDepositTONToVault.opcode,
                        ),
                        ContractMatcher(
                            # recursive matcher for jetton transfer
                            # starting from notification
                            optional=False,
                            opcode=JettonNotify.opcode,
                            parent_matcher=ContractMatcher(
                                optional=False,
                                opcode=JettonInternalTransfer.opcode,
                                include_excess=True,
                                parent_matcher=ContractMatcher(
                                    optional=False,
                                    opcode=JettonTransfer.opcode,
                                ),
                            ),
                        ),
                    ]
                ),
            ),
        )

    def test_self(self, block: Block):
        print("in matcher for dedust deposit 1st asset")
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == DedustTopUpLiquidityDepositContract.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        new_block = DEXDepositFirstAssetBlock({})
        include = [block]
        include.extend(other_blocks)
        new_block.data = await _get_deposit_one_data(block, include)
        new_block.data["dex"] = "dedust"
        new_block.merge_blocks(include)
        return [new_block]
