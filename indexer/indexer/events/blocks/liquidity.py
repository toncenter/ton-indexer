from __future__ import annotations

from loguru import logger
from pytoniq_core import Cell

from indexer.events import context
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


async def _get_provision_data(
    block: Block | CallContractBlock, other_blocks: list[Block | CallContractBlock]
) -> dict:
    deposit_info = DedustDepositLiquidityToPool(block.get_body())
    sender = AccountId(deposit_info.owner_addr)
    deposit_contract = block.get_message().source

    transfer_lp_call = find_call_contract(
        block.next_blocks, JettonInternalTransfer.opcode
    )
    if transfer_lp_call is None:
        raise Exception("LP Internal transfer not found")
    transfer_lp_call = JettonInternalTransfer(transfer_lp_call.get_body())

    lpool = AccountId(block.get_message().destination)
    lp_tokens = Amount(transfer_lp_call.amount)

    # there are 0-2 deposit jetton transfers (from user)
    jetton_deposits: list[CallContractBlock] = []
    for b in other_blocks:
        if (
            isinstance(b, CallContractBlock)
            and b.opcode == JettonTransfer.opcode
            and b.get_message().source.upper() == sender.as_str()
        ):
            jetton_deposits.append(b)

    # if jetton_deposits is empty -
    # ton was deposited and other jetton already before
    user_jetton_wallet_0 = None
    # we can't calculate the jetton wallet of another asset:
    user_jetton_wallet_1 = None

    if len(jetton_deposits) == 2:
        # we know both jwallets - just rearrange them as needed
        user_jetton_wallet_0 = jetton_deposits[0].get_message().destination
        user_jetton_wallet_1 = jetton_deposits[1].get_message().destination
        jw0 = await context.interface_repository.get().get_jetton_wallet(
            user_jetton_wallet_0.upper()
        )
        jw1 = await context.interface_repository.get().get_jetton_wallet(
            user_jetton_wallet_1.upper()
        )

        if jw0 is None or jw1 is None:
            raise Exception("Can't find jetton deposit wallets in context repository")

        # trying to swap if first is wrong
        if jw0.jetton != deposit_info.asset0.jetton_address:
            user_jetton_wallet_0 = jw1.address
            user_jetton_wallet_1 = jw0.address

        # if were mismathed - now should pass the check
        if (
            jw0.jetton != deposit_info.asset0.jetton_address
            or jw1.jetton != deposit_info.asset1.jetton_address
        ):
            raise Exception(
                "Jetton deposit wallets do not correspond to ones in context repository"
            )

    if len(jetton_deposits) == 1:
        # we know only last deposit jwallet - place it where needed
        one_known_wallet = jetton_deposits[0].get_message().destination
        w0 = await context.interface_repository.get().get_jetton_wallet(
            one_known_wallet.upper()
        )
        if w0 is None:
            raise Exception("Can't find jetton deposit wallets in context repository")

        if w0.jetton != deposit_info.asset0.jetton_address:
            user_jetton_wallet_0 = one_known_wallet
        elif w0.jetton != deposit_info.asset1.jetton_address:
            user_jetton_wallet_1 = one_known_wallet

    data = {
        "sender": sender,
        "pool_address": lpool,
        "deposit_contract": AccountId(deposit_contract),
        "lp_tokens_minted": lp_tokens,
        "asset_1": deposit_info.asset0,
        "amount_1": Amount(deposit_info.asset0_amount or 0),
        "asset_1": deposit_info.asset1,
        "amount_2": Amount(deposit_info.asset1_amount or 0),
        "user_jetton_wallet_1": AccountId(user_jetton_wallet_0),
        "user_jetton_wallet_2": AccountId(user_jetton_wallet_1),
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

    deposit_contract_address = vault_call.get_message().destination
    try:
        # if deposited TON first
        sender = vault_call.get_message().source
        body = vault_call.get_body()
        msg_data = DedustDepositTONToVault(body)
        deposit_contract_address = block.get_message().destination
        asset0 = msg_data.asset0
        asset0_amount = msg_data.asset0_target_balance
        asset1 = msg_data.asset1
        asset1_amount = msg_data.asset1_target_balance
        user_asset_wallet_0 = "addr_none"
        # the other is jetton, we don't know the wallet addr
        user_asset_wallet_1 = None
    except:
        # jetton deposit
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

        user_asset_wallet_0 = vault_call.previous_block.get_message().source
        # second may be specified only when it's TON.
        # bc we can't calculate just by jetton master
        user_asset_wallet_1 = "addr_none" if asset1.is_ton else None

    data = {
        "sender": AccountId(sender),
        "deposit_contract": AccountId(deposit_contract_address),
        "asset_1": asset0,
        "amount_1": Amount(asset0_amount or 0),
        "asset_2": asset1,
        "amount_2": Amount(asset1_amount or 0),
        "user_jetton_wallet_1": AccountId(user_asset_wallet_0),
        "user_jetton_wallet_2": AccountId(user_asset_wallet_1),
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
        new_block.data = await _get_provision_data(block, other_blocks)
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
