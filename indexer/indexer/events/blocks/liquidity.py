from __future__ import annotations

from loguru import logger

from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
    BlockTypeMatcher,
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
    DedustDestroyLiquidityDepositContract,
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
    # (well, may be more in trace, but we gonna use them only to find jwallets)
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

    a0_jetton_master = None
    a1_jetton_master = None
    if not deposit_info.asset0.is_ton:
        a0_jetton_master = str(deposit_info.asset0.jetton_address).upper()
    if not deposit_info.asset1.is_ton:
        a1_jetton_master = str(deposit_info.asset1.jetton_address).upper()

    for jdep in jetton_deposits:
        jwallet_str = jdep.get_message().destination

        jw_info = await context.interface_repository.get().get_jetton_wallet(
            jwallet_str.upper()
        )
        if not jw_info:
            continue

        if jw_info.jetton == a0_jetton_master:
            user_jetton_wallet_0 = jwallet_str

        if jw_info.jetton == a1_jetton_master:
            user_jetton_wallet_1 = jwallet_str


    # if len(jetton_deposits) == 2:
    #     # we know both jwallets - just rearrange them as needed
    #     user_jetton_wallet_0 = jetton_deposits[0].get_message().destination
    #     user_jetton_wallet_1 = jetton_deposits[1].get_message().destination
    #     jw0 = await context.interface_repository.get().get_jetton_wallet(
    #         user_jetton_wallet_0.upper()
    #     )
    #     jw1 = await context.interface_repository.get().get_jetton_wallet(
    #         user_jetton_wallet_1.upper()
    #     )

    #     if jw0 is None or jw1 is None:
    #         raise Exception("Can't find jetton deposit wallets in context repository")

    #     # trying to swap if first is wrong
    #     if jw0.jetton != deposit_info.asset0.jetton_address:
    #         user_jetton_wallet_0, user_jetton_wallet_1 = (
    #             user_jetton_wallet_1,
    #             user_jetton_wallet_0,
    #         )

    #     # if were mismathed - now should pass the check
    #     if (
    #         jw0.jetton != str(deposit_info.asset0.jetton_address).upper()
    #         or jw1.jetton != str(deposit_info.asset1.jetton_address).upper()
    #     ):
    #         raise Exception(
    #             "Jetton deposit wallets do not correspond to ones in context repository"
    #         )

    # if len(jetton_deposits) == 1:
    #     # we know only last deposit jwallet - place it where needed
    #     one_known_wallet = jetton_deposits[0].get_message().destination
    #     w0 = await context.interface_repository.get().get_jetton_wallet(
    #         one_known_wallet.upper()
    #     )
    #     if w0 is None:
    #         raise Exception("Can't find jetton deposit wallets in context repository")

    #     print(w0.jetton, deposit_info.asset0.jetton_address)
    #     if (not deposit_info.asset0.is_ton) and w0.jetton != str(
    #         deposit_info.asset0.jetton_address
    #     ).upper():
    #         user_jetton_wallet_0 = one_known_wallet
    #     elif (not deposit_info.asset1.is_ton) and w0.jetton != str(
    #         deposit_info.asset1.jetton_address
    #     ).upper():
    #         user_jetton_wallet_1 = one_known_wallet
    #     else:
    #         raise Exception(
    #             "Jetton deposit wallet does not correspond to one in context repository"
    #         )

    data = {
        "sender": sender,
        "pool_address": lpool,
        "deposit_contract": AccountId(deposit_contract),
        "lp_tokens_minted": lp_tokens,
        "asset_1": deposit_info.asset0,
        "amount_1": Amount(deposit_info.asset0_amount or 0),
        "asset_2": deposit_info.asset1,
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

    deposit_contract_address = block.get_message().destination
    try:
        # if deposited TON first
        sender = vault_call.get_message().source
        body = vault_call.get_body()
        msg_data = DedustDepositTONToVault(body)
        asset0 = msg_data.asset0
        asset0_amount = msg_data.asset0_target_balance
        asset1 = msg_data.asset1
        asset1_amount = msg_data.asset1_target_balance
        user_asset_wallet_0 = None
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
        user_asset_wallet_1 = None

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
            # to the right, after deposit msg to liquidity pool
            children_matchers=[
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
                    opcode=DedustDestroyLiquidityDepositContract.opcode,
                    children_matchers=[
                        # there's probably a TON payout (excess liq.)
                        # it doesn't use vault, just from deposit:
                        BlockTypeMatcher("ton_transfer", optional=True),
                        # and excess liq. jetton payout from vault:
                        ContractMatcher(
                            optional=True,
                            opcode=DedustReturnExcessFromVault.opcode,
                            child_matcher=JettonTransferBlockMatcher(),
                        ),
                    ],
                ),
            ],
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
