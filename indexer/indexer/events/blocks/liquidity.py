from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import (
    BlockMatcher,
    BlockTypeMatcher,
    ContractMatcher,
    GenericMatcher,
    OrMatcher,
)
from indexer.events.blocks.core import Block, EmptyBlock
from indexer.events.blocks.jettons import (
    JettonBurnBlock,
    JettonTransferBlock,
    JettonTransferBlockMatcher,
    PTonTransferMatcher,
)
from indexer.events.blocks.labels import LabelBlock, labeled
from indexer.events.blocks.messages import (
    DedustPayout,
    DedustPayoutFromPool,
    ExcessMessage,
    PTonTransfer,
    ToncoAccountV3AddLiquidity,
    ToncoPoolV3Burn,
    ToncoPoolV3FundAccount,
    ToncoPoolV3FundAccountPayload,
    ToncoPoolV3MinAndRefund,
    ToncoPoolV3StartBurn,
    ToncoPositionNftV3PositionBurn,
    ToncoPositionNftV3PositionInit,
    ToncoRouterV3PayTo,
)
from indexer.events.blocks.messages.jettons import (
    JettonBurn,
    JettonBurnNotification,
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
    StonfiV2ProvideLiquidity,
    ToncoPoolV3Init,
    ToncoRouterV3CreatePool,
)
from indexer.events.blocks.utils import AccountId, Amount, Asset
from indexer.events.blocks.utils.block_utils import find_call_contract, get_labeled


class DedustDepositLiquidity(Block):
    def __init__(self, data):
        super().__init__("dedust_deposit_liquidity", [], data)

    def __repr__(self):
        return f"dedust_deposit_liquidity {self.data}"


class DedustDepositLiquidityPartial(Block):
    def __init__(self, data):
        super().__init__("dedust_deposit_liquidity_partial", [], data)

    def __repr__(self):
        return f"dedust_deposit_liquidity_partial {self.data}"


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
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == DedustDepositLiquidityToPool.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        new_block = DedustDepositLiquidity({})
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
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == DedustTopUpLiquidityDepositContract.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        new_block = DedustDepositLiquidityPartial({})
        include = [block]
        include.extend(other_blocks)
        new_block.data = await _get_deposit_one_data(block, include)
        new_block.data["dex"] = "dedust"
        new_block.merge_blocks(include)
        return [new_block]


async def post_process_dedust_liquidity(blocks: list[Block]) -> list[Block]:
    first_deposits = []
    final_deposits = []
    for b in blocks:
        if isinstance(b, DedustDepositLiquidityPartial):
            first_deposits.append(b)
        elif isinstance(b, DedustDepositLiquidity):
            final_deposits.append(b)

    for first_deposit in first_deposits:
        final_deposit = None
        for b in final_deposits:
            if b.data['deposit_contract'] == first_deposit.data['deposit_contract']:
                final_deposit = b
                break
        if final_deposit:
            final_data = final_deposit.data
            final_data['asset_1'] = final_data['asset_1'] or first_deposit.data['asset_1']
            final_data['amount_1'] = final_data['amount_1'] or first_deposit.data['amount_1']
            final_data['asset_2'] = final_data['asset_2'] or first_deposit.data['asset_2']
            final_data['amount_2'] = final_data['amount_2'] or first_deposit.data['amount_2']
            final_data['user_jetton_wallet_1'] = final_data['user_jetton_wallet_1'] or first_deposit.data['user_jetton_wallet_1']
            final_data['user_jetton_wallet_2'] = final_data['user_jetton_wallet_2'] or first_deposit.data['user_jetton_wallet_2']
            blocks.remove(first_deposit)
            final_deposit.event_nodes.extend(first_deposit.event_nodes)
            if final_deposit.initiating_event_node != first_deposit.initiating_event_node:
                final_deposit.event_nodes.append(first_deposit.initiating_event_node)
            final_deposit.event_nodes = list(set(final_deposit.event_nodes))
            final_deposit.children_blocks.extend(first_deposit.children_blocks)
            final_deposit.children_blocks = list(set(final_deposit.children_blocks))
            final_deposit.calculate_min_max_lt()
    return blocks


class DedustWithdrawBlockMatcher(BlockMatcher):
    def __init__(self):
        # start at JettonBurn
        super().__init__(
            optional=False,
            child_matcher=ContractMatcher(
                optional=False,
                opcode=JettonBurnNotification.opcode,
                children_matchers=[
                    # two payouts - each either ton or jetton
                    ContractMatcher(
                        opcode=DedustPayoutFromPool.opcode,
                        child_matcher=OrMatcher(
                            [
                                labeled('payout_1', ContractMatcher(opcode=DedustPayout.opcode)),
                                labeled('payout_1', BlockTypeMatcher(block_type="jetton_transfer")),
                            ]
                        ),
                        optional=False,
                    ),
                    ContractMatcher(
                        opcode=DedustPayoutFromPool.opcode,
                        child_matcher=OrMatcher(
                            [
                                labeled('payout_2', ContractMatcher(opcode=DedustPayout.opcode)),
                                labeled('payout_2', BlockTypeMatcher(block_type="jetton_transfer")),
                            ]
                        ),
                        optional=False,
                    ),
                ],
            ),
        )


    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == JettonBurn.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        sender = block.get_message().source
        sender_wallet = block.get_message().destination
        burn_data = JettonBurn(block.get_body())

        burn_notify_call = block.next_blocks[0]
        pool = burn_notify_call.get_message().destination

        lp_wallet_info = await context.interface_repository.get().get_jetton_wallet(sender_wallet.upper())
        if not lp_wallet_info: return []

        lp_asset = Asset(is_ton=False, jetton_address=lp_wallet_info.jetton)

        payouts = [
            get_labeled('payout_1', other_blocks, Block),
            get_labeled('payout_2', other_blocks, Block)
        ]
        assets = []
        amounts = []
        dex_wallets = []
        dex_vaults = []
        user_wallets = []
        for payout in payouts:
            call_from_vault = payout
            payout_request = payout.previous_block if payout else None
            
            if not payout_request:
                continue

            if isinstance(call_from_vault, CallContractBlock) and call_from_vault.opcode == DedustPayout.opcode:
                asset = Asset(is_ton=True, jetton_address=None)
                dex_wallets.append(None)
                user_wallets.append(None)
                dex_vaults.append(AccountId(call_from_vault.get_message().source))
            elif isinstance(call_from_vault, JettonTransferBlock):
                dex_wallet = call_from_vault.data['sender_wallet']
                dex_wallets.append(dex_wallet)
                asset = call_from_vault.data['asset']
                user_wallets.append(call_from_vault.data['receiver_wallet'])
                dex_vaults.append(call_from_vault.data['sender'])

            else:
                # unexpected opcode
                return []

            payout_data = DedustPayoutFromPool(payout_request.get_body())
            amounts.append(payout_data.amount)
            assets.append(asset)

        new_block = Block('dex_withdraw_liquidity', [])
        new_block.merge_blocks([block] + other_blocks)
        new_block.data = {
            'dex': 'dedust',
            'sender': AccountId(sender),
            'sender_wallet': AccountId(sender_wallet),
            'pool': AccountId(pool),
            'asset': lp_asset,
            'lp_tokens_burnt': Amount(burn_data.amount),
            'is_refund': False,
            'amount1_out': Amount(amounts[0]),
            'asset1_out': assets[0],
            'dex_wallet_1': dex_vaults[0],
            'dex_jetton_wallet_1': dex_wallets[0],
            'wallet1': user_wallets[0],
            'amount2_out': Amount(amounts[1]),
            'asset2_out': assets[1],
            'dex_wallet_2': dex_vaults[1],
            'dex_jetton_wallet_2': dex_wallets[1],
            'wallet2': user_wallets[1]
        }
        return [new_block]


class StonfiV2ProvideLiquidityMatcher(BlockMatcher):
    def __init__(self):
        another_deposit = labeled('deposit_part',
                                  GenericMatcher(lambda block: block.btype == 'dex_deposit_liquidity' and
                                                       block.data['dex'] == 'stonfi_v2'))

        another_deposit_parent_matcher = BlockMatcher(child_matcher=another_deposit, optional=True)

        in_pton_transfer = ContractMatcher(opcode=JettonNotify.opcode,
                                           parent_matcher=labeled('in_transfer',
                                                                  ContractMatcher(
                                                                      opcode=0x01f3835d,
                                                                      parent_matcher=another_deposit_parent_matcher)))

        in_jetton_transfer = labeled('in_transfer', BlockTypeMatcher(block_type='jetton_transfer',
                                                                     parent_matcher=another_deposit_parent_matcher))

        in_jetton_transfer_2 = ContractMatcher(opcode=JettonNotify.opcode,
                                               parent_matcher=ContractMatcher(
                                                   opcode=JettonInternalTransfer.opcode,
                                                   parent_matcher=in_jetton_transfer))

        in_transfer = OrMatcher([in_pton_transfer, in_jetton_transfer, in_jetton_transfer_2])

        cb_add_liquidity = ContractMatcher(opcode=0x06ecd527,
                                           optional=True,
                                           child_matcher=OrMatcher([labeled('lp_token_transfer',
                                                                            ContractMatcher(opcode=JettonInternalTransfer.opcode)),
                                                                    labeled('refund_add_liquidity',
                                                                            ContractMatcher(opcode=0x50c6a654))]))

        super().__init__(parent_matcher=in_transfer,
                         child_matcher=ContractMatcher(opcode=0x50c6a654, child_matcher=cb_add_liquidity))

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == 0x37c096df

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        failed = False
        in_transfer = None
        lp_token_transfer = None
        another_deposit = None
        for b in other_blocks:
            if isinstance(b, LabelBlock):
                if b.label == 'refund_add_liquidity':
                    failed = True
                if b.label == 'in_transfer':
                    in_transfer = b.block
                if b.label == 'lp_token_transfer':
                    lp_token_transfer = b.block
                if b.label == 'deposit_part':
                    another_deposit = b.block
        lp_tokens_minted = None

        if lp_token_transfer:
            msg = JettonInternalTransfer(lp_token_transfer.get_body())
            lp_tokens_minted = Amount(msg.amount)

        new_block = Block('dex_deposit_liquidity', [])

        asset = None
        if isinstance(in_transfer, JettonTransferBlock):
            asset = in_transfer.data['asset']
        else:
            asset = Asset(is_ton=True, jetton_address=None)
        provide_liquidity_msg = StonfiV2ProvideLiquidity(block.get_body())
        amount = provide_liquidity_msg.amount1 if provide_liquidity_msg.amount1 and provide_liquidity_msg.amount1 > 0 else provide_liquidity_msg.amount2
        new_block.data = {
            'dex': 'stonfi_v2',
            'amount_1': Amount(amount or 0),
            'asset_1': asset,
            'sender': AccountId(provide_liquidity_msg.from_user),
            'sender_wallet_1': (in_transfer.data['sender_wallet']
                              if isinstance(in_transfer, JettonTransferBlock) else None),
            'amount_2': None,
            'asset_2': None,
            'sender_wallet_2': None,
            'pool': AccountId(block.event_nodes[0].message.destination),
            'lp_tokens_minted': lp_tokens_minted
        }

        if another_deposit:
            if another_deposit.data['sender'] == new_block.data['sender'] and \
                    another_deposit.data['pool'] == new_block.data['pool'] and \
                    another_deposit.data['lp_tokens_minted'] != new_block.data['lp_tokens_minted']:
                proxy_block = EmptyBlock()
                proxied_block = another_deposit.previous_block
                proxied_block.insert_between([another_deposit, in_transfer], proxy_block)
                other_blocks.append(proxy_block)
                other_blocks.remove(proxy_block.previous_block)
                new_block.data['amount_2'] = another_deposit.data['amount_1']
                new_block.data['asset_2'] = another_deposit.data['asset_1']
                new_block.data['sender_wallet_2'] = another_deposit.data['sender_wallet_1']
                if lp_tokens_minted is None:
                    new_block.data['lp_tokens_minted'] = another_deposit.data['lp_tokens_minted']


        new_block.merge_blocks([block] + other_blocks)
        new_block.failed = failed
        return [new_block]


class StonfiV2WithdrawLiquidityMatcher(BlockMatcher):
    def __init__(self):
        withdraw_refunded_liquidity = labeled('withdraw_refunded_liquidity', ContractMatcher(
            opcode=0x0f98e2b8,
            parent_matcher=ContractMatcher(opcode=0x132b9a2c)
        ))
        withdraw_liquidity = labeled('withdraw_liquidity', ContractMatcher(
            opcode=0x297437cf,
            parent_matcher=BlockTypeMatcher('jetton_burn')))
        super().__init__(parent_matcher=OrMatcher([withdraw_refunded_liquidity, withdraw_liquidity]),
                         child_matcher=BlockTypeMatcher(block_type='jetton_transfer'))

    def test_self(self, block: Block):
        return isinstance(block, CallContractBlock) and block.opcode == 0x657b54f5

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        payouts = [block]
        additional_blocks = []
        amount1 = None
        amount2 = None
        asset1 = None
        asset2 = None
        dex_sender1 = None
        dex_sender2 = None
        dex_sender1_jetton_wallet = None
        dex_sender2_jetton_wallet = None
        wallet1 = None
        wallet2 = None
        for b in block.previous_block.next_blocks:
            if b == block or b in other_blocks:
                continue
            if isinstance(b, CallContractBlock) and b.opcode == 0x657b54f5:
                jetton_transfers = [x for x in b.next_blocks if isinstance(x, JettonTransferBlock)]
                if len(jetton_transfers) > 0:
                    payouts.append(b)
                    additional_blocks.extend(jetton_transfers)
        for payout in payouts:
            jetton_transfers = [x for x in payout.next_blocks if isinstance(x, JettonTransferBlock)]
            if len(jetton_transfers) > 0:
                transfer = jetton_transfers[0]
                pton_transfer = next((x for x in transfer.next_blocks if isinstance(x, CallContractBlock)
                                      and x.opcode == PTonTransfer.opcode), None)
                if pton_transfer is not None:
                    pton_msg = PTonTransfer(pton_transfer.get_body())
                    amount = Amount(pton_msg.ton_amount or 0)
                    additional_blocks.append(pton_transfer)
                    asset = Asset(is_ton=True, jetton_address=None)
                else:
                    amount = transfer.data['amount']
                    asset = transfer.data['asset']
                
                if amount1 is None:
                    amount1 = amount
                    asset1 = asset
                    wallet1 = transfer.data['receiver_wallet']
                    dex_sender1 = transfer.data['sender']
                    dex_sender1_jetton_wallet = transfer.data['sender_wallet']
                else:
                    amount2 = amount
                    asset2 = asset
                    wallet2 = transfer.data['receiver_wallet']
                    dex_sender2 = transfer.data['sender']
                    dex_sender2_jetton_wallet = transfer.data['sender_wallet']
        sender = None
        sender_wallet = None
        asset = None
        burned_lps = None
        is_withdraw_refunded_liquidity = False

        for b in other_blocks:
            if isinstance(b, LabelBlock):
                if b.label == 'withdraw_refunded_liquidity':
                    is_withdraw_refunded_liquidity = True
                    sender = AccountId(b.block.previous_block.event_nodes[0].message.source)
                if b.label == 'withdraw_liquidity':
                    burn = b.block.previous_block
                    if isinstance(burn, JettonBurnBlock):
                        sender = burn.data['owner']
                        sender_wallet = burn.data['jetton_wallet']
                        burned_lps = burn.data['amount']
                        asset = burn.data['asset']
                    else:
                        return []
        new_block = Block('dex_withdraw_liquidity', [])
        new_block.merge_blocks(payouts + other_blocks + additional_blocks)

        new_block.data = {
            'dex': 'stonfi_v2',
            'sender': sender,
            'sender_wallet': sender_wallet,
            'pool': AccountId(block.event_nodes[0].message.source),
            'asset': asset,
            'lp_tokens_burnt': burned_lps,
            'is_refund': is_withdraw_refunded_liquidity,
            'amount1_out': amount1,
            'asset1_out': asset1,
            'dex_wallet_1': dex_sender1,
            'dex_jetton_wallet_1': dex_sender1_jetton_wallet,
            'wallet1': wallet1,
            'amount2_out': amount2,
            'asset2_out': asset2,
            'dex_wallet_2': dex_sender2,
            'dex_jetton_wallet_2': dex_sender2_jetton_wallet,
            'wallet2': wallet2
        }
        return [new_block]


@dataclass
class ToncoDepositLiquidityData:
    sender: AccountId
    pool: AccountId
    account_contract: AccountId
    position_amount_1: Amount
    position_amount_2: Amount
    lp_tokens_minted: Amount | None
    tick_lower: int
    tick_upper: int
    nft_index: int | None
    nft_address: AccountId | None
    is_complete: bool
    amount_1: Amount | None
    asset_1: Asset | None
    sender_wallet_1: AccountId | None
    amount_2: Amount | None
    asset_2: Asset | None
    sender_wallet_2: AccountId | None


class ToncoDepositLiquidityBlock(Block):
    data: ToncoDepositLiquidityData

    def __init__(self, data: ToncoDepositLiquidityData):
        super().__init__("tonco_deposit_liquidity", [], data)

    def __repr__(self):
        return f"tonco_deposit_liquidity pool={self.data.pool.address} sender={self.data.sender.address} complete={self.data.is_complete}"


class ToncoDepositLiquidityMatcher(BlockMatcher):
    def __init__(self):
        # match either TON transfer (PTonTransfer) or jetton transfer input
        ton_input = labeled(
            "ton_input",
            ContractMatcher(
                opcode=JettonNotify.opcode,
                parent_matcher=labeled(
                    "ton_input_addition", ContractMatcher(opcode=PTonTransfer.opcode)
                ),
            ),
        )

        jetton_input = labeled("jetton_input", BlockTypeMatcher("jetton_transfer"))

        input_transfer = OrMatcher([ton_input, jetton_input])

        # this will cover all cases:
        # 1. a) jetton transfer                b) jetton transfer + PTonTransfer
        # 2. a) jetton transfer + PTonTransfer b) jetton transfer
        # 3. a) jetton transfer                b) jetton transfer
        output_ton_or_jetton = BlockTypeMatcher(
            block_type="jetton_transfer",
            child_matcher=ContractMatcher(opcode=PTonTransfer.opcode, optional=True),
            optional=True,
        )

        refund_payments = ContractMatcher(
            optional=True,
            opcode=ToncoRouterV3PayTo.opcode,  # 0xa1daa96d
            children_matchers=[output_ton_or_jetton, output_ton_or_jetton],  # 2 outputs
        )

        call_pool_for_mint_and_refund = ContractMatcher(
            optional=True,
            opcode=ToncoPoolV3MinAndRefund.opcode,
            children_matchers=[
                refund_payments,
                labeled("nft_mint", BlockTypeMatcher(block_type="nft_mint")),
                ContractMatcher(opcode=ExcessMessage.opcode, optional=True),
            ],
        )

        super().__init__(
            optional=False,
            parent_matcher=ContractMatcher(
                opcode=ToncoPoolV3FundAccount.opcode,  # 0x4468de77
                parent_matcher=input_transfer,
            ),
            child_matcher=call_pool_for_mint_and_refund,
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == ToncoAccountV3AddLiquidity.opcode  # 0x3ebe5431
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        additional_blocks = []

        add_liquidity_msg = ToncoAccountV3AddLiquidity(block.get_body())
        is_first_asset_deposited = add_liquidity_msg.new_amount0 > 0  # to order assets

        # find NFT position init call
        position_init_msg = None
        lp_tokens_minted = None
        nft_index = None
        nft_address = None
        is_complete = False
        position_init_call = get_labeled("nft_mint", other_blocks)
        if position_init_call:
            position_init_msg = ToncoPositionNftV3PositionInit(
                position_init_call.get_body()
            )
            lp_tokens_minted = Amount(position_init_msg.liquidity)
            nft_index = position_init_msg.nft_index
            nft_address = AccountId(position_init_call.get_message().destination)
            is_complete = True

        # find the input transfer (jetton or PTon)
        input_transfer = get_labeled("ton_input", other_blocks) or get_labeled(
            "jetton_input", other_blocks
        )

        if not input_transfer:
            logger.error(f"Input transfer not found for {block.get_message().source}")
            return []

        # extract data from JettonNotify - the universal approach
        jetton_notify_block = None

        if input_transfer.btype == "jetton_transfer":
            # jetton transfer -> find JettonNotify in its children
            jetton_notify_block = find_call_contract(
                input_transfer.children_blocks, JettonNotify.opcode
            )
        else:
            # PTonTransfer case - it should be the notify itself
            if (
                isinstance(input_transfer, CallContractBlock)
                and input_transfer.opcode == JettonNotify.opcode
            ):
                jetton_notify_block = input_transfer

        addition = get_labeled("ton_input_addition", other_blocks)
        if addition:
            additional_blocks.append(addition)

        if not jetton_notify_block:
            logger.error(
                f"JettonNotify not found in input transfer chain {block.get_message().tx_hash}"
            )  # TODO
            return []

        try:
            jetton_notify = JettonNotify(jetton_notify_block.get_body())

            sent_amount = Amount(jetton_notify.jetton_amount or 0)
            sender = AccountId(jetton_notify.from_user)

            if not jetton_notify.forward_payload_cell:
                logger.error(
                    f"No forward payload in JettonNotify for {block.get_message().source}"
                )
                return []

            payload_slice = jetton_notify.forward_payload_cell.to_slice()
            if (
                payload_slice.preload_uint(32)
                != ToncoPoolV3FundAccountPayload.payload_opcode
            ):
                logger.error(
                    f"Invalid payload opcode in JettonNotify for {block.get_message().source}"
                )
                return []

            payload_data = ToncoPoolV3FundAccountPayload(payload_slice)
            other_jetton_wallet_str = payload_data.get_other_jetton_wallet()

            # determine sender jetton wallet
            sender_wallet = None
            if input_transfer.btype == "jetton_transfer":
                sender_wallet = input_transfer.data.get("sender_wallet")

            router_wallet_str = jetton_notify_block.get_message().source

        except Exception as e:
            logger.error(f"Failed to parse JettonNotify message: {e}")
            return []

        first_asset = None
        try:
            jetton_wallet = await context.interface_repository.get().get_jetton_wallet(
                router_wallet_str
            )
            if jetton_wallet is not None:
                if jetton_wallet.jetton in PTonTransferMatcher.pton_masters:
                    first_asset = Asset(is_ton=True, jetton_address=None)
                else:
                    first_asset = Asset(
                        is_ton=False, jetton_address=jetton_wallet.jetton
                    )
            else:
                logger.warning(
                    f"Jetton wallet not found for router_wallet_str={router_wallet_str}"
                )
                first_asset = Asset(is_ton=True, jetton_address=None)
        except Exception as e:
            logger.warning(f"Failed to determine first asset: {e}")

        second_asset = None
        try:
            jetton_wallet = await context.interface_repository.get().get_jetton_wallet(
                other_jetton_wallet_str
            )
            if jetton_wallet is not None:
                if jetton_wallet.jetton in PTonTransferMatcher.pton_masters:
                    second_asset = Asset(is_ton=True, jetton_address=None)
                else:
                    second_asset = Asset(
                        is_ton=False, jetton_address=jetton_wallet.jetton
                    )
        except Exception as e:
            logger.warning(f"Error determining second asset for liquidity: {e}")

        if is_first_asset_deposited:
            amount_1 = sent_amount
            asset_1 = first_asset
            amount_2 = None
            asset_2 = second_asset
            sender_wallet_1 = sender_wallet
            sender_wallet_2 = None
        else:
            amount_1 = None
            asset_1 = second_asset
            amount_2 = sent_amount
            asset_2 = first_asset
            sender_wallet_1 = None
            sender_wallet_2 = sender_wallet

        data = ToncoDepositLiquidityData(
            sender=sender,
            pool=AccountId(block.get_message().source),
            account_contract=AccountId(block.get_message().destination),
            position_amount_1=Amount(add_liquidity_msg.new_enough0 or 0),
            position_amount_2=Amount(add_liquidity_msg.new_enough1 or 0),
            lp_tokens_minted=lp_tokens_minted,
            tick_lower=add_liquidity_msg.tick_lower,
            tick_upper=add_liquidity_msg.tick_upper,
            nft_index=nft_index,
            nft_address=nft_address,
            is_complete=is_complete,
            amount_1=amount_1,
            asset_1=asset_1,
            sender_wallet_1=sender_wallet_1,
            amount_2=amount_2,
            asset_2=asset_2,
            sender_wallet_2=sender_wallet_2,
        )

        new_block = ToncoDepositLiquidityBlock(data)
        new_block.merge_blocks([block] + other_blocks + additional_blocks)
        return [new_block]


TONCO_ROUTER_WTTON_WALLET_ADDR = (
    "0:871DA9215B14902166F0EA2A16DB56278D528108377F8158C5F4CCFDFDD22E17"
)


@dataclass
class ToncoWithdrawLiquidityData:
    sender: AccountId
    pool: AccountId
    burned_nft_index: int | None
    burned_nft_address: AccountId | None
    liquidity_burnt: Amount
    tick_lower: int
    tick_upper: int
    amount1_out: Amount | None
    asset1_out: Asset | None
    dex_wallet_1: AccountId | None
    dex_jetton_wallet_1: AccountId | None
    wallet1: AccountId | None
    amount2_out: Amount | None
    asset2_out: Asset | None
    dex_wallet_2: AccountId | None
    dex_jetton_wallet_2: AccountId | None
    wallet2: AccountId | None


class ToncoWithdrawLiquidityBlock(Block):
    data: ToncoWithdrawLiquidityData

    def __init__(self, data: ToncoWithdrawLiquidityData):
        super().__init__("tonco_withdraw_liquidity", [], data)

    def __repr__(self):
        return f"tonco_withdraw_liquidity pool={self.data.pool.address} sender={self.data.sender.address} liquidity={self.data.liquidity_burnt}"


class ToncoWithdrawLiquidityMatcher(BlockMatcher):
    def __init__(self):
        output_ton_or_jetton = BlockTypeMatcher(
            block_type="jetton_transfer",
            child_matcher=ContractMatcher(opcode=PTonTransfer.opcode, optional=True),
            optional=True,
        )

        # router sends out two payments
        router_payouts = ContractMatcher(
            opcode=ToncoRouterV3PayTo.opcode,  # 0xa1daa96d
            optional=False,
            children_matchers=[
                labeled("payout_1", output_ton_or_jetton),
                labeled("payout_2", output_ton_or_jetton),
            ],
        )

        # pool processes the burn
        pool_burn = ContractMatcher(
            opcode=ToncoPoolV3Burn.opcode,  # 0xd73ac09d
            optional=False,
            child_matcher=router_payouts,
        )

        # NFT position burn request
        position_burn = ContractMatcher(
            opcode=ToncoPositionNftV3PositionBurn.opcode,  # 0x46ca335a
            child_matcher=pool_burn,
            optional=False,
        )

        super().__init__(
            optional=False,
            child_matcher=position_burn,
        )

    def test_self(self, block: Block):
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == ToncoPoolV3StartBurn.opcode  # 0x530b5f2c
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        additional_blocks = []

        # parse start burn message (ToncoPoolV3StartBurn)
        start_burn_msg = ToncoPoolV3StartBurn(block.get_body())
        burned_nft_index = start_burn_msg.burned_index
        liquidity_burnt = Amount(start_burn_msg.liquidity_to_burn)
        tick_lower = start_burn_msg.tick_lower
        tick_upper = start_burn_msg.tick_upper

        pool = AccountId(block.get_message().destination)

        position_burn_call = find_call_contract(
            block.next_blocks, ToncoPositionNftV3PositionBurn.opcode
        )
        if not position_burn_call:
            return []

        # parse position burn to get sender (original NFT owner) and burned_nft_address (the NFT itself)
        position_burn_msg = ToncoPositionNftV3PositionBurn(
            position_burn_call.get_body()
        )
        sender = AccountId(position_burn_msg.nft_owner)
        burned_nft_address = AccountId(position_burn_call.get_message().destination)

        pool_burn_call = find_call_contract(
            position_burn_call.next_blocks, ToncoPoolV3Burn.opcode
        )
        if not pool_burn_call:
            return []

        # parse pool burn to potentially update burned_nft_index
        try:
            pool_burn_msg_payload = ToncoPoolV3Burn(pool_burn_call.get_body())
            if pool_burn_msg_payload.burned_index:
                burned_nft_index = pool_burn_msg_payload.burned_index
        except Exception as e:
            logger.warning(f"Failed to parse pool burn message (ToncoPoolV3Burn): {e}")

        payout_1 = get_labeled("payout_1", other_blocks)
        payout_2 = get_labeled("payout_2", other_blocks)

        # find ToncoRouterV3PayTo call (Pool -> Router/User)
        router_payout_call = find_call_contract(
            pool_burn_call.next_blocks, ToncoRouterV3PayTo.opcode
        )
        if not router_payout_call:
            logger.warning(
                "Router payout call (ToncoRouterV3PayTo) not found in Tonco withdraw"
            )
            return []

        router_payout_msg = ToncoRouterV3PayTo(router_payout_call.get_body())

        # determine assets and amounts from router_payout_msg - order matters!
        asset0_data_from_router = {
            "amount": Amount(router_payout_msg.amount0 or 0),
            "wallet_addr": (
                AccountId(router_payout_msg.jetton0_address)
                if router_payout_msg.jetton0_address
                else None
            ),
            "receiver": (
                AccountId(router_payout_msg.receiver0)
                if router_payout_msg.receiver0
                else None
            ),
        }

        asset1_data_from_router = {
            "amount": Amount(router_payout_msg.amount1 or 0),
            "wallet_addr": (
                AccountId(router_payout_msg.jetton1_address)
                if router_payout_msg.jetton1_address
                else None
            ),
            "receiver": (
                AccountId(router_payout_msg.receiver1)
                if router_payout_msg.receiver1
                else None
            ),
        }
        router_assets_info = [asset0_data_from_router, asset1_data_from_router]

        # pton check
        for i in router_assets_info:
            if i["wallet_addr"].as_str() == TONCO_ROUTER_WTTON_WALLET_ADDR:
                i["wallet_addr"] = None

        # process actual transfers to extract definitive asset types
        actual_payout_transfers = [payout_1, payout_2]
        processed_payouts = []

        for i, payout_transfer_block in enumerate(actual_payout_transfers):
            # if payout_transfer_block is None:
            #     continue

            temp_processed_payout = {}
            if isinstance(payout_transfer_block, JettonTransferBlock):
                pton_transfer_block = find_call_contract(
                    payout_transfer_block.next_blocks, PTonTransfer.opcode
                )
                if pton_transfer_block is not None:
                    pton_msg = PTonTransfer(pton_transfer_block.get_body())
                    temp_processed_payout["amount"] = Amount(pton_msg.ton_amount or 0)
                    temp_processed_payout["asset"] = Asset(
                        is_ton=True, jetton_address=None
                    )
                    additional_blocks.append(pton_transfer_block)
                else:
                    temp_processed_payout["amount"] = payout_transfer_block.data[
                        "amount"
                    ]
                    temp_processed_payout["asset"] = payout_transfer_block.data["asset"]

                temp_processed_payout["dex_wallet"] = payout_transfer_block.data[
                    "sender"
                ]  # router actually
                temp_processed_payout["dex_jetton_wallet"] = payout_transfer_block.data[
                    "sender_wallet"
                ]
                temp_processed_payout["wallet"] = payout_transfer_block.data[
                    "receiver_wallet"
                ]
                processed_payouts.append(temp_processed_payout)
            elif i < len(router_assets_info):  # fallback to router message data
                router_asset_info = router_assets_info[i]
                asset = None
                if router_asset_info["wallet_addr"]:
                    jetton_wallet = (
                        await context.interface_repository.get().get_jetton_wallet(
                            router_asset_info["wallet_addr"].as_str()
                        )
                    )
                    if (
                        jetton_wallet
                        and jetton_wallet.jetton in PTonTransferMatcher.pton_masters
                    ):
                        asset = Asset(is_ton=True, jetton_address=None)
                    elif jetton_wallet:
                        asset = Asset(is_ton=False, jetton_address=jetton_wallet.jetton)
                processed_payouts.append(
                    {
                        "amount": router_asset_info["amount"],
                        "asset": asset,
                        "dex_wallet": AccountId(
                            router_payout_call.get_message().source
                        ),
                        "dex_jetton_wallet": router_asset_info["wallet_addr"],
                        "wallet": router_asset_info["receiver"],
                    }
                )
            else:
                return []

        # sort processed_payouts according to the pool's asset0 and asset1
        w1 = processed_payouts[0]["dex_jetton_wallet"]
        w2 = router_assets_info[0]["wallet_addr"]
        w1 = w1.as_str() if w1 else None
        w2 = w2.as_str() if w2 else None
        if w1 != w2:
            processed_payouts = processed_payouts[::-1]
            # check result
            w1_new = processed_payouts[0]["dex_jetton_wallet"]
            w1_new = w1_new.as_str() if w1_new else None
            if w1_new != w2:
                logger.warning(
                    f"Failed to sort Tonco withdraw liquidity: {w1} and {w1_new} != {w2}"
                )

        # fill output data class
        data = ToncoWithdrawLiquidityData(
            sender=sender,
            pool=pool,
            burned_nft_index=burned_nft_index,
            burned_nft_address=burned_nft_address,
            liquidity_burnt=liquidity_burnt,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            amount1_out=(
                processed_payouts[0].get("amount")
                if len(processed_payouts) > 0
                else None
            ),
            asset1_out=(
                processed_payouts[0].get("asset")
                if len(processed_payouts) > 0
                else None
            ),
            dex_wallet_1=(
                processed_payouts[0].get("dex_wallet")
                if len(processed_payouts) > 0
                else None
            ),
            dex_jetton_wallet_1=(
                processed_payouts[0].get("dex_jetton_wallet")
                if len(processed_payouts) > 0
                else None
            ),
            wallet1=(
                processed_payouts[0].get("wallet")
                if len(processed_payouts) > 0
                else None
            ),
            amount2_out=(
                processed_payouts[1].get("amount")
                if len(processed_payouts) > 1
                else None
            ),
            asset2_out=(
                processed_payouts[1].get("asset")
                if len(processed_payouts) > 1
                else None
            ),
            dex_wallet_2=(
                processed_payouts[1].get("dex_wallet")
                if len(processed_payouts) > 1
                else None
            ),
            dex_jetton_wallet_2=(
                processed_payouts[1].get("dex_jetton_wallet")
                if len(processed_payouts) > 1
                else None
            ),
            wallet2=(
                processed_payouts[1].get("wallet")
                if len(processed_payouts) > 1
                else None
            ),
        )

        new_block = ToncoWithdrawLiquidityBlock(data)

        blocks_to_merge = [block] + other_blocks + additional_blocks
        for call_block in [position_burn_call, pool_burn_call, router_payout_call]:
            if call_block and call_block not in blocks_to_merge:
                blocks_to_merge.append(call_block)

        new_block.merge_blocks(list(set(blocks_to_merge)))
        new_block.failed = (
            router_payout_msg.exit_code != 0 and router_payout_msg.exit_code != 201
        )

        return [new_block]


@dataclass
class ToncoDeployPoolData:
    deployer: AccountId
    router: AccountId
    pool: AccountId
    jetton0_router_wallet: AccountId
    jetton1_router_wallet: AccountId
    jetton0_minter: AccountId
    jetton1_minter: AccountId
    tick_spacing: int
    initial_price_x96: int
    protocol_fee: int
    lp_fee_base: int
    lp_fee_current: int
    pool_active: bool
    success: bool


class ToncoDeployPoolBlock(Block):
    data: ToncoDeployPoolData

    def __init__(self, data: ToncoDeployPoolData):
        super().__init__("tonco_deploy_pool", [], data)

    def __repr__(self):
        return f"tonco_deploy_pool pool={self.data.pool.address} success={self.data.success}"


class ToncoDeployPoolBlockMatcher(BlockMatcher):
    """Matches the sequence of messages for TONCO pool deployment and initialization."""

    def __init__(self):
        excesses_matcher = labeled(
            "excesses", ContractMatcher(ExcessMessage.opcode, optional=True)
        )
        init_pool_matcher = labeled(
            "init_pool",
            ContractMatcher(
                ToncoPoolV3Init.opcode, optional=False, child_matcher=excesses_matcher
            ),
        )
        super().__init__(child_matcher=init_pool_matcher)

    def test_self(self, block: Block):
        """Check if the block is the starting message: ROUTERV3_CREATE_POOL."""
        return (
            isinstance(block, CallContractBlock)
            and block.opcode == ToncoRouterV3CreatePool.opcode
        )

    async def build_block(self, block: Block, other_blocks: list[Block]) -> list[Block]:
        """Builds the ToncoDeployPoolBlock if the full sequence is found."""
        create_pool_block = block
        create_pool_msg = create_pool_block.get_message()
        try:
            create_pool_data = ToncoRouterV3CreatePool(create_pool_block.get_body())
        except Exception as e:
            logger.error(f"Failed to parse ToncoRouterV3CreatePool: {e}", exc_info=True)
            return []

        init_pool_block = get_labeled("init_pool", other_blocks, CallContractBlock)
        if not init_pool_block:
            return []

        init_pool_msg = init_pool_block.get_message()
        try:
            init_pool_data = ToncoPoolV3Init(init_pool_block.get_body())
        except Exception as e:
            logger.error(f"Failed to parse ToncoPoolV3Init: {e}", exc_info=True)
            return []

        excesses_block = get_labeled("excesses", other_blocks, CallContractBlock)

        pool_address = AccountId(init_pool_msg.destination)
        router_address = AccountId(create_pool_msg.destination)

        jetton0_minter_addr = create_pool_data.jetton0_minter
        jetton1_minter_addr = create_pool_data.jetton1_minter
        jetton0_router_wallet_addr = create_pool_data.jetton_wallet0
        jetton1_router_wallet_addr = create_pool_data.jetton_wallet1

        try:
            data = ToncoDeployPoolData(
                deployer=AccountId(create_pool_msg.source),
                router=router_address,
                pool=pool_address,
                jetton0_router_wallet=AccountId(jetton0_router_wallet_addr),
                jetton1_router_wallet=AccountId(jetton1_router_wallet_addr),
                jetton0_minter=AccountId(jetton0_minter_addr),
                jetton1_minter=AccountId(jetton1_minter_addr),
                tick_spacing=create_pool_data.tick_spacing,
                initial_price_x96=create_pool_data.initial_price_x96,
                protocol_fee=create_pool_data.protocol_fee,
                lp_fee_base=create_pool_data.lp_fee_base,
                lp_fee_current=create_pool_data.lp_fee_current,
                pool_active=True if init_pool_data.pool_active else False,
                success=True,  # successful if we found the init message
            )
        except Exception as e:
            logger.error(
                f"Failed to create ToncoDeployPoolData in trace {block.get_message().trace_id}: {e}",
                exc_info=True,
            )
            return []

        new_block = ToncoDeployPoolBlock(data=data)
        blocks_to_merge = [create_pool_block, init_pool_block]
        if excesses_block:
            blocks_to_merge.append(excesses_block)
        new_block.merge_blocks(blocks_to_merge)
        return [new_block]
