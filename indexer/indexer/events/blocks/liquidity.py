from __future__ import annotations

from collections import defaultdict

from loguru import logger

from indexer.events import context
from indexer.events.blocks.basic_blocks import CallContractBlock
from indexer.events.blocks.basic_matchers import (BlockMatcher,
                                                  BlockTypeMatcher,
                                                  ContractMatcher,
                                                  GenericMatcher, OrMatcher)
from indexer.events.blocks.core import Block, EmptyBlock
from indexer.events.blocks.jettons import (JettonBurnBlock,
                                           JettonTransferBlock,
                                           JettonTransferBlockMatcher)
from indexer.events.blocks.labels import LabelBlock, labeled
from indexer.events.blocks.messages import PTonTransfer
from indexer.events.blocks.messages.jettons import (JettonBurn,
                                                    JettonBurnNotification,
                                                    JettonInternalTransfer,
                                                    JettonNotify,
                                                    JettonTransfer)
from indexer.events.blocks.messages.liquidity import (
    DedustAskLiquidityFactory, DedustDeployDepositContract,
    DedustDepositLiquidityJettonForwardPayload, DedustDepositLiquidityToPool,
    DedustDepositTONToVault, DedustDestroyLiquidityDepositContract,
    DedustReturnExcessFromVault, DedustTopUpLiquidityDepositContract,
    StonfiV2ProvideLiquidity)
from indexer.events.blocks.messages.swaps import (DedustPayout,
                                                  DedustPayoutFromPool)
from indexer.events.blocks.utils import AccountId, Amount, Asset
from indexer.events.blocks.utils.block_utils import find_call_contract, find_call_contracts, get_labeled, \
    get_multiple_labeled


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

    transfer_lp_call = get_labeled('lp_transfer', other_blocks, CallContractBlock)
    reject = get_labeled('rejection', other_blocks, CallContractBlock)
    if transfer_lp_call:
        transfer_lp_call = JettonInternalTransfer(transfer_lp_call.get_body())

        lpool = AccountId(block.get_message().destination)
        lp_tokens = Amount(transfer_lp_call.amount)
    elif reject:
        lpool = AccountId(reject.get_message().source)
        lp_tokens = None
    else:
        raise Exception(f"No LP transfer or rejection")

    actual_asset = Asset(is_ton=True, jetton_address=None)
    actual_amount = None

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


    deposit = get_labeled('deposit', other_blocks, CallContractBlock)
    if deposit.opcode == DedustDepositTONToVault.opcode:
        actual_asset = Asset(is_ton=True, jetton_address=None)
        body = DedustDepositTONToVault(deposit.get_body())
        actual_amount = Amount(body.amount)
    else:
        jetton_notify_body = deposit.get_body()
        assert jetton_notify_body.load_uint(32) == JettonNotify.opcode
        jetton_notify_body.load_uint(64)
        actual_amount = Amount(jetton_notify_body.load_coins())
        wallet_info = await context.interface_repository.get().get_jetton_wallet(deposit.get_message().source)
        actual_asset = Asset(is_ton=False, jetton_address=wallet_info.jetton)

    for jdep in jetton_deposits:
        jwallet_str = jdep.get_message().destination

        jw_info = await context.interface_repository.get().get_jetton_wallet(
            jwallet_str.upper()
        )
        if not jw_info:
            continue

        if jw_info.jetton == actual_asset:
            user_jetton_wallet_0 = jwallet_str

    excesses = []
    for ton_excess in get_multiple_labeled('ton_excess', other_blocks, CallContractBlock):
        if ton_excess and ton_excess.get_message().destination == sender.as_str():
            excesses.append((Asset(is_ton=True, jetton_address=None), Amount(ton_excess.get_message().value)))

    for jetton_excess in get_multiple_labeled('jetton_excess', other_blocks, JettonTransferBlock):
        if jetton_excess.data['receiver'] == sender.as_str():
            excesses.append((jetton_excess.data['asset'],
                             jetton_excess.data['amount']))

    data = {
        "sender": sender,
        "pool_address": lpool,
        "deposit_contract": AccountId(deposit_contract),
        "lp_tokens_minted": lp_tokens,
        "asset_1": actual_asset,
        "asset_2": None,
        "amount_1": actual_amount,
        "amount_2": None,
        "target_asset_1": deposit_info.asset0,
        "target_amount_1": Amount(deposit_info.asset0_amount or 0),
        "target_asset_2": deposit_info.asset1,
        "target_amount_2": Amount(deposit_info.asset1_amount or 0),
        "user_jetton_wallet_1": AccountId(user_jetton_wallet_0),
        "user_jetton_wallet_2": AccountId(user_jetton_wallet_1),
        "vault_excesses": excesses,
        "success": lp_tokens is not None,
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
    actual_asset = Asset(is_ton=True, jetton_address=None)
    actual_amount = None

    # Exclude cases when final deposit matcher failed for some reason and current matcher tries to detect partial deposit
    topup_msg = find_call_contract(all_blocks, DedustTopUpLiquidityDepositContract.opcode)
    for topup_next_block in topup_msg.next_blocks:
        if isinstance(topup_next_block, CallContractBlock) and topup_next_block.opcode in \
            (DedustDepositLiquidityToPool.opcode, 0xe1a36cd4):
            raise Exception(f"Unexpected call contract after deposit top up")
    try:
        # if deposited TON first
        sender = vault_call.get_message().source
        body = vault_call.get_body()
        msg_data = DedustDepositTONToVault(body)
        actual_amount = Amount(msg_data.amount)
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
        amount = body.load_coins()
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
        dex_jetton_wallet_address = vault_call.get_message().source
        dex_jetton_wallet = await context.interface_repository.get().get_jetton_wallet(
            dex_jetton_wallet_address.upper())
        actual_asset = Asset(is_ton=False, jetton_address=dex_jetton_wallet.jetton)
        actual_amount = Amount(amount)

    data = {
        "sender": AccountId(sender),
        "deposit_contract": AccountId(deposit_contract_address),
        "asset_1": actual_asset,
        "amount_1": actual_amount,
        "asset_2": None,
        "amount_2": None,
        "user_jetton_wallet_1": AccountId(user_asset_wallet_0),
        "user_jetton_wallet_2": AccountId(user_asset_wallet_1),
        "target_asset_1": asset0,
        "target_amount_1": Amount(asset0_amount or 0),
        "target_asset_2": asset1,
        "target_amount_2": Amount(asset1_amount or 0),
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
        jetton_excess_payout = JettonTransferBlockMatcher()
        jetton_excess_payout.optional = True
        excesses = [
            # there's probably a TON payout (excess liq.)
            # it doesn't use vault, just from deposit:
            BlockTypeMatcher("ton_transfer", optional=True),
            # and excess liq. jetton payout from vault:
            ContractMatcher(
                optional=True,
                opcode=DedustReturnExcessFromVault.opcode,
                children_matchers=[
                    labeled('jetton_excess', jetton_excess_payout, call_build_block=True),
                    labeled('ton_excess', ContractMatcher(opcode=DedustPayout.opcode, optional=True))
                ]
            ),
            ContractMatcher(
                optional=True,
                opcode=DedustReturnExcessFromVault.opcode,
                children_matchers=[
                    labeled('jetton_excess', jetton_excess_payout, call_build_block=True),
                    labeled('ton_excess', ContractMatcher(opcode=DedustPayout.opcode, optional=True))
                ]
            ),
        ]
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
                            labeled('deposit', ContractMatcher(
                                optional=False,
                                opcode=DedustDepositTONToVault.opcode,
                            )),
                            labeled('deposit', ContractMatcher(
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
                            )),
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
                OrMatcher([
                    labeled('lp_transfer', ContractMatcher(
                        # mint lp tokens
                        optional=False,
                        opcode=JettonInternalTransfer.opcode,
                        child_matcher=ContractMatcher(
                            opcode=JettonNotify.opcode, optional=True
                        ),
                    )),
                    labeled('rejection', ContractMatcher(
                        optional=False,
                        opcode=0xe1a36cd4,
                        children_matchers=excesses))
                ]),

                ContractMatcher(
                    # calling excesses from deposit contract
                    # and destroing it
                    optional=True,
                    opcode=DedustDestroyLiquidityDepositContract.opcode,
                    children_matchers=excesses
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
        new_block.failed = not new_block.data.get("success", True)
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
    used_deposit_contracts = defaultdict(int)

    for b in blocks:
        if isinstance(b, DedustDepositLiquidityPartial):
            first_deposits.append(b)
            used_deposit_contracts[b.data['deposit_contract']] += 1
        elif isinstance(b, DedustDepositLiquidity):
            final_deposits.append(b)
            used_deposit_contracts[b.data['deposit_contract']] += 1

    for a, v in used_deposit_contracts.items():
        if v > 2:
            logger.warning(f"Found {v} deposits for the same contract {a}. Skipping merging")
            return blocks

    for first_deposit in first_deposits:
        final_deposit = None
        for b in final_deposits:
            if b.data['deposit_contract'] == first_deposit.data['deposit_contract']:
                final_deposit = b
                break
        if final_deposit:
            final_data = combine_deposits(first_deposit.data, final_deposit.data)
            blocks.remove(first_deposit)
            final_deposit.event_nodes.extend(first_deposit.event_nodes)
            if final_deposit.initiating_event_node != first_deposit.initiating_event_node:
                final_deposit.event_nodes.append(first_deposit.initiating_event_node)
            final_deposit.data = final_data
            final_deposit.event_nodes = list(set(final_deposit.event_nodes))
            final_deposit.children_blocks.extend(first_deposit.children_blocks)
            final_deposit.children_blocks = list(set(final_deposit.children_blocks))
            final_deposit.calculate_min_max_lt()
    return blocks


def combine_deposits(first_deposit, final_deposit):
    first_deposit_data = first_deposit.copy()
    final_deposit_data = final_deposit.copy()

    result = final_deposit_data.copy()

    # Check if target assets are consistent
    target_assets_first = {first_deposit_data['target_asset_1'], first_deposit_data['target_asset_2']}
    target_assets_final = {final_deposit_data['target_asset_1'], final_deposit_data['target_asset_2']}

    if target_assets_first != target_assets_final:
        raise Exception("Target assets in deposits don't match")

    # Gather all (asset, amount, jetton_wallet) tuples
    tuples = []

    for i in [1, 2]:
        asset = first_deposit_data[f'asset_{i}']
        if asset:
            amount = first_deposit_data[f'amount_{i}']
            wallet = first_deposit_data.get(f'user_jetton_wallet_{i}', None)
            tuples.append((asset, amount, wallet))

    for i in [1, 2]:
        asset = final_deposit_data[f'asset_{i}']
        if asset:
            amount = final_deposit_data[f'amount_{i}']
            wallet = final_deposit_data.get(f'user_jetton_wallet_{i}', None)
            # Check if this asset already exists in our tuples
            existing = next((t for t in tuples if t[0] == asset), None)
            if existing:
                if existing[2] != wallet:
                    raise Exception(f"User wallet mismatch for asset {asset}")
                idx = tuples.index(existing)
                merged_amount = existing[1] + amount
                merged_wallet = wallet if wallet else existing[2]
                tuples[idx] = (asset, merged_amount, merged_wallet)
            else:
                tuples.append((asset, amount, wallet))
    # Ensure we have exactly 2 unique assets
    unique_assets = {t[0] for t in tuples}
    if len(unique_assets) != 2:
        raise Exception("Expected exactly 2 unique assets")

    # Check if assets match target assets
    if unique_assets != target_assets_first:
        raise Exception("Assets don't match target assets")

    # Fill result with the merged tuples
    for idx, (asset, amount, wallet) in enumerate(tuples, 1):
        result[f'asset_{idx}'] = asset
        result[f'amount_{idx}'] = amount
        if wallet:
            result[f'user_jetton_wallet_{idx}'] = wallet

    # Update target assets with consistent ordering
    result['target_asset_1'] = tuples[0][0]
    result['target_asset_2'] = tuples[1][0]

    return result


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
            payout_request = payout.previous_block

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
        amount = provide_liquidity_msg.amount1 if provide_liquidity_msg.amount1 > 0 else provide_liquidity_msg.amount2
        new_block.data = {
            'dex': 'stonfi_v2',
            'amount_1': Amount(amount),
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
                    amount = Amount(PTonTransfer(pton_transfer.get_body()).ton_amount)
                    additional_blocks.append(pton_transfer)
                else:
                    amount = transfer.data['amount']
                if amount1 is None:
                    amount1 = amount
                    asset1 = transfer.data['asset']
                    wallet1 = transfer.data['receiver_wallet']
                    dex_sender1 = transfer.data['sender']
                    dex_sender1_jetton_wallet = transfer.data['sender_wallet']
                else:
                    amount2 = amount
                    asset2 = transfer.data['asset']
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
