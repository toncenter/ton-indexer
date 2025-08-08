from __future__ import annotations

import logging
import base64

from pytoniq_core import Slice
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

from indexer.core.database import Trace, Message, engine, MessageContent
from indexer.events.blocks.auction import AuctionBidMatcher
from indexer.events.blocks.basic_blocks import (
    CallContractBlock,
    ContractDeploy,
    TickTockBlock,
    TonTransferBlock,
)

from indexer.events.blocks.core import Block
from indexer.events.blocks.dns import ChangeDnsRecordMatcher
from indexer.events.blocks.elections import (
    ElectionDepositStakeBlockMatcher,
    ElectionRecoverStakeBlockMatcher,
)
from indexer.events.blocks.evaa import (
    EvaaLiquidateBlockMatcher,
    EvaaSupplyBlockMatcher,
    EvaaWithdrawBlockMatcher,
)
from indexer.events.blocks.jettons import (
    FallbackJettonTransferBlockMatcher,
    JettonBurnBlockMatcher,
    JettonMintBlockMatcher,
    JettonTransferBlockMatcher,
    PTonTransferMatcher,
)
from indexer.events.blocks.jvault import (
    JVaultClaimBlockMatcher,
    JVaultStakeBlockMatcher,
    JVaultUnstakeBlockMatcher, JVaultUnstakeRequestBlockMatcher,
)
from indexer.events.blocks.liquidity import (
    DedustDepositBlockMatcher,
    DedustDepositFirstAssetBlockMatcher,
    DedustWithdrawBlockMatcher,
    StonfiV2ProvideLiquidityMatcher,
    StonfiV2WithdrawLiquidityMatcher,
    ToncoDeployPoolBlockMatcher,
    ToncoDepositLiquidityMatcher,
    ToncoWithdrawLiquidityMatcher,
    post_process_dedust_liquidity,
)
from indexer.events.blocks.messages import TonTransferMessage
from indexer.events.blocks.messages.externals import (
    WalletV3ExternalMessage,
    WalletV4ExternalMessage,
    WalletV5R1ExternalMessage,
    extract_payload_from_wallet_message,
)
from indexer.events.blocks.multisig import (
    MultisigApproveBlockMatcher,
    MultisigCreateOrderBlockMatcher,
    MultisigExecuteBlockMatcher,
)
from indexer.events.blocks.nft import (
    NftDiscoveryBlockMatcher,
    NftMintBlockMatcher,
    NftTransferBlockMatcher,
    TelegramNftPurchaseBlockMatcher,
)
from indexer.events.blocks.staking import (
    NominatorPoolDepositMatcher,
    NominatorPoolWithdrawMatcher,
    NominatorPoolWithdrawRequestMatcher,
    TONStakersDelayedWithdrawalMatcher,
    TONStakersDepositMatcher,
    TONStakersWithdrawMatcher,
)
from indexer.events.blocks.subscriptions import (
    SubscriptionBlockMatcher,
    UnsubscribeBlockMatcher,
)
from indexer.events.blocks.swaps import (
    DedustSwapBlockMatcher,
    StonfiSwapBlockMatcher,
    StonfiV2SwapBlockMatcher,
    ToncoSwapBlockMatcher,
)
from indexer.events.blocks.utils import EventNode, NoMessageBodyException, to_tree
from indexer.events.blocks.utils.block_tree_serializer import block_to_action, create_unknown_action
from indexer.events.blocks.vesting import (
    VestingAddWhiteListBlockMatcher,
    VestingSendMessageBlockMatcher,
)
from indexer.events.blocks.layerzero import LayerZeroCommitPacketMatcher, LayerZeroDvnVerifyMatcher, LayerZeroReceiveMatcher, LayerZeroSendMatcher, LayerZeroSendTokensMatcher

async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
logger = logging.getLogger(__name__)


def init_block(node: EventNode) -> Block:
    block = None
    is_ton_transfer = (node.get_opcode() == 0 or node.get_opcode() is None or
                       node.get_opcode() == TonTransferMessage.encrypted_opcode)
    if node.is_tick_tock:
        block = TickTockBlock(node)
    elif is_ton_transfer and node.message.destination is not None and node.message.source is not None:
        block = TonTransferBlock(node)
    else:
        block = CallContractBlock(node)
    for child in node.children:
        block.connect(init_block(child))
    return block


def init_from_external(node: EventNode) -> Block:
    node.failed = True
    body = node.message.message_content.body

    payloads, _ = extract_payload_from_wallet_message(body)
    for idx, payload in enumerate(payloads):
        if payload.info is None:
            continue
        if payload.body is not None:
            body_hash = base64.b64encode(payload.body.hash).decode()
            msg_hash = base64.b64encode((payload.hash + str(idx)).encode()).decode()
            msg = Message(
                msg_hash = msg_hash,
                tx_hash = node.get_tx().hash,
                tx_lt = node.get_tx().lt,
                direction = 'in',
                trace_id= node.get_tx().trace_id,
                source = node.get_tx().account,
                destination = payload.info.dest.to_str(False).upper() if payload.info.dest else None,
                value = payload.info.value_coins,
                fwd_fee = payload.info.fwd_fee,
                ihr_fee = payload.info.ihr_fee,
                created_lt = node.get_tx().lt,
                created_at = node.get_tx().now,
                opcode = payload.opcode,
                bounce = payload.info.bounce,
                bounced = payload.info.bounced,
                import_fee = 0,
                body_hash = body_hash,
                init_state_hash = None,
                message_content=MessageContent(body=payload.body.to_boc(), hash= body_hash)
            )
            msg.transaction = node.get_tx()
            new_node = EventNode(msg, [], ghost_node=True)
            new_node.failed = True
            node.add_child(new_node)

    return init_block(node)

async def unwind_deployments(blocks: list[Block]) -> list[Block]:
    visited = set()
    for block in blocks:
        queue = block.children_blocks.copy()
        while len(queue) > 0:
            child = queue.pop(0)
            if isinstance(child, ContractDeploy) and child not in visited:
                blocks.append(child)
            else:
                queue.extend(child.children_blocks)
            visited.add(child)
    return blocks

matchers = [
    NftMintBlockMatcher(),
    TONStakersDelayedWithdrawalMatcher(),
    DedustDepositBlockMatcher(),
    DedustDepositFirstAssetBlockMatcher(),
    TONStakersDepositMatcher(),
    TONStakersWithdrawMatcher(),
    MultisigCreateOrderBlockMatcher(),
    MultisigApproveBlockMatcher(),
    MultisigExecuteBlockMatcher(),
    VestingSendMessageBlockMatcher(),
    VestingAddWhiteListBlockMatcher(),
    NominatorPoolDepositMatcher(),
    NominatorPoolWithdrawRequestMatcher(),
    NominatorPoolWithdrawMatcher(),
    JettonTransferBlockMatcher(),
    PTonTransferMatcher(),
    DedustWithdrawBlockMatcher(),
    JettonBurnBlockMatcher(),
    DedustSwapBlockMatcher(),
    StonfiSwapBlockMatcher(),
    StonfiV2SwapBlockMatcher(),
    ToncoSwapBlockMatcher(),
    NftTransferBlockMatcher(),
    TelegramNftPurchaseBlockMatcher(),
    NftDiscoveryBlockMatcher(),
    ChangeDnsRecordMatcher(),
    ElectionDepositStakeBlockMatcher(),
    ElectionRecoverStakeBlockMatcher(),
    SubscriptionBlockMatcher(),
    UnsubscribeBlockMatcher(),
    AuctionBidMatcher(),
    JettonMintBlockMatcher(),
    StonfiV2ProvideLiquidityMatcher(),
    StonfiV2WithdrawLiquidityMatcher(),
    JVaultStakeBlockMatcher(),
    JVaultUnstakeBlockMatcher(),
    JVaultClaimBlockMatcher(),
    JVaultUnstakeRequestBlockMatcher(),
    EvaaSupplyBlockMatcher(),
    EvaaWithdrawBlockMatcher(),
    EvaaLiquidateBlockMatcher(),
    ToncoDepositLiquidityMatcher(),
    ToncoDeployPoolBlockMatcher(), 
    ToncoWithdrawLiquidityMatcher(),
    LayerZeroSendMatcher(),
    LayerZeroSendTokensMatcher(),
    LayerZeroReceiveMatcher(),
    LayerZeroCommitPacketMatcher(),
    LayerZeroDvnVerifyMatcher()
]

trace_post_processors = [
    post_process_dedust_liquidity,
    unwind_deployments
]

matchers_for_failed_externals = [
    FallbackJettonTransferBlockMatcher()
]

async def process_event_async(trace: Trace) -> Block:
    try:
        node = to_tree(trace.transactions)
        root = Block('root', [])
        root.connect(init_block(node))

        for m in matchers:
            for b in root.bfs_iter():
                if b.parent is None:
                    await m.try_build(b)
        return root
    except NoMessageBodyException as e:
        raise e
    except Exception as e:
        logging.error(f"Failed to process {trace.trace_id}")
        logging.exception(e, exc_info=True)
        raise e

async def process_event_async_with_postprocessing(trace: Trace) -> list[Block]:
    block = await process_event_async(trace)
    blocks = [block] + list(block.bfs_iter())

    for post_processor in trace_post_processors:
        blocks = await post_processor(blocks)
    return blocks


async def try_process_unknown_event(trace: Trace) -> list[Block]:
    try:
        node = to_tree(trace.transactions)

        # Only external in allowed
        if len(node.children) != 0 or node.message is None or node.message.source is not None:
            return []
        root = Block('root', [])
        b = init_from_external(node)
        if b is None:
            return []
        root.connect(b)
        if len(b.next_blocks) == 0:
            return []
        for m in matchers_for_failed_externals:
            for b in root.bfs_iter():
                if b.parent is None:
                    await m.try_build(b)
        blocks = list(root.bfs_iter())

        for post_processor in trace_post_processors:
            blocks = await post_processor(blocks)
        return blocks
    except Exception as e:
        logging.error(f"Failed to process {trace.trace_id}")
        raise e

async def try_classify_unknown_trace(trace):
    actions = []
    blocks = await try_process_unknown_event(trace)
    for block in blocks:
        if block.btype in ('root', 'empty'):
            continue
        if block.btype == 'call_contract' and block.event_nodes[0].message.destination is None:
            continue
        if block.btype == 'call_contract' and block.event_nodes[0].message.source is None:
            continue
        action = block_to_action(block, trace.trace_id, trace)
        assert len(action.accounts) > 0, f"Action {action} has no accounts"
        actions.append(action)
    if len(actions) == 0:
        unknown_action = create_unknown_action(trace)
        actions.append(unknown_action)
    return actions