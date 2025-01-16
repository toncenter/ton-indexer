from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

from indexer.core.database import Trace, engine
from indexer.events.blocks.auction import AuctionBidMatcher
from indexer.events.blocks.basic_blocks import TonTransferBlock, CallContractBlock, ContractDeploy
from indexer.events.blocks.core import Block
from indexer.events.blocks.dns import ChangeDnsRecordMatcher
from indexer.events.blocks.elections import ElectionDepositStakeBlockMatcher, ElectionRecoverStakeBlockMatcher
from indexer.events.blocks.jettons import JettonTransferBlockMatcher, JettonBurnBlockMatcher, JettonMintBlockMatcher
from indexer.events.blocks.liquidity import DedustDepositBlockMatcher, DedustDepositFirstAssetBlockMatcher, DedustWithdrawBlockMatcher, \
    post_process_dedust_liquidity, StonfiV2ProvideLiquidityMatcher, StonfiV2WithdrawLiquidityMatcher
from indexer.events.blocks.messages import TonTransferMessage
from indexer.events.blocks.multisig import MultisigApproveBlockMatcher, MultisigCreateOrderBlockMatcher
from indexer.events.blocks.nft import NftTransferBlockMatcher, TelegramNftPurchaseBlockMatcher, NftMintBlockMatcher, NftDiscoveryBlockMatcher
from indexer.events.blocks.staking import TONStakersDepositMatcher, TONStakersWithdrawMatcher, \
    TONStakersDelayedWithdrawalMatcher, NominatorPoolDepositMatcher, NominatorPoolWithdrawRequestMatcher, \
    NominatorPoolWithdrawMatcher
from indexer.events.blocks.subscriptions import SubscriptionBlockMatcher, UnsubscribeBlockMatcher
from indexer.events.blocks.swaps import DedustSwapBlockMatcher, StonfiSwapBlockMatcher, StonfiV2SwapBlockMatcher
from indexer.events.blocks.utils import AccountId
from indexer.events.blocks.utils import NoMessageBodyException
from indexer.events.blocks.utils import to_tree, EventNode

async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
logger = logging.getLogger(__name__)


def init_block(node: EventNode) -> Block:
    block = None
    is_ton_transfer = (node.get_opcode() == 0 or node.get_opcode() is None or
                       node.get_opcode() == TonTransferMessage.encrypted_opcode)
    if node.is_tick_tock:
        block = Block('tick_tock', [node], {'account': AccountId(node.tick_tock_tx.account)})
    elif is_ton_transfer and node.message.destination is not None and node.message.source is not None:
        block = TonTransferBlock(node)
    else:
        block = CallContractBlock(node)
    for child in node.children:
        block.connect(init_block(child))
    return block

async def unwind_deployments(blocks: list[Block]) -> list[Block]:
    for block in blocks:
        queue = block.children_blocks.copy()
        while len(queue) > 0:
            child = queue.pop(0)
            if isinstance(child, ContractDeploy):
                blocks.append(child)
            else:
                queue.extend(child.children_blocks)
    return blocks

matchers = [
    NftMintBlockMatcher(),
    TONStakersDelayedWithdrawalMatcher(),
    DedustDepositBlockMatcher(),
    DedustDepositFirstAssetBlockMatcher(),
    DedustWithdrawBlockMatcher(),
    TONStakersDepositMatcher(),
    TONStakersWithdrawMatcher(),
    NominatorPoolDepositMatcher(),
    NominatorPoolWithdrawRequestMatcher(),
    NominatorPoolWithdrawMatcher(),
    MultisigCreateOrderBlockMatcher(),
    MultisigApproveBlockMatcher(),
    JettonTransferBlockMatcher(),
    JettonBurnBlockMatcher(),
    DedustSwapBlockMatcher(),
    StonfiSwapBlockMatcher(),
    StonfiV2SwapBlockMatcher(),
    NftTransferBlockMatcher(),
    NftDiscoveryBlockMatcher(),
    TelegramNftPurchaseBlockMatcher(),
    ChangeDnsRecordMatcher(),
    ElectionDepositStakeBlockMatcher(),
    ElectionRecoverStakeBlockMatcher(),
    SubscriptionBlockMatcher(),
    UnsubscribeBlockMatcher(),
    AuctionBidMatcher(),
    JettonMintBlockMatcher(),
    StonfiV2ProvideLiquidityMatcher(),
    StonfiV2WithdrawLiquidityMatcher()
]

trace_post_processors = [
    post_process_dedust_liquidity,
    unwind_deployments
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
        raise e

async def process_event_async_with_postprocessing(trace: Trace) -> list[Block]:
    block = await process_event_async(trace)
    blocks = [block] + list(block.bfs_iter())

    for post_processor in trace_post_processors:
        blocks = await post_processor(blocks)
    return blocks
