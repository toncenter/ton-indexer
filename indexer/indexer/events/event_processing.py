from __future__ import annotations

import base64
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

from indexer.events.blocks.utils import AccountId
from indexer.core.database import Trace, engine, TraceEdge
from indexer.events.blocks.basic_blocks import TonTransferBlock, CallContractBlock
from indexer.events.blocks.core import Block
from indexer.events.blocks.dns import ChangeDnsRecordMatcher
from indexer.events.blocks.elections import ElectionDepositStakeBlockMatcher, ElectionRecoverStakeBlockMatcher
from indexer.events.blocks.jettons import JettonTransferBlockMatcher, JettonBurnBlockMatcher
from indexer.events.blocks.messages import TonTransferMessage
from indexer.events.blocks.nft import NftTransferBlockMatcher, TelegramNftPurchaseBlockMatcher, NftMintBlockMatcher
from indexer.events.blocks.subscriptions import SubscriptionBlockMatcher, UnsubscribeBlockMatcher
from indexer.events.blocks.swaps import DedustSwapBlockMatcher, StonfiSwapBlockMatcher
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


matchers = [
    NftMintBlockMatcher(),
    JettonTransferBlockMatcher(),
    JettonBurnBlockMatcher(),
    DedustSwapBlockMatcher(),
    StonfiSwapBlockMatcher(),
    NftTransferBlockMatcher(),
    TelegramNftPurchaseBlockMatcher(),
    ChangeDnsRecordMatcher(),
    ElectionDepositStakeBlockMatcher(),
    ElectionRecoverStakeBlockMatcher(),
    SubscriptionBlockMatcher(),
    UnsubscribeBlockMatcher()
]


async def process_event_async(trace: Trace, trace_edges: list[TraceEdge] | None = None) -> Block:
    try:
        node = to_tree(trace.transactions, trace, trace_edges)
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
