from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

from events.blocks.dns import ChangeDnsRecordMatcher
from events.blocks.elections import ElectionDepositStakeBlockMatcher, ElectionRecoverStakeBlockMatcher
from events.blocks.megaton import WtonMintBlockMatcher
from events.blocks.messages import TonTransferMessage
from indexer.core.database import Event, engine
from indexer.events.blocks.basic_blocks import TonTransferBlock, CallContractBlock
from indexer.events.blocks.core import Block
from indexer.events.blocks.jettons import JettonTransferBlockMatcher, JettonBurnBlockMatcher
from indexer.events.blocks.nft import NftTransferBlockMatcher, TelegramNftPurchaseBlockMatcher
from indexer.events.blocks.swaps import DedustSwapBlockMatcher, StonfiSwapBlockMatcher
from indexer.events.blocks.utils import to_tree, EventNode

async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


# logging.basicConfig()
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

def init_block(node: EventNode) -> Block:
    block = None
    if node.get_opcode() == 0 or node.get_opcode() is None or node.get_opcode() == TonTransferMessage.encrypted_opcode:
        block = TonTransferBlock(node)
    else:
        block = CallContractBlock(node)
    for child in node.children:
        block.connect(init_block(child))
    return block


def is_event_finished(event: Event):
    """
    check if event is finished
    Check if all transaction outgoing messages with filled destination are incoming messages for other transactions
    """
    outgoing_messages = set()
    incoming_messages = set()

    for tx in event.transactions:
        for msg in tx.messages:
            if msg.direction == 'out' and msg.message.destination is not None:
                outgoing_messages.add(msg.message.hash)
            elif msg.direction == 'in' and msg.message.source is not None:
                incoming_messages.add(msg.message.hash)

    return outgoing_messages.issubset(incoming_messages)


matchers = [
    WtonMintBlockMatcher(),
    JettonTransferBlockMatcher(),
    JettonBurnBlockMatcher(),
    DedustSwapBlockMatcher(),
    StonfiSwapBlockMatcher(),
    NftTransferBlockMatcher(),
    TelegramNftPurchaseBlockMatcher(),
    ChangeDnsRecordMatcher(),
    ElectionDepositStakeBlockMatcher(),
    ElectionRecoverStakeBlockMatcher()
]


async def process_event_async(event: Event) -> Block:
    tree = to_tree(event.transactions, event)
    root = Block('root', [])
    root.connect(init_block(tree.children[0]))

    for m in matchers:
        for b in root.bfs_iter():
            await m.try_build(b)
    return root
