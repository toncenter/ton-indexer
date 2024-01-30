from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

from indexer.events import context
from indexer.events.blocks.basic_blocks import TonTransferBlock, CallContractBlock
from indexer.events.blocks.core import Block
from indexer.events.blocks.jettons import JettonTransferBlockMatcher, JettonBurnBlockMatcher
from indexer.events.blocks.nft import NftTransferBlockMatcher, TelegramNftPurchaseBlockMatcher
from indexer.events.blocks.swaps import DedustSwapBlockMatcher, StonfiSwapBlockMatcher
from indexer.events.blocks.utils import to_tree, EventNode
from indexer.core.database import Event, engine
from indexer.events.integration.repository import MongoRepository

async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


# logging.basicConfig()
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

def init_block(node: EventNode) -> Block:
    block = None
    if node.get_opcode() == 0 or node.get_opcode() is None:
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
    JettonTransferBlockMatcher(),
    JettonBurnBlockMatcher(),
    DedustSwapBlockMatcher(),
    StonfiSwapBlockMatcher(),
    NftTransferBlockMatcher(),
    TelegramNftPurchaseBlockMatcher(),
]


async def process_event_async(event: Event, repository: MongoRepository):
    finished = is_event_finished(event)
    async with async_session() as session:
        context.session.set(session)
        if not finished or repository.has_event(event.id):
            stmt = update(Event).where(Event.id == event.id).values(processed=True)
            await session.execute(stmt)
            await session.commit()
            return
        print(f"Processing event {event.id}")
        try:
            tree = to_tree(event.transactions, event)
        except Exception as e:
            print(f"Failed to process event {event.id}")
            print(e)
            stmt = update(Event).where(Event.id == event.id).values(processed=True)
            await session.execute(stmt)
            await session.commit()
            return
        root = Block('root', [])
        root.connect(init_block(tree.children[0]))

        for m in matchers:
            for b in root.bfs_iter():
                await m.try_build(b)
        stmt = update(Event).where(Event.id == event.id).values(processed=True)
        await session.execute(stmt)
        await session.commit()
    repository.save_actions(event, root)
    print(f"Finished processing event {event.id}")
