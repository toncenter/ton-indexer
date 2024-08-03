import asyncio
import logging
import time

from sqlalchemy import update, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker, contains_eager

from indexer.events.blocks.utils.block_tree_serializer import block_to_action
from indexer.events.extra_data_repository import RedisExtraDataRepository, SqlAlchemyExtraDataRepository, \
    gather_interfaces, InMemoryExtraDataRepository
from indexer.core import redis
from indexer.core.database import engine, Trace, Transaction, Message
from indexer.core.settings import Settings
from indexer.events import context
from indexer.events.blocks.utils.event_deserializer import deserialize_event
from indexer.events.event_processing import process_event_async

async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
logger = logging.getLogger(__name__)
settings = Settings()


async def start_processing_events_from_db():
    while True:
        async with async_session() as session:
            context.extra_data_repository.set(SqlAlchemyExtraDataRepository(context.session))
            batch = await fetch_events_for_processing(150)
            if len(batch) == 0:
                logger.info("No events to process")
                await asyncio.sleep(5)
                continue
            ids = []
            for (trace_id, nodes) in batch:
                if nodes > 100:
                    await session.execute(update(Trace).where(Trace.trace_id == trace_id)
                                          .values(classification_state='ok'))
                else:
                    ids.append(trace_id)
            await session.commit()
            query = select(Trace) \
                .join(Trace.transactions) \
                .join(Trace.edges, isouter=True) \
                .join(Transaction.messages, isouter=True) \
                .join(Message.message_content, isouter=True) \
                .options(contains_eager(Trace.transactions, Transaction.messages, Message.message_content),
                         contains_eager(Trace.edges)) \
                .filter(Trace.trace_id.in_(ids))
            result = await session.execute(query)
            events = result.scalars().unique().all()
            accounts = set()
            for trace in events:
                for tx in trace.transactions:
                    accounts.add(tx.account)
            interfaces = await gather_interfaces(accounts, session)
            context.extra_data_repository.set(InMemoryExtraDataRepository(interfaces,
                                                                          SqlAlchemyExtraDataRepository(context.session)))
            await asyncio.gather(*(process_event_from_db(event) for event in events))


async def start_emulated_traces_processing():
    count = 0
    total_load_time = 0
    total_setup_time = 0
    total_process_time = 0

    pubsub = redis.client.pubsub()
    await pubsub.subscribe(settings.emulated_traces_reddit_channel)
    while True:

        message = await pubsub.get_message()
        if message is not None and message['type'] == 'message':
            trace_id = message['data'].decode('utf-8')
            try:
                start = time.time()
                res = await process_emulated_trace(trace_id)
            except Exception as e:
                logger.error(f"Failed to process emulated trace {trace_id}: {e}")
            logger.info(f"Processed traces --- {count}")
        else:
            await asyncio.sleep(1)


async def process_emulated_trace(trace_id):
    trace_map = await redis.client.hgetall(trace_id)
    trace_map = dict((str(key, encoding='utf-8'), value) for key, value in trace_map.items())
    trace = deserialize_event(trace_id, trace_map)
    context.extra_data_repository.set(RedisExtraDataRepository(trace_map))
    return await process_event_async(trace)


async def fetch_events_for_processing(size: int):
    async with async_session() as session:
        query = select(Trace.trace_id, Trace.nodes_) \
            .filter(Trace.state == 'complete') \
            .filter(Trace.classification_state == 'unclassified') \
            .order_by(Trace.start_lt.asc()) \
            .limit(size)
        traces = await session.execute(query)
        return traces.all()


async def process_event_from_db(trace: Trace):
    async with async_session() as session:
        context.session.set(session)
        try:
            if len(trace.transactions) == 1 and trace.transactions[0].descr == 'tick_tock' and len(trace.edges) == 0:
                stmt = update(Trace).where(Trace.trace_id == trace.trace_id).values(classification_state='ok')
                await session.execute(stmt)
                await session.commit()
                return None
            result = await process_event_async(trace)
            actions = []
            for block in result.bfs_iter():
                if block.btype != 'root':
                    if block.btype == 'call_contract' and block.event_nodes[0].message.destination is None:
                        continue
                    action = block_to_action(block, trace.trace_id)
                    actions.append(action)
            for action in actions:
                session.add(action)
            stmt = update(Trace).where(Trace.trace_id == trace.trace_id).values(classification_state='ok')
            await session.execute(stmt)
            await session.commit()
            return result
        except Exception as e:
            logger.exception(e, exc_info=True)
            logger.error("Marking trace as failed " + trace.trace_id + " - " + str(e))
            stmt = update(Trace).where(Trace.trace_id == trace.trace_id).values(classification_state='failed')
            await session.execute(stmt)
            await session.commit()
        return None


if __name__ == '__main__':
    if settings.emulated_traces:
        logger.info("Starting processing emulated traces")
        asyncio.run(start_emulated_traces_processing())
    else:
        logger.info("Starting processing events from db")
        asyncio.run(start_processing_events_from_db())
