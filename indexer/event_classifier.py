import asyncio
import logging
import time

from sqlalchemy import update, select
from sqlalchemy.orm import sessionmaker, contains_eager

from events.extra_data_repository import RedisExtraDataRepository, SqlAlchemyExtraDataRepository
from indexer.core import redis
from indexer.core.database import engine, Event, Transaction, Message, TransactionMessage
from indexer.core.settings import Settings
from indexer.events import context
from indexer.events.blocks.utils.event_deserializer import deserialize_event
from indexer.events.event_processing import process_event_async

async_session = sessionmaker(engine, expire_on_commit=False, class_=sessionmaker)
logger = logging.getLogger(__name__)
settings = Settings()
logging.basicConfig(level=logging.INFO)


async def start_processing_events_from_db():
    while True:
        async with async_session() as session:
            context.extra_data_repository.set(SqlAlchemyExtraDataRepository(session))
            batch = await fetch_events_for_processing(250)
            if len(batch) == 0:
                print("No events to process")
                await asyncio.sleep(5)
                continue
            ids = [e.id for e in batch]
            query = select(Event) \
                .join(Event.transactions) \
                .join(Event.edges) \
                .join(Transaction.messages) \
                .join(TransactionMessage.message) \
                .join(Message.message_content, isouter=True) \
                .options(contains_eager(Event.transactions, Transaction.messages, TransactionMessage.message,
                                        Message.message_content),
                         contains_eager(Event.edges)) \
                .filter(Event.id.in_(ids))
            result = await session.execute(query)
            events = result.scalars().unique().all()
            print(f"Fully loaded {len(events)} events")

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
                # TODO: Save result
            except Exception as e:
                logger.error(f"Failed to process emulated trace {trace_id}: {e}")
            logger.info(f"Processed traces --- {count}")
        else:
            await asyncio.sleep(1)


async def process_emulated_trace(trace_id):
    trace_map = await redis.client.hgetall(trace_id)
    trace_map = dict((str(key, encoding='utf-8'), value) for key, value in trace_map.items())
    event = deserialize_event(trace_id, trace_map)
    context.extra_data_repository.set(RedisExtraDataRepository(trace_map))
    return await process_event_async(event)


async def fetch_events_for_processing(size: int):
    async with async_session() as session:
        query = select(Event) \
            .join(Event.transactions) \
            .filter(Event.processed == False) \
            .limit(size)
        events = await session.execute(query)
        return events.scalars().unique().all()


async def process_event_from_db(event: Event):
    async with async_session() as session:
        context.session.set(session)
        try:
            result = await process_event_async(event)
            return result
        except Exception as e:
            logger.exception(e, exc_info=True)
        finally:
            # Add field for better status model
            stmt = update(Event).where(Event.id == event.id).values(processed=True)
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
