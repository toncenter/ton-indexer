import asyncio

import pymongo
import os
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker, contains_eager

from indexer.core.database import engine, Event, Transaction, Message, TransactionMessage
from indexer.events.event_processing import process_event_async
from indexer.events.integration.repository import MongoRepository


async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def fetch_events_for_processing(size: int):
    async with async_session() as session:
        query = select(Event) \
            .join(Event.transactions) \
            .filter(Event.processed == False) \
            .filter(Transaction.account == '0:DED65AF00FB7799ED490B803ED436914A7F0526BC6B8B4D30C35A936D734173D') \
            .limit(size)
        events = await session.execute(query)
        return events.scalars().unique().all()


async def start_processing_async(repository: MongoRepository):
    while True:
        async with async_session() as session:
            batch = await fetch_events_for_processing(20)
            if len(batch) == 0:
                print("No events to process")
                await asyncio.sleep(5)
                continue
            print(f"Processing {len(batch)} events")
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

            await asyncio.gather(*(process_event_async(event, repository) for event in events))


if __name__ == '__main__':
    mongo_dsn = os.getenv("TON_INDEXER_MONGO_DSN", "mongodb://root:root@localhost:27017/")
    client = pymongo.MongoClient(mongo_dsn)
    repository = MongoRepository(client["events"])
    asyncio.run(start_processing_async(repository))
