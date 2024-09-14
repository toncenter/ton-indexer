import argparse
import asyncio
import logging
import multiprocessing as mp
import sys
import time
import threading

from queue import Queue

from sqlalchemy import update, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker, contains_eager

from indexer.core import redis
from indexer.core.database import engine, Trace, Transaction, Message, Action, TraceEdge, SyncSessionMaker
from indexer.core.settings import Settings
from indexer.events import context
from indexer.events.blocks.utils.block_tree_serializer import block_to_action
from indexer.events.blocks.utils.event_deserializer import deserialize_event
from indexer.events.event_processing import process_event_async
from indexer.events.interface_repository import EmulatedTransactionsInterfaceRepository, gather_interfaces, \
    RedisInterfaceRepository

async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)
settings = Settings()


async def start_processing_events_from_db(args: argparse.Namespace):
    global lt, count
    logger.info(f"Creating pool of {args.pool_size} workers")

    queue = mp.Queue(2 * args.fetch_size)
    thread = mp.Process(target=fetch_events_for_processing, args=(queue, 1000))
    thread.start()
    big_traces = 0
    count = 0
    lt = 0
    with mp.Pool(args.pool_size, initializer=init_pool) as pool:
        while True:
            async with async_session() as session:
                start_wait = time.time()
                batch = []
                while time.time() - start_wait < 1.0 or len(batch) < args.fetch_size:
                    try:
                        item = queue.get(False)
                        batch.append(item)
                    except:
                        await asyncio.sleep(0.5)
                if (time.time() - lt) > 5 and lt > 0:
                    logger.info(f"Processed {count} traces in {time.time() - lt:02f} seconds. Traces/sec: {count / (time.time() - lt):02f}, queue size: {queue.qsize()}, big traces: {big_traces}")
                else:
                    logger.info(f'Processing first batch of {len(batch)} traces, queue size: {queue.qsize()}')
                ids = []
                has_traces_to_process = False
                total_nodes = 0
                for (trace_id, nodes) in batch:
                    if nodes > 4000 or nodes == 0:
                        if nodes > 0:
                            big_traces += 1
                        await session.execute(update(Trace)
                                              .where(Trace.trace_id == trace_id)
                                              .values(classification_state='broken'))
                        has_traces_to_process = True
                    else:
                        total_nodes += nodes
                        has_traces_to_process = True
                        ids.append(trace_id)
                await session.commit()
                if count == 0:
                    lt = time.time()
                count += len(ids)
                if has_traces_to_process:
                    pool.map(process_event_batch, split_into_batches(ids, args.batch_size))
                else:
                    await asyncio.sleep(0.5)
    thread.join()
# end def

def init_pool():
    asyncio.set_event_loop(asyncio.new_event_loop())


def split_into_batches(data, batch_size):
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]


async def start_emulated_traces_processing():
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
        else:
            await asyncio.sleep(1)


async def process_emulated_trace(trace_id):
    trace_map = await redis.client.hgetall(trace_id)
    trace_map = dict((str(key, encoding='utf-8'), value) for key, value in trace_map.items())
    trace = deserialize_event(trace_id, trace_map)
    context.interface_repository.set(EmulatedTransactionsInterfaceRepository(trace_map))
    return await process_event_async(trace)


def fetch_events_for_processing(queue: mp.Queue, fetch_size: int):
    logger.info(f'fetching unclassified traces...')
    while True:
        with SyncSessionMaker() as session:
            query = session.query(Trace.trace_id, Trace.nodes_) \
                .filter(Trace.state == 'complete') \
                .filter(Trace.classification_state == 'unclassified') \
                .order_by(Trace.start_lt.desc())
            query = query.yield_per(fetch_size)
            for item in query:
                queue.put(item)
        time.sleep(1)
# end def


def process_event_batch(ids: list[str]):
    asyncio.get_event_loop().run_until_complete(process_trace_batch_async(ids))
    return None


async def process_trace_batch_async(ids: list[str]):
    async with async_session() as session:
        query = select(Trace) \
            .join(Trace.transactions) \
            .join(Transaction.messages, isouter=True) \
            .join(Message.message_content, isouter=True) \
            .options(contains_eager(Trace.transactions, Transaction.messages, Message.message_content)) \
            .filter(Trace.trace_id.in_(ids))
        result = await session.execute(query)
        traces = result.scalars().unique().all()

        # Gather interfaces for each account
        accounts = set()
        for trace in traces:
            for tx in trace.transactions:
                accounts.add(tx.account)
        interfaces = await gather_interfaces(accounts, session)
        repository = RedisInterfaceRepository(redis.sync_client)
        await repository.put_interfaces(interfaces)
        context.interface_repository.set(repository)
        # Process traces and save actions
        results = await asyncio.gather(*(process_trace(trace) for trace in traces))
        ok_traces = []
        failed_traces = []
        broken_traces = []
        for trace_id, state, actions in results:
            if state == 'ok' or state == 'broken':
                session.add_all(actions)
                if state == 'ok':
                    ok_traces.append(trace_id)
                else:
                    broken_traces.append(trace_id)
            else:
                failed_traces.append(trace_id)
        stmt = update(Trace).where(Trace.trace_id.in_(ok_traces)).values(classification_state='ok')
        await session.execute(stmt)
        if len(broken_traces) > 0:
            stmt = update(Trace).where(Trace.trace_id.in_(broken_traces)).values(classification_state='broken')
            await session.execute(stmt)
        stmt = update(Trace).where(Trace.trace_id.in_(failed_traces)).values(classification_state='failed')
        await session.execute(stmt)
        await session.commit()


async def process_trace(trace: Trace) -> tuple[str, str, list[Action]]:
    if len(trace.transactions) == 1 and trace.transactions[0].descr == 'tick_tock':
        return trace.trace_id, 'ok', []
    try:
        result = await process_event_async(trace)
        actions = []
        state = 'ok'
        for block in result.bfs_iter():
            if block.btype != 'root':
                if block.btype == 'call_contract' and block.event_nodes[0].message.destination is None:
                    continue
                if block.broken:
                    state = 'broken'
                action = block_to_action(block, trace.trace_id)
                actions.append(action)
        return trace.trace_id, state, actions
    except Exception as e:
        logger.error("Marking trace as failed " + trace.trace_id + " - " + str(e))
        return trace.trace_id, 'failed', []


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--fetch-size',
                        help='Number of traces to fetch from db in one batch',
                        type=int,
                        default=10000)
    parser.add_argument('--batch-size',
                        help='Number of traces to process in one batch',
                        type=int,
                        default=1000)
    parser.add_argument('--pool-size',
                        help='Number of workers to process traces',
                        type=int,
                        default=4)
    args = parser.parse_args()
    if settings.emulated_traces:
        logger.info("Starting processing emulated traces")
        asyncio.run(start_emulated_traces_processing())
    else:
        logger.info("Starting processing events from db")
        asyncio.run(start_processing_events_from_db(args))
