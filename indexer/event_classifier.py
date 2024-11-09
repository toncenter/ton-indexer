import argparse
import asyncio
import logging
import multiprocessing as mp
import sys
import time
import traceback

from typing import Optional
from datetime import timedelta

from sqlalchemy import update, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker, contains_eager

from indexer.core import redis
from indexer.core.database import engine, Trace, Transaction, Message, Action, TraceEdge, SyncSessionMaker
from indexer.core.settings import Settings
from indexer.events import context
from indexer.events.blocks.utils.address_selectors import extract_additional_addresses
from indexer.events.blocks.utils.block_tree_serializer import block_to_action
from indexer.events.blocks.utils.event_deserializer import deserialize_event
from indexer.events.event_processing import process_event_async
from indexer.events.interface_repository import EmulatedTransactionsInterfaceRepository, gather_interfaces, \
    RedisInterfaceRepository

async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)
settings = Settings()



# thread procedures
class UnclassifiedEventsReader(mp.Process):
    def __init__(self, task_queue: mp.Queue, result_queue: mp.Queue, stats_queue: Optional[mp.Queue]=None, batch_size: int=4096):
        super().__init__()
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.stats_queue = stats_queue
        self.batch_size = batch_size
        self.last_lt = None
        self.last_trace_id = None
        self.pending_map = {}
        logger.info(f"Reading unclassified tasks with batch size {self.batch_size}")
    
    def read_batch(self):
        # get new batch
        with SyncSessionMaker() as session:
            query = session.query(Trace.trace_id, Trace.nodes_, Trace.start_lt) \
                .filter(Trace.state == 'complete') \
                .filter(Trace.classification_state == 'unclassified')
            if self.last_lt is not None:
                query = query.filter(Trace.start_lt <= self.last_lt)
            if self.last_trace_id is not None:
                query = query.filter(Trace.trace_id < self.last_trace_id)
            query = query.order_by(Trace.start_lt.desc(), Trace.trace_id.desc()) \
                .limit(self.batch_size)
            query = query.yield_per(self.batch_size)
            items = query.all()
        # read complete tasks
        not_empty = True
        while not_empty:
            try:
                processed, big, tasks = self.result_queue.get(False)
                if self.stats_queue is not None:
                    self.stats_queue.put((processed, big))
                for trace_id, _ in tasks:
                    self.pending_map.pop(trace_id)
            except:
                not_empty = False

        # send new tasks
        trace_ids = []
        flag = False
        for trace_id, nodes, lt in items:
            if not flag:
                flag = True
                # logger.critical(f'{lt} {trace_id} {nodes}')
            if self.last_lt is None:
                logger.info(f'Reading unclassified traces from {lt}')
                self.last_lt = lt
            self.last_lt = lt
            self.last_trace_id = trace_id
            if trace_id in self.pending_map:
                continue
            self.pending_map[trace_id] = nodes
            trace_ids.append((trace_id, nodes))
        # if len(items) > 0:
        #     logger.critical(f'{lt} {trace_id} {nodes}')
        if len(items) < self.batch_size:
            logger.info(f"Incomplete batch of length {len(items)}")
            self.last_lt = None
            self.last_trace_id = None
            time.sleep(1)
        if len(trace_ids) > 0:
            self.task_queue.put(trace_ids)
        return
    
    def run(self):
        try:
            while True:
                self.read_batch()
        except KeyboardInterrupt:
            logger.info(f'Gracefully stopped in the UnclassifiedEventsReader')
        except:
            logger.info(f'Error in UnclassifiedEventsReader: {traceback.format_exc()}')
        logger.info(f'Thread UnclassifiedEventsReader finished')
        return
# end class

class EventClassifierWorker(mp.Process):
    def __init__(self, id: int, task_queue: mp.Queue, result_queue: mp.Queue, big_traces_threshold=4000):
        super().__init__()
        self.id = id
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.big_traces_threshold = big_traces_threshold

    def mark_big_traces(self, tasks: list[tuple[str, int]]) -> list[str]:
        batch = []
        big_traces = 0
        with SyncSessionMaker() as session:
            for (trace_id, nodes) in tasks:
                if nodes > self.big_traces_threshold or nodes == 0:
                    if nodes > 0:
                        big_traces += 1
                    session.execute(update(Trace)
                        .where(Trace.trace_id == trace_id)
                        .values(classification_state='broken'))
                else:
                    batch.append(trace_id)
            session.commit()
        return batch

    def process_one_batch(self):
        tasks = self.task_queue.get(True)
        # logger.info(f'Worker #{self.id} accepted batch of {len(tasks)} tasks')
        batch = self.mark_big_traces(tasks)
        asyncio.get_event_loop().run_until_complete(process_trace_batch_async(batch))
        total = len(tasks)
        big_traces = total - len(batch)
        self.result_queue.put((total - big_traces, big_traces, tasks))
        return
        
    def run(self):
        asyncio.set_event_loop(asyncio.new_event_loop())
        try:
            while True:
                self.process_one_batch()
        
        except KeyboardInterrupt:
            logger.info(f'Gracefully stopped in the EventClassifierWorker #{self.id}')
        except:
            logger.info(f'Error in EventClassifierWorker #{self.id}: {traceback.format_exc()}')
        logger.info(f'Thread EventClassifierWorker #{self.id} finished')
        return

async def start_processing_events_from_db(args: argparse.Namespace):
    logger.info(f"Creating pool of {args.pool_size} workers")

    # counting traces
    logger.info("Counting traces")
    total_traces = 0
    with SyncSessionMaker() as session:
        query = session.query(Trace.trace_id) \
                .filter(Trace.state == 'complete') \
                .filter(Trace.classification_state == 'unclassified')
        total_traces = query.count()
    logger.info(f"Total unclassified traces: {total_traces}")

    task_queue = mp.Queue(args.fetch_size)
    result_queue = mp.Queue()
    stats_queue = mp.Queue()
    thread = UnclassifiedEventsReader(task_queue, result_queue, stats_queue, args.batch_size)
    thread.start()
    workers = []
    for id in range(args.pool_size):
        worker = EventClassifierWorker(id, task_queue, result_queue, big_traces_threshold=4000)
        worker.start()
        workers.append(worker)
    
    # stats
    big_traces = 0
    count = 0
    start_time = time.time()
    last_time = start_time
    try:
        while True:
            try:
                processed, big = stats_queue.get(False)
                count += processed
                big_traces += big
            except:
                await asyncio.sleep(0.5)
            cur_time = time.time()
            if (cur_time - last_time) > 5:
                elapsed = cur_time - start_time
                tps = count / elapsed
                eta_sec = min(999999999, max(0, int((total_traces - count) / max(tps, 1e-9))))
                eta = timedelta(seconds=eta_sec)
                logger.info(f"{count} traces / {elapsed:02f} sec, traces/sec: {tps:02f} (eta: {eta}), queue size: {task_queue.qsize()}, big traces: {big_traces}")
                last_time = cur_time
    except KeyboardInterrupt:
        logger.info(f'Gracefully stopped in the Main thread')
    logger.info(f'Thread Main thread finished')
    thread.terminate()
    thread.join()
    for worker in workers:
        worker.terminate()
        worker.join()
    return

# end def

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


async def process_trace_batch_async(ids: list[str]):
    async with async_session() as session:
        try:
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
                    accounts.update(extract_additional_addresses(tx))

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
                    # logger.error(f"query: {insert(Action).values(actions).on_conflict_do_nothing()}")
                    # session.execute(insert(Action).values(actions).on_conflict_do_nothing()) 
                    with session.no_autoflush:
                        session.add_all(actions)
                    session.flush()
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
        except Exception as ee:
            logger.error(f'Failed to process batch: {ee}')
            await session.rollback()
    return


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
                        help='Number of prefetched batches',
                        type=int,
                        default=300)
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
