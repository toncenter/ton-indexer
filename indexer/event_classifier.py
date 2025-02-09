import argparse
import asyncio
import logging
import multiprocessing as mp
import sys
import time
import traceback
import codecs
from datetime import timedelta
from typing import Optional, List, Tuple
from dataclasses import asdict
import json
from collections import defaultdict

from sqlalchemy import update, select, delete, and_, or_, Column, Integer, String, Boolean
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker, contains_eager, selectinload
from sqlalchemy.sql import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm.attributes import instance_state

from indexer.core import redis
from indexer.core.database import engine, SyncSessionMaker, Base, Trace, Transaction, Message, Action, ActionAccount
from indexer.core.settings import Settings
from indexer.events import context
from indexer.events.blocks.utils.address_selectors import extract_additional_addresses
from indexer.events.blocks.utils.block_tree_serializer import block_to_action
from indexer.events.blocks.utils.dedust_pools import init_pools_data
from indexer.events.blocks.utils.event_deserializer import deserialize_event
from indexer.events.event_processing import process_event_async, process_event_async_with_postprocessing
from indexer.events.interface_repository import EmulatedTransactionsInterfaceRepository, gather_interfaces, \
    RedisInterfaceRepository

async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)
settings = Settings()



class ClassifierTask(Base):
    __tablename__ = '_classifier_tasks'
    id: int = Column(Integer, primary_key=True)
    mc_seqno: int = Column(Integer)
    trace_id: str = Column(String(44))
    pending: bool = Column(Boolean)


class ClassifierFailedTrace(Base):
    __tablename__ = '_classifier_failed_traces'
    id: int = Column(Integer, primary_key=True)
    trace_id: str = Column(String(44))
    error: str = Column(String)


# thread procedures
class UnclassifiedEventsReader(mp.Process):
    def __init__(self, task_queue: mp.Queue, result_queue: mp.Queue, stats_queue: Optional[mp.Queue]=None, batch_size: int=4096, prefetch_size: int=100000):
        super().__init__()
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.stats_queue = stats_queue
        self.batch_size = batch_size
        self.prefetch_size = prefetch_size
        logger.info(f"Reading unclassified tasks with batch size {self.batch_size}")
    
    def read_batch(self):
        rows = []
        with SyncSessionMaker() as session:
            try:
                query = f'''
                               WITH A AS (
                                   SELECT id 
                                   FROM _classifier_tasks 
                                   WHERE (claimed_at IS NULL OR claimed_at < NOW() - INTERVAL '5 minutes')
                                   ORDER BY mc_seqno DESC NULLS FIRST 
                                   LIMIT {self.batch_size} 
                                   FOR UPDATE SKIP LOCKED
                               )
                               UPDATE _classifier_tasks
                               SET claimed_at = NOW()
                               FROM A
                               WHERE _classifier_tasks.id = A.id
                               RETURNING _classifier_tasks.*;
                           '''
                # query = f'''with A as (select * from _classifier_tasks 
                # where pending is null or not pending order by mc_seqno desc nulls first limit {self.batch_size})
                # update _classifier_tasks T set pending = true from A where T.id = A.id
                # returning A.*;'''
                result = session.execute(query)
                rows.extend(result.fetchall())
                session.commit()
            except Exception as ee:
                logger.warning(f'Failed to read tasks: {ee}')
                session.rollback()
        tasks = [ClassifierTask(id=row.id, mc_seqno=row.mc_seqno, trace_id=row.trace_id, pending=row.pending) for row in rows]
        batches = self._split_tasks_into_batches(tasks)
        for batch in batches:
            self.task_queue.put(batch)

    def _split_tasks_into_batches(self, tasks: List[ClassifierTask]) -> List[List[ClassifierTask]]:
        trace_tasks_batch = []
        seqno_tasks_batches = []
        for task in tasks:
            if task.trace_id is not None:
                trace_tasks_batch.append(task)
            else:
                seqno_tasks_batches.append([task])
        if len(trace_tasks_batch) == 0:
            return seqno_tasks_batches
        else:
            return seqno_tasks_batches + [trace_tasks_batch]

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
    def __init__(self, id: int, task_queue: mp.Queue, result_queue: mp.Queue, big_traces_threshold=4000, force=False):
        super().__init__()
        self.id = id
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.big_traces_threshold = big_traces_threshold
        self.force = force

    def process_one_batch(self):
        tasks = self.task_queue.get(True)
        # logger.info(f'Worker #{self.id} accepted batch of {len(tasks)} tasks')
        try:
            ok, total, failed, broken = asyncio.get_event_loop().run_until_complete(self.process_trace_batch_async(tasks))
        except asyncio.CancelledError:
            logger.info("failed to process one batch: coroutine was cancelled")
            ok, total, failed, broken = False, 0, 0, 0
        except Exception as ee:
            logger.info(f"unexpectedly failed to process task: {ee}")
            ok, total, failed, broken = False, 0, 0, 0
        self.result_queue.put((ok, total, failed, broken))
        return
    
    async def process_trace_batch_async(self, tasks: List[ClassifierTask]) -> Tuple[bool, int]:
        ok = True
        processed = 0
        failed = 0
        broken = 0
        async with async_session() as session:
            try:
                trace_ids = []
                mc_seqnos = []
                is_trace_batch = tasks[0].trace_id is not None

                for task in tasks:
                    if is_trace_batch:
                        assert task.trace_id is not None, "All tasks must be trace tasks"
                    else:
                        assert task.trace_id is None, "All tasks must be seqno tasks"
                    mc_seqnos.append(task.mc_seqno)
                    logger.debug(f"Task with mc_seqno={task.mc_seqno}, trace_id={task.trace_id}")
                    # check existing block
                    exists = False
                    if task.trace_id is not None:
                        exists = True
                    elif task.mc_seqno is not None:
                        sql = f'select mc_seqno from blocks_classified where mc_seqno = {task.mc_seqno}'
                        result = await session.execute(sql)
                        rows = result.fetchall()
                        exists = len(rows) > 0
                    # cleanup previous traces
                    if exists:
                        if task.trace_id is not None:
                            trace_ids.append(task.trace_id)
                        elif task.mc_seqno is not None:
                            stmt = select(Trace.trace_id).filter(Trace.mc_seqno_end == task.mc_seqno)
                            result = await session.execute(stmt)
                            trace_ids.extend([x[0] for x in result.fetchall()])

                            await session.execute(f'delete from blocks_classified where mc_seqno = {task.mc_seqno}')
                if len(trace_ids) > 0:
                    stmt = delete(Action).where(Action.trace_id.in_(trace_ids))
                    # logger.info(f'stmt: {stmt}')
                    await session.execute(stmt)
                    stmt = delete(ActionAccount).where(ActionAccount.trace_id.in_(trace_ids))
                    # logger.info(f'stmt: {stmt}')
                    await session.execute(stmt)
                
                # read traces
                fltr = None
                if is_trace_batch:
                    fltr = Trace.trace_id.in_(trace_ids)
                else:
                    fltr = and_(Trace.mc_seqno_end.in_(mc_seqnos),
                                Trace.nodes_ <= self.big_traces_threshold)
                query = select(Trace).filter(fltr)
                tx_join = selectinload(Trace.transactions).selectinload(Transaction.messages).selectinload(Message.message_content)
                query = query.options(tx_join)
                
                result = await session.execute(query)
                traces = result.scalars().unique().all()
                processed = len(traces)

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
                for trace_id, state, actions, exc in results:
                    if state == 'ok' or state == 'broken':
                        # # logger.error(f"query: {insert(Action).values(actions).on_conflict_do_nothing()}")
                        # if len(actions) > 0:
                        #     # for action in actions:
                        #     #     logger.warning(f"action: {action.__dict__}")
                        #     for action in actions:
                        #         await session.execute(insert(Action).values({k: v for k, v in action.__dict__.items() if not k.startswith('_')}).on_conflict_do_nothing())
                        # session.add_all(actions)
                        # for action in actions:
                        #     for aa in action.get_action_accounts():
                        #         await session.execute(insert(ActionAccount).values({k: v for k, v in aa.__dict__.items() if not k.startswith('_')}).on_conflict_do_nothing()) 
                        #     # session.add_all(action.get_action_accounts())
                        session.add_all(actions)
                        for action in actions:
                            session.add_all(action.get_action_accounts())

                        if state == 'ok':
                            ok_traces.append(trace_id)
                        else:
                            sql = text(f"""insert into _classifier_failed_traces(trace_id, broken) 
                            values (:tid, true) on conflict do nothing;""")
                            await session.execute(sql.bindparams(tid=trace_id))
                            broken_traces.append(trace_id)
                    else:
                        sql = text(f"""insert into _classifier_failed_traces(trace_id, broken, error) 
                        values (:tid, false, :err) on conflict do nothing;""")
                        await session.execute(sql.bindparams(tid=trace_id, err=f'{exc}'))
                        failed_traces.append(trace_id)
                failed = len(failed_traces)
                broken = len(broken_traces)
                # finish task
                # await session.execute(f"delete from _classifier_tasks where id = {task.id};")
                for task in tasks:
                    if not is_trace_batch:
                        await session.execute(f"insert into blocks_classified(mc_seqno) values ({task.mc_seqno});")
                task_ids = [task.id for task in tasks]
                await session.execute(f"delete from _classifier_tasks where id in ({','.join(map(str, task_ids))});")

                await session.commit()
            except Exception as ee:
                logger.error(f'Failed to process batch: {ee}')
                await session.rollback()
                return False, 0, 0, 0
        return ok, processed, failed, broken

        
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
    total_traces = args.expected_total
    if total_traces == 0:
        with SyncSessionMaker() as session:
            query = session.query(Trace.trace_id) \
                    .filter(Trace.state == 'complete') \
                    .filter(Trace.classification_state == 'unclassified')
            total_traces = query.count()
        logger.info(f"Total unclassified traces from database: {total_traces}")
    else:
        logger.info(f"Total unclassified traces number is given: {total_traces}")

    task_queue = mp.Queue(args.prefetch_size)
    result_queue = mp.Queue()
    stats_queue = mp.Queue()
    thread = UnclassifiedEventsReader(task_queue, result_queue, stats_queue, args.batch_size, args.prefetch_size)
    thread.start()
    workers = []
    for id in range(args.pool_size):
        worker = EventClassifierWorker(id, task_queue, result_queue, big_traces_threshold=4000)
        worker.start()
        workers.append(worker)
    
    # stats
    failed_tasks = 0
    processed_tasks = 0
    failed_traces = 0
    broken_traces = 0
    processed_traces = 0
    start_time = time.time()
    last_time = start_time
    try:
        while True:
            try:
                ok, total, failed, broken = result_queue.get(False)
                processed_traces += total
                failed_traces += failed
                broken_traces += broken

                failed_tasks += not ok
                processed_tasks += 1
            except:
                await asyncio.sleep(0.5)
            cur_time = time.time()
            if (cur_time - last_time) > 2:
                elapsed = cur_time - start_time
                tps = processed_traces / elapsed
                eta_sec = min(999999999, max(0, int((total_traces - processed_traces) / max(tps, 1e-9))))
                eta = timedelta(seconds=eta_sec)
                logger.info(f"{processed_traces} traces / {elapsed:02f} sec, traces/sec: {tps:02f} (eta: {eta}), Q: {task_queue.qsize()}, "
                            f"failed: {failed_traces}, broken: {broken_traces}, failed tasks: {failed_tasks} / {processed_tasks}")
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


async def process_trace(trace: Trace) -> tuple[str, str, list[Action], Exception]:
    if len(trace.transactions) == 1 and trace.transactions[0].descr == 'tick_tock':
        return trace.trace_id, 'ok', [], None
    try:
        result = await process_event_async_with_postprocessing(trace)
        actions = []
        state = 'ok'
        for block in result:
            if block.btype != 'root':
                if block.btype == 'call_contract' and block.event_nodes[0].message.destination is None:
                    continue
                if block.btype == 'empty':
                    continue
                if block.btype == 'call_contract' and block.event_nodes[0].message.source is None:
                    continue
                if block.broken:
                    state = 'broken'
                action = block_to_action(block, trace.trace_id, trace)
                actions.append(action)
        return trace.trace_id, state, actions, None
    except Exception as e:
        logger.error("Marking trace as failed " + trace.trace_id + " - " + str(e))
        return trace.trace_id, 'failed', [], e


if __name__ == '__main__':
    init_pools_data()
    parser = argparse.ArgumentParser()
    parser.add_argument('--prefetch-size',
                        help='Number of prefetched tasks',
                        type=int,
                        default=100000)
    parser.add_argument('--batch-size',
                        help='Number of trace-tasks to process in one batch',
                        type=int,
                        default=1000)
    parser.add_argument('--pool-size',
                        help='Number of workers to process traces',
                        type=int,
                        default=4)
    parser.add_argument('--expected-total',
                        help='Expected number of tasks',
                        type=int,
                        default=4)
    args = parser.parse_args()
    if settings.emulated_traces:
        logger.info("Starting processing emulated traces")
        asyncio.run(start_emulated_traces_processing())
    else:
        logger.info("Starting processing events from db")
        asyncio.run(start_processing_events_from_db(args))
