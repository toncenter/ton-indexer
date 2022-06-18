import time
import sys
from celery.signals import worker_ready
from indexer.celery import app
from indexer.tasks import get_block, get_last_mc_block
from indexer.database import init_database, get_session
from indexer.crud import get_existing_seqnos_between_interval
from config import settings
from loguru import logger

def wait_for_broker_connection():
    while True:
        try:
            app.broker_connection().ensure_connection(max_retries=3)
        except Exception:
            logger.warning(f"Can't connect to celery broker. Trying again...")
            time.sleep(3)
            continue
        logger.info(f"Connected to celery broker.")
        break

def dispatch_seqno_list(seqnos_to_process, queue):
    parallel = 2 * settings.indexer.workers_count
    left_index = 0
    tasks_in_progress = []
    while left_index < len(seqnos_to_process):
        finished_tasks = [task for task in tasks_in_progress if task.ready()]
        for finished_task in finished_tasks:
            finished_task.forget()
        tasks_in_progress = [task for task in tasks_in_progress if task not in finished_tasks]
        if len(tasks_in_progress) >= parallel:
            time.sleep(0.05)
            continue

        right_index = min(left_index + settings.indexer.blocks_per_task, len(seqnos_to_process))
        next_chunk = seqnos_to_process[left_index:right_index]

        logger.info(f"Dispatching chunk: [{left_index} (seqno: {seqnos_to_process[left_index]}), {right_index-1} (seqno: {seqnos_to_process[right_index-1]})]")
        tasks_in_progress.append(get_block.apply_async([next_chunk], serializer='pickle', queue=queue))
        
        left_index = right_index

    # wait for tasks_in_progress to complete 
    while len(tasks_in_progress) > 0:
        finished_tasks = [task for task in tasks_in_progress if task.ready()]
        for finished_task in finished_tasks:
            finished_task.forget()
        tasks_in_progress = [task for task in tasks_in_progress if task not in finished_tasks]

def forward_main(queue):
    init_database(create=True)

    wait_for_broker_connection()

    logger.info(f"Forward scheduler started from {settings.indexer.init_mc_seqno}.")

    is_first_iteration = True

    current_seqno = settings.indexer.init_mc_seqno + 1
    while True:
        last_mc_block = get_last_mc_block.apply_async([], serializer='pickle', queue=queue).get()
        if last_mc_block['seqno'] < current_seqno:
            time.sleep(0.2)
            continue

        seqnos_to_process = range(current_seqno, last_mc_block['seqno'] + 1)
        if is_first_iteration:
            session = get_session()()
            with session.begin():
                seqnos_already_in_db = get_existing_seqnos_between_interval(session, current_seqno, last_mc_block['seqno'] + 1)
            logger.info(f"{len(seqnos_already_in_db)} seqnos already exist in DB")
            seqnos_to_process = [seqno for seqno in seqnos_to_process if (seqno,) not in seqnos_already_in_db]
            is_first_iteration = False
    

        dispatch_seqno_list(seqnos_to_process, queue)

        current_seqno = last_mc_block['seqno'] + 1

        logger.info(f"Current seqno: {current_seqno}")

def backward_main(queue):
    init_database(create=False)

    wait_for_broker_connection()

    logger.info(f"Backward scheduler started. From {settings.indexer.init_mc_seqno} to {settings.indexer.smallest_mc_seqno}.")

    session = get_session()()
    with session.begin():
        seqnos_already_in_db = get_existing_seqnos_between_interval(session, settings.indexer.smallest_mc_seqno, settings.indexer.init_mc_seqno)
    seqnos_to_process = range(settings.indexer.init_mc_seqno, settings.indexer.smallest_mc_seqno - 1, -1)
    seqnos_to_process = [seqno for seqno in seqnos_to_process if (seqno,) not in seqnos_already_in_db]
    logger.info(f"{len(seqnos_already_in_db)} seqnos already exist in DB")
    del seqnos_already_in_db

    dispatch_seqno_list(seqnos_to_process, queue)

    session = get_session()()
    with session.begin():
        seqnos_already_in_db = get_existing_seqnos_between_interval(session, settings.indexer.smallest_mc_seqno, settings.indexer.init_mc_seqno)
    seqnos_failed_to_process = [seqno for seqno in seqnos_to_process if (seqno,) not in seqnos_already_in_db]
    logger.info(f"Backward scheduler completed. Failed to process {len(seqnos_failed_to_process)} seqnos:")
    for failed_seqno in seqnos_failed_to_process:
        logger.info(f"\t{failed_seqno}")

import asyncio
from asgiref.sync import sync_to_async


async def asyncify(task, *args, **kwargs):
    delay = 0.1
    async_result = await sync_to_async(task.apply_async)(*args, **kwargs)
    while not async_result.ready():
        await asyncio.sleep(delay)
        delay = min(delay * 1.5, 2)  # exponential backoff, max 2 seconds
    return async_result.get()

class IndexScheduler:
    def __init__(self, celery_queue):
        self.celery_queue = celery_queue
        # self.seqnos_to_process_queue = LifoQueue()
        self.current_seqno = 9

    def run(self):
        self.loop = asyncio.get_event_loop()
        self.get_new_blocks_task = self.loop.create_task(self._get_new_blocks())
        self.index_blocks_task = self.loop.create_task(self._index_blocks())
        self.read_results_task = self.loop.create_task(self._read_results())
        self.loop.run_forever()

    async def _get_new_blocks(self):
        while True:
            last_mc_block = await asyncify(get_last_mc_block, [], serializer='pickle', queue=self.celery_queue)
            if last_mc_block['seqno'] < current_seqno:
                await asyncio.sleep(0.2)
                continue
            current_seqno = last_mc_block['seqno'] + 1
            logger.info(f"current_seqno {current_seqno}")

            # seqnos_to_process = range(current_seqno, last_mc_block['seqno'] + 1)



    async def _index_blocks(self):
        pass

    async def _read_results(self):
        pass




if __name__ == "__main__":
    if sys.argv[1] == 'backward':
        backward_main(sys.argv[2])
    elif sys.argv[1] == 'forward':
        scheduler = IndexScheduler(sys.argv[2])
        scheduler.run()
    else:
        raise Exception("Pass direction in argument: backward/forward")



