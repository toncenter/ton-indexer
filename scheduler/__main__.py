import asyncio
from asgiref.sync import sync_to_async
from collections import deque
import time
import sys
import traceback
from indexer.celery import app
from indexer.tasks import get_last_mc_block, fetch_blocks, insert_blocks, index_interfaces
from indexer.database import init_database, SessionMaker
from indexer.crud import get_existing_seqnos_between_interval
from pytonlib import TonlibError, LiteServerTimeout, BlockDeleted
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

async def asyncify(celery_task, *args, **kwargs):
    delay = 0.1
    celery_result = await sync_to_async(celery_task.apply_async)(*args, **kwargs)
    while not celery_result.ready():
        await asyncio.sleep(delay)
        delay = min(delay * 1.5, 1)  # exponential backoff, max 1 second
    return celery_result

class IndexScheduler:
    def __init__(self, celery_queue):
        self.celery_queue = celery_queue
        self.seqnos_to_process_queue = deque()
        self.blocks_to_insert_queue = deque()
        self.blocks_to_index_interfaces_queue = deque()
        self.max_parallel_tasks_semaphore = None
        self.fetch_blocks_running_tasks = set()
        self.insert_blocks_running_tasks = set()
        self.index_interfaces_running_tasks = set()
        self.is_liteserver_up = None
        self.reschedule_failed_blocks = None

    def run(self):
        raise RuntimeError('abstract method')

    async def _not_indexed_seqnos_between(self, low_seqno, high_seqno):
        async with SessionMaker() as session:
            seqnos_already_in_db = await get_existing_seqnos_between_interval(session, low_seqno, high_seqno)
        logger.info(f"{len(seqnos_already_in_db)} seqnos already exist in DB")
        return [seqno for seqno in range(low_seqno, high_seqno + 1) if (seqno,) not in seqnos_already_in_db]

    async def _fetch_blocks(self, return_on_empty):
        while True:
            try:
                if not self.is_liteserver_up:
                    await asyncio.sleep(1)
                    continue

                chunk = []
                while len(chunk) < settings.indexer.blocks_per_task:
                    try:
                        seqno_to_fetch = self.seqnos_to_process_queue.popleft()
                    except IndexError:
                        if return_on_empty and len(chunk) == 0:
                            return
                        break
                    chunk.append(seqno_to_fetch)
                if len(chunk) > 0:
                    await self.max_parallel_tasks_semaphore.acquire()
                    task = asyncify(fetch_blocks, [chunk], serializer='pickle', queue=self.celery_queue)
                    self.fetch_blocks_running_tasks.add(self.loop.create_task(task))
                else:
                    await asyncio.sleep(0.1)
                    continue
            except asyncio.CancelledError:
                logger.warning("Task _fetch_blocks was cancelled")
                return
            except BaseException as e:
                logger.error("Task _fetch_blocks raised exception: {exception}", exception=e)

    async def _insert_blocks(self):
        while True:
            try:
                chunk = []
                while len(chunk) < settings.indexer.blocks_per_task:
                    try:
                        block_to_insert = self.blocks_to_insert_queue.popleft()
                    except IndexError:
                        break
                    chunk.append(block_to_insert)
                if len(chunk) > 0:
                    task = asyncify(insert_blocks, [chunk], serializer='pickle', queue=self.celery_queue)
                    self.insert_blocks_running_tasks.add(self.loop.create_task(task))
                else:
                    await asyncio.sleep(0.1)
                    continue
            except asyncio.CancelledError:
                logger.warning("Task _insert_blocks was cancelled")
                return
            except BaseException as e:
                logger.error("Task _insert_blocks raised exception: {exception}", exception=e)

    def handle_fetch_blocks_result(self, seqno, seqno_result):
        if isinstance(seqno_result, tuple):
            logger.debug("Masterchain block {seqno} was fetched.", seqno=seqno)
            self.blocks_to_insert_queue.append((seqno, seqno_result))
            self.blocks_to_index_interfaces_queue.append((seqno, seqno_result))
        elif isinstance(seqno_result, LiteServerTimeout):
            logger.critical("Masterchain block {seqno} failed to fetch because Lite Server is not responding: {exception}.", 
                seqno=seqno, exception=seqno_result)
            logger.critical("Block {seqno} is rescheduled", seqno=seqno)
            self.seqnos_to_process_queue.appendleft(seqno)
        elif isinstance(seqno_result, BlockDeleted):
            logger.critical("Masterchain block {seqno} failed to fetch because Lite Server already deleted this block", seqno=seqno)
            logger.critical("Block {seqno} is skipped", seqno=seqno)
        elif isinstance(seqno_result, BaseException):
            logger.critical("Masterchain block {seqno} failed to fetch. Exception of type {exc_type} occured: {exception}", 
                seqno=seqno, exc_type=type(seqno_result).__name__, exception=seqno_result)
            if self.reschedule_failed_blocks:
                logger.critical("Block {seqno} is rescheduled", seqno=seqno)
                self.seqnos_to_process_queue.appendleft(seqno)
            else:
                logger.critical("Block {seqno} is skipped", seqno=seqno)
        else:
            raise RuntimeError(f"Unexpected fetch_blocks result type for block {seqno}: {seqno_result}")

    async def _fetch_blocks_read_results(self):
        while True:
            try:
                done_tasks = set(filter(lambda x: x.done(), self.fetch_blocks_running_tasks))
                self.fetch_blocks_running_tasks = set(filter(lambda x: x not in done_tasks, self.fetch_blocks_running_tasks))
                for task in done_tasks:
                    self.max_parallel_tasks_semaphore.release()
                    async_result = task.result()
                    try:
                        result = async_result.get()
                    except BaseException as e:
                        logger.error("Task {async_result} raised unknown exception: {exception}. Rescheduling the task's chunk.", async_result=async_result, exception=e)
                        self.seqnos_to_process_queue.extendleft(async_result.args[0])
                    else:
                        for (seqno, seqno_result) in result:
                            self.handle_fetch_blocks_result(seqno, seqno_result)

                await asyncio.sleep(0.3)
            except asyncio.CancelledError:
                logger.warning("Task _fetch_blocks_read_results was cancelled")
                return
            except BaseException as e:
                logger.error(f"Task _fetch_blocks_read_results raised exception: {e}. {traceback.format_exc()}")

    def handle_insert_blocks_result(self, seqno, seqno_result, arg):
        if seqno_result is None:
            logger.debug("Masterchain block {seqno} was inserted to DB.", seqno=seqno)
        elif isinstance(seqno_result, BaseException):
            logger.critical("Masterchain block {seqno} failed to insert to DB. Exception of type {exc_type} occured: {exception}", 
                seqno=seqno, exc_type=type(seqno_result).__name__, exception=seqno_result)
            logger.critical("Block {seqno} is rescheduled", seqno=seqno)
            self.blocks_to_insert_queue.appendleft(arg)
        else:
            raise RuntimeError(f"Unexpected insert_blocks result type for block {seqno}: {seqno_result}")

    async def _insert_blocks_read_results(self):
        while True:
            try:
                done_tasks = set(filter(lambda x: x.done(), self.insert_blocks_running_tasks))
                self.insert_blocks_running_tasks = set(filter(lambda x: x not in done_tasks, self.insert_blocks_running_tasks))
                for task in done_tasks:
                    async_result = task.result()
                    param = async_result.args[0]
                    try:
                        result = async_result.get()
                    except BaseException as e:
                        logger.error("Task {async_result} raised unknown exception: {exception}. Rescheduling the task's chunk.", async_result=async_result, exception=e)
                        self.blocks_to_insert_queue.extendleft(param)
                    else:
                        for ((seqno, seqno_result), arg) in zip(result, param):
                            self.handle_insert_blocks_result(seqno, seqno_result, arg)

                await asyncio.sleep(0.3)
            except asyncio.CancelledError:
                logger.warning("Task _insert_blocks_read_results was cancelled")
                return
            except BaseException as e:
                logger.error(f"Task _insert_blocks_read_results raised exception: {e}. {traceback.format_exc()}")

    async def _index_interfaces(self):
        while True:
            try:
                chunk = []
                while len(chunk) < settings.indexer.blocks_per_task:
                    try:
                        blocks_to_index_interfaces = self.blocks_to_index_interfaces_queue.popleft()
                    except IndexError:
                        break
                    chunk.append(blocks_to_index_interfaces)
                if len(chunk) > 0:
                    task = asyncify(index_interfaces, [chunk], serializer='pickle', queue=self.celery_queue)
                    self.index_interfaces_running_tasks.add(self.loop.create_task(task))
                else:
                    await asyncio.sleep(0.1)
                    continue
            except asyncio.CancelledError:
                logger.warning("Task _index_accounts_interfaces was cancelled")
                return
            except BaseException as e:
                logger.error("Task _index_accounts_interfaces raised exception: {exception}", exception=e)
    
    def handle_index_interfaces_result(self, seqno, seqno_result, arg):
        if seqno_result is None:
            logger.debug("Masterchain block {seqno} interfaces were indexed.", seqno=seqno)
        elif isinstance(seqno_result, BaseException):
            logger.critical("Masterchain block {seqno} failed to index interfaces. Exception of type {exc_type} occured: {exception}", 
                seqno=seqno, exc_type=type(seqno_result).__name__, exception=seqno_result)
            logger.critical("Block {seqno} indexing interfaces is rescheduled", seqno=seqno)
            self.blocks_to_index_interfaces_queue.appendleft(arg)
        else:
            raise RuntimeError(f"Unexpected insert_blocks result type for block {seqno}: {seqno_result}")

    async def _index_interfaces_read_results(self):
        while True:
            try:
                done_tasks = set(filter(lambda x: x.done(), self.index_interfaces_running_tasks))
                self.index_interfaces_running_tasks = set(filter(lambda x: x not in done_tasks, self.index_interfaces_running_tasks))
                for task in done_tasks:
                    async_result = task.result()
                    param = async_result.args[0]
                    try:
                        result = async_result.get()
                    except BaseException as e:
                        logger.error("Task {async_result} raised unknown exception: {exception}. Rescheduling the task's chunk.", async_result=async_result, exception=e)
                        self.blocks_to_index_interfaces_queue.extendleft(param)
                    else:
                        for ((seqno, seqno_result), arg) in zip(result, param):
                            self.handle_index_interfaces_result(seqno, seqno_result, arg)

                await asyncio.sleep(0.3)
            except asyncio.CancelledError:
                logger.warning("Task _index_interfaces_read_results was cancelled")
                return
            except BaseException as e:
                logger.error(f"Task _index_interfaces_read_results raised exception: {e}. {traceback.format_exc()}")

    
    async def _check_liteserver_health(self):
        while True:
            try:
                try:
                    last_mc_block_async_result = await asyncify(get_last_mc_block, [], serializer='pickle', queue=self.celery_queue)
                    last_mc_block_async_result.get()
                except LiteServerTimeout:
                    if self.is_liteserver_up or self.is_liteserver_up is None:
                        logger.critical(f"Lite Server is not responding. Pausing indexing until it's not alive.")
                        self.is_liteserver_up = False
                else:
                    if not self.is_liteserver_up:
                        logger.info(f"Lite Server is alive. Starting the indexing.")
                        self.is_liteserver_up = True
                await asyncio.sleep(2)
            except BaseException as e:
                logger.error(f"Task _check_liteserver_health raised exception: {e}. {traceback.format_exc()}")

class BackwardScheduler(IndexScheduler):
    def __init__(self, celery_queue):
        super(BackwardScheduler, self).__init__(celery_queue)
        self.reschedule_failed_blocks = False
    
    def run(self):
        self.loop = asyncio.get_event_loop()
        self.max_parallel_tasks_semaphore = asyncio.Semaphore(4 * settings.indexer.workers_count)
        self.loop.run_until_complete(self.schedule_seqnos())
        self.check_liteserver_health_task = self.loop.create_task(self._check_liteserver_health())
        self.fetch_blocks_task = self.loop.create_task(self._fetch_blocks(return_on_empty=True))
        self.fetch_blocks_read_results_task = self.loop.create_task(self._fetch_blocks_read_results())
        self.insert_blocks_task = self.loop.create_task(self._insert_blocks())
        self.insert_blocks_read_results_task = self.loop.create_task(self._insert_blocks_read_results())
        self.index_interfaces_task = self.loop.create_task(self._index_interfaces())
        self.index_interfaces_read_results_task = self.loop.create_task(self._index_interfaces_read_results())
        self.loop.run_until_complete(self._wait_finish())

    async def schedule_seqnos(self):
        logger.info(f"Backward scheduler started. From {settings.indexer.init_mc_seqno} to {settings.indexer.smallest_mc_seqno}.")

        seqnos_to_index = await self._not_indexed_seqnos_between(settings.indexer.smallest_mc_seqno, settings.indexer.init_mc_seqno)
        seqnos_to_index.reverse()

        self.seqnos_to_process_queue.extend(seqnos_to_index)

    async def _wait_finish(self):
        done, pending = await asyncio.wait([
            self.check_liteserver_health_task, 
            self.fetch_blocks_task, self.fetch_blocks_read_results_task, 
            self.insert_blocks_task, self.insert_blocks_read_results_task,
            self.index_interfaces_task, self.index_interfaces_read_results_task], return_when=asyncio.FIRST_COMPLETED)
        done_task = done.pop()
        if done_task is self.fetch_blocks_task:
            while len(self.fetch_blocks_running_tasks) > 0 or len(self.insert_blocks_running_tasks) > 0 or len(self.index_interfaces_running_tasks) > 0:
                await asyncio.sleep(0.5)
            self.fetch_blocks_read_results_task.cancel()
            await self.fetch_blocks_read_results_task
            self.insert_blocks_read_results_task.cancel()
            await self.insert_blocks_read_results_task
            logger.info('Backward scheduler finished working')
            not_indexed = await self._not_indexed_seqnos_between(settings.indexer.smallest_mc_seqno, settings.indexer.init_mc_seqno)
            if len(not_indexed) > 0:
                logger.info('Failed to index following seqnos: {seqnos}', seqnos=not_indexed)
            else:
                logger.info('All seqnos were successfully indexed')
        else:
            logger.critical(f"Task {done_task} unexpectedly stopped. Aborting the execution.")
            sys.exit(-1)

class ForwardScheduler(IndexScheduler):
    def __init__(self, celery_queue):
        super(ForwardScheduler, self).__init__(celery_queue)
        self.reschedule_failed_blocks = True
        self.current_seqno = settings.indexer.init_mc_seqno + 1
    
    def run(self):
        self.loop = asyncio.get_event_loop()
        self.max_parallel_tasks_semaphore = asyncio.Semaphore(4 * settings.indexer.workers_count)
        self.check_liteserver_health_task = self.loop.create_task(self._check_liteserver_health())
        self.get_new_blocks_task = self.loop.create_task(self._get_new_blocks())
        self.fetch_blocks_task = self.loop.create_task(self._fetch_blocks(return_on_empty=False))
        self.fetch_blocks_read_results_task = self.loop.create_task(self._fetch_blocks_read_results())
        self.insert_blocks_task = self.loop.create_task(self._insert_blocks())
        self.insert_blocks_read_results_task = self.loop.create_task(self._insert_blocks_read_results())
        self.index_interfaces_task = self.loop.create_task(self._index_interfaces())
        self.index_interfaces_read_results_task = self.loop.create_task(self._index_interfaces_read_results())
        orchestra = [
            self.check_liteserver_health_task, self.get_new_blocks_task, 
            self.fetch_blocks_task, self.fetch_blocks_read_results_task, 
            self.insert_blocks_task, self.insert_blocks_read_results_task, 
            self.index_interfaces_task, self.index_interfaces_read_results_task
        ]
        done, pending = self.loop.run_until_complete(asyncio.wait(orchestra, return_when=asyncio.FIRST_COMPLETED))
        logger.critical(f"Task {done.pop()} unexpectedly stopped. Aborting the execution.")
        sys.exit(-1)

    async def _get_new_blocks(self):
        is_first_iteration = True
        while True:
            try:
                if not self.is_liteserver_up:
                    await asyncio.sleep(5)
                    continue

                last_mc_block_async_result = await asyncify(get_last_mc_block, [], serializer='pickle', queue=self.celery_queue)
                last_mc_block = last_mc_block_async_result.get()
                if last_mc_block['seqno'] < self.current_seqno:
                    await asyncio.sleep(0.2)
                    continue
                logger.info("New masterchain block: {seqno}", seqno=last_mc_block['seqno'])

                if is_first_iteration:
                    new_seqnos = await self._not_indexed_seqnos_between(self.current_seqno, last_mc_block['seqno'])
                    is_first_iteration = False
                else:
                    new_seqnos = range(self.current_seqno, last_mc_block['seqno'] + 1)

                self.seqnos_to_process_queue.extend(new_seqnos)
                
                self.current_seqno = last_mc_block['seqno'] + 1
            except asyncio.CancelledError:
                logger.warning("Task _get_new_blocks was cancelled")
                return
            except BaseException as e:
                logger.error("Task _get_new_blocks raised exception: {exception}", exception=e)

if __name__ == "__main__":
    if sys.argv[1] == 'backward':
        init_database(create=False)
        wait_for_broker_connection()
        scheduler = BackwardScheduler(sys.argv[2])
        scheduler.run()
    elif sys.argv[1] == 'forward':
        init_database(create=True)
        wait_for_broker_connection()
        scheduler = ForwardScheduler(sys.argv[2])
        scheduler.run()
    else:
        raise Exception("Pass direction in argument: backward/forward")
