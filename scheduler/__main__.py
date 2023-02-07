import asyncio
from asgiref.sync import sync_to_async
from collections import deque
import time
import sys
import traceback
from indexer.celery import app
from indexer.tasks import get_block, get_last_mc_block, get_account
from indexer.database import init_database, SessionMaker
from indexer.crud import get_existing_seqnos_between_interval, get_known_accounts_not_indexed, get_known_accounts_long_since_check
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

class BaseScheduler:
    def __init__(self, celery_queue):
        self.celery_queue = celery_queue
        self.is_liteserver_up = None
        self.max_parallel_tasks_semaphore = None

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
    def _init_loop(self):
        self.loop = asyncio.get_event_loop()
        self.check_liteserver_health_task = self.loop.create_task(self._check_liteserver_health())
        self.max_parallel_tasks_semaphore = asyncio.Semaphore(4 * settings.indexer.workers_count)

    def run(self):
        raise RuntimeError('abstract method')

class BlockIndexScheduler(BaseScheduler):
    def __init__(self, celery_queue):
        super(BlockIndexScheduler, self).__init__(celery_queue)
        self.seqnos_to_process_queue = deque()
        self.running_tasks = set()
        self.reschedule_failed_blocks = None

    async def _not_indexed_seqnos_between(self, low_seqno, high_seqno):
        async with SessionMaker() as session:
            seqnos_already_in_db = await get_existing_seqnos_between_interval(session, low_seqno, high_seqno)
        logger.info(f"{len(seqnos_already_in_db)} seqnos already exist in DB")
        return [seqno for seqno in range(low_seqno, high_seqno + 1) if (seqno,) not in seqnos_already_in_db]

    async def _index_blocks(self, return_on_empty):
        while True:
            try:
                if not self.is_liteserver_up:
                    await asyncio.sleep(1)
                    continue

                chunk = []
                while len(chunk) < settings.indexer.blocks_per_task:
                    try:
                        seqno_to_process = self.seqnos_to_process_queue.popleft()
                    except IndexError:
                        if return_on_empty and len(chunk) == 0:
                            return
                        break
                    chunk.append(seqno_to_process)
                if len(chunk) == 0:
                    await asyncio.sleep(0.1)
                    continue

                await self.max_parallel_tasks_semaphore.acquire()
                task = asyncify(get_block, [chunk], serializer='pickle', queue=self.celery_queue)
                self.running_tasks.add(self.loop.create_task(task))
            except asyncio.CancelledError:
                logger.warning("Task _index_blocks was cancelled")
                return
            except BaseException as e:
                logger.error(f"Task _index_blocks raised exception: {exception}", exception=e)

    def handle_get_block_result(self, seqno, seqno_result):
        if seqno_result is None:
            logger.debug("Masterchain block {seqno} was indexed.", seqno=seqno)
        elif isinstance(seqno_result, LiteServerTimeout):
            logger.critical("Masterchain block {seqno} was not indexed because Lite Server is not responding: {exception}.", 
                seqno=seqno, exception=seqno_result)
            logger.critical("Block {seqno} is rescheduled", seqno=seqno)
            self.seqnos_to_process_queue.appendleft(seqno)
        elif isinstance(seqno_result, BlockDeleted):
            logger.critical("Masterchain block {seqno} was not indexed because Lite Server already deleted this block", seqno=seqno)
            logger.critical("Block {seqno} is skipped", seqno=seqno)
        elif isinstance(seqno_result, BaseException):
            logger.critical("Masterchain block {seqno} was not indexed. Exception of type {exc_type} occured: {exception}", 
                seqno=seqno, exc_type=type(seqno_result).__name__, exception=seqno_result)
            if self.reschedule_failed_blocks:
                logger.critical("Block {seqno} is rescheduled", seqno=seqno)
                self.seqnos_to_process_queue.appendleft(seqno)
            else:
                logger.critical("Block {seqno} is skipped", seqno=seqno)
        else:
            raise RuntimeError(f"Unexpected get_block result type for block {seqno}: {seqno_result}")

    async def _read_results(self):
        while True:
            try:
                done_tasks = set(filter(lambda x: x.done(), self.running_tasks))
                self.running_tasks = set(filter(lambda x: x not in done_tasks, self.running_tasks))
                for task in done_tasks:
                    self.max_parallel_tasks_semaphore.release()
                    async_result = task.result()
                    try:
                        result = async_result.get()
                    except BaseException as e:
                        logger.error(f"Task {async_result} raised unknown exception: {e}. Rescheduling the task's chunk.", async_result=async_result, exception=e)
                        self.seqnos_to_process_queue.extendleft(async_result.args[0])
                    else:
                        for (seqno, seqno_result) in result:
                            self.handle_get_block_result(seqno, seqno_result)

                await asyncio.sleep(0.3)
            except asyncio.CancelledError:
                logger.warning("Task _read_results was cancelled")
                return
            except BaseException as e:
                logger.error(f"Task _read_results raised exception: {e}. {traceback.format_exc()}")

class BackwardScheduler(BlockIndexScheduler):
    def __init__(self, celery_queue):
        super(BackwardScheduler, self).__init__(celery_queue)
        self.reschedule_failed_blocks = False
    
    def run(self, create_db=False):
        self._init_loop()
        self.loop.run_until_complete(self.schedule_seqnos())
        self.index_blocks_task = self.loop.create_task(self._index_blocks(return_on_empty=True))
        self.read_results_task = self.loop.create_task(self._read_results())
        self.loop.run_until_complete(self._wait_finish())

    async def schedule_seqnos(self):
        logger.info(f"Backward scheduler started. From {settings.indexer.init_mc_seqno} to {settings.indexer.smallest_mc_seqno}.")

        seqnos_to_index = await self._not_indexed_seqnos_between(settings.indexer.smallest_mc_seqno, settings.indexer.init_mc_seqno)
        seqnos_to_index.reverse()

        self.seqnos_to_process_queue.extend(seqnos_to_index)

    async def _wait_finish(self):
        done, pending = await asyncio.wait([self.check_liteserver_health_task, self.index_blocks_task, self.read_results_task], return_when=asyncio.FIRST_COMPLETED)
        done_task = done.pop()
        if done_task is self.index_blocks_task:
            while len(self.running_tasks) > 0:
                await asyncio.sleep(0.5)
            self.read_results_task.cancel()
            await self.read_results_task
            logger.info('Backward scheduler finished working')
            not_indexed = await self._not_indexed_seqnos_between(settings.indexer.smallest_mc_seqno, settings.indexer.init_mc_seqno)
            if len(not_indexed) > 0:
                logger.info(f'Failed to index following seqnos: {seqnos}', seqnos=not_indexed)
            else:
                logger.info('All seqnos were successfully indexed')
        else:
            logger.critical(f"Task {done_task} unexpectedly stopped. Aborting the execution.")
            sys.exit(-1)

class ForwardScheduler(BlockIndexScheduler):
    def __init__(self, celery_queue):
        super(ForwardScheduler, self).__init__(celery_queue)
        self.reschedule_failed_blocks = True
        self.current_seqno = settings.indexer.init_mc_seqno + 1
    
    def run(self, create_db=False):
        self._init_loop()
        self.loop.run_until_complete(init_database(create_db))
        self.get_new_blocks_task = self.loop.create_task(self._get_new_blocks())
        self.index_blocks_task = self.loop.create_task(self._index_blocks(return_on_empty=False))
        self.read_results_task = self.loop.create_task(self._read_results())
        done, pending = self.loop.run_until_complete(asyncio.wait([self.check_liteserver_health_task, self.get_new_blocks_task, self.index_blocks_task, self.read_results_task], return_when=asyncio.FIRST_COMPLETED))
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
                    new_seqnos = await self._not_indexed_seqnos_between(self.current_seqno, last_mc_block['seqno'] + 1)
                    is_first_iteration = False
                else:
                    new_seqnos = range(self.current_seqno, last_mc_block['seqno'] + 1)

                self.seqnos_to_process_queue.extend(new_seqnos)
                
                self.current_seqno = last_mc_block['seqno'] + 1
            except asyncio.CancelledError:
                logger.warning("Task _get_new_blocks was cancelled")
                return
            except BaseException as e:
                logger.error(f"Task _get_new_blocks raised exception: {exception}", exception=e)

class AccountsIndexer(BaseScheduler):
    def __init__(self, celery_queue):
        super(AccountsIndexer, self).__init__(celery_queue)
        self.accounts_to_process_queue = deque()
        self.running_tasks = []
        self.processing_now = set()

    def run(self, create_db=False):
        self._init_loop()
        self.loop.run_until_complete(init_database(create_db))
        self.push_to_queue_task = self.loop.create_task(self._push_to_queue())
        self.index_accounts_task = self.loop.create_task(self._index_accounts())
        self.read_results_task = self.loop.create_task(self._read_results())
        done, pending = self.loop.run_until_complete(asyncio.wait([self.check_liteserver_health_task, self.push_to_queue_task, self.index_accounts_task, self.read_results_task], return_when=asyncio.FIRST_COMPLETED))
        logger.critical(f"Task {done.pop()} unexpectedly stopped. Aborting the execution.")
        sys.exit(-1)

    def _processing_now(self, item):
        return item in self.processing_now

    async def _push_to_queue(self):
        while True:
            try:
                if len(self.accounts_to_process_queue) > 0:
                    await asyncio.sleep(1)
                    continue
                async with SessionMaker() as session:
                    accounts_to_index = await get_known_accounts_not_indexed(session, settings.indexer.accounts_index_batch)
                    accounts_to_index = [x for x in accounts_to_index if not self._processing_now(x)]
                    logger.info(f"{len(accounts_to_index)} new accounts to index, current queue size: {len(self.accounts_to_process_queue)}")
                    if len(accounts_to_index) == 0:
                        accounts_to_index = await get_known_accounts_long_since_check(session, settings.indexer.accounts_index_min_interval_days, settings.indexer.accounts_index_batch)
                        accounts_to_index = [x for x in accounts_to_index if not self._processing_now(x)]
                        logger.info(f"{len(accounts_to_index)} accounts to re-index, current queue size: {len(self.accounts_to_process_queue)}")
                        if len(accounts_to_index) == 0:
                            logger.info(f"No accounts to re-index, sleeping")
                            await asyncio.sleep(60)
                            continue
                self.accounts_to_process_queue.extend(accounts_to_index)

                await asyncio.sleep(1)
            except asyncio.CancelledError:
                logger.warning("Task _push_to_queue was cancelled")
                return
            except BaseException as e:
                logger.error(f"Task _push_to_queue raised exception: {exception}", exception=e)

    async def _index_accounts(self):
        while True:
            try:
                if not self.is_liteserver_up:
                    await asyncio.sleep(1)
                    continue

                if len(self.running_tasks) > 4 * settings.indexer.workers_count:
                    await asyncio.sleep(1)
                    continue

                chunk = []
                while len(chunk) < settings.indexer.accounts_per_task:
                    try:
                        account = self.accounts_to_process_queue.popleft()
                    except IndexError:
                        break
                    chunk.append(account[0])
                    self.processing_now.add(account)

                if len(chunk) == 0:
                    await asyncio.sleep(1)
                    continue

                task = asyncify(get_account, [chunk], serializer='pickle', queue=self.celery_queue)
                self.running_tasks.append((self.loop.create_task(task), chunk))
                logger.info(f"Running new task with {len(chunk)} accounts, total active tasks: {len(self.running_tasks)}")
            except asyncio.CancelledError:
                logger.warning("Task _index_accounts was cancelled")
                return
            except BaseException as e:
                logger.error("Task _index_accounts raised exception: {exception}", exception=e)

    async def _read_results(self):
        while True:
            try:
                done_tasks = []
                still_running = []
                for task, chunk in self.running_tasks:
                    if task.done():
                        done_tasks.append((task, chunk))
                    else:
                        still_running.append((task, chunk))
                if len(done_tasks) > 0:
                    self.running_tasks = still_running

                for task, chunk in done_tasks:
                    async_result = task.result()
                    try:
                        async_result.get()
                    except BaseException as e:
                        logger.error(f"Task {async_result} raised unknown exception: {e}. Rescheduling the task's chunk.", async_result=async_result, exception=e)
                    for item in chunk:
                        self.processing_now.remove((item,))

                await asyncio.sleep(1)
            except asyncio.CancelledError:
                logger.warning("Task _read_results was cancelled")
                return
            except BaseException as e:
                logger.error(f"Task _read_results raised exception: {e}. {traceback.format_exc()}")


if __name__ == "__main__":
    if sys.argv[1] == 'backward':
        wait_for_broker_connection()
        scheduler = BackwardScheduler(sys.argv[2])
        scheduler.run(sys.argv[3] == 'create' if len(sys.argv) > 3 else False)
    elif sys.argv[1] == 'forward':
        wait_for_broker_connection()
        scheduler = ForwardScheduler(sys.argv[2])
        scheduler.run(sys.argv[3] == 'create' if len(sys.argv) > 3 else True)
    elif sys.argv[1] == 'accounts':
        wait_for_broker_connection()
        scheduler = AccountsIndexer(sys.argv[2])
        scheduler.run(sys.argv[3] == 'create' if len(sys.argv) > 3 else False)
    else:
        raise Exception("Pass direction in argument: backward/forward or accounts")
