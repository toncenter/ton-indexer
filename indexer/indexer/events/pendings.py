from __future__ import annotations

import asyncio
import gzip
import logging
import json
import time
import traceback
from collections import defaultdict
from typing import List, Tuple, Set, Dict, Optional
from datetime import datetime
from copy import deepcopy

import msgpack
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
import multiprocessing as mp

from indexer.core import redis
from indexer.core.database import engine, FinalityState
from indexer.core.settings import Settings
from indexer.events import context
from indexer.events.blocks.utils.address_selectors import extract_accounts_from_trace
from indexer.events.blocks.utils.block_tree_serializer import serialize_blocks
from indexer.events.blocks.utils.dedust_pools import get_pools_manager, start_pools_background_updater, UPDATE_FALLBACK_INTERVAL
from indexer.events.blocks.utils.event_deserializer import deserialize_event
from indexer.events.event_processing import process_event_async_with_postprocessing, try_classify_unknown_trace
from indexer.events.interface_repository import (
    EmulatedTransactionsInterfaceRepository, gather_interfaces,
    EmulatedRepositoryWithDbFallback, ExtraAccountRequest
)
from indexer.events.trace_processor import TraceProcessor
from indexer.events.utils.lru_cache import LRUCache
from queue import Full, Empty

async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
logger = logging.getLogger(__name__)
interface_cache: LRUCache | None = None

HEALTH_KEY_EVENT_CLASSIFIER = "health:event-classifier"
HEALTH_LAST_CLASSIFIED_FIELD = "last_classified_trace_time"
HEALTH_HEARTBEAT_INTERVAL = 5
HEALTH_HEARTBEAT_TTL = 20


async def update_last_classified_trace_time():
    try:
        now = int(time.time())
        await redis.client.hset(HEALTH_KEY_EVENT_CLASSIFIER, mapping={HEALTH_LAST_CLASSIFIED_FIELD: now})
    except Exception as exc:
        logger.warning(f"Failed to update last classified trace time: {exc}")


async def health_heartbeat_loop(interval: int = HEALTH_HEARTBEAT_INTERVAL, ttl: int = HEALTH_HEARTBEAT_TTL):
    while True:
        try:
            now = int(time.time())
            await redis.client.hset(HEALTH_KEY_EVENT_CLASSIFIER, mapping={"last_heartbeat": now})
            await redis.client.expire(HEALTH_KEY_EVENT_CLASSIFIER, ttl)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"Failed to update event-classifier heartbeat: {exc}")
        await asyncio.sleep(interval)


class Measurement:
    id: int = 0
    ext_msg_hash: Optional[str] = None
    ext_msg_hash_norm: Optional[str] = None
    trace_root_tx_hash: Optional[str] = None

    timings: Dict[str, float] = dict()
    extra: Dict[str, str] = dict()

    def measure_step(self, step: str, timestamp: Optional[float] = None) -> "Measurement":
        ts = datetime.now().timestamp() if timestamp is None else timestamp
        self.timings[step] = ts
        return self

    def print_measurement(self) -> "Measurement":
        for step, ts in self.timings.items():
            meas = {
                'id': self.id,
                'msg_hash_norm': self.ext_msg_hash_norm,
                'msg_hash': self.ext_msg_hash,
                'trace_id': self.trace_root_tx_hash,
                'step': step,
                'time': ts,
                'extra': self.extra
            }
            meas_str = json.dumps(meas)
            logger.error(f"MEASURE {meas_str} END")
        return self

    def clone(self) -> "Measurement":
        return deepcopy(self)

class PendingTraceClassifierWorker(mp.Process):

    def __init__(self,
                 id,
                 task_queue: mp.Queue,
                 use_combined_repository: bool,
                 emulated_traces_redis_response_channel,
                 interface_cache_size: int = 1000,
                 interface_cache_ttl: int = 300):
        super().__init__()
        self.id = id
        self.task_queue = task_queue
        self.use_combined_repository = use_combined_repository
        self.interface_cache_size = interface_cache_size
        self.interface_cache_ttl = interface_cache_ttl
        self.emulated_traces_redis_response_channel = emulated_traces_redis_response_channel


    def run(self):
        logger.info(f'Thread PendingTraceClassifierWorker #{self.id} started')

        global interface_cache
        interface_cache = LRUCache(max_size=self.interface_cache_size, ttl=self.interface_cache_ttl)
        asyncio.set_event_loop(asyncio.new_event_loop())
        try:
            while True:
                self.process_batch()
        except KeyboardInterrupt:
            logger.info(f'Gracefully stopped in the PendingTraceClassifierWorker #{self.id}')
        except:
            logger.info(f'Error in PendingTraceClassifierWorker #{self.id}: {traceback.format_exc()}')
        logger.info(f'Thread PendingTraceClassifierWorker #{self.id} finished')
        return

    def process_batch(self):
        while True:
            try:
                batch = self.task_queue.get(block=True, timeout=1.0)
                break  # got a batch, proceed to process
            except Empty:
                # Worker is idle - attempt pool update (method checks 120s interval internally)
                asyncio.get_event_loop().run_until_complete(
                    get_pools_manager(redis.client).fetch_and_update_context_pools_from_redis()
                )
        logger.debug(f"Processing batch of {len(batch)} traces in worker #{self.id}")
        asyncio.get_event_loop().run_until_complete(self._run_batch(batch))

    async def _run_batch(self, batch):
        """Process one batch and swallow/log any exception so the parent task lives on."""
        try:
            start = time.time()

            async with async_session() as session:
                results = await self.process_emulated_trace_batch(batch, session, self.use_combined_repository)

            success_count = sum(1 for _, success in results if success)
            processing_time = time.time() - start

            logger.info("Batch completed: %s/%s traces processed successfully in %.3f seconds.",
                        success_count, len(batch), processing_time)


        except Exception as exc:  # never let an exception kill the loop
            logger.error("Batch FAILED: %s", exc, exc_info=True)

    async def process_emulated_trace_batch(
            self,
            trace_keys: List[str],
            session: AsyncSession,
            use_combined: bool = False
    ) -> List[Tuple[str, bool]]:
        measurement = Measurement().measure_step("batch_classification__start")

        results = []

        all_accounts = set()
        traces_data = {}

        await get_pools_manager(redis.client).fetch_and_update_context_pools_from_redis(
            fallback_interval=UPDATE_FALLBACK_INTERVAL
        )

        raw_trace_data = {}
        for trace_key in trace_keys:
            try:
                trace_map = await redis.client.hgetall(trace_key)
                trace_map = dict((str(key, encoding='utf-8'), value) for key, value in trace_map.items())

                if not trace_map:
                    logger.warning(f"No data found in Redis for trace {trace_key}")
                    results.append((trace_key, False))
                    continue
                raw_trace_data[trace_key] = trace_map
            except Exception as e:
                logger.error(f"Unable to get trace {trace_key}: {e}")
                results.append((trace_key, False))
        measurement.measure_step("batch_classification__traces_loaded_from_redis")
        for trace_key, trace_map in raw_trace_data.items():
            try:
                trace = deserialize_event(trace_key, trace_map)
                if 'measurement_id' in trace_map:
                    measurement.id = int(trace_map['measurement_id'].decode('utf-8'))
                traces_data[trace_key] = (trace, trace_map)

                all_accounts, extra_data_requests = extract_accounts_from_trace(trace)
            except Exception as e:
                logger.error(f"Failed to extract accounts from trace {trace_key}: {e}")
                results.append((trace_key, False))
        measurement.measure_step("batch_classification__traces_extracted")

        # Gather interfaces
        db_interfaces = {}
        if use_combined and all_accounts:
            try:
                logger.debug(f"Getting interfaces for {len(all_accounts)} accounts")
                # Use our cached interface function that uses the global cache
                db_interfaces = await self.get_interfaces_with_cache(all_accounts, session,
                                                                extra_requests=extra_data_requests)
                logger.debug(f"Got interfaces for {len(db_interfaces)} accounts")
            except Exception as e:
                logger.error(f"Failed to gather interfaces: {e}")
                # Continue with empty db_interfaces

        measurement.measure_step("batch_classification__interfaces_gathered")
        # Process traces
        for trace_key, (trace, trace_map) in traces_data.items():
            meas = measurement.clone()
            meas.extra['num_txs'] = len(trace.transactions)
            meas.ext_msg_hash_norm = trace_key
            meas.ext_msg_hash = trace.external_hash
            meas.trace_root_tx_hash = trace.trace_id

            try:
                # Setup repositories
                emulated_repository = EmulatedTransactionsInterfaceRepository(trace_map)
                if use_combined:
                    repository = EmulatedRepositoryWithDbFallback(
                        emulated_repository=emulated_repository,
                        db_interfaces=db_interfaces,
                    )
                else:
                    repository = emulated_repository
                context.interface_repository.set(repository)

                # Process trace
                processor = TraceProcessor()
                result = await processor.process_trace(trace)
                meas.measure_step("trace_classification__trace_processing_finished")

                # Fill trace external hash if needed
                action_types = []
                is_jetton_transfer = False
                jetton_senders = []
                for action in result.actions:
                    if action.type == 'jetton_transfer':
                        logger.error(f"action: {action.to_dict()}")
                        if action.source is not None:
                            is_jetton_transfer = True
                            jetton_senders.append(action.source)
                    action_types.append(action.type)
                    if trace.transactions[0].emulated:
                        action.trace_id = None
                        action.trace_external_hash = trace.external_hash
                    action.trace_external_hash_norm = trace_key
                meas.extra['action_types'] = ','.join(set(action_types))
                meas.extra['jetton_senders'] = ','.join(set(jetton_senders))
                # Store results in Redis (msgpack + gzip compression)
                action_data = msgpack.packb([a.to_dict() for a in result.actions])
                action_data_compressed = gzip.compress(action_data, compresslevel=1)
                await redis.client.hset(trace_key, 'actions', action_data_compressed)

                # Publish completion if configured
                if self.emulated_traces_redis_response_channel:
                    await redis.client.publish(
                        self.emulated_traces_redis_response_channel,
                        trace_key
                    )
                meas.measure_step("trace_classification__results_stored")

                # Build index
                index = defaultdict(set)
                for action in result.actions:
                    for account in action.get_action_accounts():
                        k = f"{trace_key}:{action.action_id}"
                        v = trace.start_lt
                        index[account.account].add((k, v))

                meas.measure_step("trace_classification__indexes_built")

                # Store referenced accounts
                transaction_accounts = set(t.account for t in trace.transactions)
                referenced_accounts = set(index.keys()) - transaction_accounts

                # Add indices to Redis
                for account, values in index.items():
                    await redis.client.zadd(f"_aai:{account}", dict(values))
                meas.measure_step("trace_classification__complete")

                # Publish referenced accounts
                for r in referenced_accounts:
                    await redis.client.publish('referenced_accounts', f"{r};{trace_key}")
                meas.measure_step("trace_classification__published_referenced_accounts_in_redis")
                meas.print_measurement()
                await update_last_classified_trace_time()
                results.append((trace_key, True))

            except Exception as e:
                logger.error(f"Failed to process emulated trace {trace_key}: {e}")
                logger.exception(e)
                results.append((trace_key, False))

        return results

    async def get_interfaces_with_cache(self, accounts: Set[str], session: AsyncSession,
                                        extra_requests: Set[ExtraAccountRequest]) -> Dict[str, Dict[str, Dict]]:
        if not accounts:
            return {}

        # Check which accounts are in cache
        cached_accounts = {}
        accounts_to_fetch = set()

        for account in accounts:
            cached = interface_cache.get(account)
            if cached is not None:
                cached_accounts[account] = cached
            else:
                accounts_to_fetch.add(account)

        # Fetch interfaces for accounts not in cache
        if accounts_to_fetch:
            logger.debug(f"Fetching interfaces for {len(accounts_to_fetch)} accounts from DB")
            db_interfaces = await gather_interfaces(accounts_to_fetch, session, extra_requests=extra_requests)

            # Update cache with new interfaces
            for account, interfaces in db_interfaces.items():
                interface_cache.put(account, interfaces)

            # Merge with cached accounts
            db_interfaces.update(cached_accounts)
            return db_interfaces
        else:
            logger.debug(f"All {len(accounts)} accounts found in cache")
            return cached_accounts


async def start_emulated_traces_processing(settings: Settings,
                                           batch_window: float = 0.1, max_batch_size: int = 50, pool_size=5,
                                           max_queue_size=10):
    asyncio.create_task(health_heartbeat_loop())
    pubsub = redis.client.pubsub()
    await pubsub.subscribe(settings.emulated_traces_redis_channel)
    use_combined = settings.use_combined_repository
    if use_combined:
        logger.info("Combined repository mode enabled")
    await start_pools_background_updater(redis.client)
    batch_queue: mp.Queue[list[str]] = mp.Queue(maxsize=max_queue_size)
    workers: List[PendingTraceClassifierWorker] = []
    for id in range(pool_size):
        worker = PendingTraceClassifierWorker(id=id,
                                              task_queue=batch_queue,
                                              use_combined_repository=use_combined,
                                              emulated_traces_redis_response_channel=settings.emulated_traces_redis_response_channel,
                                              interface_cache_size=settings.interfaces_cache_size,
                                              interface_cache_ttl=settings.interfaces_cache_ttl)
        worker.start()
        workers.append(worker)

    batching_enabled = batch_window > 0
    logger.info(f"Starting emulated trace processing with time-window batching")
    logger.info(f"  Batch window: {batch_window} seconds")
    logger.info(f"  Max batch size: {max_batch_size} traces")
    logger.info(f"  Interface cache size: {settings.interfaces_cache_size} entries")
    logger.info(f"  Interface cache TTL: {settings.interfaces_cache_ttl} seconds")
    logger.info(f"  Pool size: {pool_size}")
    logger.info(f"  Max queue size: {max_queue_size}")

    pending_traces: list[str] = []
    last_process_time: float = time.time()
    try:
        while True:
            message = await pubsub.get_message(timeout=0.01)

            current_time = time.time()
            window_elapsed = current_time - last_process_time

            if message is not None and message['type'] == 'message':
                trace_id = message['data'].decode('utf-8')
                pending_traces.append(trace_id)
                logger.info(f"Added trace {trace_id} to batch (size: {len(pending_traces)})")
                # logger.error(f"MEASURE[{trace_id}](unixtime={datetime.now().timestamp()} module=event_classifier step=classifier_trace_queued)")

            if not pending_traces:
                continue

            # Process batch if window elapsed or max size reached
            if window_elapsed >= batch_window or len(pending_traces) >= max_batch_size or not batching_enabled:
                batch = pending_traces.copy()
                pending_traces.clear()
                last_process_time = current_time
                try:
                    batch_queue.put(batch, block=False)
                    logger.debug(f"Batch of {len(batch)} traces added to queue")
                except Full:
                    logger.warning(f"Batch queue is full, discarding batch of {len(batch)} traces")
                    continue


    except asyncio.CancelledError:
        logger.info("Emulated trace processing cancelled")
    except Exception as e:
        logger.error(f"Error in emulated trace processing: {e}")
        logger.exception(e)
    finally:
        await pubsub.unsubscribe()
        logger.info("Emulated trace processing stopped")
