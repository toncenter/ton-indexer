import asyncio
import datetime
import json
import logging
import multiprocessing
import time
from asyncio import CancelledError
from multiprocessing import Process
from typing import Optional

import msgpack
import requests

from indexer.events import context
from indexer.events.blocks.utils import AccountId

logger = logging.getLogger(__name__)

POOLS_URL = 'https://api.dedust.io/v2/pools'
AGENT = 'Chrome'
FALLBACK_FILENAME = 'dedust_pools.json'

POOLS_DATA_KEY = 'I_dedust_pools:data'
UPDATE_INTERVAL = 120
UPDATE_FALLBACK_INTERVAL = 600  # 10 minutes fallback during processing


def pools_updater_worker(update_interval: int, stop_event):
    import asyncio
    from indexer.core.redis import client as redis_client

    async def worker_loop():
        try:
            while not stop_event.is_set():
                try:
                    response = requests.get(POOLS_URL, headers={'User-Agent': AGENT}, timeout=30)
                    response.raise_for_status()
                    pools_raw = response.json()

                    if len(pools_raw) == 0:
                        logger.warning("Empty pools response from API")
                        continue

                    dedust_pools = parse_raw_pools_data(pools_raw)
                    new_version = int(time.time())

                    pools_data = msgpack.packb(dedust_pools, use_bin_type=True)
                    await redis_client.set(POOLS_DATA_KEY, pools_data)

                    with open(FALLBACK_FILENAME, 'w') as f:
                        json.dump(pools_raw, f)

                    logger.info(f"Updated dedust pools data (version {new_version}, {len(dedust_pools)} pools)")

                except Exception as e:
                    logger.error(f"Failed to fetch and update pools: {e}")

                await asyncio.sleep(update_interval)
        except CancelledError:
            logger.info("Pools updater process cancelled")
        finally:
            await redis_client.close()

    asyncio.run(worker_loop())

class DedustPoolsManager:
    def __init__(self, redis_client=None):
        self.redis_client = redis_client
        self.update_task = None
        self.subscription_task = None
        self.running = False
        self.update_interval = UPDATE_INTERVAL
        self.last_context_update = datetime.datetime.now()
        self.worker_process = None
        self.stop_event = None

    async def start_background_updater(self):
        """Start the background updater process"""
        if self.redis_client is None:
            logger.warning("Redis client not available, skipping background updater")
            return

        self.running = True

        # Create stop event for clean shutdown
        self.stop_event = multiprocessing.Event()

        # Start worker process
        self.worker_process = Process(
            target=pools_updater_worker,
            args=(self.update_interval, self.stop_event),
            daemon=True
        )
        self.worker_process.start()

        logger.info(
            f"Started dedust pools background updater process (PID: {self.worker_process.pid}, interval: {self.update_interval}s)")

    async def stop(self):
        """Stop background processes"""
        self.running = False

        if self.subscription_task:
            self.subscription_task.cancel()

        if self.worker_process and self.worker_process.is_alive():
            # Signal the worker to stop
            self.stop_event.set()
            self.worker_process.join(timeout=5)

            if self.worker_process.is_alive():
                self.worker_process.terminate()
                self.worker_process.join(timeout=2)

            logger.info("Pools updater process stopped")
            
    async def fetch_and_update_context_pools_from_redis(self, fallback_interval: int = None):
        """Fetch and update context.dedust_pools from Redis (if needed)"""
        try:
            interval = fallback_interval if fallback_interval else self.update_interval
            if self.last_context_update + datetime.timedelta(seconds=interval) < datetime.datetime.now():
                data = await self.redis_client.get(POOLS_DATA_KEY)
                logger.debug(f"Updating context pools from Redis (last update: {self.last_context_update})")
                if data:
                    dedust_pools = msgpack.unpackb(data, raw=False)
                    context.dedust_pools.set(dedust_pools)
                    self.last_context_update = datetime.datetime.now()
        except Exception as e:
            logger.error(f"Failed to fetch and update context pools from Redis: {e}")

# Global manager instance
_pools_manager: Optional[DedustPoolsManager] = None

def get_pools_manager(redis_client=None) -> DedustPoolsManager:
    """Get or create the global pools manager"""
    global _pools_manager
    if _pools_manager is None:
        _pools_manager = DedustPoolsManager(redis_client)
    return _pools_manager

def init_pools_data(sync_redis_client=None):
    """Legacy initialization function - now delegates to sync version"""
    init_pools_data_sync()
    if sync_redis_client and len(context.dedust_pools.get()) > 0:
        pools_data = msgpack.packb(context.dedust_pools.get(), use_bin_type=True)
        sync_redis_client.set(POOLS_DATA_KEY, pools_data)

def init_pools_data_sync():
    """Synchronous pools initialization for backward compatibility"""
    try:
        response = requests.get(POOLS_URL, headers={'User-Agent': AGENT}, timeout=5.0)
        response.raise_for_status()
        pools_raw = response.json()
        if len(pools_raw) > 0:
            dedust_pools = parse_raw_pools_data(pools_raw)
            context.dedust_pools.set(dedust_pools)
            with open(FALLBACK_FILENAME, 'w') as f:
                json.dump(pools_raw, f)
        else:
            raise Exception('Empty response')
    except Exception as fetch_exception:
        logger.warning(f'Failed to fetch dedust pools data: {fetch_exception}')
        try:
            with open(FALLBACK_FILENAME, 'r') as f:
                pools_raw = json.load(f)
                dedust_pools = parse_raw_pools_data(pools_raw)
                context.dedust_pools.set(dedust_pools)
        except Exception as e:
            logger.error(f'Failed to load dedust pools data from file: {e}')
            raise e

def parse_raw_pools_data(pools_data: list[dict]) -> dict:
    pools = {}
    for pool in pools_data:
        account = AccountId(pool['address'])
        assets = []
        for raw_asset in pool['assets']:
            is_ton = raw_asset['type'] == 'native'
            asset = {
                'is_ton': is_ton,
                'address': AccountId(raw_asset['address']).as_str() if not is_ton else None
            }
            if is_ton:
                assert 'address' not in raw_asset

            assets.append(asset)
        pools[account.as_str()] = {
            'assets': assets,
        }
    return pools

async def start_pools_background_updater(redis_client, update_interval=UPDATE_INTERVAL):
    """Start the background updater (call this from main process)"""
    manager = get_pools_manager(redis_client)
    manager.update_interval = update_interval
    await manager.start_background_updater()