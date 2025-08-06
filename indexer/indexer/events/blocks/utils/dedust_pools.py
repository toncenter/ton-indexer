import asyncio
import datetime
import json
import logging
import time
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

class DedustPoolsManager:
    def __init__(self, redis_client=None):
        self.redis_client = redis_client
        self.update_task = None
        self.subscription_task = None
        self.running = False
        self.update_interval = UPDATE_INTERVAL
        self.last_context_update = datetime.datetime.now()

    async def start_background_updater(self):
        """Start the background updater process"""
        if self.redis_client is None:
            logger.warning("Redis client not available, skipping background updater")
            return
            
        self.running = True
        self.update_task = asyncio.create_task(self._update_loop())
        logger.info(f"Started dedust pools background updater (interval: {self.update_interval}s)")
        
    async def stop(self):
        """Stop background processes"""
        self.running = False
        if self.update_task:
            self.update_task.cancel()
        if self.subscription_task:
            self.subscription_task.cancel()
            
    async def _update_loop(self):
        """Background loop that periodically fetches and updates pool data"""
        try:
            while self.running:
                try:
                    await self._fetch_and_update_pools()
                    await asyncio.sleep(self.update_interval)
                except Exception as e:
                    logger.error(f"Error in pools update loop: {e}")
                    await asyncio.sleep(60)  # Retry after 1 minute on error
        except asyncio.CancelledError:
            logger.info("Pools update loop cancelled")

            
    async def _fetch_and_update_pools(self):
        """Fetch fresh pool data and update Redis"""
        try:
            # Fetch fresh data
            response = requests.get(POOLS_URL, headers={'User-Agent': AGENT}, timeout=30)
            response.raise_for_status()
            pools_raw = response.json()
            
            if len(pools_raw) == 0:
                logger.warning("Empty pools response from API")
                return
                
            # Parse and process data
            dedust_pools = parse_raw_pools_data(pools_raw)
            
            # Generate new version (timestamp)
            new_version = int(time.time())
            
            # Store in Redis
            pools_data = msgpack.packb(dedust_pools, use_bin_type=True)
            await self.redis_client.set(POOLS_DATA_KEY, pools_data)

            # Save fallback file
            with open(FALLBACK_FILENAME, 'w') as f:
                json.dump(pools_raw, f)
                
            logger.info(f"Updated dedust pools data (version {new_version}, {len(dedust_pools)} pools)")
            
        except Exception as e:
            logger.error(f"Failed to fetch and update pools: {e}")
            
    async def fetch_and_update_context_pools_from_redis(self):
        """Fetch and update context.dedust_pools from Redis (if needed)"""
        try:
            if self.last_context_update + datetime.timedelta(seconds=self.update_interval) > datetime.datetime.now():
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