import asyncio
import contextlib
import datetime
import logging
import multiprocessing
from asyncio import CancelledError
from multiprocessing import Process
from typing import Optional

import msgpack

from indexer.events import context
from indexer.events.blocks.utils import AccountId

logger = logging.getLogger(__name__)

POOLS_DATA_KEY = 'I_dedust_pools:data'
POOLS_PUBSUB_CHANNEL = 'dedust_pools_updates'
PG_NOTIFY_CHANNEL = 'dex_pools_changes'

RELOAD_INTERVAL = 300
UPDATE_FALLBACK_INTERVAL = 300


def parse_db_pools_data(rows) -> dict:
    """Convert dex_pools rows to the classifier context format."""
    pools = {}
    for row in rows:
        address = AccountId(row[0]).as_str()
        assets = []
        for asset_addr in (row[1], row[2]):
            if asset_addr is None:
                assets.append({'is_ton': True, 'address': None})
            else:
                assets.append({'is_ton': False, 'address': AccountId(asset_addr).as_str()})
        pools[address] = {'assets': assets}
    return pools


def pools_updater_worker(safety_interval: int, stop_event):
    """Listen for dex_pools changes and keep Redis in sync."""
    import asyncpg
    import redis.asyncio as aioredis
    from indexer.core.settings import Settings

    _settings = Settings()
    pg_dsn = _settings.pg_dsn.replace('+asyncpg', '')

    async def fetch_pools(pg) -> dict:
        rows = await pg.fetch(
            "SELECT address, asset_1, asset_2 FROM dex_pools WHERE dex = 'dedust'"
        )
        return parse_db_pools_data([
            (r['address'], r['asset_1'], r['asset_2']) for r in rows
        ])

    async def reload_pools(pg, redis_client) -> int:
        pools = await fetch_pools(pg)
        await redis_client.set(POOLS_DATA_KEY, msgpack.packb(pools, use_bin_type=True))
        return len(pools)

    async def session_loop():
        pg = await asyncpg.connect(pg_dsn)
        redis_client = aioredis.from_url(_settings.redis_dsn)
        notify_q: asyncio.Queue = asyncio.Queue()
        reload_pool_task = None

        def on_notify(*_args):
            notify_q.put_nowait(None)

        try:
            n = await reload_pools(pg, redis_client)
            await redis_client.publish(POOLS_PUBSUB_CHANNEL, b'')
            logger.info(f"Loaded {n} dedust pools")

            await pg.add_listener(PG_NOTIFY_CHANNEL, on_notify)
            logger.info(f"Listening for Postgres notifications on {PG_NOTIFY_CHANNEL}")

            async def reload_pool_loop():
                while not stop_event.is_set():
                    try:
                        await asyncio.sleep(safety_interval)
                    except CancelledError:
                        return
                    try:
                        n2 = await reload_pools(pg, redis_client)
                        logger.debug(f"Reloaded {n2} dedust pools on timer")
                    except Exception as e:
                        logger.error(f"Failed to reload dedust pools on timer: {e}")

            reload_pool_task = asyncio.create_task(reload_pool_loop())

            while not stop_event.is_set():
                try:
                    await asyncio.wait_for(notify_q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                while not notify_q.empty():
                    notify_q.get_nowait()

                try:
                    n2 = await reload_pools(pg, redis_client)
                    await redis_client.publish(POOLS_PUBSUB_CHANNEL, b'')
                    logger.info(f"Reloaded {n2} dedust pools after DB notification")
                except Exception as e:
                    logger.error(f"Failed to reload dedust pools after DB notification: {e}")
        finally:
            if reload_pool_task is not None:
                reload_pool_task.cancel()
                with contextlib.suppress(CancelledError):
                    await reload_pool_task
            try:
                await pg.close()
            except Exception:
                pass
            try:
                await redis_client.close()
            except Exception:
                pass

    async def worker_loop():
        while not stop_event.is_set():
            try:
                await session_loop()
            except CancelledError:
                logger.info("Dedust pools updater cancelled")
                return
            except Exception as e:
                logger.error(f"Dedust pools updater crashed: {e}; reconnecting in 5s")
                try:
                    await asyncio.sleep(5)
                except CancelledError:
                    return

    asyncio.run(worker_loop())


class DedustPoolsManager:
    def __init__(self, redis_client=None):
        self.redis_client = redis_client
        self.update_task = None
        self.subscription_task = None
        self.running = False
        self.reload_interval = RELOAD_INTERVAL
        self.last_context_update = datetime.datetime.min
        self.worker_process = None
        self.stop_event = None
        self._pubsub = None

    async def start_background_updater(self):
        """Start the updater subprocess."""
        if self.redis_client is None:
            logger.warning("Redis client not available, skipping background updater")
            return

        self.running = True
        self.stop_event = multiprocessing.Event()

        self.worker_process = Process(
            target=pools_updater_worker,
            args=(self.reload_interval, self.stop_event),
            daemon=True,
        )
        self.worker_process.start()

        logger.info(
            f"Started dedust pools updater, pid={self.worker_process.pid}, "
            f"safety_interval={self.reload_interval}s"
        )

    async def stop(self):
        """Stop background processes."""
        self.running = False

        if self.subscription_task:
            self.subscription_task.cancel()

        if self._pubsub is not None:
            try:
                await self._pubsub.unsubscribe(POOLS_PUBSUB_CHANNEL)
                await self._pubsub.close()
            except Exception:
                pass
            self._pubsub = None

        if self.worker_process and self.worker_process.is_alive():
            self.stop_event.set()
            self.worker_process.join(timeout=5)
            if self.worker_process.is_alive():
                self.worker_process.terminate()
                self.worker_process.join(timeout=2)
            logger.info("Pools updater process stopped")

    async def _ensure_pubsub(self):
        if self._pubsub is None and self.redis_client is not None:
            ps = self.redis_client.pubsub()
            await ps.subscribe(POOLS_PUBSUB_CHANNEL)
            self._pubsub = ps
        return self._pubsub

    async def fetch_and_update_context_pools_from_redis(self, fallback_interval: int = None):
        """Reload context.dedust_pools when Redis has changed or the timer expires."""
        try:
            force_reload = False
            pubsub = await self._ensure_pubsub()
            if pubsub is not None:
                while True:
                    msg = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=0.0
                    )
                    if msg is None:
                        break
                    force_reload = True

            interval = fallback_interval if fallback_interval else self.reload_interval
            need_update = (
                self.last_context_update + datetime.timedelta(seconds=interval)
            ) < datetime.datetime.now()
            if not (force_reload or need_update):
                return

            data = await self.redis_client.get(POOLS_DATA_KEY)
            if data:
                dedust_pools = msgpack.unpackb(data, raw=False)
                context.dedust_pools.set(dedust_pools)
                self.last_context_update = datetime.datetime.now()
                if force_reload:
                    logger.debug(f"Reloaded {len(dedust_pools)} dedust pools from Redis")
        except Exception as e:
            logger.error(f"Failed to fetch and update context pools from Redis: {e}")


_pools_manager: Optional[DedustPoolsManager] = None


def get_pools_manager(redis_client=None) -> DedustPoolsManager:
    """Get or create the process-local pools manager."""
    global _pools_manager
    if _pools_manager is None:
        _pools_manager = DedustPoolsManager(redis_client)
    return _pools_manager


def init_pools_data(sync_redis_client=None):
    """Initialize pools from Postgres and seed Redis."""
    init_pools_data_sync()
    if sync_redis_client:
        pools_data = msgpack.packb(context.dedust_pools.get(), use_bin_type=True)
        sync_redis_client.set(POOLS_DATA_KEY, pools_data)


def init_pools_data_sync():
    """Load dedust pools from the dex_pools table."""
    from indexer.core.database import SyncSessionMaker
    from sqlalchemy import text

    with SyncSessionMaker() as session:
        rows = session.execute(
            text("SELECT address, asset_1, asset_2 FROM dex_pools WHERE dex = 'dedust'")
        ).fetchall()

    if len(rows) == 0:
        logger.warning("No dedust pools found in DB, starting with empty pool set")
        context.dedust_pools.set({})
        return

    dedust_pools = parse_db_pools_data(rows)
    context.dedust_pools.set(dedust_pools)
    logger.info(f"Initialized {len(dedust_pools)} dedust pools from DB")


async def start_pools_background_updater(
    redis_client, reload_interval: int = RELOAD_INTERVAL
):
    manager = get_pools_manager(redis_client)
    manager.reload_interval = reload_interval
    await manager.start_background_updater()
