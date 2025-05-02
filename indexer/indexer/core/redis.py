from __future__ import annotations

import redis.asyncio as aioredis
import redis

from indexer.core.settings import Settings
import logging
logger = logging.getLogger(__name__)

settings = Settings()


def create_redis_client():
    dsn = settings.redis_dsn
    if dsn.startswith("tcp"):
        dsn = "redis" + dsn[3:]
    pool = aioredis.ConnectionPool.from_url(dsn, max_connections=800)
    client = aioredis.Redis(connection_pool=pool, max_connections=800)
    logger.info(f"redis: {client}")
    return client

def create_sync_redis_client():
    dsn = settings.redis_dsn
    if dsn.startswith("tcp"):
        dsn = "redis" + dsn[3:]
    pool = redis.ConnectionPool.from_url(dsn, max_connections=800)
    client = redis.Redis(connection_pool=pool, max_connections=800)
    logger.info(f"sync redis: {client}")
    return client

client: redis.Redis | None = None
sync_client: aioredis.Redis | None = None

if settings.redis_dsn:
    client = create_redis_client()
    sync_client = create_sync_redis_client()
else:
    logger.warning("No redis dsn provided")
