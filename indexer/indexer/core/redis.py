import redis.asyncio as aioredis

from core.settings import Settings

settings = Settings()


def create_redis_client():
    dsn = settings.redis_dsn
    if dsn.startswith("tcp"):
        dsn = "redis" + dsn[3:]
    return aioredis.Redis.from_url(dsn)


client = create_redis_client()
