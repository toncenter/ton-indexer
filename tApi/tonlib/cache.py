import aioredis
import ring

from abc import ABC, abstractmethod
from functools import wraps

from ring.func.asyncio import Aioredis2Storage


class BaseCacheManager(ABC):
    @abstractmethod
    def ttl_cached(self, expire, check_error=True, *args, **kwargs):
        pass

    # TODO: find a way to make distributed lru cache
    @abstractmethod
    def lru_cached(self, *args, **kwargs):
        pass


# disabled cache
class DefaultCacheManager(BaseCacheManager):
    def ttl_cached(self, expire=0, check_error=True):
        def decorator(func):
            return func
        return decorator

    def lru_cached(self, *args, **kwargs):
        def decorator(func):
            return func
        return decorator


# redis cache
class TonlibResultRedisStorage(Aioredis2Storage):
    async def set(self, key, value, expire=...):
        if value.get('@type', 'error') == 'error':
            return None
        return await super().set(key, value, expire)


class RedisCacheManager(BaseCacheManager):
    def __init__(self, endpoint='localhost', port=6379):
        self._redis = aioredis.from_url(f"redis://{endpoint}:{port}")

    def ttl_cached(self, expire, check_error=True, *args, **kwargs):
        storage_class = TonlibResultRedisStorage if check_error else Aioredis2Storage
        return ring.aioredis(self._redis, coder='pickle', expire=expire, storage_class=storage_class)
