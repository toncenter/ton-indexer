from collections import OrderedDict
import time
from typing import Any, Optional


class LRUCache:

    def __init__(self, max_size: int = 1000, ttl: int = 300):
        self.cache = OrderedDict()
        self.timestamps = {}
        self.max_size = max_size
        self.ttl = ttl

    def get(self, key: str) -> Optional[Any]:
        if key not in self.cache:
            return None

        # Check if entry has expired
        if time.time() - self.timestamps[key] > self.ttl:
            self.remove(key)
            return None

        # Move to the end (most recently used)
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key: str, value: Any) -> None:
        # If key exists, update it and move to end
        if key in self.cache:
            self.cache[key] = value
            self.timestamps[key] = time.time()
            self.cache.move_to_end(key)
            return

        # If cache is full, remove the least recently used item
        if len(self.cache) >= self.max_size:
            oldest = next(iter(self.cache))
            self.remove(oldest)

        # Add new item
        self.cache[key] = value
        self.timestamps[key] = time.time()

    def remove(self, key: str) -> None:
        if key in self.cache:
            del self.cache[key]
            del self.timestamps[key]

    def clear(self) -> None:
        self.cache.clear()
        self.timestamps.clear()

    def contains(self, key: str) -> bool:
        if key not in self.cache:
            return False

        if time.time() - self.timestamps[key] > self.ttl:
            self.remove(key)
            return False

        return True
