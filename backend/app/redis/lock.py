import asyncio
import time
import uuid
from typing import Optional
from app.redis.client import get_redis
from app.core.logging import logger


class DistributedLock:
    """
    Redis-based distributed lock with TTL and release validation
    for preventing worker duplicate processing or concurrency races.
    """
    def __init__(self, key: str, ttl_seconds: int = 15):
        self.key = f"lock:{key}"
        self.ttl = ttl_seconds
        self.token = str(uuid.uuid4())
        self._acquired = False

    async def acquire(self) -> bool:
        r = await get_redis()
        # SET key token NX EX ttl
        res = await r.set(self.key, self.token, ex=self.ttl)
        self._acquired = bool(res)
        return self._acquired

    async def release(self) -> bool:
        if not self._acquired:
            return False
        r = await get_redis()
        val = await r.get(self.key)
        if val == self.token:
            await r.delete(self.key)
            self._acquired = False
            return True
        return False

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()
