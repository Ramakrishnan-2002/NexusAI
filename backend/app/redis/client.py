import asyncio
import fnmatch
import time
from typing import Any, Dict, List, Optional, Set
import redis.asyncio as aioredis
from app.core.config import settings
from app.core.logging import logger


class InMemoryFallbackRedis:
    """
    In-memory fallback providing Redis Sorted Sets (ZADD, ZREMRANGEBYSCORE, ZCARD, ZRANGE),
    Key-Value GET/SET with TTL, and distributed lock emulation when Redis is unavailable.
    """
    def __init__(self):
        self._kv: Dict[str, Any] = {}
        self._ttls: Dict[str, float] = {}
        self._zsets: Dict[str, Dict[str, float]] = {}

    def _is_expired(self, key: str) -> bool:
        if key in self._ttls and time.time() > self._ttls[key]:
            self._kv.pop(key, None)
            self._ttls.pop(key, None)
            self._zsets.pop(key, None)
            return True
        return False

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> Optional[str]:
        if self._is_expired(key):
            return None
        return self._kv.get(key)

    async def set(self, key: str, value: Any, ex: Optional[int] = None, nx: bool = False) -> bool:
        if nx and key in self._kv and not self._is_expired(key):
            return False
        self._kv[key] = str(value)
        if ex:
            self._ttls[key] = time.time() + ex
        else:
            self._ttls.pop(key, None)
        return True

    async def incr(self, key: str) -> int:
        if self._is_expired(key):
            self._kv[key] = "0"
        val = int(self._kv.get(key, 0)) + 1
        self._kv[key] = str(val)
        return val

    async def delete(self, *keys: str) -> int:
        count = 0
        for k in keys:
            if k in self._kv or k in self._zsets:
                count += 1
            self._kv.pop(k, None)
            self._ttls.pop(k, None)
            self._zsets.pop(k, None)
        return count

    async def zadd(self, key: str, mapping: Dict[str, float]) -> int:
        self._is_expired(key)
        if key not in self._zsets:
            self._zsets[key] = {}
        added = 0
        for member, score in mapping.items():
            if member not in self._zsets[key]:
                added += 1
            self._zsets[key][member] = float(score)
        return added

    async def zremrangebyscore(self, key: str, min_score: float, max_score: float) -> int:
        self._is_expired(key)
        if key not in self._zsets:
            return 0
        to_remove = [
            m for m, s in self._zsets[key].items()
            if (min_score == float("-inf") or s >= min_score) and (max_score == float("inf") or s <= max_score)
        ]
        for m in to_remove:
            del self._zsets[key][m]
        return len(to_remove)

    async def zcard(self, key: str) -> int:
        self._is_expired(key)
        return len(self._zsets.get(key, {}))

    async def zrevrangebyscore(self, key: str, max_score: float, min_score: float, start: int = 0, num: int = 10) -> List[str]:
        self._is_expired(key)
        if key not in self._zsets:
            return []
        items = [
            (m, s) for m, s in self._zsets[key].items()
            if (min_score == float("-inf") or s >= min_score) and (max_score == float("inf") or s <= max_score)
        ]
        items.sort(key=lambda x: x[1], reverse=True)
        return [m for m, _ in items[start:start + num]]

    async def zrange(self, key: str, start: int, stop: int, desc: bool = False, withscores: bool = False) -> List[Any]:
        self._is_expired(key)
        if key not in self._zsets:
            return []
        items = list(self._zsets[key].items())
        items.sort(key=lambda x: x[1], reverse=desc)
        selected = items[start:stop + 1 if stop != -1 else None]
        if withscores:
            return [(m, s) for m, s in selected]
        return [m for m, _ in selected]

    async def keys(self, pattern: str = "*") -> List[str]:
        all_keys = set(self._kv.keys()) | set(self._zsets.keys())
        valid = [k for k in all_keys if not self._is_expired(k)]
        return [k for k in valid if fnmatch.fnmatch(k, pattern)]

    async def expire(self, key: str, seconds: int) -> bool:
        if key in self._kv or key in self._zsets:
            self._ttls[key] = time.time() + seconds
            return True
        return False

    async def aclose(self):
        pass

    async def close(self):
        pass


class RedisManager:
    """Manages Redis connection with automatic retry and graceful in-memory fallback"""
    def __init__(self):
        self._client: Optional[Any] = None
        self._use_fallback = False
        self._fallback = InMemoryFallbackRedis()

    async def get_client(self):
        if self._use_fallback:
            return self._fallback

        if self._client is not None:
            try:
                await self._client.ping()
                return self._client
            except Exception:
                try:
                    await self._client.aclose()
                except Exception:
                    pass
                self._client = None

        if self._client is None:
            try:
                self._client = aioredis.from_url(
                    settings.REDIS_URL,
                    decode_responses=True,
                    socket_connect_timeout=2.0,
                    socket_timeout=2.0,
                )
                await self._client.ping()
                logger.info("Connected to Redis successfully.")
            except Exception as e:
                logger.warning(f"Redis unavailable ({e}); falling back to graceful in-memory state.")
                self._client = None
                return self._fallback

        return self._client

    async def aclose(self):
        if self._client:
            try:
                await self._client.aclose()
            except Exception:
                pass
            self._client = None

    async def close(self):
        await self.aclose()


redis_manager = RedisManager()


async def get_redis():
    return await redis_manager.get_client()
