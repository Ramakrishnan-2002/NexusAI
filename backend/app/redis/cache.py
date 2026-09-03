import json
from typing import Any, Optional
from app.redis.client import get_redis
from app.core.config import settings
from app.core.metrics import metrics_collector


class CacheService:
    """Redis-backed application cache for API results and queries"""

    @staticmethod
    async def get_json(key: str) -> Optional[Any]:
        r = await get_redis()
        raw = await r.get(key)
        if raw is not None:
            metrics_collector.record_redis_access(hit=True)
            try:
                return json.loads(raw)
            except Exception:
                return raw
        metrics_collector.record_redis_access(hit=False)
        return None

    @staticmethod
    async def set_json(key: str, data: Any, ttl: Optional[int] = None) -> bool:
        r = await get_redis()
        exp = ttl if ttl is not None else settings.REDIS_CACHE_TTL_SECONDS
        payload = json.dumps(data) if not isinstance(data, str) else data
        return await r.set(key, payload, ex=exp)

    @staticmethod
    async def delete(key: str) -> bool:
        r = await get_redis()
        return bool(await r.delete(key))
