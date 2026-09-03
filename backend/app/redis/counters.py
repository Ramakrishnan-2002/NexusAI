import time
from typing import Dict, List, Tuple
from app.redis.client import get_redis
from app.core.logging import logger


class ActivityCounterService:
    """
    Maintains low-latency sliding window counters in Redis using Sorted Sets.
    Keys:
      - `act:art:{article_id}:edits` -> ZSET member: event_id, score: timestamp
      - `act:art:{article_id}:editors` -> ZSET member: username, score: timestamp
      - `hot:articles:ranking` -> ZSET member: article_id, score: spike_score
    """

    @staticmethod
    async def record_article_edit(article_id: int, event_id: str, editor: str, timestamp: float):
        r = await get_redis()
        edit_key = f"act:art:{article_id}:edits"
        editor_key = f"act:art:{article_id}:editors"

        # Add event to sorted sets with timestamp as score
        await r.zadd(edit_key, {event_id: timestamp})
        await r.zadd(editor_key, {editor: timestamp})

        # Set TTL on the keys (1 hour)
        await r.set(f"meta:{edit_key}:active", "1", ex=3600)

    @staticmethod
    async def get_sliding_window_metrics(article_id: int, window_seconds: int = 300) -> Tuple[int, int]:
        """
        Returns (edit_count, unique_editors_count) within the specified trailing window.
        Purges expired entries older than window_seconds.
        """
        r = await get_redis()
        now = time.time()
        min_cutoff = now - window_seconds

        edit_key = f"act:art:{article_id}:edits"
        editor_key = f"act:art:{article_id}:editors"

        # Prune old entries
        await r.zremrangebyscore(edit_key, 0, min_cutoff)
        await r.zremrangebyscore(editor_key, 0, min_cutoff)

        # Count active in window
        edit_count = await r.zcard(edit_key)
        unique_editors = await r.zcard(editor_key)

        return edit_count, unique_editors

    @staticmethod
    async def update_hot_ranking(article_id: int, score: float):
        r = await get_redis()
        ranking_key = "hot:articles:ranking"
        await r.zadd(ranking_key, {str(article_id): score})

    @staticmethod
    async def get_top_hot_articles(limit: int = 10) -> List[Tuple[int, float]]:
        r = await get_redis()
        ranking_key = "hot:articles:ranking"
        items = await r.zrevrange(ranking_key, 0, limit - 1, withscores=True)
        results = []
        for member, score in items:
            try:
                results.append((int(member), float(score)))
            except (ValueError, TypeError):
                continue
        return results
