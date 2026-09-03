from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from app.repositories.trend_repo import TrendRepository
from app.schemas.trend import TrendEventSchema, TrendListResponse
from app.redis.cache import CacheService


class TrendService:
    """Service layer for trend and activity spike queries with Redis caching"""

    @staticmethod
    async def list_active_trends(
        session: AsyncSession,
        limit: int = 20,
        min_score: float = 0.0,
    ) -> TrendListResponse:
        cache_key = f"cache:trends:list:{limit}:{min_score}"
        cached = await CacheService.get_json(cache_key)
        if cached:
            return TrendListResponse(**cached)

        trends = await TrendRepository.get_active_trends(session, limit=limit, min_score=min_score)
        items = [TrendEventSchema.model_validate(t) for t in trends]
        resp = TrendListResponse(total=len(items), trends=items)

        # Cache for 15 seconds
        await CacheService.set_json(cache_key, resp.model_dump(mode="json"), ttl=15)
        return resp

    @staticmethod
    async def get_trend_by_id(session: AsyncSession, trend_id: int) -> Optional[TrendEventSchema]:
        trend = await TrendRepository.get_trend_by_id(session, trend_id)
        if not trend:
            return None
        return TrendEventSchema.model_validate(trend)
