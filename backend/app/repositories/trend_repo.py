from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.trend import TrendEvent
from app.models.activity import ActivitySnapshot


class TrendRepository:
    """Repository for TrendEvent and ActivitySnapshot persistence and retrieval"""

    @staticmethod
    async def create_or_update_trend(
        session: AsyncSession,
        article_id: int,
        article_title: str,
        topic: str,
        activity_score: float,
        edits_per_minute: float,
        baseline_velocity: float,
        spike_multiplier: float,
        unique_editors: int,
        total_byte_delta: int,
        metadata_json: Optional[Dict[str, Any]] = None,
    ) -> TrendEvent:
        # Check if active trend already exists for article
        stmt = select(TrendEvent).where(
            TrendEvent.article_id == article_id,
            TrendEvent.status == "active",
        )
        result = await session.execute(stmt)
        trend = result.scalar_one_or_none()

        if trend is None:
            trend = TrendEvent(
                article_id=article_id,
                article_title=article_title,
                topic=topic,
                activity_score=activity_score,
                edits_per_minute=edits_per_minute,
                baseline_velocity=baseline_velocity,
                spike_multiplier=spike_multiplier,
                unique_editors=unique_editors,
                total_byte_delta=total_byte_delta,
                status="active",
                first_detected_at=datetime.now(timezone.utc),
                peak_at=datetime.now(timezone.utc),
                metadata_json=metadata_json or {},
            )
            session.add(trend)
        else:
            trend.activity_score = max(trend.activity_score, activity_score)
            trend.edits_per_minute = edits_per_minute
            trend.spike_multiplier = max(trend.spike_multiplier, spike_multiplier)
            trend.unique_editors = max(trend.unique_editors, unique_editors)
            trend.total_byte_delta += total_byte_delta
            if activity_score >= trend.activity_score:
                trend.peak_at = datetime.now(timezone.utc)
            if metadata_json:
                trend.metadata_json.update(metadata_json)

        await session.flush()
        return trend

    @staticmethod
    async def get_active_trends(
        session: AsyncSession,
        limit: int = 20,
        min_score: float = 0.0,
    ) -> List[TrendEvent]:
        stmt = (
            select(TrendEvent)
            .where(TrendEvent.status == "active", TrendEvent.activity_score >= min_score)
            .order_by(TrendEvent.activity_score.desc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    @staticmethod
    async def get_trend_by_id(session: AsyncSession, trend_id: int) -> Optional[TrendEvent]:
        stmt = select(TrendEvent).where(TrendEvent.id == trend_id)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    @staticmethod
    async def save_activity_snapshot(
        session: AsyncSession,
        article_id: int,
        window_seconds: int,
        edit_count: int,
        byte_delta: int,
        unique_editors: int,
        bot_ratio: float,
        baseline_velocity: float,
        spike_multiplier: float,
    ) -> ActivitySnapshot:
        snapshot = ActivitySnapshot(
            article_id=article_id,
            window_seconds=window_seconds,
            edit_count=edit_count,
            byte_delta=byte_delta,
            unique_editors=unique_editors,
            bot_ratio=bot_ratio,
            baseline_velocity=baseline_velocity,
            spike_multiplier=spike_multiplier,
            calculated_at=datetime.now(timezone.utc),
        )
        session.add(snapshot)
        await session.flush()
        return snapshot
