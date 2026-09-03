from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.article import Article
    from app.models.ai_analysis import AIAnalysis


class TrendEvent(Base, TimestampMixin):
    """
    Identified unusual activity spike / emerging topic trend.
    """
    __tablename__ = "trend_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    article_id: Mapped[int] = mapped_column(Integer, ForeignKey("articles.id", ondelete="CASCADE"), nullable=False, index=True)
    article_title: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    topic: Mapped[str] = mapped_column(String(128), default="General", nullable=False, index=True)
    activity_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False, index=True)
    edits_per_minute: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    baseline_velocity: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    spike_multiplier: Mapped[float] = mapped_column(Float, default=1.0, nullable=False)
    unique_editors: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    total_byte_delta: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="active", nullable=False, index=True)
    first_detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False, index=True)
    peak_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    ai_summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ai_importance: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    metadata_json: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    # Relationships
    article: Mapped["Article"] = relationship("Article", back_populates="trends")
    analyses: Mapped[List["AIAnalysis"]] = relationship("AIAnalysis", back_populates="trend", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_trend_events_status_score", "status", "activity_score"),
    )
