from datetime import datetime, timezone
from typing import TYPE_CHECKING
from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.article import Article


class ActivitySnapshot(Base, TimestampMixin):
    """
    Sliding window aggregated activity metrics for an article.
    Calculated periodically by analytics worker.
    """
    __tablename__ = "activity_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    article_id: Mapped[int] = mapped_column(Integer, ForeignKey("articles.id", ondelete="CASCADE"), nullable=False, index=True)
    window_seconds: Mapped[int] = mapped_column(Integer, default=300, nullable=False)  # 60s, 300s, 900s
    edit_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    byte_delta: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    unique_editors: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    bot_ratio: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    baseline_velocity: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    spike_multiplier: Mapped[float] = mapped_column(Float, default=1.0, nullable=False)
    calculated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False, index=True)

    # Relationship
    article: Mapped["Article"] = relationship("Article", back_populates="snapshots")

    __table_args__ = (
        Index("ix_activity_snapshots_article_calc", "article_id", "calculated_at"),
    )
