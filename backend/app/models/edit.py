from datetime import datetime
from typing import Optional, TYPE_CHECKING
from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.article import Article
    from app.models.editor import Editor


class Edit(Base, TimestampMixin):
    __tablename__ = "edits"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)
    article_id: Mapped[int] = mapped_column(Integer, ForeignKey("articles.id", ondelete="CASCADE"), nullable=False, index=True)
    editor_username: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    is_bot: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)
    is_minor: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    revision_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, index=True)
    parent_revision_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    change_size: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    byte_diff: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    comment: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)

    # Relationships
    article: Mapped["Article"] = relationship("Article", back_populates="edits")

    __table_args__ = (
        Index("ix_edits_article_occurred", "article_id", "occurred_at"),
    )
