from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from sqlalchemy import DateTime, ForeignKey, Index, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base, TimestampMixin

try:
    from pgvector.sqlalchemy import Vector
    HAS_PGVECTOR = True
except ImportError:
    HAS_PGVECTOR = False

if TYPE_CHECKING:
    from app.models.article import Article


class KnowledgeChunk(Base, TimestampMixin):
    """
    Indexed knowledge representation extracted from Wikipedia edits.
    Supports pgvector semantic embeddings and PostgreSQL full-text search.
    """
    __tablename__ = "knowledge_chunks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    article_id: Mapped[int] = mapped_column(Integer, ForeignKey("articles.id", ondelete="CASCADE"), nullable=False, index=True)
    edit_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    revision_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    article_title: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    
    # Vector column: 384 dimensions for all-MiniLM-L6-v2 or Ollama embeddings
    # If pgvector is present in PostgreSQL, Vector(384); otherwise fallback JSON/Text
    if HAS_PGVECTOR:
        embedding = mapped_column(Vector(384), nullable=True)
    else:
        embedding = mapped_column(JSON, nullable=True)

    chunk_metadata: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False, index=True)

    # Relationships
    article: Mapped["Article"] = relationship("Article", back_populates="knowledge_chunks")

    __table_args__ = (
        Index("ix_knowledge_chunks_article_occurred", "article_id", "occurred_at"),
    )
