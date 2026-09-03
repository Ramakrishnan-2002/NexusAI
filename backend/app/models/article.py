from typing import List, TYPE_CHECKING
from sqlalchemy import Integer, String, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.models.edit import Edit
    from app.models.activity import ActivitySnapshot
    from app.models.knowledge_chunk import KnowledgeChunk
    from app.models.trend import TrendEvent


class Article(Base, TimestampMixin):
    __tablename__ = "articles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    wiki: Mapped[str] = mapped_column(String(32), default="enwiki", nullable=False, index=True)
    namespace: Mapped[int] = mapped_column(Integer, default=0, nullable=False, index=True)
    page_id: Mapped[int] = mapped_column(Integer, nullable=True, index=True)
    url: Mapped[str] = mapped_column(String(1024), nullable=True)

    # Relationships
    edits: Mapped[List["Edit"]] = relationship("Edit", back_populates="article", cascade="all, delete-orphan")
    snapshots: Mapped[List["ActivitySnapshot"]] = relationship("ActivitySnapshot", back_populates="article", cascade="all, delete-orphan")
    knowledge_chunks: Mapped[List["KnowledgeChunk"]] = relationship("KnowledgeChunk", back_populates="article", cascade="all, delete-orphan")
    trends: Mapped[List["TrendEvent"]] = relationship("TrendEvent", back_populates="article", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_articles_wiki_title", "wiki", "title", unique=True),
    )
