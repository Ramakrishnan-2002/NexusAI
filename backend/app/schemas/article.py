from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, ConfigDict


class EditItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    event_id: str
    article_title: Optional[str] = None
    editor_username: str
    is_bot: bool
    is_minor: bool
    revision_id: Optional[int] = None
    change_size: int
    byte_diff: int
    comment: Optional[str] = None
    occurred_at: datetime


class ArticleBase(BaseModel):
    title: str
    wiki: str = "enwiki"
    namespace: int = 0
    url: Optional[str] = None


class ArticleSummary(ArticleBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    total_edits: int = 0
    created_at: datetime
    updated_at: datetime
    recent_velocity: float = 0.0


class ArticleDetail(ArticleSummary):
    model_config = ConfigDict(from_attributes=True)

    edits: List[EditItem] = []
    unique_editors_count: int = 0
    latest_activity_score: float = 0.0
    latest_ai_summary: Optional[str] = None
