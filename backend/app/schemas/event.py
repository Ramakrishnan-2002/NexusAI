from datetime import datetime, timezone
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, ConfigDict


class RawWikimediaEvent(BaseModel):
    """
    Schema for raw un-normalized SSE Wikimedia event payload.
    Tolerates optional/missing fields and schema evolution from Wikimedia.
    """
    model_config = ConfigDict(extra="ignore")

    id: Optional[int] = None
    meta: Optional[Dict[str, Any]] = None
    title: Optional[str] = None
    title_url: Optional[str] = None
    comment: Optional[str] = None
    timestamp: Optional[int] = None
    user: Optional[str] = None
    bot: Optional[bool] = False
    minor: Optional[bool] = False
    patrolled: Optional[bool] = None
    length: Optional[Dict[str, Optional[int]]] = None
    revision: Optional[Dict[str, Optional[int]]] = None
    server_name: Optional[str] = None
    server_url: Optional[str] = None
    server_script_path: Optional[str] = None
    wiki: Optional[str] = "enwiki"
    parsedcomment: Optional[str] = None
    namespace: Optional[int] = 0
    type: Optional[str] = "edit"


class IngestedEvent(BaseModel):
    """
    Standardized, normalized, versioned event published by stream-ingestor to Kafka.
    """
    model_config = ConfigDict(extra="ignore")

    event_id: str = Field(..., description="Unique event identifier (UUID or meta.id)")
    event_type: str = Field("wikimedia.recent_change", description="Type of event")
    schema_version: int = Field(1, description="Schema version for compatibility")
    occurred_at: datetime = Field(..., description="Timestamp when event occurred on Wikipedia")
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Ingestion timestamp")
    source: str = Field("wikimedia", description="Event source")
    wiki: str = Field("enwiki", description="Wiki domain e.g. enwiki, dewiki")
    namespace: int = Field(0, description="Page namespace (0 = main article)")
    article_title: str = Field(..., description="Article title")
    article_url: Optional[str] = None
    editor_username: str = Field(..., description="User or IP address of the editor")
    is_bot: bool = Field(False, description="Whether the editor is a registered bot")
    is_minor: bool = Field(False, description="Whether the edit was marked minor")
    revision_id: Optional[int] = Field(None, description="New revision ID")
    parent_revision_id: Optional[int] = Field(None, description="Parent/old revision ID")
    change_size: int = Field(0, description="New total page size in bytes")
    byte_diff: int = Field(0, description="Difference in byte size (+/-)")
    comment: Optional[str] = Field(None, description="Edit summary comment")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Original raw event payload excerpt")


class ProcessedArticleEvent(BaseModel):
    """
    Event emitted after basic normalization and relational persistence.
    """
    event_id: str
    article_id: int
    article_title: str
    wiki: str
    namespace: int
    edit_id: int
    revision_id: Optional[int]
    editor_username: str
    byte_diff: int
    is_minor: bool
    is_bot: bool
    comment: Optional[str]
    occurred_at: datetime
    processed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
