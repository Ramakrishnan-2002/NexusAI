from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=500, description="Natural language or keyword search query")
    top_k: int = Field(10, ge=1, le=50, description="Number of results to return")
    search_type: str = Field("hybrid", description="'hybrid', 'vector', or 'keyword'")
    wiki: Optional[str] = Field(None, description="Optional wiki filter e.g. enwiki")
    namespace: Optional[int] = Field(None, description="Optional namespace filter")
    min_score: Optional[float] = Field(0.0, description="Minimum relevance threshold")


class SearchResultItem(BaseModel):
    chunk_id: int
    article_id: int
    article_title: str
    revision_id: Optional[int] = None
    title: str
    content: str
    score: float = Field(..., description="Normalized composite relevance score [0-1]")
    vector_score: Optional[float] = None
    keyword_score: Optional[float] = None
    occurred_at: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SearchResponse(BaseModel):
    query: str
    search_type: str
    total_found: int
    took_ms: float
    results: List[SearchResultItem]
