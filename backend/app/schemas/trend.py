from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field


class TrendEventSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    article_id: int
    article_title: str
    topic: str
    activity_score: float = Field(..., description="Weighted unusual activity score")
    edits_per_minute: float = Field(..., description="Current edit velocity")
    baseline_velocity: float = Field(..., description="Calculated baseline edit velocity")
    spike_multiplier: float = Field(..., description="Ratio of current velocity to baseline")
    unique_editors: int = Field(..., description="Count of distinct contributors in window")
    total_byte_delta: int = Field(0, description="Cumulative content delta")
    status: str = Field("active", description="active, cooled_down, or archived")
    first_detected_at: datetime
    peak_at: Optional[datetime] = None
    ai_summary: Optional[str] = None
    ai_importance: Optional[str] = None
    metadata_json: Dict[str, Any] = Field(default_factory=dict)


class TrendListResponse(BaseModel):
    total: int
    trends: List[TrendEventSchema]
