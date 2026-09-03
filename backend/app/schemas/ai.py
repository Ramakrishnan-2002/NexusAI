from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class Citation(BaseModel):
    article_title: str
    revision_id: Optional[int] = None
    occurred_at: Optional[str] = None
    snippet: str
    relevance_reason: Optional[str] = None


class AIAnalysisOutput(BaseModel):
    """
    Strictly validated structured Pydantic output produced by LLM Gateway
    """
    summary: str = Field(..., description="Concise explanation of the knowledge activity or answer")
    importance: str = Field("medium", description="critical, high, medium, or low")
    detected_topic: str = Field("General", description="Topic/category identified")
    change_type: str = Field("factual_update", description="breaking_development, factual_update, expansion, controversy, vandalism_cleanup, or minor_tweak")
    reasoning: str = Field(..., description="Evidence-backed rationale explaining why this activity is occurring")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score backed by retrieved evidence")
    evidence_points: List[str] = Field(default_factory=list, description="Bullet points of concrete retrieved facts")
    citations: List[Citation] = Field(default_factory=list, description="Explicit source citations")


class AIAskRequest(BaseModel):
    question: str = Field(..., min_length=3, max_length=1000, description="Natural language question to ask the knowledge base")
    top_k_evidence: int = Field(5, ge=1, le=15, description="Number of evidence chunks to retrieve for RAG context")
    model_override: Optional[str] = None
    provider_override: Optional[str] = None


class AIAskResponse(BaseModel):
    question: str
    answer: str
    structured_analysis: AIAnalysisOutput
    citations: List[Citation]
    provider_used: str
    model_used: str
    was_fallback: bool = False
    retrieval_took_ms: float
    llm_took_ms: float
    total_took_ms: float


class AIAnalyzeTrendRequest(BaseModel):
    trend_id: int
    force_refresh: bool = False
