from fastapi import APIRouter, HTTPException, Path, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import DB
from app.services.trend_service import TrendService
from app.services.ai_service import AIService
from app.schemas.trend import TrendEventSchema, TrendListResponse
from app.schemas.ai import AIAnalysisOutput

router = APIRouter(prefix="/trends", tags=["Activity Trends & Spikes"])


@router.get("", response_model=TrendListResponse)
async def list_trends(
    limit: int = Query(20, ge=1, le=100),
    min_score: float = Query(0.0, ge=0.0),
    db: AsyncSession = DB,
):
    """Retrieve active unusual activity spikes and emerging topic trends."""
    return await TrendService.list_active_trends(db, limit=limit, min_score=min_score)


@router.get("/{trend_id}", response_model=TrendEventSchema)
async def get_trend_detail(
    trend_id: int = Path(..., ge=1),
    db: AsyncSession = DB,
):
    """Get detailed telemetry for a specific activity spike."""
    trend = await TrendService.get_trend_by_id(db, trend_id)
    if not trend:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Trend event with ID {trend_id} not found.",
        )
    return trend


@router.post("/{trend_id}/analyze", response_model=AIAnalysisOutput)
async def analyze_trend(
    trend_id: int = Path(..., ge=1),
    db: AsyncSession = DB,
):
    """Trigger AI Gateway explanation and synthesis for an activity spike."""
    analysis = await AIService.explain_trend(db, trend_id)
    if not analysis:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Trend event with ID {trend_id} not found.",
        )
    return analysis
