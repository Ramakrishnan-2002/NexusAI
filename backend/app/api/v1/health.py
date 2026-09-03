from datetime import datetime, timezone
from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import DB
from app.redis.client import get_redis
from app.schemas.metrics import HealthResponse, ReadinessResponse

router = APIRouter(tags=["Health & Probes"])


@router.get("/livez", response_model=HealthResponse)
@router.get("/health", response_model=HealthResponse)
async def liveness_probe():
    """
    Liveness probe: verifies that the FastAPI process is alive and responsive.
    MUST NOT depend on external infrastructure (DB, Redis) to prevent restart loops.
    """
    return HealthResponse(
        status="healthy",
        service="WikiPulse Control Plane",
        version="1.0.0",
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@router.get("/readyz", response_model=ReadinessResponse)
@router.get("/ready", response_model=ReadinessResponse)
async def readiness_probe(db: AsyncSession = DB):
    """
    Readiness probe: validates that critical backing infrastructure
    (PostgreSQL and Redis) is reachable and ready to serve live traffic.
    """
    db_ok = False
    redis_ok = False
    details: Dict[str, Any] = {}

    # 1. Check Database connection
    try:
        await db.execute(text("SELECT 1"))
        db_ok = True
        details["database"] = "connected"
    except Exception as e:
        details["database"] = f"error: {str(e)}"

    # 2. Check Redis connection
    try:
        r = await get_redis()
        await r.ping()
        redis_ok = True
        details["redis"] = "connected"
    except Exception as e:
        details["redis"] = f"error: {str(e)}"

    # Core is considered ready if durable DB is responsive
    is_ready = db_ok

    if not is_ready:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "ready": False,
                "components": {"database": db_ok, "redis": redis_ok},
                "details": details,
            }
        )

    return ReadinessResponse(
        ready=is_ready,
        components={"database": db_ok, "redis": redis_ok},
        details=details,
    )
