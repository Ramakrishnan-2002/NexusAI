from fastapi import APIRouter, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from app.core.metrics import metrics_collector

router = APIRouter(prefix="/metrics", tags=["Observability & Metrics"])


@router.get("", response_model=dict)
async def get_system_metrics():
    """Returns real-time in-process metrics snapshot for dashboard telemetry."""
    return metrics_collector.get_snapshot()


@router.get("/prometheus")
async def get_prometheus_metrics():
    """Prometheus exposition format metrics for Prometheus scrapers."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
