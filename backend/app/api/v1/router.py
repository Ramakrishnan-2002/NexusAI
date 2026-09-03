from fastapi import APIRouter
from app.api.v1.health import router as health_router
from app.api.v1.events import router as events_router
from app.api.v1.articles import router as articles_router
from app.api.v1.trends import router as trends_router
from app.api.v1.search import router as search_router
from app.api.v1.ai import router as ai_router
from app.api.v1.stream import router as stream_router
from app.api.v1.metrics import router as metrics_router

api_v1_router = APIRouter()

api_v1_router.include_router(health_router)
api_v1_router.include_router(events_router)
api_v1_router.include_router(articles_router)
api_v1_router.include_router(trends_router)
api_v1_router.include_router(search_router)
api_v1_router.include_router(ai_router)
api_v1_router.include_router(stream_router)
api_v1_router.include_router(metrics_router)
