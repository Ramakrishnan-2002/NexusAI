from contextlib import asynccontextmanager
import os
import time
import uuid
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app.core.config import settings
from app.core.logging import logger
from app.core.metrics import API_REQUEST_DURATION, metrics_collector
from app.core.security import global_rate_limiter
from app.db.session import init_db_models
from app.kafka.producer import event_producer
from app.redis.client import redis_manager
from app.api.v1.router import api_v1_router
from app.api.v1.health import router as health_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info(f"Starting {settings.PROJECT_NAME} Control Plane (Environment: {settings.ENVIRONMENT})")
    await init_db_models()
    await event_producer.start()
    yield
    # Shutdown
    logger.info("Gracefully shutting down WikiPulse Control Plane...")
    await event_producer.stop()
    await redis_manager.close()
    logger.info("WikiPulse shutdown complete.")


app = FastAPI(
    title=settings.PROJECT_NAME,
    description="Real-Time Knowledge Change Intelligence Platform Control Plane API",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_observability_and_rate_limit(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    client_ip = request.client.host if request.client else "127.0.0.1"

    # Rate limiting check (exclude docs, static assets & health probes)
    path = request.url.path
    if not (path.startswith("/docs") or path.startswith("/openapi") or path.startswith("/health") or path.startswith("/livez") or path.startswith("/readyz") or path.startswith("/api/v1/health") or path.startswith("/api/v1/livez") or path.startswith("/api/v1/readyz")):
        is_allowed = await global_rate_limiter.is_allowed(client_ip)
        if not is_allowed:
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "error": {
                        "code": "RATE_LIMIT_EXCEEDED",
                        "message": "Too many requests. Please throttle your queries.",
                        "request_id": request_id,
                    }
                },
            )

    start_time = time.time()
    try:
        response = await call_next(request)
        duration = time.time() - start_time
        metrics_collector.record_api_latency(duration * 1000)

        # Record Prometheus metric
        API_REQUEST_DURATION.labels(
            endpoint=path,
            method=request.method,
            status_code=response.status_code,
        ).observe(duration)

        response.headers["X-Request-ID"] = request_id
        response.headers["X-Response-Time-Ms"] = str(round(duration * 1000, 2))
        return response

    except Exception as exc:
        duration = time.time() - start_time
        logger.error(f"Unhandled Exception on {request.method} {path}: {exc}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": {
                    "code": "INTERNAL_SERVER_ERROR",
                    "message": "An unexpected error occurred processing your request.",
                    "request_id": request_id,
                }
            },
        )


# Top-level Health Probes (/livez, /readyz, /health)
app.include_router(health_router)

# Include API v1 Router (/api/v1/...)
app.include_router(api_v1_router, prefix=settings.API_V1_STR)

# Mount Frontend static files if directory exists
frontend_dir = os.path.join(os.path.dirname(__file__), "..", "..", "frontend", "src")
if os.path.isdir(frontend_dir):
    app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")
