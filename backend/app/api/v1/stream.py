import asyncio
import json
from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse
from app.kafka.bus import global_event_bus
from app.core.config import settings
from app.core.logging import logger

router = APIRouter(prefix="/stream", tags=["Live Real-Time Streaming"])


@router.get("/live")
async def stream_live_events(request: Request):
    """
    Server-Sent Events (SSE) endpoint providing real-time feed
    of processed Wikipedia events and trend alerts to connected dashboards.
    Enforces bounded buffers and graceful unsubscribe on disconnect.
    """
    async def event_generator():
        q = global_event_bus.register_subscriber(settings.TOPIC_ARTICLE_PROCESSED)
        try:
            while True:
                if await request.is_disconnected():
                    logger.debug("SSE client disconnected, terminating stream generator.")
                    break
                try:
                    # Wait for next live event with a 15-second heartbeat timeout
                    msg = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield {
                        "event": "recent_change",
                        "data": json.dumps(msg, default=str),
                    }
                except asyncio.TimeoutError:
                    # Periodic heartbeat to keep proxy / load balancer connections alive
                    yield {
                        "event": "ping",
                        "data": json.dumps({"status": "heartbeat"}),
                    }
                except asyncio.CancelledError:
                    break
        finally:
            global_event_bus.unregister_subscriber(settings.TOPIC_ARTICLE_PROCESSED, q)

    return EventSourceResponse(
        event_generator(),
        ping=15,
        media_type="text/event-stream",
    )
