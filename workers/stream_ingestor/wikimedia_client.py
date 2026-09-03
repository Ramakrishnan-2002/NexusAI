import asyncio
import json
from typing import AsyncGenerator, Dict, Any, Optional
import httpx
from app.core.config import settings
from app.core.logging import logger


class WikimediaStreamClient:
    """
    Long-running client connecting to the public Wikimedia SSE stream
    (https://stream.wikimedia.org/v2/stream/recentchange) with automatic reconnection.
    """

    def __init__(
        self,
        url: Optional[str] = None,
        user_agent: Optional[str] = None,
        initial_backoff: float = 1.0,
        max_backoff: float = 60.0,
    ):
        self.url = url or settings.WIKIMEDIA_STREAM_URL
        self.user_agent = user_agent or settings.WIKIMEDIA_USER_AGENT
        self.initial_backoff = initial_backoff
        self.max_backoff = max_backoff
        self._running = True

    async def stream_events(self) -> AsyncGenerator[Dict[str, Any], None]:
        backoff = self.initial_backoff
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "text/event-stream",
        }

        while self._running:
            try:
                logger.info(f"Connecting to Wikimedia recentchange stream at {self.url}...")
                async with httpx.AsyncClient(timeout=None, headers=headers) as client:
                    async with client.stream("GET", self.url) as response:
                        if response.status_code != 200:
                            logger.error(f"Wikimedia SSE stream returned status {response.status_code}")
                            raise RuntimeError(f"HTTP {response.status_code}")

                        logger.info("Connected to Wikimedia recentchange SSE stream successfully.")
                        backoff = self.initial_backoff  # reset backoff on success

                        async for line in response.aiter_lines():
                            if not self._running:
                                break
                            if not line or line.startswith(":"):
                                continue  # Keep-alive comment
                            if line.startswith("data:"):
                                data_str = line[5:].strip()
                                try:
                                    payload = json.loads(data_str)
                                    yield payload
                                except Exception as json_err:
                                    logger.warning(f"Malformed Wikimedia SSE line: {json_err}")

            except Exception as e:
                if self._running:
                    logger.warning(f"Wikimedia SSE stream connection lost: {e}. Reconnecting in {backoff:.1f}s...")
                    await asyncio.sleep(backoff)
                    backoff = min(self.max_backoff, backoff * 2.0)

    def stop(self):
        self._running = False
