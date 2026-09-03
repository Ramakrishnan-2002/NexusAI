import asyncio
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional
from app.core.config import settings
from app.core.logging import logger


class RetryPolicy:
    """
    Manages exponential backoff and retry tracking for event consumer handlers.
    """
    def __init__(self, max_retries: Optional[int] = None, initial_backoff_ms: Optional[int] = None, backoff_multiplier: float = 2.0):
        self.max_retries = max_retries if max_retries is not None else settings.KAFKA_RETRIES
        self.initial_backoff_ms = initial_backoff_ms if initial_backoff_ms is not None else settings.KAFKA_RETRY_BACKOFF_MS
        self.backoff_multiplier = backoff_multiplier

    async def execute_with_retry(
        self,
        handler: Callable[..., Any],
        event_data: Dict[str, Any],
        on_dlq: Optional[Callable[[Dict[str, Any], Exception], Any]] = None,
    ) -> bool:
        """
        Executes handler with exponential backoff on failure.
        Routes to DLQ if max retries are exhausted.
        """
        retries = 0
        current_backoff = self.initial_backoff_ms / 1000.0

        while retries <= self.max_retries:
            try:
                await handler(event_data)
                return True
            except Exception as e:
                retries += 1
                logger.warning(f"Handler failed on attempt {retries}/{self.max_retries + 1}: {e}")
                if retries <= self.max_retries:
                    await asyncio.sleep(current_backoff)
                    current_backoff *= self.backoff_multiplier
                else:
                    logger.error(f"Exhausted retries ({self.max_retries}) for event: {event_data.get('event_id')}")
                    if on_dlq:
                        await on_dlq(event_data, e)
                    return False
        return False
