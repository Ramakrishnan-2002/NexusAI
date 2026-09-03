from datetime import datetime, timezone
import traceback
from typing import Any, Dict
from app.core.config import settings
from app.core.logging import logger
from app.core.metrics import metrics_collector
from app.kafka.producer import event_producer


class DeadLetterQueueHandler:
    """
    Publishes failed or unparseable messages to the Dead Letter Queue (DLQ) topic
    with enriched error metadata, stack traces, and failure timestamps.
    """

    @staticmethod
    async def route_to_dlq(topic: str, raw_payload: Any, error: Exception, context: Dict[str, Any] = None):
        metrics_collector.record_dlq()
        dlq_event = {
            "original_topic": topic,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "error_type": type(error).__name__,
            "error_message": str(error),
            "stack_trace": traceback.format_exc(),
            "context": context or {},
            "raw_payload": raw_payload,
        }

        logger.error(f"Routing failed event to DLQ ({settings.TOPIC_DLQ}): {error}")
        try:
            await event_producer.send(settings.TOPIC_DLQ, dlq_event)
        except Exception as e:
            logger.critical(f"Failed to publish to DLQ topic: {e}")
