from datetime import datetime, timezone
import uuid
from typing import Any, Dict, Optional
from app.core.config import settings
from app.core.logging import logger
from app.core.metrics import metrics_collector
from app.kafka.producer import event_producer
from app.schemas.event import RawWikimediaEvent, IngestedEvent
from workers.stream_ingestor.synthetic_generator import SyntheticWikimediaGenerator
from workers.stream_ingestor.wikimedia_client import WikimediaStreamClient


class StreamIngestorService:
    """
    Dedicated Stream Ingestor:
      1. Consumes live Wikimedia SSE stream (or synthetic generator)
      2. Validates incoming schema
      3. Normalizes fields and creates unique event_id
      4. Publishes to Kafka topic 'wikimedia.recentchange' with partition key on article_title
    """

    def __init__(self, stream_mode: Optional[str] = None):
        self.stream_mode = stream_mode or settings.STREAM_MODE
        self._running = False

    def normalize_event(self, raw_data: Dict[str, Any]) -> Optional[IngestedEvent]:
        try:
            raw = RawWikimediaEvent.model_validate(raw_data)
            if not raw.title or raw.type != "edit":
                return None

            # Optional bot filtering
            if settings.FILTER_BOTS and raw.bot:
                return None

            # Calculate byte diff
            length_old = (raw.length or {}).get("old") or 0
            length_new = (raw.length or {}).get("new") or 0
            byte_diff = length_new - length_old if (length_new and length_old) else 0

            # Generate or extract event id
            meta_id = (raw.meta or {}).get("id") or str(uuid.uuid4())
            occurred_ts = raw.timestamp or int(datetime.now(timezone.utc).timestamp())
            occurred_dt = datetime.fromtimestamp(occurred_ts, tz=timezone.utc)

            revision_new = (raw.revision or {}).get("new")
            revision_old = (raw.revision or {}).get("old")

            return IngestedEvent(
                event_id=meta_id,
                event_type="wikimedia.recent_change",
                schema_version=1,
                occurred_at=occurred_dt,
                ingested_at=datetime.now(timezone.utc),
                source="wikimedia",
                wiki=raw.wiki or "enwiki",
                namespace=raw.namespace or 0,
                article_title=raw.title,
                article_url=raw.title_url,
                editor_username=raw.user or "Unknown Editor",
                is_bot=bool(raw.bot),
                is_minor=bool(raw.minor),
                revision_id=revision_new,
                parent_revision_id=revision_old,
                change_size=length_new,
                byte_diff=byte_diff,
                comment=raw.comment,
                payload={"parsedcomment": raw.parsedcomment, "server_name": raw.server_name},
            )
        except Exception as e:
            logger.warning(f"Failed normalizing raw event: {e}")
            return None

    async def run(self):
        self._running = True
        await event_producer.start()
        logger.info(f"Starting Stream Ingestor in '{self.stream_mode}' mode...")

        if self.stream_mode == "wikimedia":
            client = WikimediaStreamClient()
            stream = client.stream_events()
        else:
            generator = SyntheticWikimediaGenerator(
                events_per_second=settings.SYNTHETIC_EVENTS_PER_SECOND
            )
            stream = generator.event_stream()

        async for raw_payload in stream:
            if not self._running:
                break
            normalized = self.normalize_event(raw_payload)
            if normalized:
                metrics_collector.record_ingested(1)
                # Partition by article_title so revisions for the same article stay in-order
                await event_producer.send(
                    topic=settings.TOPIC_RECENT_CHANGE,
                    value=normalized.model_dump(mode="json"),
                    key=normalized.article_title,
                )

    def stop(self):
        self._running = False
