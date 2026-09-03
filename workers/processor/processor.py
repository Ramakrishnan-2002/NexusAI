from datetime import datetime, timezone
from typing import Any, Dict
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from app.core.config import settings
from app.core.logging import logger
from app.core.metrics import metrics_collector
from app.db.session import AsyncSessionLocal
from app.kafka.consumer import EventConsumer
from app.kafka.producer import event_producer
from app.models.processing_job import ProcessingJob
from app.repositories.article_repo import ArticleRepository
from app.repositories.edit_repo import EditRepository
from app.schemas.event import IngestedEvent, ProcessedArticleEvent
from app.redis.counters import ActivityCounterService


class EventProcessorWorker:
    """
    Processor Worker:
      1. Consumes 'wikimedia.recentchange' from Kafka
      2. Ensures strict application-level idempotency via ProcessingJob
      3. Handles concurrent deduplication races via IntegrityError catching
      4. Normalizes & persists Article, Editor, and Edit entities in PostgreSQL
      5. Updates low-latency Redis sliding window counters
      6. Publishes 'wikimedia.article.processed' for downstream Analytics & Embedding workers
    """

    def __init__(self):
        self.consumer = EventConsumer(
            topics=[settings.TOPIC_RECENT_CHANGE],
            group_id="processor",
        )
        self._running = False

    async def handle_event(self, event_data: Dict[str, Any]):
        event = IngestedEvent.model_validate(event_data)
        idempotency_key = f"proc:{event.event_id}"

        async with AsyncSessionLocal() as session:
            # 1. Check if already completed
            stmt = select(ProcessingJob).where(ProcessingJob.idempotency_key == idempotency_key)
            result = await session.execute(stmt)
            job = result.scalar_one_or_none()

            if job and job.status == "completed":
                logger.debug(f"Event {event.event_id} already processed. Skipping safely.")
                return

            if job is None:
                try:
                    job = ProcessingJob(
                        idempotency_key=idempotency_key,
                        job_type="event_process",
                        status="in_progress",
                        payload_json={"event_id": event.event_id, "title": event.article_title},
                    )
                    session.add(job)
                    await session.flush()
                except IntegrityError:
                    # Concurrent worker already claimed this idempotency_key
                    await session.rollback()
                    logger.debug(f"Idempotency collision on {idempotency_key}. Handled safely as duplicate skip.")
                    return

            try:
                # 2. Persist Article & Editor
                article = await ArticleRepository.get_or_create_article(
                    session=session,
                    title=event.article_title,
                    wiki=event.wiki,
                    namespace=event.namespace,
                    url=event.article_url,
                )

                await ArticleRepository.get_or_create_editor(
                    session=session,
                    username=event.editor_username,
                    is_bot=event.is_bot,
                )

                # 3. Persist Edit revision
                edit = await EditRepository.create_edit(
                    session=session,
                    event=event,
                    article_id=article.id,
                )

                # 4. Mark processing job completed
                job.status = "completed"
                job.completed_at = datetime.now(timezone.utc)
                await session.commit()

                # 5. Record hot sliding window state in Redis
                await ActivityCounterService.record_article_edit(
                    article_id=article.id,
                    event_id=event.event_id,
                    editor=event.editor_username,
                    timestamp=event.occurred_at.timestamp(),
                )

                metrics_collector.record_processed(wiki=event.wiki, namespace=str(event.namespace))

                # 6. Publish downstream ProcessedArticleEvent with correlation key
                processed_evt = ProcessedArticleEvent(
                    event_id=event.event_id,
                    article_id=article.id,
                    article_title=article.title,
                    wiki=article.wiki,
                    namespace=article.namespace,
                    edit_id=edit.id,
                    revision_id=event.revision_id,
                    editor_username=event.editor_username,
                    byte_diff=event.byte_diff,
                    is_minor=event.is_minor,
                    is_bot=event.is_bot,
                    comment=event.comment,
                    occurred_at=event.occurred_at,
                )

                await event_producer.send(
                    topic=settings.TOPIC_ARTICLE_PROCESSED,
                    value=processed_evt.model_dump(mode="json"),
                    key=article.title,
                )

            except Exception as e:
                await session.rollback()
                try:
                    async with AsyncSessionLocal() as err_session:
                        err_job = await err_session.get(ProcessingJob, job.id)
                        if err_job:
                            err_job.status = "failed"
                            err_job.retries += 1
                            err_job.error_message = str(e)
                            await err_session.commit()
                except Exception:
                    pass
                raise e

    async def run(self):
        self._running = True
        logger.info("Starting Processor Worker...")
        await event_producer.start()
        await self.consumer.consume_loop(self.handle_event)

    async def stop(self):
        self._running = False
        await self.consumer.stop()
