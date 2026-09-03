from typing import Any, Dict
from app.core.config import settings
from app.core.logging import logger
from app.core.metrics import metrics_collector
from app.db.session import AsyncSessionLocal
from app.kafka.consumer import EventConsumer
from app.kafka.producer import event_producer
from app.redis.counters import ActivityCounterService
from app.repositories.trend_repo import TrendRepository
from app.schemas.event import ProcessedArticleEvent
from workers.analytics.detector import SpikeDetector


class AnalyticsWorker:
    """
    Analytics Worker:
      1. Consumes 'wikimedia.article.processed' events
      2. Reads sliding window counters (1m, 5m, 15m) from Redis
      3. Computes transparent activity spike score and velocity
      4. Persists ActivitySnapshots and TrendEvents to PostgreSQL
      5. Updates hot article rankings in Redis
      6. Emits 'wikimedia.trend.detected' if threshold is met
    """

    def __init__(self):
        self.consumer = EventConsumer(
            topics=[settings.TOPIC_ARTICLE_PROCESSED],
            group_id="analytics",
        )
        self._running = False

    async def handle_event(self, event_data: Dict[str, Any]):
        evt = ProcessedArticleEvent.model_validate(event_data)

        # 1. Fetch sliding window metrics from Redis
        edits_1m, _ = await ActivityCounterService.get_sliding_window_metrics(evt.article_id, window_seconds=60)
        edits_5m, unique_editors = await ActivityCounterService.get_sliding_window_metrics(evt.article_id, window_seconds=300)
        edits_15m, _ = await ActivityCounterService.get_sliding_window_metrics(evt.article_id, window_seconds=900)

        # 2. Compute activity score
        score, velocity, baseline_vel, spike_mult = SpikeDetector.calculate_activity_score(
            edits_1m=edits_1m,
            edits_5m=edits_5m,
            edits_15m=edits_15m,
            unique_editors_5m=unique_editors,
            total_byte_delta=evt.byte_diff,
        )

        # 3. Update Redis hot ranking
        await ActivityCounterService.update_hot_ranking(evt.article_id, score)

        # 4. Check if activity triggers a Trend Event
        is_trend = (
            spike_mult >= settings.TREND_SPIKE_MULTIPLIER_THRESHOLD and
            edits_5m >= settings.TREND_MIN_EDITS_THRESHOLD
        )

        async with AsyncSessionLocal() as session:
            # Save periodic activity snapshot
            await TrendRepository.save_activity_snapshot(
                session=session,
                article_id=evt.article_id,
                window_seconds=300,
                edit_count=edits_5m,
                byte_delta=evt.byte_diff,
                unique_editors=unique_editors,
                bot_ratio=1.0 if evt.is_bot else 0.0,
                baseline_velocity=baseline_vel,
                spike_multiplier=spike_mult,
            )

            if is_trend:
                logger.info(
                    f"Trend / Spike Detected on '{evt.article_title}'! "
                    f"Score: {score}, Multiplier: {spike_mult}x, Velocity: {velocity} edits/min"
                )
                metrics_collector.record_trend(topic="Detected Trend")

                trend = await TrendRepository.create_or_update_trend(
                    session=session,
                    article_id=evt.article_id,
                    article_title=evt.article_title,
                    topic="Knowledge Change Spike",
                    activity_score=score,
                    edits_per_minute=velocity,
                    baseline_velocity=baseline_vel,
                    spike_multiplier=spike_mult,
                    unique_editors=unique_editors,
                    total_byte_delta=evt.byte_diff,
                    metadata_json={
                        "edits_1m": edits_1m,
                        "edits_5m": edits_5m,
                        "edits_15m": edits_15m,
                        "trigger_edit_id": evt.edit_id,
                    }
                )
                await session.commit()

                # Publish Trend Detected event to Kafka for AI analysis
                await event_producer.send(
                    topic=settings.TOPIC_TREND_DETECTED,
                    value={
                        "trend_id": trend.id,
                        "article_id": evt.article_id,
                        "article_title": evt.article_title,
                        "activity_score": score,
                        "spike_multiplier": spike_mult,
                        "velocity": velocity,
                        "detected_at": trend.first_detected_at.isoformat(),
                    },
                    key=evt.article_title,
                )
            else:
                await session.commit()

    async def run(self):
        self._running = True
        logger.info("Starting Analytics Worker...")
        await event_producer.start()
        await self.consumer.consume_loop(self.handle_event)

    async def stop(self):
        self._running = False
        await self.consumer.stop()
