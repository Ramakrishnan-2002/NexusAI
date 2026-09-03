from typing import Any, Dict
from app.core.config import settings
from app.core.logging import logger
from app.db.session import AsyncSessionLocal
from app.kafka.consumer import EventConsumer
from app.kafka.producer import event_producer
from app.services.ai_service import AIService


class AIAnalysisWorker:
    """
    AI Analysis Worker:
      1. Consumes 'wikimedia.trend.detected' events
      2. Gathers verified evidence context from hybrid search index
      3. Invokes LLM Gateway (Gemini / Ollama / Fallback)
      4. Saves AIAnalysis with verifiable citations to PostgreSQL
      5. Emits 'wikimedia.analysis.completed'
    """

    def __init__(self):
        self.consumer = EventConsumer(
            topics=[settings.TOPIC_TREND_DETECTED],
            group_id="ai_analyzer",
        )
        self._running = False

    async def handle_event(self, event_data: Dict[str, Any]):
        trend_id = event_data.get("trend_id")
        article_title = event_data.get("article_title")

        if not trend_id:
            return

        logger.info(f"AI Worker generating automated evidence-backed synthesis for trend #{trend_id} ('{article_title}')...")

        async with AsyncSessionLocal() as session:
            try:
                analysis = await AIService.explain_trend(session, trend_id)
                await session.commit()

                if analysis:
                    await event_producer.send(
                        topic=settings.TOPIC_ANALYSIS_COMPLETED,
                        value={
                            "trend_id": trend_id,
                            "article_title": article_title,
                            "summary": analysis.summary,
                            "importance": analysis.importance,
                            "confidence": analysis.confidence,
                        },
                        key=article_title,
                    )
            except Exception as e:
                await session.rollback()
                logger.error(f"AI Worker failed synthesizing trend #{trend_id}: {e}")
                raise e

    async def run(self):
        self._running = True
        logger.info("Starting AI Analysis Worker...")
        await event_producer.start()
        await self.consumer.consume_loop(self.handle_event)

    async def stop(self):
        self._running = False
        await self.consumer.stop()
