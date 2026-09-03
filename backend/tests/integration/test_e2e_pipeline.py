from datetime import datetime, timezone
import pytest
from app.kafka.bus import global_event_bus
from app.core.config import settings
from app.schemas.ai import AIAskRequest
from app.services.ai_service import AIService
from workers.stream_ingestor.ingestor import StreamIngestorService
from workers.processor.processor import EventProcessorWorker
from workers.analytics.analytics import AnalyticsWorker
from workers.embedding.embedding_worker import EmbeddingWorker


@pytest.mark.asyncio
async def test_end_to_end_knowledge_pipeline(db_session):
    # 1. Simulate Raw Wikimedia Stream Event
    ingestor = StreamIngestorService(stream_mode="synthetic")
    raw_event = {
        "id": 888888,
        "meta": {"id": "e2e-event-101", "uri": "https://en.wikipedia.org/wiki/Superconductivity"},
        "title": "Superconductivity Breakthrough",
        "title_url": "https://en.wikipedia.org/wiki/Superconductivity",
        "comment": "Added verified replication of room temperature anomalous diamagnetism.",
        "timestamp": int(datetime.now(timezone.utc).timestamp()),
        "user": "DrQuantum",
        "bot": False,
        "minor": False,
        "length": {"old": 30000, "new": 31200},
        "revision": {"old": 1001, "new": 1002},
        "wiki": "enwiki",
        "namespace": 0,
        "type": "edit",
    }

    normalized = ingestor.normalize_event(raw_event)
    assert normalized is not None
    assert normalized.article_title == "Superconductivity Breakthrough"

    # 2. Processor Worker: Normalizes & Persists
    processor = EventProcessorWorker()
    await processor.handle_event(normalized.model_dump(mode="json"))

    # Verify event published to wikimedia.article.processed
    processed_msgs = global_event_bus.get_published_messages(settings.TOPIC_ARTICLE_PROCESSED)
    assert len(processed_msgs) >= 1
    processed_evt_data = processed_msgs[0]

    # 3. Analytics Worker: Sliding Window & Spike Evaluation
    analytics = AnalyticsWorker()
    await analytics.handle_event(processed_evt_data)

    # 4. Embedding Worker: Knowledge Chunk & pgvector indexing
    embedding_worker = EmbeddingWorker()
    await embedding_worker.handle_event(processed_evt_data)

    # Verify embedding created event emitted
    embed_msgs = global_event_bus.get_published_messages(settings.TOPIC_EMBEDDING_CREATED)
    assert len(embed_msgs) >= 1

    # 5. RAG Intelligence QA: Retrieve evidence and synthesize answer
    ask_req = AIAskRequest(
        question="What was reported regarding anomalous diamagnetism in superconductivity?",
        top_k_evidence=3,
        provider_override="mock",
    )

    qa_response = await AIService.ask_question(db_session, ask_req)
    assert qa_response.structured_analysis is not None
    assert "Superconductivity Breakthrough" in qa_response.answer or len(qa_response.citations) >= 1
    assert qa_response.total_took_ms > 0
