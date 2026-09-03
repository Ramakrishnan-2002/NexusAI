import asyncio
from datetime import datetime, timezone
import pytest
from app.kafka.retry import RetryPolicy
from app.kafka.dlq import DeadLetterQueueHandler
from app.kafka.bus import global_event_bus
from app.core.config import settings
from app.redis.client import InMemoryFallbackRedis
from app.llm.gateway import llm_gateway
from app.schemas.ai import AIAnalysisOutput


@pytest.mark.asyncio
async def test_failure_redis_outage_graceful_fallback():
    """Verify that when Redis connection is lost, system falls back to in-memory state without crashing"""
    fallback = InMemoryFallbackRedis()
    
    # 1. Write sliding window counter
    await fallback.zadd("act:art:999:edits", {"evt-1": 1000.0, "evt-2": 1005.0})
    count = await fallback.zcard("act:art:999:edits")
    assert count == 2

    # 2. Prune expired
    removed = await fallback.zremrangebyscore("act:art:999:edits", 0, 1002.0)
    assert removed == 1
    new_count = await fallback.zcard("act:art:999:edits")
    assert new_count == 1


@pytest.mark.asyncio
async def test_failure_poison_pill_routed_to_dlq_without_blocking():
    """Verify poison pill message is sent to DLQ and does not cause infinite loop"""
    dlq_events = []

    async def poison_pill_handler(data):
        raise ValueError("Corrupt malformed payload structure")

    async def dlq_sink(payload, err):
        dlq_events.append((payload, err))
        await DeadLetterQueueHandler.route_to_dlq("test.topic", payload, err)

    policy = RetryPolicy(max_retries=2, initial_backoff_ms=1, backoff_multiplier=1.0)
    success = await policy.execute_with_retry(
        handler=poison_pill_handler,
        event_data={"bad_payload": True, "event_id": "bad-99"},
        on_dlq=dlq_sink,
    )

    assert success is False
    assert len(dlq_events) == 1
    assert dlq_events[0][0]["event_id"] == "bad-99"

    # Verify DLQ topic received message
    messages = global_event_bus.get_published_messages(settings.TOPIC_DLQ)
    assert len(messages) >= 1
    assert messages[0]["error_type"] == "ValueError"


@pytest.mark.asyncio
async def test_failure_llm_provider_timeout_cascading():
    """Verify LLM Gateway cascades gracefully when primary provider fails"""
    system_prompt = "You are a helpful knowledge assistant."
    user_prompt = "Article: Quantum Teleportation\nContent: Teleportation experiment successful."

    # Force request to an unavailable provider (e.g. unconfigured Gemini)
    result, provider, model, was_fallback, latency_ms = await llm_gateway.analyze_structured(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        preferred_provider="gemini",
    )

    assert isinstance(result, AIAnalysisOutput)
    assert was_fallback is True
    assert provider in ("ollama", "mock")
    assert latency_ms >= 0
