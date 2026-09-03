import pytest
from app.kafka.retry import RetryPolicy
from app.kafka.dlq import DeadLetterQueueHandler
from app.kafka.bus import global_event_bus
from app.core.config import settings


@pytest.mark.asyncio
async def test_retry_policy_transient_failure_then_success():
    attempts = 0

    async def flaky_handler(data):
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise ValueError("Transient network glitch")
        return "success"

    policy = RetryPolicy(max_retries=3, initial_backoff_ms=10, backoff_multiplier=1.5)
    success = await policy.execute_with_retry(
        handler=flaky_handler,
        event_data={"event_id": "test-flaky"},
    )

    assert success is True
    assert attempts == 2


@pytest.mark.asyncio
async def test_retry_policy_exhaustion_routes_to_dlq():
    dlq_received = []

    async def always_failing_handler(data):
        raise RuntimeError("Permanent database corruption")

    async def dlq_callback(payload, err):
        dlq_received.append((payload, err))
        await DeadLetterQueueHandler.route_to_dlq("test.topic", payload, err)

    policy = RetryPolicy(max_retries=2, initial_backoff_ms=10, backoff_multiplier=1.0)
    success = await policy.execute_with_retry(
        handler=always_failing_handler,
        event_data={"event_id": "test-poison-pill"},
        on_dlq=dlq_callback,
    )

    assert success is False
    assert len(dlq_received) == 1
    assert dlq_received[0][0]["event_id"] == "test-poison-pill"

    # Verify message sent to DLQ topic in event bus
    dlq_messages = global_event_bus.get_published_messages(settings.TOPIC_DLQ)
    assert len(dlq_messages) >= 1
    assert dlq_messages[0]["error_type"] == "RuntimeError"
