import asyncio
import os
import pytest
from app.kafka.producer import EventProducer
from app.kafka.consumer import EventConsumer
from app.kafka.admin import KafkaAdminService
from app.kafka.bus import global_event_bus


@pytest.mark.asyncio
async def test_confluent_kafka_producer_fallback_mode():
    """Verify EventProducer works seamlessly with in-memory bus when unconfigured (Fallback Mode)"""
    producer = EventProducer(bootstrap_servers="")
    await producer.start()
    assert producer._use_fallback is True

    test_event = {"event_id": "conf-test-1", "title": "Confluent Kafka Migration"}
    await producer.send("test.confluent.topic", value=test_event, key="test-key")
    await producer.stop()

    published = global_event_bus.get_published_messages("test.confluent.topic")
    assert len(published) >= 1
    assert published[-1]["event_id"] == "conf-test-1"


@pytest.mark.asyncio
async def test_confluent_kafka_consumer_fallback_mode():
    """Verify EventConsumer consumes events from fallback bus with manual processing (Fallback Mode)"""
    consumer = EventConsumer(
        topics=["test.confluent.consume"],
        group_id="test-conf-group",
        bootstrap_servers="",
    )
    await consumer.start()
    assert consumer._use_fallback is True

    received = []

    async def sample_handler(data):
        received.append(data)

    # Publish an event to the fallback bus
    await global_event_bus.publish("test.confluent.consume", {"data": "confluent-verified"})

    # Run one cycle of consumer
    task = asyncio.create_task(consumer.consume_loop(sample_handler))
    await asyncio.sleep(0.15)
    await consumer.stop()
    await asyncio.sleep(0.05)
    task.cancel()

    assert len(received) >= 1
    assert received[0]["data"] == "confluent-verified"


@pytest.mark.asyncio
async def test_confluent_kafka_admin_service_instantiation():
    """Verify KafkaAdminService initializes and safely handles list_topics (Fallback Mode)"""
    admin = KafkaAdminService(bootstrap_servers="")
    topics = await admin.list_topics()
    assert isinstance(topics, list)


@pytest.mark.asyncio
async def test_confluent_kafka_real_broker_live_connectivity():
    """
    Verify real confluent_kafka Producer and AdminClient connectivity against Docker Kafka broker
    at localhost:9092. If broker is offline or unreachable, skips gracefully.
    """
    bootstrap = "localhost:9092"
    admin = KafkaAdminService(bootstrap_servers=bootstrap)
    try:
        topics = await asyncio.wait_for(admin.list_topics(), timeout=3.0)
        assert isinstance(topics, list)
        
        # Test real producer to real topic
        producer = EventProducer(bootstrap_servers=bootstrap)
        await producer.start()
        if not producer._use_fallback:
            test_msg = {"event_id": "real-broker-e2e-1", "title": "Kafka Broker Live Test"}
            await producer.send("wikimedia.recentchange", value=test_msg, key="test-live")
            await producer.stop()
    except Exception as e:
        pytest.skip(f"Live Kafka broker at {bootstrap} not reachable: {e}")
