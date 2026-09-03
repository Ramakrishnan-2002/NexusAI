import asyncio
import json
from typing import Any, Dict, Optional
from confluent_kafka import Producer, KafkaError, KafkaException
from app.core.config import settings
from app.core.logging import logger
from app.kafka.bus import global_event_bus


class EventProducer:
    """
    High-performance Confluent Kafka Event Producer with:
      - librdkafka C-engine backing
      - Delivery callbacks for error tracking
      - Automatic fallback to InMemoryEventBus when Kafka is unavailable
      - Non-blocking async interface compatible with FastAPI and worker event loops
    """
    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers if bootstrap_servers is not None else settings.KAFKA_BOOTSTRAP_SERVERS
        self._producer: Optional[Producer] = None
        self._use_fallback = False
        self._started = False

    def _delivery_report(self, err, msg):
        """Delivery callback invoked by librdkafka once message is delivered or permanently failed"""
        if err is not None:
            logger.error(f"Kafka message delivery failed to topic '{msg.topic()}': {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    async def start(self):
        if self._started:
            return
        if not self.bootstrap_servers:
            logger.info("No KAFKA_BOOTSTRAP_SERVERS configured; using InMemoryEventBus.")
            self._use_fallback = True
            self._started = True
            return

        try:
            conf = {
                "bootstrap.servers": self.bootstrap_servers,
                "client.id": "wikipulse-producer",
                "acks": "1",
                "retries": 3,
                "retry.backoff.ms": 500,
                "linger.ms": 5,
                "queue.buffering.max.messages": 100000,
            }
            self._producer = Producer(conf)
            self._started = True
            self._use_fallback = False
            logger.info(f"Confluent Kafka producer started on {self.bootstrap_servers}")
        except Exception as e:
            logger.warning(f"Kafka unavailable ({e}); switching producer to in-memory event bus.")
            self._producer = None
            self._use_fallback = True
            self._started = True

    async def send(self, topic: str, value: Dict[str, Any], key: Optional[str] = None):
        if not self._started:
            await self.start()

        # In testing environment or fallback mode, publish to in-memory event bus for assertions
        if settings.ENVIRONMENT == "testing" or self._use_fallback or not self._producer:
            await global_event_bus.publish(topic, value, key=key)
            if self._use_fallback or not self._producer:
                return

        try:
            val_bytes = json.dumps(value, default=str).encode("utf-8")
            key_bytes = key.encode("utf-8") if key else None

            # Produce message into librdkafka's internal C-memory queue
            self._producer.produce(
                topic=topic,
                value=val_bytes,
                key=key_bytes,
                on_delivery=self._delivery_report,
            )
            # Poll(0) serves delivery callbacks without blocking
            self._producer.poll(0)
        except (KafkaException, KafkaError, BufferError) as e:
            logger.warning(f"Failed sending to Kafka topic {topic}: {e}; publishing to in-memory bus.")
            if settings.ENVIRONMENT != "testing":
                await global_event_bus.publish(topic, value, key=key)
        except Exception as e:
            logger.warning(f"Unexpected error producing to {topic}: {e}; publishing to in-memory bus.")
            if settings.ENVIRONMENT != "testing":
                await global_event_bus.publish(topic, value, key=key)

    async def flush(self, timeout: float = 3.0):
        if self._producer and not self._use_fallback:
            try:
                await asyncio.to_thread(self._producer.flush, timeout)
            except Exception as e:
                logger.error(f"Error flushing Kafka producer: {e}")

    async def stop(self):
        if self._producer and not self._use_fallback:
            try:
                await self.flush(timeout=3.0)
            except Exception:
                pass
            self._producer = None
        self._started = False


# Global singleton producer
event_producer = EventProducer()
