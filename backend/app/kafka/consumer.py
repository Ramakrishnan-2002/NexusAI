import asyncio
import json
from typing import Any, Callable, Dict, List, Optional
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from app.core.config import settings
from app.core.logging import logger
from app.kafka.bus import global_event_bus
from app.kafka.retry import RetryPolicy
from app.kafka.dlq import DeadLetterQueueHandler


class EventConsumer:
    """
    High-performance Confluent Kafka Event Consumer with:
      - librdkafka C-engine backing
      - Non-blocking asyncio execution using worker thread polling
      - At-least-once delivery semantics via explicit manual offset commits
      - Automatic fallback to InMemoryEventBus
      - Retry policy and Dead Letter Queue (DLQ) routing
    """
    def __init__(
        self,
        topics: List[str],
        group_id: str,
        bootstrap_servers: Optional[str] = None,
        max_poll_records: int = 50,
    ):
        self.topics = topics
        self.group_id = f"{settings.KAFKA_CONSUMER_GROUP_PREFIX}.{group_id}"
        self.bootstrap_servers = bootstrap_servers if bootstrap_servers is not None else settings.KAFKA_BOOTSTRAP_SERVERS
        self.max_poll_records = max_poll_records
        self._consumer: Optional[Consumer] = None
        self._use_fallback = False
        self._running = False
        self._retry_policy = RetryPolicy()
        self._fallback_queues: List[asyncio.Queue] = []

    def _on_assign(self, consumer, partitions):
        logger.info(f"Consumer group '{self.group_id}' partitions assigned: {[f'{p.topic}[{p.partition}]' for p in partitions]}")

    def _on_revoke(self, consumer, partitions):
        logger.info(f"Consumer group '{self.group_id}' partitions revoked: {[f'{p.topic}[{p.partition}]' for p in partitions]}")

    async def start(self):
        self._running = True
        if not self.bootstrap_servers:
            logger.info("No KAFKA_BOOTSTRAP_SERVERS configured; falling back to InMemoryEventBus.")
            self._use_fallback = True
            for topic in self.topics:
                q = global_event_bus.register_subscriber(topic)
                self._fallback_queues.append(q)
            return

        try:
            conf = {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": self.group_id,
                "auto.offset.reset": settings.KAFKA_AUTO_OFFSET_RESET,
                "enable.auto.commit": False,  # Manual commit after successful processing
                "session.timeout.ms": 45000,
                "max.poll.interval.ms": 300000,
                "enable.partition.eof": False,
            }
            self._consumer = Consumer(conf)
            self._consumer.subscribe(
                self.topics,
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
            )
            logger.info(f"Confluent Kafka consumer subscribed to {self.topics} (group: {self.group_id})")
        except Exception as e:
            logger.warning(f"Kafka consumer connection failed ({e}); falling back to InMemoryEventBus.")
            self._consumer = None
            self._use_fallback = True
            for topic in self.topics:
                q = global_event_bus.register_subscriber(topic)
                self._fallback_queues.append(q)

    async def consume_loop(self, handler: Callable[[Dict[str, Any]], Any]):
        """Continuous event consumption loop"""
        if not self._running:
            await self.start()

        if self._use_fallback or not self._consumer:
            await self._consume_fallback(handler)
        else:
            await self._consume_kafka(handler)

    async def _consume_fallback(self, handler: Callable[[Dict[str, Any]], Any]):
        logger.info(f"Consuming events from InMemoryEventBus for topics {self.topics}")
        while self._running:
            for q in self._fallback_queues:
                try:
                    while not q.empty():
                        msg = q.get_nowait()
                        await self._process_single(msg, handler, topic=self.topics[0])
                except Exception as e:
                    logger.error(f"Fallback queue error: {e}")
            await asyncio.sleep(0.05)

    async def _consume_kafka(self, handler: Callable[[Dict[str, Any]], Any]):
        logger.info(f"Consuming events from Kafka topics {self.topics}")
        while self._running:
            try:
                # Poll message off the asyncio main loop to avoid blocking FastAPI / event loops
                msg = await asyncio.to_thread(self._consumer.poll, 1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Kafka consumer error: {msg.error()}")
                    await asyncio.sleep(0.5)
                    continue

                # Deserialize payload
                val_bytes = msg.value()
                try:
                    payload = json.loads(val_bytes.decode("utf-8")) if val_bytes else {}
                except Exception as parse_err:
                    logger.error(f"Failed deserializing Kafka message: {parse_err}")
                    payload = {"raw_payload": str(val_bytes), "corrupted": True}

                # Process single with retry & DLQ routing
                success = await self._process_single(payload, handler, topic=msg.topic())
                
                # Commit offset after processing (whether success or routed to DLQ)
                if self._consumer and self._running:
                    await asyncio.to_thread(self._consumer.commit, msg, asynchronous=False)

            except Exception as e:
                if self._running:
                    logger.error(f"Kafka consumer polling exception: {e}")
                    await asyncio.sleep(1.0)

    async def _process_single(self, value: Dict[str, Any], handler: Callable, topic: str) -> bool:
        async def dlq_callback(payload, err):
            await DeadLetterQueueHandler.route_to_dlq(topic, payload, err)

        return await self._retry_policy.execute_with_retry(
            handler=handler,
            event_data=value,
            on_dlq=dlq_callback,
        )

    async def stop(self):
        self._running = False
        if self._consumer and not self._use_fallback:
            try:
                await asyncio.to_thread(self._consumer.close)
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
            self._consumer = None
