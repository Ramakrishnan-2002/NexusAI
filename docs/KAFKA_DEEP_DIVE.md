# WikiPulse / NexusAI — Apache Kafka & confluent-kafka Architecture

This document provides a deep dive into the Apache Kafka streaming architecture, `confluent-kafka` (librdkafka) client integration, partition strategies, and failure behaviors in WikiPulse.

---

## 1. Topic Topology & Event Routing

| Topic Name | Partitions | Key Strategy | Emitted By | Consumed By |
| :--- | :---: | :--- | :--- | :--- |
| `wikimedia.recentchange` | 3 | `article_title` | `stream-ingestor` | `processor-worker`, `analytics-worker` |
| `wikimedia.article.processed` | 3 | `article_title` | `processor-worker` | `embedding-worker` |
| `wikimedia.trend.detected` | 3 | `article_title` | `analytics-worker` | `ai-worker` |
| `wikimedia.embedding.created`| 3 | `article_title` | `embedding-worker` | Downstream indexing listeners |
| `wikimedia.dlq` | 1 | `event_id` | `RetryPolicy` / `DLQHandler` | Ops / Dead Letter Consumer |

### Partition Key Total Ordering Guarantee
Keying by `article_title` guarantees that all edit revisions for a specific Wikipedia article land on the same Kafka partition, ensuring strict chronological per-article processing order without cross-thread race conditions.

---

## 2. The `confluent-kafka` (librdkafka) Asyncio Bridge

`confluent-kafka` is backed by the native C `librdkafka` engine. Because it is a synchronous C-extension, WikiPulse implements a non-blocking bridge to Python's single-threaded `asyncio` event loop:

### 2.1 Producer Bridge (Non-Blocking C Enqueue)
```python
# backend/app/kafka/producer.py
self._producer.produce(
    topic=topic,
    key=key.encode("utf-8") if key else None,
    value=value_json.encode("utf-8"),
    on_delivery=self._delivery_report,
)
self._producer.poll(0)  # Dispatches completed delivery callbacks
```
- `produce()` appends messages directly to librdkafka's C-memory buffer in ~1µs without socket blocking.
- `linger.ms: 5` enables micro-batching in C memory without latency penalty.

### 2.2 Consumer Bridge (Thread-Isolated Polling)
```python
# backend/app/kafka/consumer.py
msg = await asyncio.to_thread(self._consumer.poll, 1.0)
```
- Delegating `consumer.poll()` to `asyncio.to_thread` releases the Python GIL during the 1.0s network wait.
- The Python asyncio event loop continues serving incoming FastAPI HTTP requests and worker coroutines uninterrupted.

---

## 3. Offset Commit Ordering & Delivery Semantics

WikiPulse enforces **strict at-least-once delivery**:
1. `enable.auto.commit = False` is configured on all consumers.
2. The consumer polls an event from Kafka.
3. The processor worker executes the PostgreSQL transaction and commits to disk.
4. The Redis sliding-window counter is updated.
5. The consumer explicitly commits the offset via `await asyncio.to_thread(self._consumer.commit, msg, asynchronous=False)`.

```text
Kafka Message ──► Schema Validation ──► PostgreSQL Commit ──► Redis Update ──► Kafka Offset Commit
```
If a worker container crashes before Step 5, Kafka reassigns the uncommitted offset to a surviving replica during consumer group rebalance. The surviving worker skips database insertion via PostgreSQL `idempotency_key` deduplication and advances the Kafka offset.
