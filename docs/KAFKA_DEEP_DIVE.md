# WikiPulse — Kafka & Confluent Client Architecture Deep Dive

This document provides a comprehensive technical breakdown of Apache Kafka and the `confluent-kafka` (librdkafka) client integration in WikiPulse.

---

## 1. Why Confluent Kafka & librdkafka?

### 1.1 Pure Python Asyncio (`aiokafka`) vs Native C Engine (`confluent-kafka`)
In high-throughput event processing, pure-Python Kafka clients suffer from interpreter limitations:
1. **Python GIL Overhead:** Constructing record batches, computing CRCs, and managing socket buffers in pure Python consumes valuable CPU cycles on the event loop.
2. **Socket Lifecycle Binding:** Pure-Python async sockets can break across separate event loop lifecycles (e.g. during test isolation or worker rebalance).
3. **librdkafka Architecture:** `librdkafka` is written in optimized C. It maintains dedicated background OS threads for socket I/O, network multiplexing, and compression, exposing a clean, high-performance interface to Python.

```text
┌─────────────────────────────────────────────────────────────┐
│                       PYTHON RUNTIME                        │
│                                                             │
│   FastAPI / Worker Coroutine                                │
│       │                                                     │
│       ├──► producer.produce()   [Enqueues to C Queue in ~1µs]
│       │                                                     │
│       └──► producer.poll(0)     [Dispatches callbacks]      │
└──────────────────────┬──────────────────────────────────────┘
                       │ C-Extension FFI Boundary
┌──────────────────────▼──────────────────────────────────────┐
│                    LIBRDKAFKA (C ENGINE)                    │
│                                                             │
│   • Background Network Thread Pool                          │
│   • Micro-Batch Assembly (linger.ms = 5ms)                  │
│   • Direct TCP Socket Buffer Management                     │
│   • Automatic Reconnection & Broker Discovery               │
└──────────────────────┬──────────────────────────────────────┘
                       │ TCP Wire Protocol
┌──────────────────────▼──────────────────────────────────────┐
│                    APACHE KAFKA BROKER                      │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. Producer Architecture & Delivery Callbacks

### 2.1 Non-Blocking Producer Execution
In [`backend/app/kafka/producer.py`](file:///d:/NexusAI/backend/app/kafka/producer.py), messages are enqueued directly into librdkafka's internal C-memory queue:
```python
self._producer.produce(
    topic=topic,
    value=val_bytes,
    key=key_bytes,
    on_delivery=self._delivery_report,
)
self._producer.poll(0)
```
- **`produce()`** returns immediately without network I/O blocking.
- **`poll(0)`** serves pending delivery callbacks on loop ticks.
- **`flush()`** is executed asynchronously during graceful shutdown via `await asyncio.to_thread(self._producer.flush, timeout=3.0)`.

### 2.2 Producer Configuration
- `acks: "1"` — Leader broker commits to disk before acknowledging write.
- `retries: 3` with `retry.backoff.ms: 500` — Absorbs transient broker blips.
- `linger.ms: 5` — Waits up to $5\text{ms}$ to accumulate batches of messages, maximizing TCP packet payload efficiency.

---

## 3. Consumer Architecture & Offset Semantics

### 3.1 Non-Blocking Worker Thread Polling
To prevent synchronous C network polling from freezing the worker's asyncio event loop, polling is executed in a dedicated worker thread pool:
```python
# backend/app/kafka/consumer.py
msg = await asyncio.to_thread(self._consumer.poll, 1.0)
```
This design allows worker coroutines to perform async PostgreSQL queries and Redis updates without thread starvation.

### 3.2 Manual Offset Commits for At-Least-Once Guarantees
- `enable.auto.commit = False` is explicitly configured.
- Offsets are committed via `await asyncio.to_thread(self._consumer.commit, msg, asynchronous=False)` only AFTER:
  1. The event passes Pydantic schema validation.
  2. The entity is committed to PostgreSQL in an ACID transaction.
  3. The Redis sliding window counters are updated.

If a worker container crashes during processing, the uncommitted message is redelivered to a surviving worker replica upon consumer group rebalance.

---

## 4. Consumer Group Rebalancing & Scaling

### 4.1 Rebalance Protocol & Partition Assignment
When scaling worker replicas from 1 to 3 (`docker compose up -d --scale processor-worker=3`):
1. New consumer instances send `JoinGroup` requests to the Kafka Group Coordinator.
2. Kafka initiates a group rebalance and triggers `on_revoke` on existing consumers.
3. The coordinator assigns partitions evenly across all healthy replicas:
   - **Worker 1:** Partition 0
   - **Worker 2:** Partition 1
   - **Worker 3:** Partition 2
4. `on_assign` callback logs the newly assigned partition set and consumption resumes.

---

## 5. Kafka vs. Alternative Streaming Technologies

| Criteria | Apache Kafka (confluent-kafka) | RabbitMQ | Redis Streams |
| :--- | :--- | :--- | :--- |
| **Storage Architecture** | Append-only disk commit log | RAM-heavy queue with message deletion on ACK | In-memory log with configurable trimming |
| **Ordering Guarantee** | Strict total order **per partition key** (`article_title`) | FIFO per queue, but concurrency disrupts order | In-order per stream key |
| **Replayability** | **Unlimited** (re-read from offset 0 to replay history) | Non-replayable (consumed messages are removed) | Replayable within trimmed memory window |
| **Backpressure Handling** | Consumers pull at their own rate; disk absorbs bursts | Broker memory limits can cause publisher blocking | Memory ceiling (OOM risk under huge backlogs) |
