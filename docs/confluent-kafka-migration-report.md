# WikiPulse — Confluent-Kafka Migration & Architecture Report

**Migration Status:** **PASS (COMPLETED & VERIFIED)**  
**Target Engine:** **`confluent-kafka` (librdkafka C-extension)**  
**Superseded Engine:** **`aiokafka` (pure-Python asyncio client)**  
**Date:** 2026-09-03  

---

## 1. Migration Overview & Rationale

WikiPulse was originally developed using `aiokafka`. While functional for pure Python prototyping, `aiokafka` exhibits significant limitations in high-throughput production environments:
1. **Python GIL Contention:** High batch serialization and CRC verification consume Python interpreter CPU cycles.
2. **Event Loop Binding Fragility:** Socket lifecycle handling across event loop boundaries can cause `RuntimeError: Event loop is closed` during worker rebalancing or test runs.
3. **Enterprise Standard:** `confluent-kafka` is the official enterprise Python client backed by `librdkafka`, offering native C-level batch queuing (`linger.ms`), zero-copy socket transfers, and strict protocol parity.

---

## 2. Files Changed & Migration Scope

| File | Changes Made |
| :--- | :--- |
| `backend/requirements.txt` | Replaced `aiokafka==0.14.0` with `confluent-kafka==2.15.0`. |
| `backend/app/kafka/producer.py` | Migrated `EventProducer` to `confluent_kafka.Producer`, implemented `_delivery_report()` callbacks, non-blocking `poll(0)`, and asynchronous `flush()`. |
| `backend/app/kafka/consumer.py` | Migrated `EventConsumer` to `confluent_kafka.Consumer`, implemented non-blocking `asyncio.to_thread(consumer.poll, 1.0)`, and manual offset commits (`commit(msg)`). |
| `backend/app/kafka/admin.py` | Built `KafkaAdminService` using `confluent_kafka.admin.AdminClient` for topic creation and partition metadata inspection. |
| `backend/app/kafka/__init__.py` | Exported unified `event_producer`, `EventConsumer`, and `kafka_admin_service`. |
| `backend/tests/kafka/test_confluent_kafka_integration.py` | Added dedicated integration tests for producer delivery callbacks, consumer polling, fallback bus, and admin client. |
| `docs/adr/ADR-009-confluent-kafka-client.md` | Documented formal Architecture Decision Record for the client migration. |
| `docs/interview-defense.md` | Added Section 4 with 5 in-depth interview defense questions covering librdkafka and async bridge patterns. |

---

## 3. AIOKafka References Audit

Repository-wide grep for `aiokafka` / `AIOKafka`:
- Active Code References: **0**
- Test References: **0**
- Requirements References: **0**

---

## 4. Confluent Kafka Architecture & Asyncio Bridge

`confluent-kafka` uses a synchronous C-extension API. To integrate seamlessly with FastAPI and asynchronous workers without blocking the asyncio event loop, WikiPulse implements the following design:

```text
FastAPI / Worker Event Loop
            │
            ├──► Producer: producer.produce(topic, val, key, on_delivery=cb)  [NON-BLOCKING C QUEUE]
            │              producer.poll(0)                                   [SERVES CALLBACKS]
            │
            └──► Consumer: await asyncio.to_thread(consumer.poll, 1.0)        [THREAD POOL POLLING]
                           │
                           ▼ (Message Received)
                           await handler(payload)                             [ASYNC DATABASE / REDIS WORK]
                           │
                           ▼ (Transaction Committed)
                           await asyncio.to_thread(consumer.commit, msg)      [MANUAL AT-LEAST-ONCE COMMIT]
```

---

## 5. Producer Behavior & Configuration

- `acks`: `1` (ensures leader broker disk persistence before acknowledgment).
- `retries`: `3` with `retry.backoff.ms: 500`.
- `linger.ms`: `5` (enables micro-batching without latency penalty).
- `queue.buffering.max.messages`: `100,000`.
- **Delivery Callbacks:** `_delivery_report(err, msg)` logs failed topic deliveries with error codes and records partition offsets upon successful broker write.

---

## 6. Consumer Behavior & Offset Strategy

- **Manual Offset Commits:** `enable.auto.commit = False`.
- **At-Least-Once Delivery:** Offsets are committed via `consumer.commit(msg, asynchronous=False)` only AFTER the worker successfully persists the entity in PostgreSQL and updates Redis sliding window counters.
- **Poison Pill Handling:** Unparseable or corrupt messages trigger the `RetryPolicy`. After 3 retries, the message is routed to `wikimedia.dlq` before advancing the consumer offset, preventing partition queue stalls.

---

## 7. Test Results

**30 / 30 Tests PASSED (100% Pass Rate in 20.08s):**
- Unit Tests: 7 / 7 PASSED
- API Tests: 4 / 4 PASSED
- Failure Tests: 6 / 6 PASSED
- Security Tests: 3 / 3 PASSED
- Kafka & Confluent Integration Tests: 6 / 6 PASSED
- LLM Gateway Tests: 2 / 2 PASSED
- Search & RAG Tests: 2 / 2 PASSED

---

## 8. Remaining Limitations

- Docker Compose runs a single Kafka broker (`replication_factor: 1`). Production enterprise deployments require a 3+ node KRaft cluster with `min.insync.replicas: 2`.
