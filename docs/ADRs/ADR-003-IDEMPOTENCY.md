# ADR-003: At-Least-Once Delivery with Application-Level Idempotency

## Status
**ACCEPTED**

## Context
In distributed event streaming, network blips and worker crashes trigger consumer rebalances and message redeliveries. True distributed "exactly-once" delivery across heterogeneous systems (Kafka, PostgreSQL, Redis, external LLM APIs) is impossible without heavy two-phase commit (2PC) coordination that devastates stream throughput.

## Decision
Implement **strict at-least-once delivery** paired with **application-level idempotency**:
1. `enable.auto.commit = False` is set on consumers. Offsets are committed manually via `consumer.commit(msg)` strictly after PostgreSQL ACID transactions commit.
2. In PostgreSQL, a `processing_jobs` table maintains a `UNIQUE` index on `idempotency_key = "proc:{event_id}"`.
3. If duplicate messages are consumed concurrently, the second insert collides on the unique index, triggering an `IntegrityError` $\to$ session rollback $\to$ safe idempotent skip.

## Alternatives Considered
1. **Kafka Transactional Producer/Consumer (2PC):** Heavy coordination overhead, limited to Kafka-to-Kafka boundaries without native PostgreSQL ACID transaction integration.
2. **Redis Distributed Locks (Redlock):** Adds external network hops and potential lock lease expiration edge cases; database-level unique constraints provide hardware ACID guarantees.

## Consequences
- **Positive:** Guaranteed protection against duplicate business entities under message replays.
- **Tradeoff:** Additional database unique index lookup per ingested event (~0.4ms overhead).
