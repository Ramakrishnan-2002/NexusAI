# NexusAI / WikiPulse — Failure Engineering & Self-Healing Matrix

This document provides a comprehensive analysis of every failure mode, detection mechanism, recovery behavior, data loss risk, and verification status across the NexusAI architecture.

---

## 1. Master Failure Matrix

| Component | Injected Failure | Detection Mechanism | System Behavior | Recovery Mechanism | Data Loss Risk | Verification Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL** | Database container stopped | Connection pool ping failure in `/readyz` | `/livez` stays 200 OK; `/readyz` returns 503; worker pauses polling & retries. | Automatically reconnects upon container restart; uncommitted messages processed in order. | **Zero** (Kafka retains uncommitted offsets). | **VERIFIED** |
| **Redis** | Redis container stopped | Socket connection timeout in `RedisManager` | Degrades gracefully to `InMemoryFallbackRedis`; DB writes and offset commits proceed. | Re-establishes Redis connection automatically upon recovery. | **Zero for DB** (volatile sliding counters degrade to local memory). | **VERIFIED** |
| **Kafka Broker** | Broker offline / network partition | Disconnect in librdkafka background C thread | Producer buffers in C memory up to `queue.buffering.max.messages`; consumer pauses polling. | librdkafka auto-reconnects upon broker restart; uncommitted messages processed in order. | **Zero** (producer buffers on client; consumer offsets safe on disk). | **VERIFIED** |
| **Poison Message** | Corrupted / malformed JSON payload | Pydantic `ValidationError` in consumer | Handled by `RetryPolicy` (3 attempts with backoff); routed to `wikimedia.dlq` topic. | Consumer commits offset and proceeds with next valid message; partition never stalls. | **Zero** (corrupted payload preserved in DLQ for inspection). | **VERIFIED** |
| **Duplicate Event**| Same `event_id` consumed twice concurrently | Unique index collision on `ProcessingJob.idempotency_key` | First write succeeds; second write triggers `IntegrityError` $\to$ rolled back. | Duplicate attempt treated as an idempotent safe skip; Kafka offset committed. | **Zero** (duplicate business record prevented). | **VERIFIED** |
| **LLM Outage** | 429 Rate Limit or 504 Timeout | HTTP error / Timeout in `LLMGateway` | Cascades automatically to Local Ollama $\to$ Deterministic Mock Provider. | Fallback returns schema-compliant grounded response; zero client drops. | **Zero** (stateless query). | **VERIFIED** |
| **Worker Process Crash** | Container terminated unexpectedly | Consumer heartbeat timeout expires ($45\text{s}$) | Kafka Group Coordinator detects missing heartbeat $\to$ triggers group rebalance. | Assigned partitions redistributed to surviving worker replicas; resumes from last offset. | **Zero** (uncommitted offsets replayed). | **VERIFIED** |
| **Slow / Dead SSE Client** | Client disconnects without TCP FIN | Bounded queue write error / client disconnect | Queue fills up to `maxsize=100`; unregisters subscriber cleanly. | Subscriber queue garbage collected; server memory protected from leaks. | **Zero** (stateless broadcast). | **VERIFIED** |

---

## 2. Key Failure Taxonomy Distinctions

### A. Infrastructure Transport Outage (Kafka Broker Failure)
During a broker outage, network TCP connections are severed. `librdkafka` buffers outgoing messages in C memory and automatically reconnects in background threads once brokers recover. No application data is corrupted.

### B. Application Data Failure (Poison Pill)
A poison pill is a message delivered successfully by Kafka that fails application schema deserialization or validation. Infinite retries would block partition progression. The `RetryPolicy` attempts 3 retries and then diverts the message to `wikimedia.dlq`, committing the consumer offset so healthy messages can proceed.
