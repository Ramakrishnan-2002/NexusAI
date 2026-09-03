# WikiPulse — Senior Backend & Distributed Systems Interview Defense Guide

This comprehensive guide prepares you to defend every architectural and technical decision made in WikiPulse under aggressive technical scrutiny.

---

## 1. Core Architecture & Design Decisions

### Q1: Explain WikiPulse in 60 seconds.
- **Short Answer:** WikiPulse is an event-driven knowledge change intelligence platform that continuously ingests Wikipedia's real-time recent-change stream, processes events through Kafka, identifies multi-window activity spikes, indexes enriched knowledge using PostgreSQL + pgvector and full-text search, and synthesizes evidence-grounded AI answers with citations.
- **Deep Answer:** The system addresses the impedance mismatch between high-throughput bursty ingestion and compute-heavy downstream AI operations. Ingestion is decoupled via Apache Kafka, hot sliding window metrics ($1\text{m}, 5\text{m}, 15\text{m}$) are managed in Redis Sorted Sets, durable relational history and 384d dense vectors are unified in PostgreSQL 16 + pgvector, and an LLM Gateway routes between Google Gemini and local Ollama with fallback chains.
- **Evidence:** `docker-compose.yml`, `workers/`, `backend/app/main.py`.

### Q2: Why is Kafka necessary instead of direct FastAPI background tasks?
- **Short Answer:** Kafka provides durable append-only buffering, independent consumer group scaling, partition ordering, and offset-based replayability that FastAPI in-memory background tasks cannot provide.
- **Deep Answer:** If a surge of 500 edits/sec arrives, in-memory FastAPI tasks risk memory exhaustion (OOM) and tie ingestion uptime to worker health. If a worker container crashes during processing, in-memory tasks are lost forever. Kafka retains messages on disk, allows the embedding worker to process at its own rate without backpressure on ingestion, and replays uncommitted offsets after recovery.
- **Tradeoff:** Operational overhead of running a Kafka cluster.

### Q3: Why use Redis for sliding windows instead of PostgreSQL queries?
- **Short Answer:** Running sliding window count queries (`COUNT(*) WHERE occurred_at > NOW() - 5m`) against PostgreSQL on every event causes severe table lock contention and CPU bottlenecks. Redis Sorted Sets provide sub-millisecond atomic window pruning in $O(\log N + M)$.
- **Deep Answer:** With `ZADD`, `ZREMRANGEBYSCORE`, and `ZCARD`, Redis maintains a rolling window of event timestamps. Pruning expired scores and reading counts takes $< 0.5\text{ms}$, allowing the analytics worker to evaluate multi-window multipliers ($1\text{m}, 5\text{m}, 15\text{m}$) without querying the primary database.

### Q4: Why pgvector instead of a dedicated vector database like Pinecone or Qdrant?
- **Short Answer:** pgvector co-locates vector embeddings directly inside PostgreSQL with relational metadata, eliminating two-phase distributed sync, extra network hops, and external vector database costs.
- **Deep Answer:** In WikiPulse, every knowledge chunk has relational foreign keys to `Article` and `Edit`, as well as timestamp and namespace filters. A dedicated vector DB creates a dual-write consistency problem (e.g. vector written but DB rollback occurs). pgvector allows atomic ACID transactions and combined vector + full-text search in a single database.
- **When to Reconsider:** When vector count exceeds tens of millions of chunks requiring distributed vector sharding across multiple dedicated nodes.

### Q5: Why Hybrid Search (Vector + Full-Text Search + RRF)?
- **Short Answer:** Vector search captures semantic concepts and synonyms but frequently misses exact proper nouns, acronyms (e.g. "JWST", "CRISPR"), or IDs. Full-text search guarantees keyword precision. Combining both with Reciprocal Rank Fusion (RRF) delivers superior retrieval quality.
- **Evidence:** `backend/app/search/hybrid.py`, `backend/tests/rag/test_hybrid_search.py`.

---

## 2. Distributed Systems & Reliability

### Q6: What happens if Kafka crashes?
- **Short Answer:** In production, Kafka runs as a multi-broker cluster with replication. If the broker is unreachable, producers retry with exponential backoff; uncommitted offsets remain on disk, and consumers resume safely upon reconnection.

### Q7: How do you prevent duplicate event processing under at-least-once delivery?
- **Short Answer:** Every event has a deterministic UUID (`event_id`). Before processing, the worker checks the `ProcessingJob` table for `idempotency_key = "proc:{event_id}"`. If already completed or in progress, the duplicate is skipped and the Kafka offset is committed.
- **Evidence:** `workers/processor/processor.py` with `IntegrityError` collision handling.

### Q8: What happens when consumer processing rate is slower than ingestion rate (Backpressure)?
- **Short Answer:** Kafka acts as an elastic buffer. Ingestion continues publishing without delay; consumer lag increases temporarily in Kafka logs. Workers can scale horizontally by adding replicas to the consumer group up to the partition count.

---

## 3. RAG, Citations & AI Architecture

### Q9: How do you prevent hallucinations in AI answers?
- **Short Answer:** Answers are strictly grounded in retrieved knowledge chunks. If retrieval yields insufficient evidence, the prompt instructs the model to explicitly return "Insufficient evidence in current knowledge stream". Every claim is mapped to verified citations `[Article, Revision ID, Timestamp]`.
- **Evidence:** `backend/app/rag/context_builder.py`, `backend/app/rag/citations.py`.

### Q10: How do you defend against prompt injection inside Wikipedia content?
- **Short Answer:** All external content is treated as untrusted data. `sanitize_external_text()` neutralizes instruction override phrases, chunks are wrapped inside explicit `<untrusted_wikipedia_content>` tags, and the LLM output is enforced to adhere to a strict Pydantic JSON schema (`AIAnalysisOutput`).
- **Evidence:** `backend/app/core/security.py`, `backend/tests/security/test_security_hardening.py`.

---

## 4. Kafka Client Architecture & Confluent-Kafka Migration

### Q11: Why confluent-kafka instead of aiokafka?
- **Short Answer:** `confluent-kafka` is the official, enterprise-standard Python client backed by `librdkafka` (native C). It provides higher raw throughput, zero-copy buffer management, robust TCP connection handling, and official Kafka protocol support.
- **Deep Answer:** While `aiokafka` is pure Python asyncio, it suffers from Python GIL bottlenecks during high-throughput serialization, lacks native C micro-batching, and exhibits socket lifecycle fragility across event loop closures. `confluent-kafka` delegates low-level wire protocols and TCP buffering to `librdkafka`, drastically reducing CPU overhead.

### Q12: What is librdkafka and why is it faster?
- **Answer:** `librdkafka` is a high-performance C library implementation of the Apache Kafka protocol. It handles message compression, batch assembly (`linger.ms`), CRC verification, and network I/O in dedicated background C threads directly in memory, bypassing Python interpreter overhead.

### Q13: How do you integrate synchronous confluent-kafka with FastAPI/Asyncio without blocking?
- **Short Answer:** 
  1. **Producer:** `producer.produce(...)` is a fast non-blocking in-memory C queue append. Callbacks are served via `producer.poll(0)` on loop ticks. Flushes during shutdown run via `await asyncio.to_thread(producer.flush)`.
  2. **Consumer:** `consumer.poll(timeout)` is executed in a dedicated worker thread via `await asyncio.to_thread(consumer.poll, 1.0)`. This allows the worker's asyncio event loop to remain fully responsive for database queries and HTTP endpoints.

### Q14: How are offsets managed and what delivery guarantee exists?
- **Answer:** The system implements **strict at-least-once delivery**. `enable.auto.commit` is disabled (`False`). Consumers commit offsets manually (`consumer.commit(msg)`) only AFTER database transactions commit and Redis sliding windows are updated. If a worker crashes mid-processing, uncommitted offsets are replayed to another replica, and the `ProcessingJob` unique constraint prevents duplicate entity persistence.

### Q15: How does the system handle partition rebalancing during scaling?
- **Answer:** Consumers subscribe to topics with `on_assign` and `on_revoke` rebalance callbacks. When a worker replica is added (e.g. scaling from 1 to 3 workers), Kafka triggers a consumer group rebalance, distributing partitions evenly (e.g. Partition 0 $\to$ Worker 1, Partition 1 $\to$ Worker 2, Partition 2 $\to$ Worker 3). In-flight messages are committed before revocation to prevent rebalance offset regressions.
