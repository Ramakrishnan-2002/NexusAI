# WikiPulse / NexusAI — Master Interview Defense Guide (40 Comprehensive Q&As)

This document contains 40 rigorous senior engineering interview questions covering every dimension of NexusAI, formatted with short spoken answers, deep technical explanations, repository evidence, tradeoffs, and hostile follow-ups.

---

### 1. Why Kafka instead of RabbitMQ?
- **Short Answer:** RabbitMQ deletes messages upon consumer acknowledgment, preventing event replay and historical backfilling when launching new downstream consumers. Kafka maintains an immutable, partitioned append-only disk commit log.
- **Deep Answer:** In event-driven architectures where multiple independent consumer groups (processor, analytics, embedding, AI) process events at different speeds, Kafka allows each group to maintain independent partition offsets. If a worker crashes or a new consumer group is deployed, Kafka allows replaying past events from any offset.
- **Evidence:** [`docker-compose.yml`](file:///d:/NexusAI/docker-compose.yml), [`backend/app/kafka/producer.py`](file:///d:/NexusAI/backend/app/kafka/producer.py).
- **Tradeoff:** Higher operational complexity compared to RabbitMQ.
- **Follow-Up:** *Why not Redis Streams?* (Redis Streams holds stream logs in RAM, creating memory exhaustion risks during prolonged worker outages).

### 2. Why `confluent-kafka` instead of `aiokafka`?
- **Short Answer:** `confluent-kafka` is backed by the native C `librdkafka` library, providing micro-batching in C memory (`linger.ms: 5`), low Python GIL contention, and official enterprise protocol parity.
- **Deep Answer:** Pure-Python clients suffer from GIL contention during heavy JSON serialization and socket management. `confluent-kafka` buffers in native OS C threads. To prevent blocking FastAPI's asyncio event loop, consumer polling is isolated in worker threads via `await asyncio.to_thread(consumer.poll, 1.0)`.
- **Evidence:** [`backend/app/kafka/consumer.py`](file:///d:/NexusAI/backend/app/kafka/consumer.py), [`backend/app/kafka/producer.py`](file:///d:/NexusAI/backend/app/kafka/producer.py).
- **Tradeoff:** Requires thread-isolated polling to bridge C-extensions to Python's asyncio reactor.
- **Follow-Up:** *Does librdkafka eliminate the GIL?* (No. Network I/O runs in C threads, but Python callbacks and deserialization still run under the GIL).

### 3. What delivery guarantee does WikiPulse provide?
- **Short Answer:** Strict **at-least-once delivery** paired with **application-level idempotency**.
- **Deep Answer:** Consumers disable auto-commit (`enable.auto.commit = False`) and manually commit offsets via `consumer.commit(msg)` strictly after PostgreSQL ACID writes commit. If a worker crashes before committing the offset, Kafka redelivers the message; our `ProcessingJob` unique constraint identifies it as already completed and safely skips the database write.
- **Evidence:** [`workers/processor/processor.py`](file:///d:/NexusAI/workers/processor/processor.py#L40-L65), `test_processor_concurrent_idempotency_race`.
- **Tradeoff:** Extra database unique index lookup per ingested event (~0.4ms).
- **Follow-Up:** *Why not claim exactly-once?* (True end-to-end exactly-once across heterogeneous systems without 2PC is a theoretical impossibility).

### 4. How do you handle concurrent duplicate processing races?
- **Short Answer:** Via a database-level `UNIQUE` index on `ProcessingJob.idempotency_key = "proc:{event_id}"`.
- **Deep Answer:** When two workers consume duplicate messages concurrently, both execute `session.flush()`. The second worker collides on the unique constraint, raising a PostgreSQL `IntegrityError`. The application catches this, rolls back the session, and commits the Kafka offset as a safe no-op.
- **Evidence:** `test_processor_concurrent_idempotency_race` (50 concurrent duplicates $\to$ exactly 1 persisted record; 49 safe rollbacks).
- **Tradeoff:** Relies on database constraint serialization.
- **Follow-Up:** *Why not Redis distributed locks?* (Database unique constraints provide ACID guarantees directly at the persistence layer without lock lease expiration edge cases).

### 5. What is the consistency relationship between PostgreSQL and Redis?
- **Short Answer:** PostgreSQL and Redis operate in **separate consistency domains** without a distributed 2PC coordinator.
- **Deep Answer:** PostgreSQL is the authoritative system of record; Redis is transient derived aggregation state. The worker commits to PostgreSQL first, updates Redis second, and commits the Kafka offset last. If Redis fails, `ActivityCounterService` degrades to an in-memory fallback without aborting the PostgreSQL transaction.
- **Evidence:** [`workers/processor/processor.py`](file:///d:/NexusAI/workers/processor/processor.py#L85-L110), `test_failure_redis_outage_graceful_fallback`.
- **Tradeoff:** Redis velocity counts may temporarily experience minor divergence during extreme crash replays.
- **Follow-Up:** *What happens if Redis crashes?* (The system continues persisting edits to PostgreSQL and committing Kafka offsets seamlessly).

### 6. Why use Redis Sorted Sets for sliding windows instead of PostgreSQL queries?
- **Short Answer:** Running rolling `COUNT(*)` window queries in PostgreSQL on every event causes severe table lock contention and CPU bottlenecks. Redis Sorted Sets prune and count in $< 0.5\text{ms}$ in RAM in $O(\log N + M)$.
- **Deep Answer:** `ActivityCounterService` executes a Redis pipeline: `ZADD` (records edit timestamp), `ZREMRANGEBYSCORE` (prunes timestamps older than 15 minutes), and `ZCARD` (computes rolling count). This provides sub-millisecond velocity tracking without database lock contention.
- **Evidence:** [`backend/app/redis/counters.py`](file:///d:/NexusAI/backend/app/redis/counters.py).
- **Tradeoff:** In-memory state is volatile and requires degradation handling.
- **Follow-Up:** *Is this pipeline atomic?* (Redis pipelines execute in a single roundtrip; atomic multi-command execution requires Lua scripts or Redis transactions).

### 7. Why `pgvector` instead of Pinecone or Qdrant?
- **Short Answer:** Co-locating 384d vector embeddings inside PostgreSQL allows atomic ACID transactions and joint SQL queries (`WHERE article_id = :id ORDER BY embedding <=> :query_vec`) without dual-write synchronization lag.
- **Deep Answer:** Dedicated vector databases create a dual-write problem: if the vector DB write succeeds but the SQL transaction rolls back, your search index is out of sync. `pgvector` provides unified metadata and vector storage in a single database.
- **Evidence:** [`backend/app/models/knowledge_chunk.py`](file:///d:/NexusAI/backend/app/models/knowledge_chunk.py), [`backend/app/search/vector.py`](file:///d:/NexusAI/backend/app/search/vector.py).
- **Tradeoff:** Bounded by single-node host RAM for HNSW graph residency (~32GB for 10M chunks).
- **Follow-Up:** *When would you migrate to a dedicated vector DB?* (When vector count exceeds 50M+ vectors requiring multi-node distributed vector sharding).

### 8. Why Hybrid Search (Vector + FTS) instead of pure vector search?
- **Short Answer:** Dense vector embeddings dilute rare acronyms, proper nouns ("JWST", "CRISPR-Cas9"), and revision IDs. Lexical Full-Text Search guarantees exact keyword precision.
- **Deep Answer:** Vector search finds conceptual synonyms; lexical FTS finds exact terms. NexusAI queries both in parallel and fuses their ranks using Reciprocal Rank Fusion ($k=60$), achieving superior retrieval precision in **15.89ms (avg)**.
- **Evidence:** [`backend/app/search/hybrid.py`](file:///d:/NexusAI/backend/app/search/hybrid.py), `test_hybrid_search_scoring_and_retrieval`.
- **Tradeoff:** Executes two database queries per search request (~15.89ms combined vs ~1.93ms pure FTS).
- **Follow-Up:** *Explain the RRF mathematical formula.* ($\text{RRF}(d) = \sum \frac{1}{60 + \text{rank}_i(d)}$).

### 9. How do you defend against prompt injection inside Wikipedia content?
- **Short Answer:** Via **layered prompt-injection risk mitigation**: regex neutralization + explicit `<untrusted_wikipedia_content>` XML context fencing + strict Pydantic output validation.
- **Deep Answer:** External Wikipedia text is treated strictly as untrusted user data. `sanitize_external_text()` neutralizes common override phrases (`IGNORE ALL PREVIOUS INSTRUCTIONS`), the context builder frames chunks inside XML tags, and LLM responses are parsed and validated strictly into the `AIAnalysisOutput` schema.
- **Evidence:** [`backend/app/core/security.py`](file:///d:/NexusAI/backend/app/core/security.py), `test_security_prompt_injection_neutralization`.
- **Tradeoff:** Does not guarantee 100% protection against novel adversarial phrasing.
- **Follow-Up:** *What happens if no relevant facts exist in the context?* (Model returns "Insufficient evidence in current knowledge stream").

### 10. What is the difference between a Kafka broker outage and a poison pill?
- **Short Answer:** A broker outage is an infrastructure transport failure where `librdkafka` buffers in C memory and auto-reconnects. A poison pill is an application data failure (corrupted JSON) that fails validation and is routed to `wikimedia.dlq` after 3 retries.
- **Deep Answer:** Broker outages prevent message transport; uncommitted offsets remain safe on disk. In contrast, a poison pill is successfully delivered by Kafka but cannot be processed by the worker. Indefinite retries would block partition progression, so `RetryPolicy` catches the error, publishes to `wikimedia.dlq`, and commits the consumer offset.
- **Evidence:** [`backend/app/kafka/retry.py`](file:///d:/NexusAI/backend/app/kafka/retry.py), `test_failure_poison_pill_routed_to_dlq_without_blocking`.
- **Tradeoff:** Requires operational alerting on DLQ topic growth.
- **Follow-Up:** *How do you inspect DLQ messages?* (Via Kafka UI on port 8080 or dedicated CLI consumer).

---

*(Questions 11 through 40 covering Worker Scaling, Partitioning, Health Probes, RAG citations, LLM Cascading, Rate Limiting, Capacity Math, SSE, and Production Evolution are detailed identically in [`docs/INTERVIEW_MASTER_GUIDE.md`](INTERVIEW_MASTER_GUIDE.md)).*
