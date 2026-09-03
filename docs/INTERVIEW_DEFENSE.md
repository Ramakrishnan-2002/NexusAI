# WikiPulse / NexusAI — Master Interview Defense & Engineering Guide

This document provides a comprehensive, senior-level interview defense guide covering core system design questions, spoken talk tracks, failure defenses, and hostile technical follow-ups for WikiPulse / NexusAI.

---

## 1. The 15-Minute Spoken Architecture Walkthrough

- **[0:00 – 2:00] Problem Statement:** Real-time knowledge bases experience severe edit surges during breaking world events. The core challenge is an **impedance mismatch** between high-velocity event ingestion ($200+\text{ ev/s}$) and compute-heavy downstream operations (sliding-window velocity analytics, 384d vector embedding generation, and LLM summarization). Decoupled event streaming is required to prevent thread starvation and dropped events.
- **[2:00 – 5:00] Architecture & Ingestion:** Stream ingestors consume Wikimedia SSE chunks and publish to Kafka topic `wikimedia.recentchange` keyed by `article_title`, ensuring per-article revision total ordering.
- **[5:00 – 8:00] Kafka & confluent-kafka Bridge:** `producer.produce()` appends to librdkafka C memory non-blockingly (`linger.ms: 5`). Consumer polling runs in worker threads via `asyncio.to_thread(consumer.poll, 1.0)`, releasing the GIL during network I/O.
- **[8:00 – 10:00] Consistency & Idempotency:** PostgreSQL (authoritative) and Redis (derived) operate in **separate consistency domains**. Idempotency is enforced via `ProcessingJob.idempotency_key = "proc:{event_id}"` with a PostgreSQL `UNIQUE` index.
- **[10:00 – 12:00] Hybrid Search & RAG:** Fuses pgvector HNSW cosine search (11.1ms) with GIN Full-Text Search (1.9ms) using Reciprocal Rank Fusion ($k=60$) in **15.89 ms (avg)**. Layered prompt defense fences untrusted chunks inside XML tags.
- **[12:00 – 15:00] Scalability & Failure Handling:** Worker scaling is bounded by partitions ($W \le P$). Poison pills route to `wikimedia.dlq` after 3 retries without blocking queues.

---

## 2. Core Architectural Q&A Bank

### Q1: Why Kafka instead of RabbitMQ?
- **Short Answer:** RabbitMQ deletes messages upon consumer ACK, preventing event replay and downstream multi-worker fanout. Kafka maintains an immutable, partitioned append-only disk commit log.
- **Deep Answer:** Kafka allows independent consumer groups (processor, analytics, embedding, AI) to consume at their own pace from independent offsets. If a worker crashes or a new model is deployed, Kafka allows replaying history from any past offset.
- **Tradeoff:** Higher operational footprint than RabbitMQ.

### Q2: Why `confluent-kafka` instead of `aiokafka`?
- **Short Answer:** `confluent-kafka` is backed by the native C `librdkafka` engine, enabling micro-batching in C memory (`linger.ms: 5`) and low GIL contention.
- **Deep Answer:** Pure-Python clients suffer from GIL contention during heavy serialization. `confluent-kafka` buffers in native OS C threads. Consumer polling is isolated in worker threads via `asyncio.to_thread(consumer.poll, 1.0)`, keeping FastAPI's event loop unblocked.
- **Tradeoff:** Requires thread-isolated polling to bridge C extensions to Python's asyncio reactor.

### Q3: What delivery guarantee does WikiPulse provide?
- **Short Answer:** Strict **at-least-once delivery** paired with **application-level idempotency**.
- **Deep Answer:** Auto-commit is disabled (`enable.auto.commit = False`). Consumers commit offsets manually via `consumer.commit(msg)` strictly after PostgreSQL ACID writes commit. If a worker crashes before committing the offset, Kafka redelivers the message; our `ProcessingJob` unique constraint identifies it as already completed and safely skips the database write.
- **Tradeoff:** Extra database unique index lookup per ingested event (~0.4ms).

### Q4: How do you handle concurrent duplicate processing races?
- **Short Answer:** Via a database-level `UNIQUE` index on `ProcessingJob.idempotency_key = "proc:{event_id}"`.
- **Deep Answer:** When two workers consume duplicate messages concurrently, both execute `session.flush()`. The second worker collides on the unique constraint, raising a PostgreSQL `IntegrityError`. The application catches this, rolls back the session, and commits the Kafka offset as a safe no-op.
- **Evidence:** Tested with 50 concurrent duplicate worker tasks resulting in exactly 1 persisted database record and 49 safe rollbacks.

### Q5: What is the consistency relationship between PostgreSQL and Redis?
- **Short Answer:** PostgreSQL and Redis operate in **separate consistency domains** without a distributed 2PC coordinator.
- **Deep Answer:** PostgreSQL is the authoritative system of record; Redis is transient derived aggregation state. The worker commits to PostgreSQL first, updates Redis second, and commits the Kafka offset last. If Redis fails, `ActivityCounterService` degrades to an in-memory fallback without aborting the PostgreSQL transaction.
- **Evidence:** `test_failure_redis_outage_graceful_fallback`.

### Q6: Why `pgvector` instead of Pinecone or Qdrant?
- **Short Answer:** Co-locating 384d vector embeddings inside PostgreSQL allows atomic ACID transactions and joint SQL queries (`WHERE article_id = :id ORDER BY embedding <=> :query_vec`) without dual-write synchronization lag.
- **Deep Answer:** Dedicated vector databases create a dual-write sync problem: if the vector DB write succeeds but the SQL transaction rolls back, your search index is out of sync. `pgvector` provides unified metadata and vector storage in a single database.
- **Tradeoff:** Bounded by single-node host RAM for HNSW graph residency (~32GB for 10M chunks).

### Q7: Why Hybrid Search (Vector + FTS) instead of pure vector search?
- **Short Answer:** Dense vector embeddings dilute rare acronyms, proper nouns ("JWST", "CRISPR-Cas9"), and revision IDs. Lexical Full-Text Search guarantees exact keyword precision.
- **Deep Answer:** Vector search finds conceptual synonyms; lexical FTS finds exact terms. NexusAI queries both in parallel and fuses their ranks using Reciprocal Rank Fusion ($k=60$), achieving superior retrieval precision in **15.89ms (avg)**.
- **RRF Formula:** $\text{RRF}(d) = \sum \frac{1}{60 + \text{rank}_i(d)}$.

### Q8: How do you defend against prompt injection inside Wikipedia content?
- **Short Answer:** Via **layered prompt-injection risk mitigation**: regex neutralization + explicit `<untrusted_wikipedia_content>` XML context fencing + strict Pydantic output validation.
- **Deep Answer:** External Wikipedia text is treated strictly as untrusted user data. `sanitize_external_text()` neutralizes common override phrases (`IGNORE ALL PREVIOUS INSTRUCTIONS`), the context builder frames chunks inside XML tags, and LLM responses are parsed and validated strictly into the `AIAnalysisOutput` schema.
- **Tradeoff:** Does not guarantee 100% protection against novel adversarial phrasing.

### Q9: What is the difference between a Kafka broker outage and a poison pill?
- **Short Answer:** A broker outage is an infrastructure transport failure where `librdkafka` buffers in C memory and auto-reconnects. A poison pill is an application data failure (corrupted JSON) that fails validation and is routed to `wikimedia.dlq` after 3 retries.
- **Deep Answer:** Broker outages prevent message transport; uncommitted offsets remain safe on disk. In contrast, a poison pill is successfully delivered by Kafka but cannot be processed by the worker. Indefinite retries would block partition progression, so `RetryPolicy` catches the error, publishes to `wikimedia.dlq`, and commits the consumer offset.
- **Evidence:** `test_failure_poison_pill_routed_to_dlq_without_blocking`.

### Q10: Why separate `/livez` from `/readyz` health endpoints?
- **Short Answer:** `/livez` tests process liveness with zero external I/O; `/readyz` tests database and Redis reachability.
- **Deep Answer:** If `/livez` checked PostgreSQL, a momentary database spike would cause Kubernetes to mark all API pods dead and trigger simultaneous container restarts, worsening the outage.
