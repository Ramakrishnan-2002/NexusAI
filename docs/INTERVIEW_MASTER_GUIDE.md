# WikiPulse — Master Backend & Distributed Systems Interview Guide

This master guide prepares engineers to defend every architectural layer of WikiPulse under senior backend, distributed systems, SRE, and AI engineering interview panels.

---

## 1. Verbal Project Explanations

### 1.1 The 2-Minute Elevator Pitch (Recruiter / Initial Screen)
> "WikiPulse is a real-time knowledge change intelligence platform that processes Wikipedia's live recent-change stream. When breaking news occurs, hundreds of edits surge on specific articles. WikiPulse decouples ingestion via Apache Kafka using confluent-kafka and librdkafka, calculates multi-window velocity spikes using Redis Sorted Sets, indexes enriched knowledge chunks in PostgreSQL using pgvector and Full-Text Search, and serves evidence-grounded AI queries with exact citation attribution. The entire platform runs on Docker with strict at-least-once delivery, database idempotency, and automated multi-provider LLM fallback."

### 1.2 The 5-Minute Technical Overview (Senior Engineer Interview)
> "The core engineering challenge in WikiPulse is the impedance mismatch between high-throughput bursty ingestion and compute-heavy downstream AI operations. 
> 
> Ingestion connects to Wikipedia's SSE stream, normalizes schemas, and publishes events to Kafka with partition keys set to `article_title`. This guarantees that revisions for the same article are processed strictly in chronological order by dedicated worker replicas.
> 
> Our processor worker enforces strict at-least-once delivery with manual offset commits after committing transactions to PostgreSQL. To prevent duplicate processing during consumer rebalances, we check a `ProcessingJob` table with a unique constraint on `idempotency_key`, handling concurrent insertion races via `IntegrityError` rollbacks.
> 
> For low-latency trend detection, the analytics worker reads Redis Sorted Sets (`ZREMRANGEBYSCORE`), pruning rolling timestamps across 1m, 5m, and 15m windows in sub-millisecond time without relational table locks.
> 
> On the retrieval side, we implement Hybrid Search: we retrieve semantic candidates via 384d vector cosine distance in `pgvector` and exact keyword candidates via PostgreSQL Full-Text Search, merging them via Reciprocal Rank Fusion (RRF). Finally, our LLM Gateway wraps Google Gemini, local Ollama, and a deterministic mock provider with untrusted content boundaries and Pydantic schema validation to eliminate hallucinations and prompt injection."

### 1.3 The 15-Minute Whiteboard System Design Walkthrough
1. **Requirements & Sizing:** Functional (Ingestion, Spike Detection, Hybrid Search, Grounded QA, Live SSE) & Non-Functional ($200+\text{ edits/sec}$ peak, $< 50\text{ms}$ search latency, zero message loss).
2. **Data Ingestion & Buffering:** Wikimedia SSE $\to$ `StreamIngestorService` $\to$ `confluent-kafka` Producer (`linger.ms: 5`, `acks: 1`) $\to$ Kafka topic `wikimedia.recentchange` (3 partitions, key=`article_title`).
3. **Storage & Workers:**
   - `ProcessorWorker` $\to$ PostgreSQL `articles`, `editors`, `edits`, and `processing_jobs` (idempotency).
   - `AnalyticsWorker` $\to$ Redis rolling ZSETs $\to$ `trend_events`.
   - `EmbeddingWorker` $\to$ 384d dense vectors $\to$ `knowledge_chunks` in `pgvector` (HNSW cosine index).
4. **Retrieval & RAG Pipeline:** pgvector cosine search + GIN FTS index $\to$ Reciprocal Rank Fusion ($k=60$) $\to$ Candidate Reranker $\to$ `<untrusted_wikipedia_content>` framing $\to$ LLM Gateway (`Gemini -> Ollama -> Mock`) $\to$ `AIAnalysisOutput` validation.
5. **Reliability & Edge Cases:** Kubernetes `/livez` vs `/readyz` probes, Redis outage fallback (`InMemoryFallbackRedis`), poison-pill DLQ routing (`wikimedia.dlq`), and bounded SSE subscriber queues.

---

## 2. Comprehensive Interview Question Bank (Levels 1 – 8)

---

### LEVEL 1 — PROJECT BASICS & HIGH-LEVEL DESIGN

#### Q1.1: What is WikiPulse and what problem does it solve?
- **Short Answer:** WikiPulse is an event-driven knowledge intelligence platform that ingests real-time Wikipedia changes, detects velocity spikes, indexes knowledge in pgvector, and generates evidence-grounded AI summaries.
- **Deep Answer:** High-velocity knowledge streams generate hundreds of edits during breaking events. Standard relational databases lock under rolling window aggregation queries, while LLMs hallucinate without grounding. WikiPulse decouples ingestion through Kafka, evaluates sliding windows in Redis, stores embeddings in pgvector, and synthesizes answers using RAG with citations.
- **Project Implementation:** [`backend/app/main.py`](file:///d:/NexusAI/backend/app/main.py), [`docker-compose.yml`](file:///d:/NexusAI/docker-compose.yml).
- **Tradeoff:** Requires running a multi-service distributed architecture (Kafka, Redis, PostgreSQL, Workers) rather than a single monolith.
- **Follow-up:** *How do you prevent worker crashes from losing in-flight edits?* (See Q1.2 & Q5.4).

#### Q1.2: Why did you choose Apache Kafka instead of direct HTTP calls or FastAPI background tasks?
- **Short Answer:** Kafka acts as a durable, disk-persisted elastic buffer that absorbs $10\times$ traffic bursts, isolates slow embedding workers, and supports replayability.
- **Deep Answer:** If 500 edits/sec burst during breaking news, FastAPI in-memory background tasks cause memory exhaustion (OOM) and tie ingestion uptime to worker availability. Kafka persists events to an append-only commit log on disk, allowing workers to pull at their own pace without dropping messages.
- **Project Implementation:** [`backend/app/kafka/producer.py`](file:///d:/NexusAI/backend/app/kafka/producer.py), [`workers/stream_ingestor/ingestor.py`](file:///d:/NexusAI/workers/stream_ingestor/ingestor.py).
- **Tradeoff:** Higher operational complexity and infrastructure overhead than lightweight task queues.
- **Follow-up:** *Why not RabbitMQ or Redis Streams?* (See Q3.3 & Q5.2).

#### Q1.3: Why PostgreSQL with pgvector instead of a dedicated vector database like Pinecone?
- **Short Answer:** pgvector co-locates 384d vector embeddings directly in PostgreSQL with relational metadata, eliminating two-phase distributed sync and dual-write inconsistency.
- **Deep Answer:** In WikiPulse, every knowledge chunk has relational foreign keys to `Article` and `Edit`, as well as timestamp and namespace filters. A dedicated vector DB creates a dual-write sync lag and requires external network hops. pgvector allows ACID transactions and joint relational + vector filtering in a single SQL query.
- **Project Implementation:** [`backend/app/models/knowledge_chunk.py`](file:///d:/NexusAI/backend/app/models/knowledge_chunk.py), [`backend/app/search/vector.py`](file:///d:/NexusAI/backend/app/search/vector.py).
- **Tradeoff:** pgvector is bounded by single-node RAM for HNSW graph residency, scaling to tens of millions rather than billions of vectors.
- **Follow-up:** *At what scale would you migrate to a dedicated vector DB?* (See Q6.4).

#### Q1.4: Why use Redis for sliding window metrics instead of SQL queries?
- **Short Answer:** Running sliding window count queries (`COUNT(*) WHERE occurred_at > NOW() - 5m`) against PostgreSQL on every event causes severe table lock contention. Redis Sorted Sets prune rolling timestamps in $O(\log N + M)$ in $< 0.5\text{ms}$.
- **Deep Answer:** With `ZADD`, `ZREMRANGEBYSCORE`, and `ZCARD`, Redis maintains rolling windows of edit timestamps in memory. Pruning expired scores and reading counts takes $< 0.5\text{ms}$, allowing the analytics worker to evaluate multi-window multipliers ($1\text{m}, 5\text{m}, 15\text{m}$) without touching the primary database.
- **Project Implementation:** [`backend/app/redis/counters.py`](file:///d:/NexusAI/backend/app/redis/counters.py).
- **Tradeoff:** Redis state is volatile, requiring graceful degradation logic if Redis goes down.
- **Follow-up:** *What happens if Redis crashes?* (See Q7.3).

#### Q1.5: Why Hybrid Search (Vector + Full-Text Search + RRF)?
- **Short Answer:** Vector search captures semantic concepts and synonyms but misses exact acronyms (e.g. "JWST", "CRISPR") or IDs. Full-text search guarantees keyword precision. Reciprocal Rank Fusion fuses both streams with minimal latency delta.
- **Deep Answer:** Dense embeddings map queries to semantic clusters in latent space. However, domain-specific acronyms and entity names are often diluted in embedding models. PostgreSQL Full-Text Search (`to_tsvector` + GIN index) guarantees exact lexical hits. RRF ($k=60$) balances both rankings mathematically.
- **Project Implementation:** [`backend/app/search/hybrid.py`](file:///d:/NexusAI/backend/app/search/hybrid.py).
- **Tradeoff:** Doubles retrieval query execution overhead ($\approx 15.9\text{ms}$ vs $1.9\text{ms}$ pure FTS).
- **Follow-up:** *How is the RRF score calculated?* (See Q6.2).

---

### LEVEL 2 — BACKEND ENGINEERING & PYTHON ASYNC

#### Q2.1: How do you integrate synchronous confluent-kafka with FastAPI/Asyncio without blocking the event loop?
- **Short Answer:** Producers enqueue non-blocking C-memory writes via `produce()` and serve callbacks with `poll(0)`. Consumers run polling in dedicated worker threads via `await asyncio.to_thread(consumer.poll, 1.0)`.
- **Deep Answer:** `confluent-kafka` is a synchronous C-extension (`librdkafka`). Running `consumer.poll()` directly on an asyncio event loop blocks the single-threaded reactor, freezing HTTP request handling. Polling via `asyncio.to_thread` delegates the blocking C call to the Python thread pool, allowing coroutines to perform async DB queries without thread starvation.
- **Project Implementation:** [`backend/app/kafka/consumer.py`](file:///d:/NexusAI/backend/app/kafka/consumer.py#L90-L105).
- **Tradeoff:** Small thread context-switch overhead ($\approx 10\mu\text{s}$).
- **Follow-up:** *Why not use aiokafka instead?* (See Q5.1).

#### Q2.2: How is database connection pooling configured in SQLAlchemy Async?
- **Short Answer:** We use `create_async_engine` with `asyncpg`, setting `pool_size=20`, `max_overflow=10`, and `pool_pre_ping=True`.
- **Deep Answer:** `asyncpg` provides native asynchronous binary wire protocol communication with PostgreSQL. `pool_pre_ping=True` validates connection health before returning it from the pool, preventing stale connection errors during database restarts.
- **Project Implementation:** [`backend/app/db/session.py`](file:///d:/NexusAI/backend/app/db/session.py).
- **Tradeoff:** Pool size must be sized in conjunction with PostgreSQL's `max_connections` to avoid database connection exhaustion.
- **Follow-up:** *How do you prevent connection leaks during worker exceptions?* (See Q2.3).

#### Q2.3: How do you prevent database connection leaks during worker execution?
- **Short Answer:** All database operations are wrapped inside `async with AsyncSessionLocal() as session:` context managers.
- **Deep Answer:** The async context manager automatically handles session rollback on exceptions and releases the connection back to the pool in its `__aexit__` handler, even if unhandled errors or cancellation occur.
- **Project Implementation:** [`workers/processor/processor.py`](file:///d:/NexusAI/workers/processor/processor.py#L40-L70).
- **Tradeoff:** Requires disciplined use of context managers across all database access layers.
- **Follow-up:** *What happens if a transaction hangs?* (Use database-level `statement_timeout`).

#### Q2.4: Why separate /livez from /readyz health probes?
- **Short Answer:** `/livez` verifies the process is running without checking external databases to prevent restart loops. `/readyz` validates database and Redis connectivity before routing traffic.
- **Deep Answer:** If `/livez` pings PostgreSQL and the database suffers a transient 5-second connection spike, Kubernetes would mark all API pods dead and trigger simultaneous container restarts, worsening the outage. Separating probes ensures liveness keeps containers alive while readiness takes them out of the load balancer rotation until dependencies recover.
- **Project Implementation:** [`backend/app/api/v1/health.py`](file:///d:/NexusAI/backend/app/api/v1/health.py).
- **Tradeoff:** Requires configuring two separate probe endpoints in deployment manifests.
- **Follow-up:** *What HTTP status code does /readyz return when DB is down?* (`503 Service Unavailable`).

---

### LEVEL 3 — SYSTEM DESIGN & DISTRIBUTED SYSTEMS

#### Q3.1: How is strict idempotency guaranteed under at-least-once delivery?
- **Short Answer:** Every event has a deterministic UUID (`event_id`). Before processing, the worker checks the `ProcessingJob` table for `idempotency_key = "proc:{event_id}"`. If duplicate writes collide concurrently, the PostgreSQL unique constraint throws an `IntegrityError`, which is caught and skipped safely.
- **Deep Answer:** When two worker replicas consume duplicate events concurrently, both could check `ProcessingJob` before either commits. The unique constraint on `idempotency_key` ensures that the second commit fails at the database level. Catching `IntegrityError` in `processor.py` rolls back the session and commits the Kafka offset as a safe no-op.
- **Project Implementation:** [`workers/processor/processor.py`](file:///d:/NexusAI/workers/processor/processor.py#L40-L65).
- **Tradeoff:** Additional database index lookup per event ($\approx 0.4\text{ms}$).
- **Follow-up:** *Why is application-level deduplication alone not enough?* (Race conditions between check and commit across distributed workers).

#### Q3.2: How does WikiPulse handle backpressure when ingestion outpaces worker processing?
- **Short Answer:** Apache Kafka acts as an elastic shock absorber. Ingestion continues enqueuing events to disk logs, while worker consumer lag increases temporarily without crashing the system.
- **Deep Answer:** Because Kafka uses a pull-based consumer model, workers pull batches at their sustainable throughput. Backpressure does not propagate upstream to the stream ingestor or client SSE connections. Workers can scale horizontally by adding replicas to the consumer group up to the partition count.
- **Project Implementation:** [`backend/app/kafka/consumer.py`](file:///d:/NexusAI/backend/app/kafka/consumer.py).
- **Tradeoff:** Temporary lag accumulation during massive traffic bursts.
- **Follow-up:** *How do you monitor consumer lag?* (`kafka-consumer-groups.sh` or Prometheus lag metrics).

#### Q3.3: Why choose Kafka over RabbitMQ or Redis Streams?
- **Short Answer:** Kafka provides disk-backed commit log persistence, partition-level total ordering, independent consumer groups, and unlimited offset replayability.
- **Deep Answer:** RabbitMQ deletes messages upon consumer ACK, preventing historical event replay or adding new downstream workers (e.g. embedding worker) to re-read past events. Redis Streams stores all messages in RAM, risking OOM during extended consumer outages. Kafka stores partitioned logs on disk with configurable retention periods.
- **Project Implementation:** [`docs/tradeoffs.md`](file:///d:/NexusAI/docs/tradeoffs.md).
- **Tradeoff:** Requires running a Kafka cluster with dedicated storage and JVM/KRaft controller.
- **Follow-up:** *What is the delivery guarantee in Kafka?* (At-least-once with manual offset commits).

---

### LEVEL 4 — KAFKA & CONFLUENT-KAFKA CLIENT

#### Q4.1: Why did you migrate from aiokafka to confluent-kafka?
- **Short Answer:** `confluent-kafka` is the official enterprise Python client backed by `librdkafka` (native C). It provides higher raw throughput, zero-copy buffer management, robust TCP connection handling, and official Kafka protocol support.
- **Deep Answer:** While `aiokafka` is pure Python asyncio, it suffers from Python GIL bottlenecks during high-throughput serialization, lacks native C micro-batching, and exhibits socket lifecycle fragility across event loop closures. `confluent-kafka` delegates low-level wire protocols and TCP buffering to `librdkafka`, drastically reducing CPU overhead.
- **Project Implementation:** [`backend/app/kafka/producer.py`](file:///d:/NexusAI/backend/app/kafka/producer.py), [`docs/adr/ADR-009-confluent-kafka-client.md`](file:///d:/NexusAI/docs/adr/ADR-009-confluent-kafka-client.md).
- **Tradeoff:** Synchronous C-extension requires `asyncio.to_thread` for consumer polling.
- **Follow-up:** *How are delivery callbacks handled?* (See Q4.2).

#### Q4.2: How do producer delivery callbacks work in confluent-kafka?
- **Short Answer:** `_delivery_report(err, msg)` is passed to `producer.produce()`. librdkafka invokes the callback in background C threads, and `producer.poll(0)` dispatches them to Python on loop ticks.
- **Deep Answer:** When `produce()` is called, the message is queued in C memory. Once the broker sends a TCP ACK (or fails after retries), librdkafka marks the delivery status. Calling `poll(0)` triggers the callback, allowing WikiPulse to log errors, record metrics, or capture partition offsets without blocking.
- **Project Implementation:** [`backend/app/kafka/producer.py`](file:///d:/NexusAI/backend/app/kafka/producer.py#L23-L30).
- **Tradeoff:** Callbacks are asynchronous and not executed until `poll()` or `flush()` is called.
- **Follow-up:** *What happens on application shutdown?* (`await asyncio.to_thread(producer.flush)`).

#### Q4.3: Why is enable.auto.commit set to False?
- **Short Answer:** Auto-commit commits offsets periodically in the background regardless of whether the database write succeeded, causing message loss if a worker crashes mid-transaction.
- **Deep Answer:** Setting `enable.auto.commit = False` allows manual offset commits (`consumer.commit(msg)`) strictly after the worker commits PostgreSQL and Redis transactions. This guarantees at-least-once delivery: if a worker crashes before committing, the message is re-delivered upon rebalance.
- **Project Implementation:** [`backend/app/kafka/consumer.py`](file:///d:/NexusAI/backend/app/kafka/consumer.py#L55-L65).
- **Tradeoff:** Requires manual offset management code in consumer loops.
- **Follow-up:** *What happens if the offset commit fails after the DB write?* (Database idempotency prevents duplicate entities on redelivery).

---

### LEVEL 5 — RAG, EMBEDDINGS & VECTOR SEARCH

#### Q5.1: How does the hybrid search pipeline balance lexical and semantic results?
- **Short Answer:** It retrieves top-20 candidates from vector search and top-20 candidates from full-text search, then fuses their ranks using Reciprocal Rank Fusion ($k=60$) before applying a cross-scoring candidate reranker.
- **Deep Answer:** FTS scores are BM25-based text frequencies, while vector scores are cosine distances ($[-1, 1]$). Normalizing raw scores directly across different mathematical distributions is fragile. RRF converts raw scores into rank positions ($1, 2, \dots, N$) and applies $1/(60 + \text{rank})$, creating a stable, scale-invariant combined ranking.
- **Project Implementation:** [`backend/app/search/hybrid.py`](file:///d:/NexusAI/backend/app/search/hybrid.py#L60-L85).
- **Tradeoff:** Requires running two independent database queries per search request.
- **Follow-up:** *What is the measured latency of hybrid search?* (Avg: 15.89ms, p95: 21.86ms).

#### Q5.2: How do you defend against prompt injection inside Wikipedia edits?
- **Short Answer:** External Wikipedia content is treated strictly as untrusted data. We filter injection patterns via regex, fence chunks inside `<untrusted_wikipedia_content>` XML tags, and strictly validate LLM output against Pydantic schemas.
- **Deep Answer:** Attackers can embed strings like `IGNORE ALL PREVIOUS INSTRUCTIONS` in edit summaries. `sanitize_external_text()` neutralizes these patterns. The prompt instructs the model that content within `<untrusted_wikipedia_content>` tags is reference data only. Output is parsed into `AIAnalysisOutput`, ensuring malicious instructions cannot hijack response format.
- **Project Implementation:** [`backend/app/core/security.py`](file:///d:/NexusAI/backend/app/core/security.py#L11-L38), [`backend/app/rag/context_builder.py`](file:///d:/NexusAI/backend/app/rag/context_builder.py).
- **Tradeoff:** Regex sanitization adds a minor CPU string-processing overhead ($\approx 0.05\text{ms}$).
- **Follow-up:** *How do you prevent hallucinations on out-of-domain queries?* (See Q5.3).

#### Q5.3: How do you prevent hallucinations when retrieved context is irrelevant?
- **Short Answer:** The context builder enforces strict evidence-only instructions. If retrieved chunks do not contain relevant facts, the model is instructed to return "Insufficient evidence in current knowledge stream".
- **Deep Answer:** Hallucinations occur when LLMs attempt to answer ungrounded queries from parametric memory. By setting clear confidence thresholds and empty evidence instructions, the model defaults to explicit uncertainty rather than fabricating claims. All returned claims must map to verified citations.
- **Project Implementation:** [`backend/app/rag/context_builder.py`](file:///d:/NexusAI/backend/app/rag/context_builder.py).
- **Tradeoff:** Reduces response verbosity on ambiguous or edge-case queries.
- **Follow-up:** *What metadata is included in a citation?* (`article_title`, `revision_id`, `occurred_at`, `snippet`, `relevance_reason`).

---

### LEVEL 6 — FAILURE ENGINEERING & RELIABILITY

#### Q6.1: What is the difference between a Kafka broker outage and a poison-pill message?
- **Short Answer:** A Kafka broker outage is an infrastructure network/server disconnect where producers buffer and retry. A poison pill is an application data failure where malformed payloads are routed to a Dead Letter Queue (DLQ).
- **Deep Answer:** During a broker outage, the transport layer is unavailable; librdkafka handles reconnection in background threads without data loss. A poison pill is successfully delivered by Kafka but fails application schema validation; retrying infinitely would block the partition, so it is routed to `wikimedia.dlq` after 3 retries, advancing the offset.
- **Project Implementation:** [`backend/app/kafka/retry.py`](file:///d:/NexusAI/backend/app/kafka/retry.py), [`backend/app/kafka/dlq.py`](file:///d:/NexusAI/backend/app/kafka/dlq.py).
- **Tradeoff:** DLQ messages require operational monitoring to inspect corrupted payloads.
- **Follow-up:** *How do you inspect DLQ messages?* (Kafka UI on port 8080 or dedicated DLQ consumer).

#### Q6.2: What happens when the primary LLM provider (Google Gemini) fails or rate-limits?
- **Short Answer:** `LLMGateway` catches the HTTP error or timeout ($10\text{s}$) and automatically cascades to local Ollama (`llama3.2:1b`), and finally to a deterministic mock provider.
- **Deep Answer:** The gateway abstracts provider logic behind a unified interface. If Gemini returns HTTP 429/504, the router cascades down the fallback chain without dropping the user request. In development and offline testing, the mock provider generates schema-compliant responses in $0.20\text{ms}$.
- **Project Implementation:** [`backend/app/llm/gateway.py`](file:///d:/NexusAI/backend/app/llm/gateway.py).
- **Tradeoff:** Fallback models may produce less sophisticated summaries than frontier models.
- **Follow-up:** *How is the fallback tracked?* (Prometheus metric `LLM_FALLBACK_COUNT`).

---

### LEVEL 7 — SCALABILITY & PERFORMANCE ENGINEERING

#### Q7.1: What is the primary bottleneck in the WikiPulse pipeline?
- **Short Answer:** The primary compute bottleneck is dense vector embedding generation (SentenceTransformers matrix multiplication on CPU, $\approx 0.8\text{ms}$ per chunk).
- **Deep Answer:** PostgreSQL relational writes and Kafka enqueues execute in $< 2\text{ms}$. However, transforming text into 384-dimensional dense vectors requires heavy float matrix multiplication. Under bursts $> 1,500\text{ chunks/sec}$, scaling requires GPU-accelerated worker instances or batch embedding inference.
- **Project Implementation:** [`workers/embedding/embedding_worker.py`](file:///d:/NexusAI/workers/embedding/embedding_worker.py), [`docs/PERFORMANCE_AND_SCALABILITY.md`](file:///d:/NexusAI/docs/PERFORMANCE_AND_SCALABILITY.md).
- **Tradeoff:** Running GPU instances increases cloud infrastructure costs.
- **Follow-up:** *How do you scale embedding workers horizontally?* (Deploy multiple worker replicas in the `wikipulse.embedding` consumer group).

#### Q7.2: Why does adding more worker replicas than Kafka partitions not increase throughput?
- **Short Answer:** Kafka assigns at most one consumer per partition in a consumer group. Any extra worker replicas remain idle as standby spares.
- **Deep Answer:** Kafka guarantees in-order message consumption within a partition by assigning each partition to exactly one consumer thread in a group. If a topic has 3 partitions and you launch 5 worker replicas, 2 replicas will receive zero partition assignments. To scale to 16 workers, the topic must be partitioned into 16 partitions.
- **Project Implementation:** [`docs/capacity-planning.md`](file:///d:/NexusAI/docs/capacity-planning.md).
- **Tradeoff:** Partition counts cannot be easily decreased after topic creation.
- **Follow-up:** *What partition key does WikiPulse use?* (`article_title` to ensure in-order revision processing per article).

---

### LEVEL 8 — DEEP ARCHITECTURAL FOLLOW-UPS

#### Q8.1: If WikiPulse were deployed across multiple Kubernetes pods, how would Server-Sent Events (SSE) scale?
- **Short Answer:** Because SSE holds persistent HTTP connections on specific API pods, multi-pod scaling requires Redis Pub/Sub or Kafka-backed fanout to broadcast live events across all API instances.
- **Deep Answer:** In a single-node setup, an in-memory queue broadcasts events to connected SSE clients. In a multi-pod cluster behind a load balancer, clients are distributed across different pods. A message published by a worker would only reach clients on that specific pod. Introducing Redis Pub/Sub allows workers to publish once, and all API pods receive the event and push it to their local SSE clients.
- **Project Implementation:** [`docs/SYSTEM_DESIGN_DEEP_DIVE.md`](file:///d:/NexusAI/docs/SYSTEM_DESIGN_DEEP_DIVE.md).
- **Tradeoff:** Adds a Redis Pub/Sub network hop per broadcast event.
- **Follow-up:** *Why not WebSockets instead of SSE?* (SSE is unidirectional, simpler, runs over standard HTTP/2, and handles reconnections natively).

#### Q8.2: How would you evolve WikiPulse from Docker Compose to Enterprise Cloud Production?
- **Short Answer:** Replace local containers with Managed Kafka (AWS MSK / Confluent Cloud), Amazon RDS for PostgreSQL 16 with Aurora Serverless pgvector, Amazon ElastiCache for Redis Cluster, and run workers on Amazon EKS with GPU auto-scaling for embeddings.
- **Deep Answer:** 
  1. **Kafka:** AWS MSK with 3 brokers and multi-AZ replication (`min.insync.replicas=2`).
  2. **Storage:** Aurora PostgreSQL with Read Replicas dedicated to hybrid search queries.
  3. **Workers:** EKS pods with KEDA (Kubernetes Event-driven Autoscaling) scaling workers based on Kafka consumer lag.
  4. **Security:** AWS Secrets Manager for LLM API keys and TLS encryption for all Kafka wire communication.
- **Project Implementation:** [`docs/deployment.md`](file:///d:/NexusAI/docs/deployment.md), [`docs/PHASE_3_TECHNICAL_REVIEW.md`](file:///d:/NexusAI/docs/PHASE_3_TECHNICAL_REVIEW.md).
- **Tradeoff:** Significantly higher cloud hosting and managed service operational costs.
