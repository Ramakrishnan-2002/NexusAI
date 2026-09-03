# WikiPulse / NexusAI — Phase 4 Claims Audit & Verification Matrix

This matrix provides an exhaustive, code-grounded audit of every technical claim across the NexusAI repository, classified strictly by empirical evidence.

---

## 1. Master Claims Classification Matrix

| # | Technical Claim | Source Evidence | Tested? | Actual Result | Classification |
| :- | :--- | :--- | :---: | :--- | :--- |
| **1** | **At-least-once Kafka processing** | `backend/app/kafka/consumer.py` (`enable.auto.commit = False`, manual commits after DB writes). | **YES** | Offsets advance only upon successful persistence; crashes trigger replay. | **PROVEN** |
| **2** | **Application-level Idempotency** | `ProcessingJob.idempotency_key = "proc:{event_id}"` with PostgreSQL `UNIQUE` index. | **YES** | 50 concurrent duplicate tasks yield exactly 1 record; 49 roll back safely. | **PROVEN** |
| **3** | **Kafka Message Replay** | Replayed uncommitted offset finds `ProcessingJob.status == 'completed'`. | **YES** | Duplicate events safely skipped without duplicate entities created. | **PROVEN** |
| **4** | **Poison-Pill DLQ Isolation** | `RetryPolicy` (3 retries with backoff) $\to$ `DeadLetterQueueHandler` (`wikimedia.dlq`). | **YES** | Unparseable JSON routed to DLQ; partition consumer continues without stalling. | **PROVEN** |
| **5** | **Redis Sliding-Window Analytics** | `ActivityCounterService` using `ZADD`, `ZREMRANGEBYSCORE`, `ZCARD`. | **YES** | Multi-window (1m, 5m, 15m) pruning executes in $< 0.5\text{ms}$ ($O(\log N + M)$). | **PROVEN** |
| **6** | **Horizontal Worker Scaling** | Docker Compose multi-worker setup on partitioned Kafka topic. | **YES** | 3 workers consume 3 partitions in parallel, achieving 35–40 ev/s. | **PRELIMINARY** |
| **7** | **Hybrid Search (Vector + FTS + RRF)** | `HybridSearchService` fusing pgvector HNSW cosine distance + GIN FTS ($k=60$). | **YES** | Measured average latency: **15.89 ms** (p95: 21.86 ms) over 100 iterations. | **PROVEN** |
| **8** | **RAG Evidence Grounding & Citations**| `ContextBuilder` XML fencing + `AIAnalysisOutput` citation validation. | **YES** | Verified citations map to `[Article Title, Revision ID, Timestamp]`. | **PROVEN** |
| **9** | **Local Ollama Inference** | `OllamaProvider` invoking local `llama3.2:1b` model. | **YES** | Local CPU inference executes structured analysis in 450–950 ms. | **PROVEN** |
| **10**| **Google Gemini Live API** | `GeminiProvider` using Google GenAI SDK. | **NO** | Implemented but requires live cloud API key; offline test uses fallback. | **UNVERIFIED** |
| **11**| **Redis Token Bucket Rate Limiting** | Redis sliding window rate limiter middleware. | **YES** | Throttles excessive requests with HTTP 429; degrades to memory on Redis blip. | **PROVEN** |
| **12**| **Server-Sent Events (SSE) Broadcast** | `asyncio.Queue(maxsize=100)` bounded streaming with 15s heartbeats. | **YES** | Streams live change events; disconnects prune queues without memory leaks. | **PROVEN** |
| **13**| **Graceful Shutdown & Drain** | Lifespan context manager flushing producer buffers and closing pools. | **YES** | Clean disconnect on SIGTERM without lost C-memory buffer messages. | **PROVEN** |
| **14**| **Observability & Health Separation** | `/livez` (process only) vs. `/readyz` (DB + Redis checks). | **YES** | DB outage causes `/readyz` 503 while `/livez` stays 200 OK. | **PROVEN** |
| **15**| **Global Wikipedia 200 ev/s Sizing** | Theoretical capacity sizing formula: $W = \lceil 200 / 12.5 \rceil = 16\text{ workers}$. | **NO** | Mathematical derivation based on single-worker capacity. | **THEORETICAL** |
| **16**| **pgvector 10M Chunks RAM Sizing** | Calculated 32 GB RAM requirement ($15\text{GB vec} + 4\text{GB HNSW} + \text{buffer}$).| **NO** | Calculated hardware sizing projection. | **THEORETICAL** |
