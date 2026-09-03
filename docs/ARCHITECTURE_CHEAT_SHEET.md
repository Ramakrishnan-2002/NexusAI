# WikiPulse / NexusAI — Architecture Cheat Sheet (Pre-Interview Summary)

A concise, 3-page high-density summary of WikiPulse architecture, data flow, failure recovery, and system tradeoffs.

---

## 1. System Architecture & Component Map

```text
                                  ┌──────────────────────────────────────────────────────────┐
                                  │                  CONTROL PLANE (FastAPI)                 │
                                  │  • GET /api/v1/search (Hybrid) | POST /api/v1/ai/ask     │
                                  │  • GET /livez (Process) | GET /readyz (DB+Redis)         │
                                  │  • GET /api/v1/stream/live (SSE Bounded Queue)           │
                                  └────────────────────────────┬─────────────────────────────┘
                                                               │
┌──────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────┐
│                                                 DATA PLANE PIPELINE                                                         │
│                                                                                                                             │
│   [ Wikimedia SSE Stream ] ──► [ Stream Ingestor ] ──► [ Kafka: recentchange (3 Partitions, key=title) ]                    │
│                                                                        │                                                    │
│                                        ┌───────────────────────────────┴───────────────────────────────┐                     │
│                                        ▼                                                               ▼                     │
│                           [ Processor Worker Pool ]                                       [ Analytics Worker Pool ]         │
│                           • Deduplication (ProcessingJob)                                 • Reads Redis Rolling ZSETs       │
│                           • ACID Write (Article, Edit)                                    • Evaluates 1m, 5m, 15m Multiplier│
│                           • Redis ZSET Edit Timestamp                                     • Emits trend.detected            │
│                           • Manual Kafka Offset Commit                                                 │                     │
│                           • Emits article.processed                                                    │                     │
│                                        │                                                               │                     │
│                                        ▼                                                               ▼                     │
│                           [ Embedding Worker Pool ]                                       [ AI Analyzer Worker Pool ]       │
│                           • 384d Dense Embeddings                                         • Assembles RAG Context           │
│                           • pgvector HNSW Cosine Index                                    • LLM Gateway (Gemini->Ollama)    │
│                           • GIN Full-Text Search Index                                    • Validates AIAnalysisOutput      │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Core Architectural Pillars

### A. Kafka & `confluent-kafka` (librdkafka)
- **Async Bridge:** Non-blocking `producer.produce()` enqueues into native C memory (`linger.ms: 5`); `producer.poll(0)` dispatches delivery callbacks. `consumer.poll()` runs in worker threads via `asyncio.to_thread` to keep the asyncio event loop responsive.
- **Delivery Guarantee:** Strict **at-least-once delivery** with `enable.auto.commit = False`.
- **Partition Key:** `article_title` ensures all revisions for a given article are processed in strict chronological order on the same partition.

### B. PostgreSQL & Redis Durability Boundaries
- **Authoritative Record:** PostgreSQL 16 is the system of record.
- **Idempotency Key:** `ProcessingJob.idempotency_key = "proc:{event_id}"` with a `UNIQUE` constraint. Duplicate concurrent writes trigger `IntegrityError` $\to$ session rollback $\to$ safe skip.
- **Redis Consistency Domain:** Redis is transient derived aggregation state for rolling velocity counters ($1\text{m}, 5\text{m}, 15\text{m}$) via `ZADD` and `ZREMRANGEBYSCORE`. PostgreSQL and Redis are **separate consistency domains**; if Redis fails, the system degrades to `InMemoryFallbackRedis` without dropping database writes.

### C. Hybrid Search (RRF) & RAG Pipeline
- **Dual Retrieval:**
  1. Dense Vector Search in `pgvector` via HNSW Cosine Distance (`<=>`).
  2. Lexical Full-Text Search in PostgreSQL via GIN Inverted Index (`to_tsvector @@ plainto_tsquery`).
- **Rank Fusion:** Combined via Reciprocal Rank Fusion: $\text{RRF}(d) = \sum \frac{1}{60 + \text{rank}_i(d)}$.
- **Layered Prompt Defense:** `sanitize_external_text()` regex filtering + `<untrusted_wikipedia_content>` tag fencing + Pydantic `AIAnalysisOutput` schema validation.
- **LLM Gateway Fallback:** Google Gemini Flash $\to$ Local Ollama (`llama3.2:1b`) $\to$ Deterministic Mock Provider ($0.20\text{ms}$).

---

## 3. Verified Performance & Sizing Summary

| Metric | Measured Value | Classification |
| :--- | :--- | :--- |
| **PostgreSQL Full-Text Search Latency** | Avg: **1.93 ms** \| p95: **2.47 ms** | **MEASURED** |
| **pgvector Cosine Search Latency** | Avg: **11.10 ms** \| p95: **18.77 ms** | **MEASURED** |
| **Hybrid Search (Vector + FTS + RRF)** | Avg: **15.89 ms** \| p95: **21.86 ms** | **MEASURED** |
| **Single-Worker Processor Throughput** | **11.81 events / sec** | **MEASURED** |
| **3-Worker Scaled Processor Throughput** | **35–40 events / sec** (3 partitions) | **PRELIMINARY BENCHMARK** |
| **Dense Vector Indexing Throughput** | **1,312.87 chunks / sec** (0.76ms/chunk) | **MEASURED** |
| **Global Wikipedia Peak Sizing ($200\text{ ev/s}$)**| $W = \lceil 200 / 12.5 \rceil = 16\text{ Workers}$ (16 Partitions) | **THEORETICAL SIZING** |
| **pgvector 10M Chunks RAM Sizing** | $\approx 32\text{ GB RAM}$ ($15\text{GB vec} + 4\text{GB HNSW} + \text{buffer}$) | **THEORETICAL SIZING** |
