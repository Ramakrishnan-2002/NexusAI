# WikiPulse / NexusAI — System Design Deep Dive

This comprehensive system design document explains the complete architectural blueprint, data flows, database models, vector indexing mechanisms, and distributed streaming pipeline of WikiPulse.

---

## 1. Requirements Analysis

### 1.1 Functional Requirements
1. **Real-Time Stream Ingestion:** Continuously consume Wikipedia's public recent-change SSE stream (or synthetic generator) with schema normalization and deduplication IDs.
2. **Asynchronous Event Processing:** Ingested changes must be reliably buffered and processed asynchronously into relational models (`articles`, `editors`, `edits`).
3. **Multi-Window Spike Detection:** Track edit velocity and unique editor activity over $1\text{m}$, $5\text{m}$, and $15\text{m}$ rolling windows, detecting unusual knowledge surges.
4. **Vector Embedding & Knowledge Chunking:** Convert revision deltas and edit summaries into 384-dimensional dense semantic embeddings and index them in `pgvector`.
5. **Hybrid Knowledge Retrieval:** Provide natural language hybrid search fusing lexical Full-Text Search (FTS) and dense vector search via Reciprocal Rank Fusion (RRF).
6. **Evidence-Grounded AI Synthesis:** Synthesize concise, evidence-backed answers using an LLM Gateway with deterministic fallback and verifiable citation attribution (`[Article Title, Revision ID, Timestamp]`).
7. **Real-Time Live Dashboard:** Stream live activity updates and detected spikes to client web dashboards via Server-Sent Events (SSE).

### 1.2 Non-Functional Requirements
- **High Ingestion Availability:** Ingestion service must never crash or block due to slow downstream vector/AI consumers (buffered via Apache Kafka).
- **Strict At-Least-Once Delivery:** No edit event may be lost due to worker restarts.
- **Durable Idempotency:** Duplicate messages caused by Kafka consumer rebalancing must not result in duplicate relational records.
- **Low-Latency Metric Aggregation:** Multi-window velocity calculations must execute in $< 1\text{ms}$ without relational table locks.
- **Sub-50ms Retrieval Latency:** Hybrid vector + FTS retrieval must return in $< 50\text{ms}$ (p95).
- **Prompt Injection Defense:** External Wikipedia content must be fenced and sanitized to prevent system instruction overrides.

---

## 2. End-to-End System Architecture

```text
                                  ┌──────────────────────────────────────────────────────────┐
                                  │                  CONTROL PLANE (FastAPI)                 │
                                  │  • REST Endpoints (/api/v1/search, /api/v1/ai/ask)      │
                                  │  • Probes (/livez, /readyz) | SSE (/api/v1/stream/live)  │
                                  │  • Prometheus Metrics (/metrics)                         │
                                  └────────────────────────────┬─────────────────────────────┘
                                                               │
┌──────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────┐
│                                                 DATA PLANE PIPELINE                                                         │
│                                                                                                                             │
│   [ Wikimedia SSE Stream ]                                                                                                  │
│              │                                                                                                              │
│              ▼                                                                                                              │
│   ┌──────────────────────┐      produce(key=title)      ┌─────────────────────────┐                                         │
│   │ Stream Ingestor Svc  │ ───────────────────────────► │  Apache Kafka 3.7.0     │                                         │
│   │ (confluent-kafka)    │                              │  Topic: recentchange    │                                         │
│   └──────────────────────┘                              │  Partitions: 0, 1, 2    │                                         │
│                                                         └────────────┬────────────┘                                         │
│                                                                      │                                                      │
│                                                  Consumer Group:     │                                                      │
│                                                  wikipulse.processor │                                                      │
│                                                                      ▼                                                      │
│                                                         ┌─────────────────────────┐                                         │
│                                                         │  Processor Worker Pool  │                                         │
│                                                         │  (confluent-kafka)      │                                         │
│                                                         └───────┬─────────┬───────┘                                         │
│                                                                 │         │                                                 │
│                     ┌───────────────────────────────────────────┘         └─────────────────────────┐                       │
│                     ▼                                                                               ▼                       │
│        ┌────────────────────────┐                                                      ┌────────────────────────┐           │
│        │ PostgreSQL 16          │                                                      │ Redis 7                │           │
│        │ • processing_jobs      │                                                      │ • ZSET: act:art:{id}   │           │
│        │ • articles, editors    │                                                      │ • ZSET: hot:ranking    │           │
│        │ • edits                │                                                      │ • Rate Limit Buckets   │           │
│        └────────────┬───────────┘                                                      └────────────┬───────────┘           │
│                     │                                                                               │                       │
│                     │ emit('article.processed')                                                     │ read rolling windows  │
│                     ▼                                                                               ▼                       │
│        ┌─────────────────────────┐                                                     ┌────────────────────────┐           │
│        │ Embedding Worker Pool   │                                                     │ Analytics Worker Pool  │           │
│        │ • 384d Dense Embeddings │                                                     │ • Velocity Multipliers │           │
│        │ • pgvector Indexing     │                                                     │ • Spike Thresholding   │           │
│        └────────────┬────────────┘                                                     └────────────┬───────────┘           │
│                     │                                                                               │                       │
│                     │ writes 'knowledge_chunks'                                                     │ emit('trend.detected')│
│                     ▼                                                                               ▼                       │
│        ┌─────────────────────────┐                     writes 'ai_analyses'            ┌────────────────────────┐           │
│        │ PostgreSQL + pgvector   │ ◄────────────────────────────────────────────────── │ AI Worker Pool         │           │
│        │ • knowledge_chunks      │                                                     │ • Evidence Gathering   │           │
│        │ • HNSW Vector Index     │                                                     │ • LLM Gateway Fallback │           │
│        │ • GIN Full-Text Index   │                                                     └────────────────────────┘           │
│        └─────────────────────────┘                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Detailed Request & Event Flows

### Flow 1: Wikimedia Event Ingestion
1. `StreamIngestorService` reads live JSON payloads from Wikipedia SSE.
2. `normalize_event()` extracts revision IDs, calculates byte delta, and assigns a deterministic UUID.
3. `EventProducer` enqueues message to `wikimedia.recentchange` partitioned by `article_title` using librdkafka.

### Flow 2: Idempotent Event Processing
1. `EventProcessorWorker` polls message in worker thread via `asyncio.to_thread(consumer.poll, 1.0)`.
2. Checks PostgreSQL `ProcessingJob` for `idempotency_key = "proc:{event_id}"`.
3. If not processed, persists `Article`, `Editor`, and `Edit` inside an ACID transaction.
4. Updates Redis Sorted Sets (`ZADD act:art:{id}:edits <timestamp> <event_id>`).
5. Commits offset via `asyncio.to_thread(consumer.commit, msg)`.

### Flow 3: Concurrent Duplicate Event Race Resolution
```text
Worker Replica A                      Worker Replica B
       │                                     │
       ├─► SELECT ProcessingJob (None)       ├─► SELECT ProcessingJob (None)
       │                                     │
       ├─► INSERT ProcessingJob (Succeeds)   ├─► INSERT ProcessingJob (Collides on UNIQUE key)
       │                                     │
       ├─► COMMIT Transaction                ├─► CATCH IntegrityError -> ROLLBACK
       │                                     │
       └─► Commit Kafka Offset               └─► Safe Skip -> Commit Kafka Offset
```

### Flow 4: Poison Message & DLQ Routing
1. Consumer receives invalid JSON payload.
2. `RetryPolicy` catches deserialization/validation failure and retries with exponential backoff ($10\text{ms}, 20\text{ms}, 40\text{ms}$).
3. Upon 3rd failure, `DeadLetterQueueHandler` enriches payload with stack trace, error type, and timestamp, publishing to `wikimedia.dlq`.
4. Consumer advances offset, ensuring the partition queue never hangs.

### Flow 5: RAG Query & Citation Generation
```text
User Query: "What updates occurred on Quantum Computing?"
      │
      ├──► 1. Dense Embedding: get_embedding(query) -> 384d vector
      │
      ├──► 2. pgvector Query: SELECT FROM knowledge_chunks ORDER BY embedding <=> query_vec LIMIT 20
      │
      ├──► 3. PostgreSQL FTS: SELECT FROM knowledge_chunks WHERE to_tsvector @@ plainto_tsquery LIMIT 20
      │
      ├──► 4. Reciprocal Rank Fusion: RRF(doc) = 1/(60 + rank_vec) + 1/(60 + rank_fts)
      │
      ├──► 5. Candidate Reranker: Boost exact phrase matches & recency
      │
      ├──► 6. Context Builder: Wrap top-5 chunks inside <untrusted_wikipedia_content> tags
      │
      ├──► 7. LLM Gateway: Route to Gemini Flash -> Ollama (llama3.2:1b) -> Mock Provider
      │
      └──► 8. Schema Validation & Citations: Validate AIAnalysisOutput & attach [Article, Revision, Time]
```

### Flow 6: Live SSE Dashboard & Client Backpressure
1. Client establishes persistent connection to `GET /api/v1/stream/live`.
2. Server registers a bounded `asyncio.Queue(maxsize=100)`.
3. Keep-alive heartbeat sends `: ping - <timestamp>\n\n` every 15 seconds.
4. If a client disconnects silently, queue write fails $\to$ subscriber is automatically unregistered.

---

## 4. Database Schema Deep Dive

### 4.1 Relational & Vector Entities in PostgreSQL 16
```sql
-- 1. Idempotency & Job Tracking Table
CREATE TABLE processing_jobs (
    id SERIAL PRIMARY KEY,
    idempotency_key VARCHAR(128) UNIQUE NOT NULL,
    job_type VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'in_progress',
    payload_json JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_processing_jobs_key ON processing_jobs(idempotency_key);

-- 2. Knowledge Chunks with pgvector & Full-Text Search
CREATE TABLE knowledge_chunks (
    id SERIAL PRIMARY KEY,
    article_id INTEGER REFERENCES articles(id) ON DELETE CASCADE,
    article_title VARCHAR(255) NOT NULL,
    revision_id BIGINT,
    title VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    embedding vector(384),
    metadata JSONB,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Full-Text Search GIN Index
CREATE INDEX idx_chunks_fts ON knowledge_chunks USING gin(to_tsvector('english', title || ' ' || content));

-- Vector Index (HNSW for Cosine Distance)
CREATE INDEX idx_chunks_vec_hnsw ON knowledge_chunks USING hnsw(embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);
```

---

## 5. pgvector vs. Standalone Vector Databases

| Dimension | PostgreSQL + `pgvector` | Dedicated Vector DB (Pinecone / Qdrant) |
| :--- | :--- | :--- |
| **Consistency Model** | **ACID Transactions** (atomic writes with relational entities) | **Eventual Consistency** (dual-write sync lag) |
| **Filtered Search** | Joint SQL filtering (`WHERE article_id = 10 AND occurred_at > ...`) | Payload filtering with external sync overhead |
| **Operational Simplicity** | **Single Database** for OLTP, vector search, and FTS | Multi-database orchestration & billing |
| **Scale Boundary** | Practical up to **10M – 50M chunks** per node (RAM-dependent) | Scales to **billions of vectors** via distributed sharding |
| **Migration Threshold** | When HNSW vector index exceeds available PostgreSQL shared RAM buffer. | Production scale $> 100\text{M}$ vectors with distributed clustering. |
