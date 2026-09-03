# WikiPulse / NexusAI — Comprehensive Code Map & Implementation Directory

This document provides an exhaustive, function-by-function architectural directory mapping every subsystem to its exact repository path, class names, execution responsibilities, dependencies, failure behaviors, and corresponding interview questions.

---

## 1. Subsystem: Ingestion & Streaming (Kafka Producer)

- **Directory:** `workers/stream_ingestor/` & `backend/app/kafka/`
- **File:** [`workers/stream_ingestor/ingestor.py`](file:///d:/NexusAI/workers/stream_ingestor/ingestor.py)
  - **Class:** `StreamIngestorService`
  - **Primary Function:** `start_ingestion()` / `_process_event(raw_json)`
  - **Responsibility:** Connects to Wikimedia SSE stream, parses raw payloads, generates deterministic UUID `event_id`, calculates byte deltas, and enqueues messages to Kafka.
  - **File:** [`backend/app/kafka/producer.py`](file:///d:/NexusAI/backend/app/kafka/producer.py)
  - **Class:** `EventProducer`
  - **Primary Function:** `send(topic, value, key)` / `_delivery_report(err, msg)`
  - **Responsibility:** Non-blocking C-memory message enqueue via `confluent_kafka.Producer.produce()`, `linger.ms: 5` batching, and async flush on shutdown.
  - **Dependencies:** `confluent_kafka`, `app.schemas.event.IngestedEvent`, `global_event_bus`.
  - **Failure Behavior:** If Kafka broker is unreachable, buffers in librdkafka C memory; if unconfigured, routes to in-memory fallback bus.
  - **Key Interview Question:** *"How do you publish messages without blocking the async event loop?"* $\implies$ `producer.produce()` enqueues to C memory in ~1µs; callbacks are served via `producer.poll(0)`.

---

## 2. Subsystem: Event Processing & Idempotent Persistence

- **Directory:** `workers/processor/`
- **File:** [`workers/processor/processor.py`](file:///d:/NexusAI/workers/processor/processor.py)
  - **Class:** `EventProcessorWorker`
  - **Primary Function:** `handle_event(event_data: Dict[str, Any])`
  - **Responsibility:** Consumes `wikimedia.recentchange`, checks `ProcessingJob` table for duplicate `idempotency_key = "proc:{event_id}"`, persists `Article`, `Editor`, and `Edit` entities in an ACID transaction, updates Redis rolling ZSETs, emits `wikimedia.article.processed`, and commits Kafka offset.
  - **Dependencies:** `SQLAlchemy AsyncSession`, `confluent_kafka.Consumer`, `ActivityCounterService`, `ArticleRepository`, `EditRepository`.
  - **Failure Behavior:** Concurrent duplicate insertions collide on `ProcessingJob.idempotency_key UNIQUE` constraint $\to$ catches `IntegrityError` $\to$ rolls back session $\to$ commits offset as a safe no-op.
  - **Key Interview Question:** *"What happens if a worker crashes after database commit but before Kafka offset commit?"* $\implies$ Replayed message finds `ProcessingJob.status == 'completed'`, skips insertion, and commits offset.

---

## 3. Subsystem: Kafka Consumer & Async Bridge

- **Directory:** `backend/app/kafka/`
- **File:** [`backend/app/kafka/consumer.py`](file:///d:/NexusAI/backend/app/kafka/consumer.py)
  - **Class:** `EventConsumer`
  - **Primary Function:** `_consume_kafka(handler)` / `_process_single(value, handler, topic)`
  - **Responsibility:** Manages consumer group subscriptions, executes non-blocking thread polling via `asyncio.to_thread(consumer.poll, 1.0)`, integrates `RetryPolicy`, and executes manual offset commits (`enable.auto.commit = False`).
  - **Dependencies:** `confluent_kafka.Consumer`, `RetryPolicy`, `DeadLetterQueueHandler`.
  - **Failure Behavior:** Unparseable messages trigger `RetryPolicy` (3 attempts with exponential backoff) and route to `wikimedia.dlq` before advancing partition offset.
  - **Key Interview Question:** *"Why use asyncio.to_thread for consumer polling?"* $\implies$ `confluent-kafka` polling is a blocking C network call; offloading to an OS thread prevents freezing the asyncio event loop.

---

## 4. Subsystem: Activity Analytics & Velocity Spike Detection

- **Directory:** `workers/analytics/` & `backend/app/redis/`
- **File:** [`backend/app/redis/counters.py`](file:///d:/NexusAI/backend/app/redis/counters.py)
  - **Class:** `ActivityCounterService`
  - **Primary Function:** `record_article_edit(article_id, event_id, editor, timestamp)`
  - **Responsibility:** Executes atomic Redis pipeline: `ZADD act:art:{id}:edits <ts> <event_id>`, `ZREMRANGEBYSCORE` (prunes expired timestamps), and `ZCARD` across 1m, 5m, 15m windows.
  - **File:** [`workers/analytics/analytics.py`](file:///d:/NexusAI/workers/analytics/analytics.py)
  - **Class:** `AnalyticsWorker`
  - **Primary Function:** `handle_event(event_data)`
  - **Responsibility:** Compares short-window velocity (1m) against baseline (15m). If ratio $\ge 3.0\times$ and edit count $\ge 5$, flags a trend and publishes to `wikimedia.trend.detected`.
  - **Dependencies:** `RedisManager`, `InMemoryFallbackRedis`, `event_producer`.
  - **Failure Behavior:** Redis connection timeout degrades gracefully to `InMemoryFallbackRedis`.
  - **Key Interview Question:** *"Why use Redis Sorted Sets instead of PostgreSQL queries for sliding windows?"* $\implies$ $O(\log N + M)$ sub-millisecond in-memory pruning eliminates database table lock contention.

---

## 5. Subsystem: Vector Embedding & Knowledge Chunking

- **Directory:** `workers/embedding/` & `backend/app/models/`
- **File:** [`workers/embedding/embedding_worker.py`](file:///d:/NexusAI/workers/embedding/embedding_worker.py)
  - **Class:** `EmbeddingWorker`
  - **Primary Function:** `handle_event(event_data)` / `get_embedding(text)`
  - **Responsibility:** Vectorizes article revision summaries into 384d floating-point embeddings via `SentenceTransformer('all-MiniLM-L6-v2')` and persists records to `knowledge_chunks` in PostgreSQL.
  - **File:** [`backend/app/models/knowledge_chunk.py`](file:///d:/NexusAI/backend/app/models/knowledge_chunk.py)
  - **Database Indexes:** `embedding vector(384)` with HNSW cosine index (`vector_cosine_ops`) and GIN Full-Text Search index.
  - **Key Interview Question:** *"What is the main compute bottleneck in WikiPulse?"* $\implies$ CPU-bound matrix multiplication during SentenceTransformers embedding generation (~0.8ms/chunk).

---

## 6. Subsystem: Hybrid Knowledge Retrieval (pgvector + FTS + RRF)

- **Directory:** `backend/app/search/`
- **File:** [`backend/app/search/hybrid.py`](file:///d:/NexusAI/backend/app/search/hybrid.py)
  - **Class:** `HybridSearchService`
  - **Primary Function:** `search_knowledge(query, limit)` / `_reciprocal_rank_fusion(vector_results, keyword_results)`
  - **Responsibility:** Executes dual asynchronous queries (pgvector cosine ANN + PostgreSQL GIN FTS), applies Reciprocal Rank Fusion ($k=60$), and runs candidate reranking.
  - **File:** [`backend/app/search/reranker.py`](file:///d:/NexusAI/backend/app/search/reranker.py)
  - **Class:** `CandidateReranker`
  - **Primary Function:** `rerank(candidates, query)`
  - **Responsibility:** Applies exact phrase match multipliers ($1.25\times$), temporal recency decay ($1.15\times$), and deduplicates by article ID.
  - **Key Interview Question:** *"Why is Reciprocal Rank Fusion preferred over score averaging?"* $\implies$ Vector cosine distance and BM25 term frequency scores follow different mathematical scales; RRF provides scale-invariant rank-based fusion.

---

## 7. Subsystem: LLM Gateway & Prompt Defense

- **Directory:** `backend/app/llm/` & `backend/app/rag/` & `backend/app/core/`
- **File:** [`backend/app/llm/gateway.py`](file:///d:/NexusAI/backend/app/llm/gateway.py)
  - **Class:** `LLMGateway`
  - **Primary Function:** `analyze_structured(prompt, context)`
  - **Responsibility:** Cascading provider fallback: Google Gemini Flash $\to$ Local Ollama (`llama3.2:1b`) $\to$ Deterministic Mock Provider.
  - **File:** [`backend/app/core/security.py`](file:///d:/NexusAI/backend/app/core/security.py)
  - **Primary Function:** `sanitize_external_text(text)`
  - **Responsibility:** Regex neutralization of prompt override phrases and length truncation.
  - **File:** [`backend/app/rag/context_builder.py`](file:///d:/NexusAI/backend/app/rag/context_builder.py)
  - **Primary Function:** `build_rag_context(chunks, query)`
  - **Responsibility:** Frames retrieved chunks inside `<untrusted_wikipedia_content>` XML fences and enforces empty evidence fallback instructions.
  - **Key Interview Question:** *"How do you defend against prompt injection inside Wikipedia edits?"* $\implies$ Layered mitigation: regex neutralization + XML context boundary fencing + strict Pydantic output validation.

---

## 8. Subsystem: FastAPI Control Plane & Live SSE Stream

- **Directory:** `backend/app/api/v1/`
- **File:** [`backend/app/api/v1/health.py`](file:///d:/NexusAI/backend/app/api/v1/health.py)
  - **Endpoints:** `GET /livez` (process liveness, zero external I/O), `GET /readyz` (PostgreSQL + Redis connectivity checks).
- **File:** [`backend/app/api/v1/stream.py`](file:///d:/NexusAI/backend/app/api/v1/stream.py)
  - **Endpoint:** `GET /api/v1/stream/live`
  - **Responsibility:** Subscribes connected clients to bounded `asyncio.Queue(maxsize=100)`, streams Server-Sent Events, transmits 15-second heartbeat pings, and handles clean subscriber disconnects.
