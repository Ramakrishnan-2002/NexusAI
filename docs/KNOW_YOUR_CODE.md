# WikiPulse / NexusAI — Know Your Code Guide

This quick-navigation guide maps every major subsystem to its exact repository entry points, core classes, database interactions, Kafka topics, failure handlers, and verification tests.

---

## 1. Subsystem Navigation Directory

```text
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                  SUBSYSTEM NAVIGATION MAP                                   │
├──────────────────────────┬─────────────────────────────────────┬────────────────────────────┤
│ Subsystem                │ Core Implementation Files           │ Verification Test          │
├──────────────────────────┼─────────────────────────────────────┼────────────────────────────┤
│ 1. Wikimedia Ingestion   │ workers/stream_ingestor/ingestor.py │ test_schemas.py            │
│ 2. Kafka Event Bus       │ backend/app/kafka/producer.py       │ test_confluent_kafka_...   │
│                          │ backend/app/kafka/consumer.py       │                            │
│ 3. Event Processor       │ workers/processor/processor.py      │ test_kafka_idempotency.py  │
│ 4. Sliding Window Metric │ backend/app/redis/counters.py       │ test_spike_detector.py     │
│ 5. Analytics & Spikes    │ workers/analytics/analytics.py      │ test_spike_detector.py     │
│ 6. Vector Embedding      │ workers/embedding/embedding_work... │ test_e2e_pipeline.py       │
│ 7. Hybrid Search (RRF)   │ backend/app/search/hybrid.py        │ test_hybrid_search.py      │
│ 8. RAG Context & LLM     │ backend/app/rag/context_builder.py  │ test_gateway_fallback.py   │
│                          │ backend/app/llm/gateway.py          │                            │
│ 9. Prompt Defense        │ backend/app/core/security.py        │ test_prompt_defense.py     │
│ 10. Live SSE Stream      │ backend/app/api/v1/stream.py        │ test_api_endpoints.py      │
│ 11. Health Probes        │ backend/app/api/v1/health.py        │ test_api_endpoints.py      │
└──────────────────────────┴─────────────────────────────────────┴────────────────────────────┘
```

---

## 2. Deep Component Breakdown

### 1. Stream Ingestor
- **Entry Point:** `workers/stream_ingestor/main.py`
- **Main Class / Function:** `StreamIngestorService.start_ingestion()` in [`workers/stream_ingestor/ingestor.py`](file:///d:/NexusAI/workers/stream_ingestor/ingestor.py)
- **Kafka Interaction:** Produces normalized JSON to `wikimedia.recentchange` (key: `article_title`).
- **Failure Handling:** Reconnection with exponential backoff on SSE disconnects; in-memory fallback if broker is unconfigured.
- **Verification Test:** `backend/tests/unit/test_schemas.py`

### 2. Event Processor Worker (Idempotent Persistence)
- **Entry Point:** `workers/processor/main.py`
- **Main Class / Function:** `EventProcessorWorker.handle_event(event_data)` in [`workers/processor/processor.py`](file:///d:/NexusAI/workers/processor/processor.py)
- **Database Interaction:** Queries & inserts `ProcessingJob` (`idempotency_key = "proc:{event_id}"`), `articles`, `editors`, and `edits` in PostgreSQL 16.
- **Kafka Interaction:** Consumes `wikimedia.recentchange`; emits `wikimedia.article.processed`; manual offset commit via `consumer.commit(msg)`.
- **Failure Handling:** Catches `IntegrityError` on concurrent duplicate collision $\to$ `await session.rollback()`, returns safely as idempotent skip.
- **Verification Test:** `backend/tests/kafka/test_kafka_idempotency.py`

### 3. Redis Rolling Sliding Windows
- **Main Class / Function:** `ActivityCounterService.record_article_edit()` in [`backend/app/redis/counters.py`](file:///d:/NexusAI/backend/app/redis/counters.py)
- **Redis Commands:** `ZADD act:art:{id}:edits <ts> <event_id>`, `ZREMRANGEBYSCORE act:art:{id}:edits -inf (now - window)`, `ZCARD act:art:{id}:edits`.
- **Failure Handling:** Catches connection timeouts and degrades to `InMemoryFallbackRedis`.
- **Verification Test:** `backend/tests/failure/test_failure_scenarios.py`

### 4. Embedding Worker & Vector Storage
- **Entry Point:** `workers/embedding/main.py`
- **Main Class / Function:** `EmbeddingWorker.handle_event(event_data)` in [`workers/embedding/embedding_worker.py`](file:///d:/NexusAI/workers/embedding/embedding_worker.py)
- **Model Used:** `SentenceTransformer('all-MiniLM-L6-v2')` producing 384-dimensional dense vectors.
- **Database Interaction:** Inserts into `knowledge_chunks` with `embedding vector(384)`.
- **Verification Test:** `backend/tests/integration/test_e2e_pipeline.py`

### 5. Hybrid Search & Reciprocal Rank Fusion (RRF)
- **Main Class / Function:** `HybridSearchService.search_knowledge()` in [`backend/app/search/hybrid.py`](file:///d:/NexusAI/backend/app/search/hybrid.py)
- **Database Queries:** Dual asynchronous queries in PostgreSQL:
  1. Vector search: `ORDER BY embedding <=> :query_vec LIMIT :limit`
  2. Full-Text Search: `WHERE to_tsvector('english', title || ' ' || content) @@ plainto_tsquery(:query)`
- **Mathematical Formula:** $\text{RRF}(d) = \sum \frac{1}{60 + \text{rank}_i(d)}$
- **Verification Test:** `backend/tests/rag/test_hybrid_search.py`, `backend/tests/unit/test_rrf_math.py`

### 6. LLM Gateway & Prompt Injection Mitigation
- **Main Class / Function:** `LLMGateway.analyze_structured()` in [`backend/app/llm/gateway.py`](file:///d:/NexusAI/backend/app/llm/gateway.py)
- **Security Sanitization:** `sanitize_external_text()` in [`backend/app/core/security.py`](file:///d:/NexusAI/backend/app/core/security.py) and `<untrusted_wikipedia_content>` tag fencing in `context_builder.py`.
- **Fallback Chain:** Google Gemini Flash $\to$ Local Ollama (`llama3.2:1b`) $\to$ Deterministic `MockProvider`.
- **Verification Test:** `backend/tests/security/test_security_hardening.py`, `backend/tests/llm/test_gateway_fallback.py`
