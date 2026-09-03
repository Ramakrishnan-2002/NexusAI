# WikiPulse / NexusAI — Know Your Code: Source Code Reference

This document maps the architectural concepts in WikiPulse to exact source code files, classes, methods, and line ranges in the repository.

---

## 1. Master Code Mapping Matrix

| Architectural Capability | Source Code File | Key Class / Function | Verification Test |
| :--- | :--- | :--- | :--- |
| **Stream Ingestion & SSE Client** | `workers/stream_ingestor/wikimedia_client.py` | `WikimediaSSEClient` | `test_ingested_event_normalization` |
| **Kafka Producer (confluent-kafka)** | `backend/app/kafka/producer.py` | `EventProducer.publish()` | `test_confluent_kafka_producer_fallback_mode` |
| **Kafka Consumer (confluent-kafka)** | `backend/app/kafka/consumer.py` | `EventConsumer._consume_kafka()` | `test_confluent_kafka_consumer_fallback_mode` |
| **Retry Policy & DLQ Routing** | `backend/app/kafka/retry.py`, `dlq.py` | `RetryPolicy.execute_with_retry()`, `DeadLetterQueueHandler` | `test_retry_policy_exhaustion_routes_to_dlq` |
| **Event Processor & Idempotency** | `workers/processor/processor.py` | `EventProcessorWorker.handle_event()` | `test_processor_concurrent_idempotency_race` |
| **Redis Sliding-Window Counters** | `backend/app/redis/counters.py` | `ActivityCounterService.record_article_edit()` | `test_spike_detector.py` |
| **Trend & Spike Detection** | `workers/analytics/detector.py` | `SpikeDetector.evaluate_article()` | `test_unusual_activity_spike_detection` |
| **Dense Vector Embeddings** | `backend/app/search/embeddings.py` | `EmbeddingService.embed_text()` | `test_hybrid_search_scoring_and_retrieval` |
| **Full-Text Lexical Search (FTS)** | `backend/app/search/fts.py` | `FullTextSearchService.search()` | `test_hybrid_search_scoring_and_retrieval` |
| **Vector Similarity Search (pgvector)** | `backend/app/search/vector.py` | `VectorSearchService.search()` | `test_hybrid_search_scoring_and_retrieval` |
| **Hybrid Search & RRF Fusion** | `backend/app/search/hybrid.py` | `HybridSearchService.search_hybrid()`, `_reciprocal_rank_fusion()` | `test_rrf_scoring_formula` |
| **Candidate Reranking & Decay** | `backend/app/search/reranker.py` | `CandidateReranker.rerank()` | `test_candidate_reranking_phrase_match` |
| **RAG Context Window Builder** | `backend/app/rag/context_builder.py` | `RAGContextBuilder.build_context()` | `test_security_rag_untrusted_data_barrier` |
| **Multi-Provider LLM Gateway** | `backend/app/llm/gateway.py` | `LLMGateway.analyze_structured()` | `test_llm_gateway_cascading_fallback` |
| **Prompt Injection Defense** | `backend/app/core/security.py` | `sanitize_external_text()` | `test_sanitize_prompt_injection_patterns` |
| **Live SSE Dashboard Streaming** | `backend/app/api/v1/stream.py` | `stream_live_events()` | `test_api_endpoints.py` |
| **Liveness & Readiness Probes** | `backend/app/api/v1/health.py` | `liveness_probe()`, `readiness_probe()` | `test_health_and_readiness_endpoints` |

---

## 2. Execution Tracing Guide

```text
1. Ingestion:     workers/stream_ingestor/wikimedia_client.py ──► backend/app/kafka/producer.py
2. Processing:    workers/processor/processor.py ──► backend/app/db/session.py
3. Persistence:   backend/app/models/article.py, edit.py, processing_job.py
4. Aggregation:   backend/app/redis/counters.py (act:art:{id}:edits)
5. Analytics:     workers/analytics/detector.py ──► 'wikimedia.trend.detected'
6. Embedding:     workers/embedding/embedding_worker.py ──► backend/app/models/knowledge_chunk.py
7. Hybrid Search: backend/app/search/hybrid.py (pgvector <=> + GIN FTS + RRF k=60)
8. RAG Synthesis: backend/app/rag/context_builder.py ──► backend/app/llm/gateway.py
9. Broadcasting:  backend/app/api/v1/stream.py (asyncio.Queue maxsize=100)
```
