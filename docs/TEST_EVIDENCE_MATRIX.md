# WikiPulse / NexusAI — Test Evidence Matrix

This matrix provides a complete, honest audit and classification of every test in the repository, documenting the exact test scope, infrastructure dependencies, and what each test proves versus what it does NOT prove.

---

## 1. Complete Test Suite Classification

| Test Name | File | Type | Real Infrastructure? | What It Proves | What It Does NOT Prove |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `test_health_and_readiness_endpoints` | `api/test_api_endpoints.py` | Integration (Mocked) | Mock DB/Redis | Proves `/livez` returns 200 without DB; `/readyz` checks connection flags. | Does not prove multi-node load balancer health flapping. |
| `test_articles_and_events_api` | `api/test_api_endpoints.py` | Integration (AsyncSession) | SQLite / AsyncSession | Proves `/api/v1/articles` and `/api/v1/events` schema serialization. | Does not prove 10,000 req/s API concurrency. |
| `test_trends_and_analysis_api` | `api/test_api_endpoints.py` | Integration (AsyncSession) | SQLite / AsyncSession | Proves trend listing and AI analysis retrieval endpoints. | Does not prove live SSE connection persistence. |
| `test_rag_ai_ask_api` | `api/test_api_endpoints.py` | End-to-End (Mock LLM) | SQLite + Mock LLM | Proves end-to-end `/api/v1/ai/ask` RAG request parsing and citation response structure. | Does not prove Google Gemini external API network latency. |
| `test_failure_redis_outage_graceful_fallback` | `failure/test_failure_scenarios.py` | Fallback / Unit | `InMemoryFallbackRedis` | Proves `ActivityCounterService` degrades to memory without raising exceptions. | Does not prove multi-replica Redis Sentinel automatic failover. |
| `test_failure_poison_pill_routed_to_dlq_without_blocking` | `failure/test_failure_scenarios.py` | Integration (Fallback Bus)| `InMemoryEventBus` | Proves malformed payloads trigger `RetryPolicy`, exhaust retries, and route to `wikimedia.dlq`. | Does not prove multi-broker Kafka disk partition log corruption. |
| `test_failure_llm_provider_timeout_cascading` | `failure/test_failure_scenarios.py` | Integration (Mocked) | Mock HTTP Client | Proves `LLMGateway` cascades down provider chain on 504 timeout or 429 rate limit. | Does not prove frontier LLM model reasoning quality. |
| `test_end_to_end_knowledge_pipeline` | `integration/test_e2e_pipeline.py` | End-to-End | SQLite + In-Memory Bus | Proves ingestion $\to$ normalization $\to$ chunking $\to$ vector storage $\to$ hybrid query flow. | Does not prove multi-datacenter network partition resilience. |
| `test_confluent_kafka_producer_fallback_mode` | `kafka/test_confluent_kafka_integration.py` | Fallback / Unit | `InMemoryEventBus` | Proves `EventProducer` functions seamlessly in fallback mode when Kafka is unconfigured. | Does not prove Kafka broker cluster TCP backpressure. |
| `test_confluent_kafka_consumer_fallback_mode` | `kafka/test_confluent_kafka_integration.py` | Fallback / Unit | `InMemoryEventBus` | Proves `EventConsumer` handles in-memory subscriber queue message processing. | Does not prove Kafka group coordinator heartbeat timeout. |
| `test_confluent_kafka_admin_service_instantiation` | `kafka/test_confluent_kafka_integration.py` | Fallback / Unit | `confluent_kafka.admin` | Proves `KafkaAdminService` handles empty bootstrap configurations gracefully. | Does not prove dynamic topic partition re-assignment. |
| `test_confluent_kafka_real_broker_live_connectivity` | `kafka/test_confluent_kafka_integration.py` | Integration (Live/Skip) | Docker Kafka (`:9092`)| Proves live connectivity to running Kafka broker; skips gracefully when offline. | Does not prove KRaft controller split-brain resilience. |
| `test_processor_idempotency_duplicate_events` | `kafka/test_kafka_idempotency.py` | Integration | SQLite / AsyncSession | Proves sequential duplicate `event_id` is safely skipped via `ProcessingJob.status`. | Does not prove distributed multi-master DB replication conflict resolution. |
| `test_processor_concurrent_idempotency_race` | `kafka/test_kafka_idempotency.py` | Integration (Concurrency) | SQLite / AsyncSession | Proves 50 concurrent duplicate tasks result in exactly 1 DB record; 49 roll back via `IntegrityError`. | Does not prove cross-region distributed multi-active database locking. |
| `test_retry_policy_transient_failure_then_success` | `kafka/test_retry_dlq.py` | Unit | None | Proves `RetryPolicy` executes exponential backoff on transient errors and succeeds on retry. | Does not prove network jitter compensation. |
| `test_retry_policy_exhaustion_routes_to_dlq` | `kafka/test_retry_dlq.py` | Unit | None | Proves `RetryPolicy` invokes `on_dlq` callback after max attempts are exhausted. | Does not prove disk exhaustion on DLQ topic partition. |
| `test_llm_gateway_mock_provider` | `llm/test_gateway_fallback.py` | Unit | None | Proves `MockProvider` generates schema-compliant `AIAnalysisOutput` responses in $< 1\text{ms}$. | Does not prove deep contextual language comprehension. |
| `test_llm_gateway_cascading_fallback` | `llm/test_gateway_fallback.py` | Unit | None | Proves provider fallback order (`gemini -> ollama -> mock`). | Does not prove Ollama GPU inference performance. |
| `test_hybrid_search_scoring_and_retrieval` | `rag/test_hybrid_search.py` | Integration | SQLite / Memory | Proves Reciprocal Rank Fusion ($k=60$) combines vector and keyword results properly. | Does not prove 100M+ vector ANN recall accuracy. |
| `test_security_prompt_injection_neutralization` | `security/test_security_hardening.py` | Unit | None | Proves prompt injection patterns (`IGNORE ALL PREVIOUS INSTRUCTIONS`) are sanitized. | Does not prove protection against novel zero-day prompt injection obfuscations. |
| `test_security_rag_untrusted_data_barrier` | `security/test_security_hardening.py` | Unit | None | Proves context builder frames content in `<untrusted_wikipedia_content>` XML fences. | Does not prove LLM strict adherence to system instructions. |
| `test_security_rate_limiter_throttling` | `security/test_security_hardening.py` | Integration | `InMemoryFallbackRedis` | Proves rate limiter rejects requests when bucket tokens exceed threshold. | Does not prove distributed DDoS mitigation across edge CDNs. |
| `test_sanitize_prompt_injection_patterns` | `unit/test_prompt_defense.py` | Unit | None | Proves regex substitution for known adversarial instruction strings. | Does not prove semantic intent classification. |
| `test_sanitize_length_truncation` | `unit/test_prompt_defense.py` | Unit | None | Proves input length truncation to 4,000 characters to prevent buffer exhaustion. | Does not prove optimal token chunk sizing for RAG. |
| `test_candidate_reranking_phrase_match` | `unit/test_reranker.py` | Unit | None | Proves candidate reranker boosts exact query phrase matches. | Does not prove cross-encoder transformer ranking quality. |
| `test_rrf_scoring_formula` | `unit/test_rrf_math.py` | Unit | None | Proves exact mathematical calculation of $1/(60 + \text{rank})$. | Does not prove optimal hyperparameter $k$ for all domain corpora. |
| `test_reranker_deduplication` | `unit/test_rrf_math.py` | Unit | None | Proves candidate list deduplication by article ID. | Does not prove multi-hop graph relationship resolution. |
| `test_raw_wikimedia_event_validation` | `unit/test_schemas.py` | Unit | None | Proves Pydantic parsing of raw Wikipedia JSON payloads. | Does not prove parsing of future Wikipedia API schema breaking changes. |
| `test_ingested_event_normalization` | `unit/test_schemas.py` | Unit | None | Proves byte delta calculation and default values during event ingestion. | Does not prove real-time clock drift synchronization. |
| `test_ai_structured_output_validation` | `unit/test_schemas.py` | Unit | None | Proves strict Pydantic parsing of `AIAnalysisOutput` JSON schema. | Does not prove model adherence without structured output mode. |
| `test_baseline_activity_scoring` | `unit/test_spike_detector.py` | Unit | None | Proves baseline velocity calculations across rolling windows. | Does not prove seasonal holiday Wikimedia traffic modeling. |
| `test_unusual_activity_spike_detection` | `unit/test_spike_detector.py` | Unit | None | Proves multiplier thresholding flags breaking news spikes. | Does not prove coordinated vandalism ring detection. |
