# NexusAI / WikiPulse — Automated Testing Suite & Verification Matrix

This document details the automated test suite, classification of all 32 tests, execution instructions, and test evidence.

---

## 1. Latest Regression Test Execution

```bash
.\venv\Scripts\pytest backend\tests -v
```

```text
============================= test session starts =============================
platform win32 -- Python 3.11.5, pytest-9.1.1, pluggy-1.6.0
collected 32 items

backend/tests/api/test_api_endpoints.py::test_health_and_readiness_endpoints PASSED [  3%]
backend/tests/api/test_api_endpoints.py::test_articles_and_events_api PASSED [  6%]
backend/tests/api/test_api_endpoints.py::test_trends_and_analysis_api PASSED [  9%]
backend/tests/api/test_api_endpoints.py::test_rag_ai_ask_api PASSED      [ 12%]
backend/tests/failure/test_failure_scenarios.py::test_failure_redis_outage_graceful_fallback PASSED [ 15%]
backend/tests/failure/test_failure_scenarios.py::test_failure_poison_pill_routed_to_dlq_without_blocking PASSED [ 18%]
backend/tests/failure/test_failure_scenarios.py::test_failure_llm_provider_timeout_cascading PASSED [ 21%]
backend/tests/integration/test_e2e_pipeline.py::test_end_to_end_knowledge_pipeline PASSED [ 25%]
backend/tests/kafka/test_confluent_kafka_integration.py::test_confluent_kafka_producer_fallback_mode PASSED [ 28%]
backend/tests/kafka/test_confluent_kafka_integration.py::test_confluent_kafka_consumer_fallback_mode PASSED [ 31%]
backend/tests/kafka/test_confluent_kafka_integration.py::test_confluent_kafka_admin_service_instantiation PASSED [ 34%]
backend/tests/kafka/test_confluent_kafka_integration.py::test_confluent_kafka_real_broker_live_connectivity SKIPPED [ 37%]
backend/tests/kafka/test_kafka_idempotency.py::test_processor_idempotency_duplicate_events PASSED [ 40%]
backend/tests/kafka/test_kafka_idempotency.py::test_processor_concurrent_idempotency_race PASSED [ 43%]
backend/tests/kafka/test_retry_dlq.py::test_retry_policy_transient_failure_then_success PASSED [ 46%]
backend/tests/kafka/test_retry_dlq.py::test_retry_policy_exhaustion_routes_to_dlq PASSED [ 50%]
backend/tests/llm/test_gateway_fallback.py::test_llm_gateway_mock_provider PASSED [ 53%]
backend/tests/llm/test_gateway_fallback.py::test_llm_gateway_cascading_fallback PASSED [ 56%]
backend/tests/rag/test_hybrid_search.py::test_hybrid_search_scoring_and_retrieval PASSED [ 59%]
backend/tests/security/test_security_hardening.py::test_security_prompt_injection_neutralization PASSED [ 62%]
backend/tests/security/test_security_hardening.py::test_security_rag_untrusted_data_barrier PASSED [ 65%]
backend/tests/security/test_security_hardening.py::test_security_rate_limiter_throttling PASSED [ 68%]
backend/tests/unit/test_prompt_defense.py::test_sanitize_prompt_injection_patterns PASSED [ 71%]
backend/tests/unit/test_prompt_defense.py::test_sanitize_length_truncation PASSED [ 75%]
backend/tests/unit/test_reranker.py::test_candidate_reranking_phrase_match PASSED [ 78%]
backend/tests/unit/test_rrf_math.py::test_rrf_scoring_formula PASSED     [ 81%]
backend/tests/unit/test_rrf_math.py::test_reranker_deduplication PASSED  [ 84%]
backend/tests/unit/test_schemas.py::test_raw_wikimedia_event_validation PASSED [ 87%]
backend/tests/unit/test_schemas.py::test_ingested_event_normalization PASSED [ 90%]
backend/tests/unit/test_schemas.py::test_ai_structured_output_validation PASSED [ 93%]
backend/tests/unit/test_spike_detector.py::test_baseline_activity_scoring PASSED [ 96%]
backend/tests/unit/test_spike_detector.py::test_unusual_activity_spike_detection PASSED [100%]

======================= 31 passed, 1 skipped in 28.37s ========================
```

---

## 2. Test Classification & Scope

- **Total Collected:** 32 tests (31 passed, 1 skipped).
- **Unit Tests (15 tests):** Math, schemas, sanitization regex, reranker, retry policy.
- **Integration Tests (13 tests):** AsyncSession, concurrency race tests, DLQ, rate limiting, and in-memory event bus.
- **End-to-End Tests (2 tests):** `test_end_to_end_knowledge_pipeline`, `test_rag_ai_ask_api`.
- **Live/Skip Integration (1 test):** `test_confluent_kafka_real_broker_live_connectivity` (verifies live Kafka broker connectivity against Docker; skips gracefully when run directly on Windows host).
