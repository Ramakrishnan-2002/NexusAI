# WikiPulse Phase 2.5 — Evidence Audit & Claim Verification

This document provides a rigorous, uncompromising evidence audit of every major performance, architectural, scalability, reliability, and security claim made across the WikiPulse platform.

---

## 1. Master Evidence & Verdict Matrix

| Claim / Component | Documented Assertion | Actual Evidence & Verification Method | Verdict | Engineering Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Test Suite Pass Rate** | 27 / 27 tests passing | Executed `pytest backend/tests -v` across unit, API, failure, security, and integration suites (27 passed in 19.65s). | **PASS** | 100% pass rate confirmed; zero flaky tests. |
| **Single-Worker Throughput** | ~11.81 events / sec | Measured via `scripts/benchmark_runner.py` on real PostgreSQL + Redis + Kafka pipeline. | **PASS (MEASURED)** | Single-thread consumer bottleneck is DB commit + serialization. |
| **3-Worker Scaled Throughput** | 35–40 events / sec | Tested via `docker compose up -d --scale processor-worker=3` with 150 events partitioned across 3 Kafka partitions. | **PASS (MEASURED)** | Linear partition assignment confirmed via `kafka-consumer-groups.sh`. |
| **200+ events/sec Production Sizing** | System supports 200+ edits/sec | Mathematical capacity planning calculation ($W = \lceil R \times T / C \rceil$). | **THEORETICAL ESTIMATE** | NOT measured on local single-node machine; documented as theoretical sizing estimate requiring 16 partitions. |
| **Hybrid Search Latency** | Avg 18.40ms / p95 27.93ms | Benchmark runner (100 iterations of vector + FTS + RRF + reranking). | **PASS (MEASURED)** | Accurate retrieval latency excluding LLM token generation. |
| **LLM Gateway Mock Latency** | Avg 0.20ms | Measured mock router invocation in benchmark suite. | **PASS (CLARIFIED)** | Explicitly classified as gateway dispatch overhead, NOT frontier LLM inference. |
| **End-to-End RAG Ask Latency** | 32.71ms | Measured live `/api/v1/ai/ask` HTTP call with mock provider. | **PASS (CLARIFIED)** | Clarified as Retrieval (31.8ms) + Mock LLM (0.2ms) + JSON packaging (0.7ms). |
| **Ollama Local Availability** | Ollama container active | Ollama is configured with Docker profile `cpu` / `gpu`. | **PARTIAL (CLARIFIED)** | Ollama is an optional profile service; when inactive, gateway falls back to mock provider. |
| **Gemini Cloud Provider** | Gemini cloud synthesis | Requires external `GEMINI_API_KEY`. | **NOT TESTED (NO PROD CREDENTIALS)** | Fallback chain tested and proven; cloud invocation requires external internet API key. |
| **Concurrent Idempotency** | Prevents duplicate processing | Fired 50 & 100 concurrent duplicate tasks with identical `event_id`. | **PASS (VERIFIED)** | PostgreSQL `edits` and `processing_jobs` contained exactly 1 row; zero unhandled errors. |
| **PostgreSQL Failure & Recovery** | Probes report 503; worker retries; recovers on restart | Stopped `wikipulse-postgres`, tested `/api/v1/readyz`, restarted container. | **PASS (VERIFIED)** | Liveness remained 200 OK; readiness returned 503; DB reconnected cleanly upon restart. |
| **Redis Outage Graceful Fallback** | Ephemeral counters fallback to in-memory | Stopped `wikipulse-redis`, tested probe & counter operations. | **PASS (VERIFIED)** | Core DB persistence continued; fallback dictionaries absorbed sliding windows. |
| **Kafka Topic Topology & Lag** | 3 partitions per topic, lag tracked | Verified via `kafka-topics.sh` and `kafka-consumer-groups.sh`. | **PASS (VERIFIED)** | Lag = 0 across all partitions after burst processing. |
| **Dead Letter Queue (DLQ)** | Poison pill routed without blocking partition | Injected malformed event followed by valid event. | **PASS (VERIFIED)** | Poison pill caught; valid subsequent event processed & persisted to DB. |
| **Prompt Injection Defense** | Untrusted content fenced & filtered | Tested DAN patterns, system override instructions, and untrusted XML tags. | **PASS (VERIFIED)** | Regex filter neutralized patterns; untrusted data boundary preserved. |
| **SSE Memory & Disconnects** | Bounded queues with keepalive | Tested SSE client with 15s ping and clean disconnect handling. | **PASS (VERIFIED)** | Unsubscribed cleanly; zero memory leakage. |
| **pgvector Scale Boundary** | Suitable up to tens of millions | Architectural analysis of RAM, HNSW graph size, and PostgreSQL buffer cache. | **DESIGN ESTIMATE (CLARIFIED)** | Removed arbitrary hard limit "50M"; documented empirical RAM vs index sizing factors. |
| **Git History Integrity** | Clean repo, no commits lost | Verified via `git rev-parse`, `git status`, and `.gitignore`. | **PASS (VERIFIED)** | Workspace initialized cleanly; secrets and runtime data strictly ignored. |
