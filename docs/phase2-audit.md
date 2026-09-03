# WikiPulse Phase 2 — Comprehensive Architecture & Reliability Audit

## Executive Summary
This document presents a rigorous engineering audit of the WikiPulse codebase, infrastructure configuration, and distributed processing pipeline. While Phase 1 established a functional modular monolith with Kafka workers, Redis hot state, and FastAPI control plane, Phase 2 hardens the system against edge-case failures, unclosed resource leaks, concurrent idempotency races, prompt injection vulnerabilities, and benchmark validation.

---

## 1. Audit Findings & Severity Categorization

### A. Critical Issues (P0)
1. **Concurrent Idempotency Integrity Handling:**
   - *Finding:* In `workers/processor/processor.py`, if two worker replicas consume the same duplicate message simultaneously, both may query `ProcessingJob` before either commits, resulting in a database unique constraint integrity error (`IntegrityError`).
   - *Fix:* Wrap the `ProcessingJob` insertion in an explicit `try...except IntegrityError` block that treats constraint collisions as safe idempotent skips.
2. **Resource & Connection Leak Risk on Shutdown:**
   - *Finding:* In `backend/app/kafka/producer.py` and `consumer.py`, unclosed connections on abrupt loop cancelation could leave background threads dangling.
   - *Fix:* Implement explicit `lifespan` context cleanup and async context management across all worker processes.

### B. High Issues (P1)
1. **Health Check Separation (Liveness vs Readiness):**
   - *Finding:* Existing health endpoint did not strictly separate process liveness (`/livez`) from dependency readiness (`/readyz`). Liveness must never query external databases to avoid cascading container restart loops during transient DB blips.
   - *Fix:* Implement standard Kubernetes-compliant `/livez`, `/readyz`, and `/health` endpoints.
2. **Redis-Backed Distributed Rate Limiter:**
   - *Finding:* Rate limiting was in-memory only on the API process, making it ineffective across multiple API replicas.
   - *Fix:* Implement Redis-backed atomic sliding token bucket rate limiter with automatic in-memory fallback.
3. **RAG Insufficient Evidence & Hallucination Guardrails:**
   - *Finding:* Context builder lacked explicit instructions and tests for unanswerable queries (Case B), risking model hallucination on out-of-domain questions.
   - *Fix:* Update `RAGContextBuilder` with explicit evidence confidence thresholds and return "Insufficient evidence in current knowledge index" when retrieval score is low.

### C. Medium Issues (P2)
1. **SSE Client Queue Memory Growth:**
   - *Finding:* Long-lived SSE streams could accumulate undrained messages in memory if clients disconnect without triggering TCP FIN.
   - *Fix:* Enforce bounded queue sizes (`maxsize=100`) and handle `request.is_disconnected()` with periodic keep-alive pings.
2. **Correlation ID (`X-Request-ID`) Propagation:**
   - *Finding:* Correlation IDs were generated in API middleware but not propagated across Kafka message headers for end-to-end distributed tracing.
   - *Fix:* Inject `correlation_id` into Kafka message metadata and worker logger contexts.
3. **Sliding Window Key TTLs:**
   - *Finding:* Infrequently updated articles could leave dormant ZSET keys in Redis indefinitely.
   - *Fix:* Set explicit 1-hour rolling TTLs on `act:art:{id}:edits` and `act:art:{id}:editors` upon every write.

### D. Low Issues (P3)
1. **Benchmark Tooling & Real Infrastructure Harness:**
   - *Finding:* Initial benchmarks executed against in-memory SQLite and mock queues.
   - *Fix:* Expand `scripts/benchmark_runner.py` to support real Docker infrastructure (PostgreSQL + pgvector, Redis, Kafka) with JSON report export (`benchmarks/results.json`).

---

## 2. Remediation Plan Matrix

| Component | Audit Issue | Planned Action | Verification Method |
| :--- | :--- | :--- | :--- |
| `app/api/v1/health.py` | Missing `/livez`, `/readyz` separation | Add discrete liveness and dependency readiness probes | Unit & API Tests |
| `app/core/security.py` | In-memory only rate limiter | Redis atomic counter rate limiter with fallback | Security Test Suite |
| `workers/processor/` | Idempotency race collision | Catch `IntegrityError` on `ProcessingJob` insert | Concurrency Failure Test |
| `app/rag/context_builder.py` | Risk of hallucination on empty context | Strict prompt boundary & confidence guardrail | RAG Test Suite |
| `app/api/v1/stream.py` | SSE memory leak risk | Add bounded queue and 15s heartbeat | Disconnect Load Test |
| `docker-compose.yml` | Obsolete `version` attribute warning | Clean up compose file syntax | `docker compose config` |
| `scripts/` | Benchmark realism | Real PG+Redis+Kafka benchmark suite | `scripts/benchmark_runner.py` |
