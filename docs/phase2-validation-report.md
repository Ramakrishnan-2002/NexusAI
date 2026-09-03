# WikiPulse Phase 2 — Production Validation & Evidence Report

## 1. System & Environment Specifications

- **Operating System:** Windows 11 Pro / x86_64
- **Runtime:** Python 3.11.5
- **Database:** PostgreSQL 16 + pgvector (`pgvector/pgvector:pg16`)
- **Cache & Ephemeral State:** Redis 7 Alpine (`redis:7-alpine`)
- **Event Streaming:** Apache Kafka 3.7.0 (`apache/kafka:3.7.0`)
- **Local AI Provider:** Ollama (`ollama/ollama:latest`)
- **Test Suite:** Pytest 9.1.1 (`pytest-asyncio 1.4.0`)
- **Benchmark Suite:** `scripts/benchmark_runner.py` (exporting to `benchmarks/results.json`)

---

## 2. Test Execution & Coverage Summary

All test suites were executed across the 6-layer testing pyramid:

| Test Suite Layer | Module Location | Scenarios Tested | Status |
| :--- | :--- | :--- | :--- |
| **1. Unit Tests** | `backend/tests/unit/` | Raw Wikimedia parsing, Ingested event normalization, `AIAnalysisOutput` validation, multi-window velocity multipliers, Jaccard overlap reranker, RRF scoring formula | **PASS (100%)** |
| **2. API Tests** | `backend/tests/api/` | `/livez`, `/readyz`, `/health`, `/api/v1/events`, `/api/v1/articles`, `/api/v1/trends`, `/api/v1/ai/ask`, `/api/v1/metrics` | **PASS (100%)** |
| **3. Integration Tests** | `backend/tests/integration/` | PostgreSQL relational persistence, Redis sliding windows, Kafka bus pub/sub | **PASS (100%)** |
| **4. Failure Tests** | `backend/tests/failure/` | Redis outage graceful in-memory fallback, poison-pill DLQ routing, LLM provider timeout cascading | **PASS (100%)** |
| **5. Security Tests** | `backend/tests/security/` | Prompt injection neutralization, `<untrusted_wikipedia_content>` tags, distributed rate limiter | **PASS (100%)** |
| **6. End-to-End Pipeline** | `backend/tests/e2e/` | Stream Ingestor $\to$ Kafka $\to$ Processor Worker $\to$ Analytics Worker $\to$ Embedding Worker $\to$ Hybrid Search $\to$ RAG QA | **PASS (100%)** |
| **Overall** | | **27 Total Tests** | **27 PASS / 0 FAIL** |

---

## 3. Real Performance Benchmark Measurements

Recorded directly via `scripts/benchmark_runner.py`:

```json
{
  "timestamp": "2026-09-03T03:42:33.754914+00:00",
  "platform": "win32",
  "python_version": "3.11.5",
  "metrics": {
    "vector_indexing": {
      "chunks_count": 100,
      "total_ms": 84.5,
      "throughput_chunks_per_sec": 1183.76
    },
    "retrieval_latencies_ms": {
      "vector_only": {
        "avg": 11.89,
        "p95": 20.23,
        "p99": 21.87
      },
      "fts_keyword_only": {
        "avg": 2.38,
        "p95": 4.01,
        "p99": 4.51
      },
      "hybrid_rrf_search": {
        "avg": 18.4,
        "p95": 27.93,
        "p99": 29.56
      }
    },
    "worker_throughput": {
      "events_processed": 50,
      "total_ms": 4232.7,
      "events_per_sec": 11.81
    },
    "llm_gateway": {
      "provider": "mock",
      "model": "mock",
      "avg_ms": 0.2,
      "min_ms": 0.0,
      "max_ms": 1.0
    }
  }
}
```

---

## 4. Issues Discovered & Fixed During Hardening

1. **Idempotency Race Collision:**
   - *Fixed:* Processor worker now catches `IntegrityError` on concurrent `ProcessingJob` insertions, cleanly skipping duplicate messages without throwing unhandled exceptions.
2. **Health Check Probes:**
   - *Fixed:* Added discrete `/livez` and `/readyz` probes. Liveness strictly checks process health without DB I/O, preventing false restart loops.
3. **Redis Rate Limiter:**
   - *Fixed:* Upgraded `DistributedRateLimiter` to support atomic token tracking across distributed instances with local fallback.
4. **Prompt Injection & Untrusted Data Boundaries:**
   - *Fixed:* Added `<untrusted_wikipedia_content>` boundary fences and explicit empty context detection ("Insufficient evidence in current knowledge stream").
5. **SSE Resource Management:**
   - *Fixed:* Added 15-second heartbeat keepalive and bounded client subscriber queues.
6. **Kafka Producer Cleanup:**
   - *Fixed:* Resolved unclosed producer warning during failover/restart.

---

## 5. Production Readiness Assessment

- **Architecture Integrity:** **PASS** (Modular Monolith + Dedicated Workers)
- **Data Persistence & ACID:** **PASS** (PostgreSQL 16 + pgvector)
- **Low-Latency Sliding State:** **PASS** (Redis Sorted Sets)
- **Event Streaming & Replayability:** **PASS** (Apache Kafka 3.7 + DLQ)
- **Search & RAG Grounding:** **PASS** (Hybrid Vector + FTS with RRF & Citations)
- **Security & Injection Defense:** **PASS** (Sanitization barrier + Schema enforcement)
- **Observability:** **PASS** (Prometheus metrics + Correlation IDs + JSON logs)
