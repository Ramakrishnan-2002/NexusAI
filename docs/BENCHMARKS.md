# WikiPulse / NexusAI — Performance Benchmarks & Empirical Measurements

This document details the official performance benchmarks, latency distributions, measurement methodologies, and capacity classifications for WikiPulse / NexusAI.

---

## 1. Verified Empirical Benchmark Matrix

*Measured on Python 3.11 / Windows 11 host (100 iterations per benchmark, see `benchmarks/results.json`):*

| Operation | Metric | Average Latency / Throughput | p50 | p95 | p99 | Classification | Test Methodology |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL Full-Text Search (FTS)** | Latency | **1.93 ms** | **1.82 ms** | **2.47 ms** | **3.02 ms** | **MEASURED** | 100 iterations of GIN inverted index queries over knowledge chunks. |
| **pgvector Cosine Distance Search** | Latency | **11.10 ms** | **10.45 ms** | **18.77 ms** | **21.24 ms** | **MEASURED** | 100 iterations of 384d cosine distance calculations using HNSW index. |
| **Hybrid Search (Vector + FTS + RRF)** | Latency | **15.89 ms** | **15.12 ms** | **21.86 ms** | **24.28 ms** | **MEASURED** | 100 iterations of dual vector+FTS retrieval, RRF ($k=60$), and candidate reranking. |
| **Dense Vector Indexing Throughput** | Throughput | **1,312.87 chunks / sec** | — | — | — | **MEASURED** | Vectorizing batches with `SentenceTransformer('all-MiniLM-L6-v2')` ($0.762\text{ ms/chunk}$). |
| **Single-Worker Processor Throughput** | Throughput | **11.81 events / sec** | — | — | — | **MEASURED** | 50 events through full DB transaction, idempotency check, Redis update, and offset commit. |
| **3-Worker Scaled Processor Throughput**| Throughput | **35–40 events / sec** | — | — | — | **PRELIMINARY** | 150 events across 3 worker replicas consuming 3 Kafka partitions in parallel. |
| **LLM Gateway Mock Dispatch Overhead** | Latency | **0.20 ms** | **0.18 ms** | **0.85 ms** | **1.00 ms** | **MEASURED** | Pydantic schema validation and deterministic mock dispatch overhead. |
| **Local Ollama Inference (`llama3.2:1b`)**| Latency | **450–950 ms** | **620 ms** | **890 ms** | **945 ms** | **MEASURED** | Local CPU inference for structured JSON analysis via Ollama. |
| **Google Gemini Cloud API Latency** | Latency | Variable / Network dependent | — | — | — | **UNVERIFIED** | Unmeasured in offline local development environments without live cloud credentials. |

---

## 2. Benchmark Methodology & Reproducibility

### 2.1 Test Environment & Hardware Specification
- **Operating System:** Windows 11 / Docker Desktop (WSL2 backend)
- **Runtime:** Python 3.11.5, `asyncpg` 0.31, `confluent-kafka` 2.15.0 (librdkafka 2.15.0)
- **Database:** PostgreSQL 16.2 with `pgvector` 0.8.6 (HNSW index: $M=16, \text{ef\_construction}=64$)
- **Cache:** Redis 7.2.4 Alpine
- **Streaming Broker:** Apache Kafka 3.7.0 (KRaft mode, 3 partitions)
- **Benchmark Runner:** `scripts/benchmark_runner.py` (includes 10 warm-up iterations prior to capturing metrics)

### 2.2 Latency Distribution Analysis
```text
Hybrid Search Query Latency (100 Iterations):
┌────────────────────────────────────────────────────────────┐
│ Mean:   15.89 ms                                           │
│ Median: 15.12 ms                                           │
│ p95:    21.86 ms                                           │
│ p99:    24.28 ms                                           │
│ Max:    27.15 ms                                           │
└────────────────────────────────────────────────────────────┘
```
- The GIN Full-Text Search executes in parallel with the pgvector cosine distance search.
- The Reciprocal Rank Fusion (RRF) and Candidate Reranker add $< 0.8\text{ms}$ of CPU computation to the combined database response time.

---

## 3. Measured vs. Theoretical Capacity Classifications

To prevent misleading claims, all performance characteristics in WikiPulse are categorized into strict evidence tiers:

1. **MEASURED:** Empirically benchmarked and verified with reproducible logs in `benchmarks/results.json`.
2. **PRELIMINARY:** Measured under local Docker Compose conditions; reflects short-duration single-node behavior rather than multi-host production cluster guarantees.
3. **THEORETICAL SIZING:** Mathematically derived capacity models based on single-worker baselines ($W = \lceil R_{\text{peak}} / C_{\text{worker}} \rceil$).
4. **UNVERIFIED:** Implemented in code but not measured due to offline test environments (e.g. live cloud Gemini API).
