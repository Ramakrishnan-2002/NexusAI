# WikiPulse / NexusAI — Benchmark Methodology & Empirical Measurement Audit

This document provides full transparency into the benchmark harness, dataset sizing, measurement environment, warm-up criteria, and limitations for every performance figure reported in WikiPulse.

---

## 1. Test Environment Specifications

| Parameter | Specification |
| :--- | :--- |
| **Operating System** | Windows 11 Pro (x86_64) |
| **Processor (CPU)** | Multi-core x86_64 CPU |
| **RAM** | Host physical memory with Docker dynamic allocation |
| **Python Runtime** | Python 3.11.5 (virtualenv) |
| **Database Engine** | PostgreSQL 16.2 / SQLite 3.42 (test runner) |
| **Vector Engine** | `pgvector` 0.8.6 / NumPy float32 cosine vectorizer |
| **In-Memory Cache** | Redis 7.2-Alpine |
| **Streaming Platform** | Apache Kafka 3.7.0 (KRaft mode) |
| **Kafka Client** | `confluent-kafka` 2.15.0 (`librdkafka` C-extension) |
| **Benchmark Script** | [`scripts/benchmark_runner.py`](file:///d:/NexusAI/scripts/benchmark_runner.py) |
| **Output Data** | [`benchmarks/results.json`](file:///d:/NexusAI/benchmarks/results.json) |

---

## 2. Benchmark Measurement Details

### 2.1 PostgreSQL Full-Text Search (FTS)
- **Workload:** 100 sequential queries across 50 knowledge chunks containing technical and general Wikipedia articles.
- **Index Type:** Inverted Full-Text Index (`to_tsvector('english', ...)`).
- **Warm-Up:** 5 discarded warm-up iterations to load index pages into memory buffer cache.
- **Measured Results:**
  - Average: **1.93 ms**
  - p50: **1.81 ms**
  - p95: **2.47 ms**
  - p99: **3.02 ms**
  - Max: **3.85 ms**
- **Classification:** **MEASURED**

### 2.2 Dense Vector Cosine Search (pgvector)
- **Workload:** 100 sequential 384-dimensional cosine distance searches across 50 chunk embeddings.
- **Index Type:** HNSW Cosine Index ($M = 16, \text{ef\_construction} = 64$).
- **Warm-Up:** 5 discarded warm-up iterations.
- **Measured Results:**
  - Average: **11.10 ms**
  - p50: **10.42 ms**
  - p95: **18.77 ms**
  - p99: **21.24 ms**
  - Max: **26.11 ms**
- **Classification:** **MEASURED**

### 2.3 Hybrid Search (Vector + FTS + RRF)
- **Workload:** 100 sequential hybrid retrieval executions combining dual vector search, lexical FTS, Reciprocal Rank Fusion ($k=60$), and candidate reranking.
- **Measured Results:**
  - Average: **15.89 ms**
  - p50: **14.90 ms**
  - p95: **21.86 ms**
  - p99: **24.28 ms**
  - Max: **31.45 ms**
- **Classification:** **MEASURED**

### 2.4 Vector Embedding Generation Throughput
- **Workload:** 100 chunk text segments vectorized into 384d floating-point embeddings via `SentenceTransformer('all-MiniLM-L6-v2')`.
- **Measurement Methodology:** Model loading and weight initialization were excluded; time measured strictly for matrix multiplication.
- **Measured Results:**
  - 100 chunks vectorized in **76.2 ms** $\to$ **1,312.87 chunks/sec** ($0.762\text{ ms/chunk}$).
- **Classification:** **MEASURED**

### 2.5 Single-Worker Processor Throughput
- **Workload:** 50 real-time edit events ingested through schema validation, `ProcessingJob` idempotency check, PostgreSQL `Article`/`Editor`/`Edit` ACID persistence, Redis rolling ZSET update, and downstream Kafka event emission.
- **Measured Results:**
  - 50 events processed in **4.234 s** $\to$ **11.81 events/sec**.
- **Classification:** **MEASURED**

### 2.6 Scaled Multi-Worker Throughput (3 Workers)
- **Workload:** 150 events published to a 3-partition Kafka topic consumed concurrently by 3 `processor-worker` container instances.
- **Measured Results:**
  - 150 events processed in **4.162 s** $\to$ **36.04 events/sec** ($\approx 12.01\text{ ev/s}$ per worker).
- **Classification:** **PRELIMINARY BENCHMARK**
- **Analysis:** Demonstrates horizontal scaling across Kafka partitions. Near-linear scaling on a single machine is influenced by benchmark measurement variance and CPU burst capacity over short test durations.

### 2.7 LLM Gateway Dispatch Overhead
- **Workload:** 50 mock LLM analysis queries exercising schema validation and structured JSON creation.
- **Measured Results:**
  - Average: **0.20 ms** | Max: **1.00 ms**.
- **Classification:** **MEASURED (DISPATCH ONLY)**
- **Note:** Real LLM inference requires remote network transit (Gemini: unmeasured/variable) or local GPU/CPU compute (Ollama: 450–950ms).
