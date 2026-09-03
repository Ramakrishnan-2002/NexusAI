# NexusAI / WikiPulse — Performance Benchmarks & Empirical Measurements

This document details the official benchmark results, latency distributions, test environments, and capacity sizing models for NexusAI.

---

## 1. Verified Empirical Benchmark Matrix

*Measured on Python 3.11 / Windows 11 host (100 iterations per benchmark, see `benchmarks/results.json`):*

| Operation | Metric | Value | Classification | Test Methodology |
| :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL Full-Text Search (FTS)** | Latency | Avg: **1.93 ms** \| p95: **2.47 ms** \| p99: **3.02 ms** | **MEASURED** | 100 iterations of inverted index queries over knowledge chunks. |
| **pgvector Cosine Distance Search** | Latency | Avg: **11.10 ms** \| p95: **18.77 ms** \| p99: **21.24 ms** | **MEASURED** | 100 iterations of 384d cosine distance calculations with HNSW index. |
| **Hybrid Search (Vector + FTS + RRF)** | Latency | Avg: **15.89 ms** \| p95: **21.86 ms** \| p99: **24.28 ms** | **MEASURED** | 100 iterations of dual vector+FTS retrieval, RRF ($k=60$), and candidate reranking. |
| **Dense Vector Indexing Throughput** | Throughput | **1,312.87 chunks / sec** ($0.762\text{ ms/chunk}$) | **MEASURED** | 100 chunks vectorized with `all-MiniLM-L6-v2` (model load time excluded). |
| **Single-Worker Processor Throughput** | Throughput | **11.81 events / sec** | **MEASURED** | 50 events through full DB transaction, idempotency check, Redis update, and Kafka emission. |
| **3-Worker Scaled Processor Throughput**| Throughput | **35–40 events / sec** | **PRELIMINARY BENCHMARK** | 150 events across 3 worker replicas consuming 3 Kafka partitions in parallel. |
| **LLM Gateway Mock Dispatch Overhead** | Latency | Avg: **0.20 ms** \| Max: **1.00 ms** | **MEASURED** | Schema validation and deterministic mock dispatch overhead. |
| **Local Ollama Inference (llama3.2:1b)** | Latency | **450–950 ms** | **MEASURED / HARDWARE BOUND** | Local CPU inference for structured JSON analysis. |
| **Google Gemini Cloud API Latency** | Latency | Variable / Network dependent | **NOT VERIFIED** | Unmeasured in offline local development environments. |

---

## 2. Capacity Sizing & Memory Models

### 2.1 Worker Capacity Sizing (Theoretical Sizing)
$$\text{Worker Capacity } C = 12.5\text{ events/sec}$$
$$\text{Global Wikipedia Peak } R_{\text{peak}} = 200\text{ edits/sec}$$
$$\text{Required Workers } W = \left\lceil \frac{R_{\text{peak}}}{C} \right\rceil = \left\lceil \frac{200}{12.5} \right\rceil = 16\text{ Worker Replicas}$$
$$\text{Required Kafka Partitions } P \ge W = 16\text{ Partitions}$$

### 2.2 pgvector 10M Chunks Memory Sizing (Theoretical Sizing)
- **Raw Float32 Embeddings (384d):** $10,000,000 \times 1.5\text{ KB} \approx 15\text{ GB}$.
- **HNSW Index Overhead ($M=16, \text{ef}=64$):** $\approx 4\text{ GB}$.
- **Relational Metadata & Page Overhead:** $\approx 8\text{ GB}$.
- **Total Recommended PostgreSQL RAM:** $\approx 32\text{ GB RAM}$ (ensures full HNSW graph residency in buffer cache).
