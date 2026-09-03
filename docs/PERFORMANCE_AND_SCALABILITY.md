# WikiPulse — Performance Benchmarks & Horizontal Scalability Guide

This document provides the official performance measurements, latency percentiles, horizontal scaling calculations, and capacity sizing models for WikiPulse.

---

## 1. Verified Performance Benchmarks

*Empirically measured via `scripts/benchmark_runner.py` and exported to `benchmarks/results.json`:*

| Operation | Metric | Value | Classification | Test Methodology |
| :--- | :--- | :--- | :--- | :--- |
| **Vector Indexing & Storage** | Throughput | **1,312.87 chunks / sec** | **MEASURED** | 100 chunks indexed in 76.2ms with dense vectorizer on Python 3.11 / Windows 11. |
| **PostgreSQL Full-Text Search** | Latency | Avg: **1.93 ms** \| p95: **2.47 ms** \| p99: **3.02 ms** | **MEASURED** | 100 iterations of inverted index queries over knowledge chunks. |
| **pgvector Cosine Search** | Latency | Avg: **11.10 ms** \| p95: **18.77 ms** \| p99: **21.24 ms** | **MEASURED** | 100 iterations of 384d cosine distance calculations in `pgvector`. |
| **Hybrid Search (Vector+FTS+RRF)** | Latency | Avg: **15.89 ms** \| p95: **21.86 ms** \| p99: **24.28 ms** | **MEASURED** | 100 iterations of dual vector+FTS retrieval, RRF ($k=60$), and candidate reranking. |
| **Processor Worker (Single-Thread)** | Throughput | **11.81 events / sec** | **MEASURED** | 50 events through DB transaction, idempotency check, Redis ZSET update, and Kafka emission. |
| **Processor Worker (3-Worker Replicas)**| Throughput | **35–40 events / sec** | **MEASURED** | 3 `processor-worker` instances consuming 3 Kafka partitions in parallel under 150-event burst. |
| **LLM Gateway Mock Dispatch** | Latency | Avg: **0.20 ms** \| Max: **1.00 ms** | **MEASURED** | Schema validation and deterministic mock dispatch overhead. |
| **Local Ollama Inference (llama3.2:1b)** | Latency | **450–950 ms** | **MEASURED / HARDWARE BOUND** | Local CPU inference for structured JSON analysis. |
| **End-to-End RAG Ask API Latency** | Latency | **48.68 ms** (with Mock) | **MEASURED** | Retrieval (47.8ms) + Mock LLM (0.1ms) + JSON packaging (0.7ms). |

---

## 2. Horizontal Scalability & Partition Sizing Models

### 2.1 The Golden Rule of Kafka Parallelism
In Apache Kafka, **parallelism within a consumer group is strictly bounded by the number of topic partitions**:

$$\text{Max Active Consumers} = \text{Number of Partitions } P$$

Adding more worker replicas than topic partitions results in idle consumers.

### 2.2 Global Wikipedia Sizing Model
Global Wikipedia English generates an average of $\sim 30 - 50\text{ edits/sec}$, with breaking news bursts peaking at $\sim 200+\text{ edits/sec}$.

To size the worker cluster:

$$\text{Required Workers } W = \left\lceil \frac{\text{Peak Traffic } R}{\text{Per-Worker Throughput } C} \right\rceil = \left\lceil \frac{200\text{ ev/s}}{12.5\text{ ev/s}} \right\rceil = 16\text{ Workers}$$

$$\text{Required Kafka Partitions } P \ge W = 16\text{ Partitions}$$

---

## 3. pgvector Memory & Index Capacity Planning

### 3.1 Vector Index Size Calculation (384-Dimensional Vectors)
Each 384-dimensional floating-point vector requires:

$$\text{Raw Vector Size} = 384 \times 4\text{ bytes} = 1,536\text{ bytes } (\approx 1.5\text{ KB / row})$$

With HNSW index overhead ($M = 16, \text{ef\_construction} = 64$):
- **Raw Vector Data (10M Chunks):** $10,000,000 \times 1.5\text{ KB} \approx 15\text{ GB}$
- **HNSW Graph Overhead:** $\approx 4\text{ GB}$
- **Relational Metadata:** $\approx 8\text{ GB}$
- **Total PostgreSQL RAM Sizing for 10M Chunks:** $\approx 32\text{ GB RAM}$ (ensures full HNSW graph residency in buffer cache).
