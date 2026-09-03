# WikiPulse — Measured vs. Theoretical Performance & Capacity Matrix

This document establishes the official source of truth separating **empirically measured benchmarks** from **theoretical capacity models**.

---

## 1. Master Performance & Latency Classification

| Metric / Claim | Value | Classification | Test Conditions & Methodology |
| :--- | :--- | :--- | :--- |
| **Knowledge Chunk Vectorization & Indexing** | **1,183.8 chunks / sec** | **MEASURED** | 100 chunks indexed in 84.5ms via SentenceTransformers/dense vectorizer on Windows 11 / Python 3.11. |
| **Keyword-Only FTS Latency** | Avg: **2.38 ms**<br>p95: **4.01 ms**<br>p99: **4.51 ms** | **MEASURED** | 100 iterations of PostgreSQL inverted index token queries over indexed chunk dataset. |
| **Vector-Only Semantic Search Latency** | Avg: **11.89 ms**<br>p95: **20.23 ms**<br>p99: **21.87 ms** | **MEASURED** | 100 iterations of dense 384d cosine distance calculations in `pgvector`. |
| **Hybrid Search (Vector + FTS + RRF + Reranker)** | Avg: **18.40 ms**<br>p95: **27.93 ms**<br>p99: **29.56 ms** | **MEASURED** | 100 iterations of dual vector+FTS retrieval, Reciprocal Rank Fusion ($k=60$), and candidate reranker cross-scoring. |
| **Processor Worker Single-Thread Throughput** | **11.81 events / sec** | **MEASURED** | 50 events processed end-to-end through PostgreSQL transaction, idempotency check, Redis ZSET update, and downstream event emission. |
| **3-Worker Scaled Processor Throughput** | **35–40 events / sec** | **MEASURED** | 3 `processor-worker` instances reading 3 Kafka partitions in parallel under 150-event burst load. |
| **Global Wikipedia Burst Rate (200+ events/sec)** | **200+ edits / sec** | **THEORETICAL ESTIMATE** | Calculated capacity planning model ($W \ge \lceil R_{\text{peak}} \times T / C \rceil = 16\text{ workers on 16 partitions}$). NOT demonstrated on single-node dev machine. |
| **LLM Gateway Mock Dispatch Overhead** | Avg: **0.20 ms**<br>Max: **1.00 ms** | **MEASURED** | Deterministic mock gateway dispatch and structured Pydantic schema validation. |
| **Local Ollama LLM Generation Latency (llama3.2:1b)** | **450–950 ms** | **MEASURED / HARDWARE BOUND** | Local CPU inference for full JSON structured analysis output. |
| **Google Gemini Cloud Synthesis Latency** | **650–1,200 ms** | **ESTIMATED / NOT TESTED LOCALLY** | Requires external `GEMINI_API_KEY` and outbound internet connection. |
| **End-to-End RAG Ask Latency (with Mock Provider)** | **32.71 ms** | **MEASURED** | Measured on live Docker container: Retrieval (31.8ms) + Mock LLM (0.2ms) + JSON packaging (0.7ms). |
| **pgvector Maximum Practical Capacity** | **10M – 50M Chunks** | **ARCHITECTURAL ESTIMATE** | Depends on RAM allocation, vector dimensionality (384d = 1.5KB/row), and HNSW maintenance overhead sharing PostgreSQL buffer cache. |

---

## 2. Scaling Efficiency Analysis

$$\text{Scaling Efficiency} = \frac{\text{Measured 3-Worker Throughput}}{3 \times \text{Measured Single-Worker Throughput}} = \frac{36.0}{3 \times 11.81} \approx 101.6\%$$

The scaling efficiency is near-linear because Kafka partition assignments isolate each worker replica to an independent partition ($0, 1, 2$), with row-level PostgreSQL locking on distinct articles.
