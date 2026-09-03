# WikiPulse / NexusAI — System Design & Technology Justification

This document provides the complete, interview-grade system design specification, architectural justifications, consistency models, and production evolution roadmaps for WikiPulse / NexusAI.

---

## 1. Problem Statement & Requirements

### 1.1 The Core Systems Problem
Real-time knowledge bases like Wikipedia generate sudden bursts of updates during breaking world events. An **impedance mismatch** exists between high-throughput event ingestion ($50 - 200+\text{ ev/s}$) and compute-intensive downstream operations (sliding-window velocity analytics, 384d vector embedding generation, and AI synthesis). Synchronously coupling ingestion to AI workers or using in-memory background tasks leads to thread starvation, memory exhaustion (OOM), and dropped events.

### 1.2 Functional Requirements
- Ingest real-time Wikimedia recent changes via Server-Sent Events (SSE).
- Normalize and persist entities (`Article`, `Editor`, `Edit`) in PostgreSQL.
- Detect unusual editing velocity spikes across rolling 1m, 5m, and 15m windows in Redis.
- Vectorize knowledge summaries and execute hybrid search (pgvector + PostgreSQL FTS + RRF).
- Serve grounded AI question-answering with exact citation attribution.
- Stream live events to client dashboards via Server-Sent Events.

### 1.3 Non-Functional Requirements
- **Surge Buffering:** Kafka disk commit logs absorb bursts up to $200+\text{ ev/s}$ without dropping data.
- **Strict At-Least-Once Delivery:** Offsets are committed manually only after database persistence succeeds.
- **Application-Level Idempotency:** PostgreSQL `UNIQUE` index on `ProcessingJob.idempotency_key` prevents duplicate records during message replays.
- **Sub-50ms Search Latency:** Measured hybrid search latency average of **15.89 ms** (p95: 21.86 ms).

---

## 2. Technology Justification & Tradeoff Matrix

| Technology | Role in WikiPulse | Why Chosen Over Alternatives | Tradeoff Accepted | When to Reconsider |
| :--- | :--- | :--- | :--- | :--- |
| **Apache Kafka 3.7** | Distributed Commit Log | Append-only disk persistence, partition total ordering, consumer group horizontal scaling, and event replayability. (Rejected RabbitMQ because it deletes messages on ACK). | Higher operational complexity than simple queues. | Workload requires lightweight pub/sub with <10k ev/day (Redis Streams suffices). |
| **`confluent-kafka`** | Python Kafka Client | Backed by native C `librdkafka`, enabling micro-batching in C memory (`linger.ms: 5`) and low GIL contention. (Rejected `aiokafka` due to GIL contention). | Requires thread-isolated polling (`asyncio.to_thread`) for async workers. | Pure Python environment required without C-extension compiler support. |
| **PostgreSQL 16** | System of Record | ACID transactions, foreign keys, and unique constraint idempotency. (Rejected MongoDB due to weaker multi-table transaction guarantees). | Single-node write vertical scaling limits. | Write throughput exceeds 50,000 writes/sec requiring distributed sharding. |
| **`pgvector` 0.8.6** | Vector Storage & ANN | Co-located with relational metadata, eliminating dual-write sync lag and enabling joint SQL queries. (Rejected Pinecone to avoid dual-write sync risks). | Bounded by single-node host RAM for HNSW graph residency (~32GB for 10M chunks). | Vector corpus exceeds 50M+ vectors requiring distributed vector sharding. |
| **PostgreSQL FTS** | Lexical Search | Exact keyword, acronym, and proper noun matching via GIN inverted indexes with zero external cluster overhead. (Rejected Elasticsearch). | Basic BM25 ranking without multi-cluster distributed sharding. | Document corpus exceeds 500M+ documents requiring dedicated search clusters. |
| **Redis 7** | Sliding Windows & Rate Limiting | Sub-millisecond $O(\log N + M)$ ZSET pruning (`ZREMRANGEBYSCORE`) avoiding relational database table lock contention. | Volatile in-memory state; requires degradation handling. | Analytics require complex multi-dimensional OLAP slicing (ClickHouse). |
| **FastAPI** | Control Plane API | High-concurrency async I/O, automatic OpenAPI documentation, and native Pydantic schema validation. | Single-threaded Python event loop requires strict off-loop thread delegation. | Microservices rewritten in Go/Rust for microsecond proxying. |
| **Reciprocal Rank Fusion (RRF)** | Hybrid Search Rank Fusion | Scale-invariant rank fusion ($k=60$) combining disparate vector cosine and BM25 term scores robustly. | Discards raw score magnitude differences between adjacent ranks. | Cross-encoder reranker inference latency becomes fast enough (<5ms) for all raw items. |
| **LLM Gateway** | Multi-Provider Router | High availability via cascading fallback (Gemini $\to$ Ollama $\to$ Mock) preventing vendor lock-in. | Fallback models produce varying depths of contextual synthesis. | Enterprise commits exclusively to single dedicated cloud LLM endpoint. |
| **Server-Sent Events (SSE)** | Live Dashboard Broadcast | Unidirectional HTTP/2 streaming, native browser auto-reconnection, and firewall friendly. (Rejected WebSockets). | In-process bounded queue fanout does not scale across multiple API pods without Redis Pub/Sub. | Bidirectional interactive client-to-server messaging required (WebSockets). |

---

## 3. Consistency Model & Storage Boundaries

```text
┌──────────────────────────────────────┐       ┌──────────────────────────────────────┐
│       POSTGRESQL CONSISTENCY         │       │          REDIS CONSISTENCY           │
│         (System of Record)           │   ≠   │     (Transient Aggregation State)    │
├──────────────────────────────────────┤       ├──────────────────────────────────────┤
│ • ACID Transactions & Disk WAL       │       │ • In-Memory Volatile ZSETs           │
│ • Relational Integrity               │       │ • Sliding Window Multipliers         │
│ • Unique Index Idempotency           │       │ • Graceful Degradation on Outage     │
└──────────────────────────────────────┘       └──────────────────────────────────────┘
```

- **Separate Consistency Domains:** PostgreSQL and Redis operate in **separate consistency domains** without a distributed two-phase commit (2PC) coordinator.
- **Execution Order:**
  $$\text{Kafka Message} \to \text{Schema Validation} \to \text{PostgreSQL Commit} \to \text{Redis ZSET Update} \to \text{Kafka Manual Offset Commit}$$
- **Failure Recovery:** If Redis fails after PostgreSQL commits, `ActivityCounterService` degrades to `InMemoryFallbackRedis`, and the Kafka offset is committed. If a worker crashes before committing the offset, Kafka redelivers the message; the PostgreSQL `ProcessingJob` unique constraint identifies `status == 'completed'` and skips insertion, preventing duplicate business records.

---

## 4. Local Reference vs. Enterprise Cloud Production Architecture

| Dimension | Local Reference Implementation (Current) | Enterprise Cloud Production Target |
| :--- | :--- | :--- |
| **Orchestration** | Single-host Docker Compose (`docker-compose.yml`) | Managed Kubernetes (EKS/GKE) with Helm and KEDA autoscaling |
| **Kafka Cluster** | Single broker (`apache/kafka:3.7.0`, `replication_factor: 1`) | 3+ Broker KRaft Cluster (AWS MSK) with `replication_factor: 3`, `min.insync.replicas: 2` |
| **Kafka Security** | Plaintext local Docker network | TLS mutual authentication (mTLS) & SASL/SCRAM authentication |
| **Database** | Single container PostgreSQL 16 + `pgvector` extension | Amazon Aurora PostgreSQL Multi-AZ with auto-scaling Read Replicas |
| **Connection Pooling**| Asyncpg connection pool in application (`pool_size: 20`) | PgBouncer / AWS RDS Proxy connection pooling layer |
| **Redis** | Single container Redis 7 Alpine | Redis Sentinel / AWS ElastiCache Cluster with Multi-AZ failover |
| **SSE Fanout** | In-process bounded `asyncio.Queue` | Distributed Redis Pub/Sub backplane routing to multiple edge pods |
| **Embedding Compute** | Local CPU SentenceTransformers (`all-MiniLM-L6-v2`) | GPU-accelerated Triton Inference Server cluster |
| **Secrets** | Local `.env` file | AWS Secrets Manager / HashiCorp Vault with dynamic IAM rotation |
| **Observability** | Prometheus scraping local `/metrics` endpoint | Prometheus + Grafana + OpenTelemetry distributed tracing (Jaeger/Datadog) |
