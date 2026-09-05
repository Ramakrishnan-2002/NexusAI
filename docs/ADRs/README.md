# NexusAI Architecture Decision Records (ADRs)

This directory documents the key architectural decisions made in the NexusAI (WikiPulse) system.

---

## Index of Architectural Decisions

| ADR | Title | Status | Primary Decision Summary |
| :--- | :--- | :--- | :--- |
| **[`ADR-001`](ADR-001-KAFKA.md)** | Apache Kafka for Event Ingestion | **Accepted** | Use Apache Kafka 3.7 (KRaft mode) for durable, replayable event streaming and decoupled multi-worker consumer fanout. |
| **[`ADR-002`](ADR-002-CONFLUENT-KAFKA.md)** | `confluent-kafka` Client Library | **Accepted** | Use `confluent-kafka` (C-bindings to `librdkafka`) for high-throughput, low-latency streaming rather than pure Python clients. |
| **[`ADR-003`](ADR-003-IDEMPOTENCY.md)** | Database-Level Idempotency Store | **Accepted** | Use PostgreSQL `ProcessingJob` table with unique constraint on `idempotency_key` to guarantee safe at-least-once message processing. |
| **[`ADR-004`](ADR-004-CONSISTENCY.md)** | Eventual Consistency for Spikes & Embeddings | **Accepted** | Accept eventual consistency across asynchronous analytics and embedding workers to maximize API write throughput. |
| **[`ADR-005`](ADR-005-HYBRID-RAG.md)** | Hybrid Search Architecture | **Accepted** | Combine PostgreSQL GIN Full-Text Search and pgvector HNSW dense semantic retrieval to maximize recall across keyword and semantic queries. |
| **[`ADR-006`](ADR-006-RRF.md)** | Reciprocal Rank Fusion ($k=60$) | **Accepted** | Use rank-based Reciprocal Rank Fusion ($k=60$) to combine dense and sparse scores without calibration distortion. |
| **[`ADR-007`](ADR-007-LLM-GATEWAY.md)** | Multi-Provider Resilient LLM Gateway | **Accepted** | Implement automated cascading fallback (Gemini 1.5 Flash $\to$ Local Ollama $\to$ Deterministic Mock) to guarantee high availability. |
| **[`ADR-008`](ADR-008-SSE.md)** | Server-Sent Events for Live Dashboards | **Accepted** | Use lightweight unidirectional Server-Sent Events (SSE) over HTTP rather than WebSockets for real-time live edit streaming. |
