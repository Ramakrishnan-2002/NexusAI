# NexusAI Backend Engineering Study Roadmap

This roadmap defines the **exact dependency-ordered study path and prerequisite DAG** across the NexusAI backend engineering stories.

---

## 1. The 5-Pass Study Methodology

```mermaid
flowchart TD
    Pass1["PASS 1: Python & FastAPI Foundations\n(Asyncio, Lifespan, Pydantic, Dependency Injection)"]
    Pass2["PASS 2: Persistence & Ingestion Pipeline\n(SQLAlchemy Async, Idempotency, Ingestor, Processor)"]
    Pass3["PASS 3: Event-Driven Streaming & Redis State\n(Kafka Partitions, Consumer Offsets, DLQ, ZSET Velocity)"]
    Pass4["PASS 4: Hybrid Search, RRF & RAG AI Gateway\n(GIN FTS, pgvector HNSW, RRF k=60, Fallback Chain)"]
    Pass5["PASS 5: Production System Design & Scaling\n(Capacity Math, Failure Modes, Tradeoffs, Scaling)"]

    Pass1 --> Pass2
    Pass2 --> Pass3
    Pass3 --> Pass4
    Pass4 --> Pass5
```

---

## 2. Story Dependency Graph (DAG)

```mermaid
graph TD
    classDef foundation fill:#1e293b,stroke:#64748b,stroke-width:2px,color:#fff;
    classDef persistence fill:#0f172a,stroke:#3b82f6,stroke-width:2px,color:#fff;
    classDef streaming fill:#1e1b4b,stroke:#8b5cf6,stroke-width:2px,color:#fff;
    classDef search fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#fff;
    classDef ai fill:#451a03,stroke:#f59e0b,stroke-width:2px,color:#fff;
    classDef system fill:#3b0764,stroke:#d946ef,stroke-width:2px,color:#fff;

    %% Module 1: Foundations
    S01["STORY-01: Asyncio Event Loop"]:::foundation
    S02["STORY-02: Pydantic v2 Schemas"]:::foundation
    S03["STORY-03: ASGI Lifespan"]:::foundation
    S04["STORY-04: SSE Broadcaster"]:::foundation

    S01 --> S03
    S01 --> S04
    S02 --> S03

    %% Module 2: Persistence & Redis
    S05["STORY-05: Postgres Schema & Models"]:::persistence
    S06["STORY-06: Idempotency Store"]:::persistence
    S07["STORY-07: Redis Sliding ZSET"]:::persistence
    S08["STORY-08: Redis Mutex Lock"]:::persistence

    S01 --> S05
    S03 --> S05
    S05 --> S06
    S01 --> S07
    S07 --> S08

    %% Module 3: Kafka Streaming & Workers
    S09["STORY-09: Kafka Producer & Key Routing"]:::streaming
    S10["STORY-10: Dead Letter Queue (DLQ)"]:::streaming
    S19["STORY-19: Stream Ingestor"]:::streaming
    S20["STORY-20: Processor Worker"]:::streaming
    S21["STORY-21: Analytics Worker"]:::streaming

    S02 --> S09
    S09 --> S10
    S09 --> S19
    S06 --> S20
    S09 --> S20
    S10 --> S20
    S07 --> S21
    S08 --> S21
    S20 --> S21

    %% Module 4: Search & RAG
    S11["STORY-11: PostgreSQL GIN FTS"]:::search
    S12["STORY-12: pgvector Cosine Search"]:::search
    S13["STORY-13: Reciprocal Rank Fusion (RRF)"]:::search
    S22["STORY-22: Embedding Worker"]:::search

    S05 --> S11
    S05 --> S12
    S20 --> S22
    S22 --> S12
    S11 --> S13
    S12 --> S13

    %% Module 5: LLM Gateway & AI
    S14["STORY-14: LLM Gateway Fallback"]:::ai
    S15["STORY-15: Prompt Defense (XML)"]:::ai
    S23["STORY-23: AI Worker"]:::ai

    S01 --> S14
    S02 --> S14
    S14 --> S15
    S13 --> S15
    S21 --> S23
    S14 --> S23

    %% Module 6: System Design & Scaling
    S35["STORY-35: Docker Compose Topology"]:::system
    S36["STORY-36: Pytest Async Suite"]:::system
    S37["STORY-37: Horizontal Scaling Evolution"]:::system

    S20 --> S35
    S21 --> S35
    S22 --> S35
    S23 --> S35
    S35 --> S36
    S35 --> S37
```

---

## 3. Detailed Pass Breakdown

### PASS 1: Python & FastAPI Foundations
* **Target Stories:** `STORY-01` to `STORY-04`
* **Key Files:** [`backend/app/main.py`](file:///d:/NexusAI/backend/app/main.py), [`backend/app/api/deps.py`](file:///d:/NexusAI/backend/app/api/deps.py), [`backend/app/schemas/event.py`](file:///d:/NexusAI/backend/app/schemas/event.py)
* **Mastery Criteria:** Master the single-threaded event loop, ASGI lifespan startup/shutdown, dependency injection rollback safety, and Pydantic v2 validation.

---

### PASS 2: Persistence & Ingestion Pipeline
* **Target Stories:** `STORY-05` to `STORY-06`, `STORY-19` to `STORY-20`
* **Key Files:** [`backend/app/db/session.py`](file:///d:/NexusAI/backend/app/db/session.py), [`backend/app/models/`](file:///d:/NexusAI/backend/app/models/), [`workers/processor/processor.py`](file:///d:/NexusAI/workers/processor/processor.py)
* **Mastery Criteria:** Master SQLAlchemy 2.0 `AsyncSession`, connection pool pre-pinging, and `ProcessingJob` database-level idempotency deduplication.

---

### PASS 3: Distributed Streaming & Redis State
* **Target Stories:** `STORY-07` to `STORY-10`, `STORY-21` to `STORY-23`
* **Key Files:** [`backend/app/kafka/`](file:///d:/NexusAI/backend/app/kafka/), [`backend/app/redis/client.py`](file:///d:/NexusAI/backend/app/redis/client.py), [`workers/analytics/analytics.py`](file:///d:/NexusAI/workers/analytics/analytics.py)
* **Mastery Criteria:** Trace Kafka partition keys (`article_title`), manual offset commit loops, poison pill Dead Letter Queue routing, and Redis ZSET rolling sliding windows.

---

### PASS 4: Hybrid Search, RRF & RAG AI Gateway
* **Target Stories:** `STORY-11` to `STORY-15`, `STORY-24` to `STORY-34`
* **Key Files:** [`backend/app/search/`](file:///d:/NexusAI/backend/app/search/), [`backend/app/rag/`](file:///d:/NexusAI/backend/app/rag/), [`backend/app/llm/gateway.py`](file:///d:/NexusAI/backend/app/llm/gateway.py)
* **Mastery Criteria:** Understand dual-index retrieval (pgvector + GIN FTS), Reciprocal Rank Fusion ($k=60$), candidate reranking, XML prompt isolation, and multi-provider LLM fallback cascades.

---

### PASS 5: System Design, Testing & Horizontal Scaling
* **Target Stories:** `STORY-35` to `STORY-40`
* **Key Files:** [`docker-compose.yml`](file:///d:/NexusAI/docker-compose.yml), [`docs/mastery/SYSTEM_DESIGN.md`](file:///d:/NexusAI/docs/mastery/SYSTEM_DESIGN.md)
* **Mastery Criteria:** Calculate storage growth and Kafka partition requirements ($W = \lceil R / C \rceil$), defend architectural tradeoffs, and articulate horizontal scaling paths.
