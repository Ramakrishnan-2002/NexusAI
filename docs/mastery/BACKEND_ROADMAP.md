# NexusAI Backend Engineering Study Roadmap

This roadmap defines the **exact dependency-ordered study path** across the NexusAI codebase.

---

## The 5-Pass Study Methodology

```mermaid
flowchart TD
    Pass1["PASS 1: Python & FastAPI Foundations\n(Asyncio, Lifespan, Pydantic, Dependency Injection)"]
    Pass2["PASS 2: Persistence & Ingestion Pipeline\n(SQLAlchemy Async, Idempotency, Ingestor, Processor)"]
    Pass3["PASS 3: Event-Driven Streaming & Redis State\n(Kafka Partitions, Consumer Offsets, DLQ, ZSET Velocity)"]
    Pass4["PASS 4: Hybrid Search, RRF & RAG AI Gateway\n(GIN FTS, pgvector HNSW, RRF k=60, Fallback Chain)"]
    Pass5["PASS 5: Production System Design & Interview Defense\n(Capacity Math, Failure Modes, Tradeoffs, Scaling)"]

    Pass1 --> Pass2
    Pass2 --> Pass3
    Pass3 --> Pass4
    Pass4 --> Pass5
```

---

## Detailed Pass Breakdown

### PASS 1: Python & FastAPI Foundations (Days 1–2)
* **Target Stories:** `STORY-01` to `STORY-04`
* **Key Files:** [`backend/app/main.py`](file:///d:/NexusAI/backend/app/main.py), [`backend/app/api/deps.py`](file:///d:/NexusAI/backend/app/api/deps.py), [`backend/app/schemas/event.py`](file:///d:/NexusAI/backend/app/schemas/event.py)
* **Mastery Criteria:** Understand the single-threaded event loop, ASGI lifespan startup/shutdown, dependency injection rollback safety, and Pydantic v2 validation.

---

### PASS 2: Persistence & Ingestion Pipeline (Days 3–4)
* **Target Stories:** `STORY-05` to `STORY-06`, `STORY-19` to `STORY-20`
* **Key Files:** [`backend/app/db/session.py`](file:///d:/NexusAI/backend/app/db/session.py), [`backend/app/models/`](file:///d:/NexusAI/backend/app/models/), [`workers/processor/processor.py`](file:///d:/NexusAI/workers/processor/processor.py)
* **Mastery Criteria:** Master SQLAlchemy 2.0 `AsyncSession`, connection pool pre-pinging, and `ProcessingJob` database-level idempotency deduplication.

---

### PASS 3: Distributed Streaming & Redis State (Days 5–7)
* **Target Stories:** `STORY-07` to `STORY-10`, `STORY-21` to `STORY-23`
* **Key Files:** [`backend/app/kafka/`](file:///d:/NexusAI/backend/app/kafka/), [`backend/app/redis/client.py`](file:///d:/NexusAI/backend/app/redis/client.py), [`workers/analytics/analytics.py`](file:///d:/NexusAI/workers/analytics/analytics.py)
* **Mastery Criteria:** Trace Kafka partition keys (`article_title`), manual offset commit loops, poison pill Dead Letter Queue routing, and Redis ZSET rolling sliding windows.

---

### PASS 4: Hybrid Search, RRF & RAG AI Gateway (Days 8–10)
* **Target Stories:** `STORY-11` to `STORY-15`, `STORY-24` to `STORY-34`
* **Key Files:** [`backend/app/search/`](file:///d:/NexusAI/backend/app/search/), [`backend/app/rag/`](file:///d:/NexusAI/backend/app/rag/), [`backend/app/llm/gateway.py`](file:///d:/NexusAI/backend/app/llm/gateway.py)
* **Mastery Criteria:** Understand dual-index retrieval (pgvector + GIN FTS), Reciprocal Rank Fusion ($k=60$), candidate reranking, XML prompt isolation, and multi-provider LLM fallback cascades.

---

### PASS 5: System Design, Testing & Horizontal Scaling (Days 11–12)
* **Target Stories:** `STORY-35` to `STORY-40`
* **Key Files:** [`docker-compose.yml`](file:///d:/NexusAI/docker-compose.yml), [`docs/mastery/SYSTEM_DESIGN.md`](file:///d:/NexusAI/docs/mastery/SYSTEM_DESIGN.md), [`docs/mastery/INTERVIEW_PREP.md`](file:///d:/NexusAI/docs/mastery/INTERVIEW_PREP.md)
* **Mastery Criteria:** Calculate storage growth and Kafka partition requirements ($W = \lceil R / C \rceil$), defend architectural tradeoffs under interview scrutiny, and articulate horizontal scaling paths.
