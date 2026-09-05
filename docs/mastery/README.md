# NexusAI Backend Mastery Curriculum & Engineering Story Tracker

Welcome to the canonical backend engineering curriculum, codebase audit, and system design mastery system for **NexusAI (WikiPulse)**.

This repository serves as a real-world case study in **Python Backend Engineering, Distributed Event Streaming, Hybrid Search (pgvector + GIN FTS), Resilient LLM Gateways, and Production System Design**.

---

## 1. Learning & Development Goals

1. **Backend Mechanics Mastery:** Trace every request from ASGI network ingress to database write and Kafka emission.
2. **Distributed Systems Foundations:** Understand partition mechanics, consumer group rebalancing, offset commits, idempotency keys, and Dead Letter Queues (DLQ).
3. **Hybrid RAG Deep Dive:** Master dense semantic vector search, sparse BM25/FTS lexical retrieval, Reciprocal Rank Fusion ($k=60$), cross-encoder reranking, and prompt injection defense.
4. **Resilience & Fault Isolation:** Understand multi-provider LLM gateways, circuit breakers, exponential backoff, and Redis degradation modes.
5. **System Design & Interview Defense:** Defend architecture tradeoffs, capacity limits, database bottlenecks, scaling strategies, and consistency models under senior interviewer scrutiny.

---

## 2. Canonical Document Suite

The documentation suite is organized into 4 focused mastery documents and architectural decision records:

| Document | Purpose & Contents |
| :--- | :--- |
| **[`BACKEND_ENGINEERING_STORIES.md`](BACKEND_ENGINEERING_STORIES.md)** | **Central Tracker:** 40 concrete, dependency-ordered engineering stories across 12 modules with exact symbols, build exercises, and break/debug flows. |
| **[`ARCHITECTURE.md`](ARCHITECTURE.md)** | Verified executable architecture, multi-container Docker topology, storage ER diagrams, and Mermaid data flows. |
| **[`SYSTEM_DESIGN.md`](SYSTEM_DESIGN.md)** | Requirements, capacity math, latency budgets, bottleneck analysis, and horizontal scaling evolutions. |
| **[`BACKEND_ROADMAP.md`](BACKEND_ROADMAP.md)** | 5-pass chronological study path from Python/FastAPI basics to distributed scaling mastery, including the complete Mermaid Dependency Graph (DAG). |
| **[`ADRs Index`](../ADRs/README.md)** | Key architectural decision records (Kafka, confluent-kafka, idempotency, hybrid search, RRF, LLM gateway, SSE). |

---

## 3. Strict Scope & Boundaries

### What IS in Scope:
* **Python Backend Engineering:** Asyncio event loops, FastAPI ASGI lifecycle, Pydantic schemas, dependency injection, SQLAlchemy 2.0 AsyncSession.
* **Storage & Caching:** PostgreSQL 16 relational tables, GIN full-text indexes, pgvector HNSW indexes, Redis 7 sorted sets (ZSET) and atomic pipelines.
* **Distributed Streaming:** Apache Kafka 3.7 (via `confluent-kafka` C-bindings), partition keys, consumer groups, offset manual commit, poison pill DLQ isolation.
* **AI & RAG Orchestration:** SentenceTransformers / deterministic embeddings, Hybrid Search, Reciprocal Rank Fusion, Candidate Rerankers, Gemini/Ollama LLM Gateway.
* **Backend Docker:** Multi-service `docker-compose.yml`, container networks, volume lifecycle, healthchecks (`/livez`, `/readyz`).

### What is OUT of Scope (Frontend & DevOps Exclusions):
* **No Frontend Frameworks:** No React, Next.js, Tailwind styling, CSS animations, or UI component state management. The frontend is treated strictly as an HTTP/SSE client.
* **No Cloud/Orchestration Bloat:** No Kubernetes, Helm charts, Terraform, GitOps, Prometheus/Grafana server setups, or AWS multi-region infrastructure unless explicitly discussed as `[FUTURE]` system design evolution.

---

## 4. Priority Tiers

* `[ESSENTIAL]` (26 stories) — Must master first to understand, build, trace, and explain NexusAI core workflows.
* `[IMPORTANT]` (10 stories) — Critical backend knowledge for failure resilience, edge cases, and performance tuning.
* `[ADVANCED]` (4 stories) — Deep distributed systems mechanics, mathematical derivations, and horizontal scaling design.

---

## 5. Architectural Status Taxonomy

* `[CURRENT]` (36 stories) — Actually implemented in source code and verified by execution.
* `[FUTURE]` (4 stories) — Theoretical scaling path for horizontal distributed expansion (`[NOT IMPLEMENTED]`).
