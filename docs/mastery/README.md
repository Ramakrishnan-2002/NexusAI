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

## 2. Document Navigation Suite

The `docs/mastery/` directory contains 9 focused canonical documents:

| Document | Purpose & Contents |
| :--- | :--- |
| **[`README.md`](file:///d:/NexusAI/docs/mastery/README.md)** | Curriculum orientation, learning goals, study tracks, and out-of-scope boundaries. |
| **[`ARCHITECTURE.md`](file:///d:/NexusAI/docs/mastery/ARCHITECTURE.md)** | Verified executable architecture, multi-container Docker topology, storage contracts, and Mermaid data flows. |
| **[`BACKEND_MASTER_BOOK.md`](file:///d:/NexusAI/docs/mastery/BACKEND_MASTER_BOOK.md)** | Comprehensive engineering textbook covering backend fundamentals through distributed mechanics and RAG. |
| **[`BACKEND_ENGINEERING_STORIES.md`](file:///d:/NexusAI/docs/mastery/BACKEND_ENGINEERING_STORIES.md)** | 40 concrete, dependency-ordered engineering stories with exact symbols, build exercises, and break/debug flows. |
| **[`SYSTEM_DESIGN.md`](file:///d:/NexusAI/docs/mastery/SYSTEM_DESIGN.md)** | Requirements, capacity math, latency budgets, bottleneck analysis, and horizontal scaling evolutions. |
| **[`INTERVIEW_PREP.md`](file:///d:/NexusAI/docs/mastery/INTERVIEW_PREP.md)** | 30s/2m/5m project pitches, 40+ senior backend Q&As, hostile defenses, and tradeoff matrices. |
| **[`ACTIVE_RECALL.md`](file:///d:/NexusAI/docs/mastery/ACTIVE_RECALL.md)** | Self-assessment drills, failure diagnosis walkthroughs, and code reconstruction exercises. |
| **[`BACKEND_ROADMAP.md`](file:///d:/NexusAI/docs/mastery/BACKEND_ROADMAP.md)** | 5-pass chronological study path from Python/FastAPI basics to distributed scaling mastery. |
| **[`BACKEND_DEPENDENCY_GRAPH.md`](file:///d:/NexusAI/docs/mastery/BACKEND_DEPENDENCY_GRAPH.md)** | Visual Mermaid Directed Acyclic Graph (DAG) mapping prerequisite relations across all 40 stories. |

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

Every story and concept is tagged with a priority level:

* `[ESSENTIAL]` (26 stories) — Must master first to understand, build, trace, and explain NexusAI core workflows.
* `[IMPORTANT]` (10 stories) — Critical backend knowledge for failure resilience, edge cases, and performance tuning.
* `[ADVANCED]` (4 stories) — Deep distributed systems mechanics, mathematical derivations, and horizontal scaling design.

---

## 5. Architectural Status Taxonomy

* `[CURRENT]` (36 stories) — Actually implemented in source code and verified by execution.
* `[PARTIAL]` (0 stories) — Foundational code exists but lacks production-grade edge-case handling.
* `[THEORY]` (0 stories) — Conceptual foundation required to understand the implementation.
* `[FUTURE]` (4 stories) — Theoretical scaling path for horizontal distributed expansion (`[NOT IMPLEMENTED]`).

---

## 6. Development Status Taxonomy

* `[COMPLETE]` (36 stories) — Implementation exists and is validated in repository.
* `[IN PROGRESS]` (0 stories) — Actively being implemented.
* `[NEEDS VERIFICATION]` (0 stories) — Awaiting test confirmation.
* `[NOT IMPLEMENTED]` (4 stories) — Future architectural roadmap stories (Stories 37–40).
