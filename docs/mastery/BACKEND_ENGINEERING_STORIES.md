# NexusAI Backend Engineering Stories & Workflow Tracker

---

# EXECUTIVE DEVELOPMENT BOARD

```text
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ COMPLETE (Verified in Executable Source Code & Tests) — 36 Stories                                     │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ • STORY-01 (Async Engine & Event Loop)       • STORY-02 (Pydantic v2 Ingress Schemas)                   │
│ • STORY-03 (FastAPI Lifespan Startup/Exit)   • STORY-04 (SSE Live Stream Broadcaster)                  │
│ • STORY-05 (FastAPI REST CRUD Routers)       • STORY-06 (Dependency Injection & Rollback Safety)       │
│ • STORY-07 (PostgreSQL Schema & Models)      • STORY-08 (SQLAlchemy AsyncSession Repository)           │
│ • STORY-09 (Postgres Connection Pool Tuning) • STORY-10 (ProcessingJob Idempotency Store)              │
│ • STORY-11 (Async Redis Client Pool)         • STORY-12 (ZSET Rolling Sliding-Window Velocity)         │
│ • STORY-13 (Redis Distributed Mutex Locks)   • STORY-14 (Redis Cache-Aside KPI Engine)                 │
│ • STORY-15 (Kafka KRaft Broker Topology)     • STORY-16 (librdkafka Producer & Key Partitioning)       │
│ • STORY-17 (Kafka Manual Offset Consumer)    • STORY-18 (Dead Letter Queue & Poison Pill Isolation)   │
│ • STORY-19 (Stream Ingestor SSE Pipeline)    • STORY-20 (Processor Worker Event Normalization)         │
│ • STORY-21 (Analytics Worker Spike Detector) • STORY-22 (Embedding Worker Chunk Vectorization)         │
│ • STORY-23 (AI Worker Automated Explanation) • STORY-24 (PostgreSQL GIN Full-Text Search)              │
│ • STORY-25 (pgvector HNSW Dense Indexing)    • STORY-26 (Reciprocal Rank Fusion k=60 Algorithm)        │
│ • STORY-27 (Candidate Reranker with Boosting)• STORY-28 (Hybrid Search Service Coordinator)            │
│ • STORY-29 (RAG Context XML Sanitization)    • STORY-30 (Citation Verification & Grounding Builder)    │
│ • STORY-31 (LLM Gateway Provider Hierarchy)  • STORY-32 (Circuit Breakers & Upstream Outage Isolation) │
│ • STORY-33 (Structured JSON AI Extraction)   • STORY-34 (Deterministic Mock LLM Provider)              │
│ • STORY-35 (Multi-Container Docker Compose)  • STORY-36 (Pytest Async Testing Suite & Fixtures)        │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ NOT IMPLEMENTED (Future Horizontal Scaling Design) — 4 Stories                                         │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ • STORY-37 (Distributed Partition Expansion) • STORY-38 (PostgreSQL Read Replicas & CQRS)              │
│ • STORY-39 (Redis Cluster Hash Slot Partition) • STORY-40 (Cross-Encoder Micro-Reranker Daemon)        │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# MODULE 1: Python & Async Foundations

---

### STORY-01: Asyncio Event Loop & Non-Blocking Coroutine Architecture
* **Module:** Python & Async Foundations
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** None
* **Leads To:** STORY-03, STORY-04, STORY-08
* **Primary Code Files:** [`backend/app/main.py`](file:///d:/NexusAI/backend/app/main.py), [`backend/app/api/deps.py`](file:///d:/NexusAI/backend/app/api/deps.py)
* **Concrete Symbols:** `asyncio`, `async def`, `await`, `async_sessionmaker`
* **Verification / Tests:** `backend/tests/api/test_api_endpoints.py`

#### 1. Why This Matters
Synchronous backend frameworks dedicate 1 OS thread per connection. At 1,000 concurrent network-bound requests, 1,000 OS threads cause severe memory overhead ($\sim 8\text{MB}$ stack per thread) and expensive CPU context switches. Cooperative non-blocking async handles thousands of connections on a single OS thread.

#### 2. Core Concept
An event loop continuously polls OS file descriptors via `epoll`/`kqueue`/`IOCP`. When an I/O operation (e.g. database query, network socket) is requested, execution yields via `await`, allowing the loop to process other runnable tasks without blocking.

#### 3. Nexus AI Implementation
In [`backend/app/main.py`](file:///d:/NexusAI/backend/app/main.py) and [`backend/app/db/session.py`](file:///d:/NexusAI/backend/app/db/session.py), all database operations use `AsyncEngine` and `await session.execute()`, ensuring the FastAPI worker process never blocks on network I/O.

---

### STORY-02: Pydantic v2 Ingress Normalization & Type Contracts
* **Module:** Python & Async Foundations
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-01
* **Leads To:** STORY-05, STORY-20
* **Primary Code Files:** [`backend/app/schemas/event.py`](file:///d:/NexusAI/backend/app/schemas/event.py), [`backend/app/schemas/article.py`](file:///d:/NexusAI/backend/app/schemas/article.py)
* **Concrete Symbols:** `IngestedEvent`, `ArticleSummary`, `EditItem`, `BaseModel`, `ConfigDict`
* **Verification / Tests:** `backend/tests/unit/test_schemas.py`

#### 1. Why This Matters
Real-time streaming feeds contain malformed fields, unexpected nulls, and type mismatches. If untrusted raw payloads reach domain logic or database layers, they cause unhandled crashes.

#### 2. Nexus AI Implementation
In [`backend/app/schemas/event.py`](file:///d:/NexusAI/backend/app/schemas/event.py), `IngestedEvent` parses incoming Wikimedia edits, automatically validating timestamps and generating UUIDs for events missing explicit identifiers.

---

# MODULE 2: FastAPI Control Plane & API Architecture

---

### STORY-03: ASGI Lifespan Dependency Orchestration
* **Module:** FastAPI Control Plane
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-01
* **Leads To:** STORY-05, STORY-09
* **Primary Code Files:** [`backend/app/main.py`](file:///d:/NexusAI/backend/app/main.py)
* **Concrete Symbols:** `@asynccontextmanager`, `lifespan(app: FastAPI)`, `init_db_models()`, `ensure_topics_exist()`
* **Verification / Tests:** `backend/tests/api/test_api_endpoints.py::test_health_and_readiness_endpoints`

#### 1. Why This Matters
If a web API accepts user traffic before the database connection pool is warm or Kafka topics are initialized, the first burst of requests will fail with 500 errors.

#### 2. Nexus AI Implementation
[`backend/app/main.py`](file:///d:/NexusAI/backend/app/main.py) defines an `@asynccontextmanager lifespan(app: FastAPI)` that executes `init_db_models()` and `KafkaAdminService.ensure_topics_exist()` during startup before yielding to request traffic.

---

### STORY-04: Server-Sent Events (SSE) Live Feed Broadcaster
* **Module:** FastAPI Control Plane
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-01, STORY-03
* **Leads To:** STORY-19
* **Primary Code Files:** [`backend/app/api/v1/stream.py`](file:///d:/NexusAI/backend/app/api/v1/stream.py), [`backend/app/kafka/bus.py`](file:///d:/NexusAI/backend/app/kafka/bus.py)
* **Concrete Symbols:** `EventSourceResponse`, `stream_live_events()`, `InMemoryEventBus`
* **Verification / Tests:** `backend/tests/api/test_api_endpoints.py`

#### 1. Why This Matters
Polling an API every second (`GET /api/v1/events`) creates massive request overhead. Server-Sent Events (SSE) allows the backend to push live revision updates over a single persistent HTTP connection.

#### 2. Nexus AI Implementation
In [`backend/app/api/v1/stream.py`](file:///d:/NexusAI/backend/app/api/v1/stream.py), `stream_live_events()` subscribes the client to `global_event_bus` and yields `event: recent_change` with 15-second heartbeat pings to keep proxy connections alive.

---

### STORY-05: FastAPI REST CRUD Routers & OpenAPI Contracts
* **Module:** FastAPI Control Plane
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-02, STORY-03
* **Leads To:** STORY-06, STORY-28
* **Primary Code Files:** [`backend/app/api/v1/articles.py`](file:///d:/NexusAI/backend/app/api/v1/articles.py), [`backend/app/api/v1/trends.py`](file:///d:/NexusAI/backend/app/api/v1/trends.py)
* **Concrete Symbols:** `APIRouter`, `get_articles()`, `get_article_by_id()`, `get_trends()`
* **Verification / Tests:** `backend/tests/api/test_api_endpoints.py`

#### 1. Why This Matters
REST endpoints must enforce clear status codes, query pagination bounds, and auto-generated OpenAPI documentation.

#### 2. Nexus AI Implementation
[`backend/app/api/v1/articles.py`](file:///d:/NexusAI/backend/app/api/v1/articles.py) exposes structured endpoints (`GET /api/v1/articles`, `GET /api/v1/articles/{id}`) with Pydantic response models and limit validations (`ge=1, le=100`).

---

### STORY-06: Dependency Injection & Async Transaction Rollback Safety
* **Module:** FastAPI Control Plane
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-01, STORY-05
* **Leads To:** STORY-08
* **Primary Code Files:** [`backend/app/api/deps.py`](file:///d:/NexusAI/backend/app/api/deps.py)
* **Concrete Symbols:** `get_db()`, `DB`, `Depends`
* **Verification / Tests:** `backend/tests/api/test_api_endpoints.py`

#### 1. Why This Matters
If an exception occurs during request execution without proper session handling, the database connection can leak or leave uncommitted transactions open.

#### 2. Nexus AI Implementation
In [`backend/app/api/deps.py`](file:///d:/NexusAI/backend/app/api/deps.py), `get_db()` wraps every database session in a `try...except...finally` block that automatically calls `await session.rollback()` on error and guarantees `await session.close()` on exit.

---

# MODULE 3: Persistence & PostgreSQL with SQLAlchemy Async

---

### STORY-07: Relational Schema Design & pgvector Embedding Table
* **Module:** Persistence & PostgreSQL
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-01
* **Leads To:** STORY-08, STORY-25
* **Primary Code Files:** [`backend/app/models/article.py`](file:///d:/NexusAI/backend/app/models/article.py), [`backend/app/models/knowledge_chunk.py`](file:///d:/NexusAI/backend/app/models/knowledge_chunk.py)
* **Concrete Symbols:** `Article`, `Edit`, `KnowledgeChunk`, `Vector(384)`, `mapped_column`
* **Verification / Tests:** `backend/tests/api/test_api_endpoints.py::test_articles_and_events_api`

#### 1. Why This Matters
Unstructured Wikipedia edits require relational schema models linked to dense 384-dimensional vector embeddings without dual-write inconsistency.

#### 2. Nexus AI Implementation
In [`backend/app/models/`](file:///d:/NexusAI/backend/app/models/), SQLAlchemy declarative models define `Article`, `Edit`, and `KnowledgeChunk` with `Vector(384)` mapping to pgvector.

---

### STORY-08: SQLAlchemy 2.0 AsyncSession Repository Pattern
* **Module:** Persistence & PostgreSQL
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-06, STORY-07
* **Leads To:** STORY-09, STORY-20
* **Primary Code Files:** [`backend/app/repositories/article_repo.py`](file:///d:/NexusAI/backend/app/repositories/article_repo.py), [`backend/app/repositories/edit_repo.py`](file:///d:/NexusAI/backend/app/repositories/edit_repo.py)
* **Concrete Symbols:** `ArticleRepository`, `EditRepository`, `create_or_get_article()`, `create_edit()`
* **Verification / Tests:** `backend/tests/api/test_api_endpoints.py`

#### 1. Why This Matters
Direct raw SQL scattered across handlers creates maintenance bottlenecks. The repository pattern encapsulates SQL queries, joins, and transaction boundaries cleanly.

#### 2. Nexus AI Implementation
`ArticleRepository.create_or_get_article()` uses `select(Article).where(Article.title == event.article_title)` to look up or insert articles with transactional atomicity.

---

### STORY-09: Async Database Connection Pool & Pre-Ping Probing
* **Module:** Persistence & PostgreSQL
* **Priority:** `[IMPORTANT]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-07, STORY-08
* **Leads To:** STORY-20
* **Primary Code Files:** [`backend/app/db/session.py`](file:///d:/NexusAI/backend/app/db/session.py)
* **Concrete Symbols:** `create_async_engine`, `pool_size`, `max_overflow`, `pool_pre_ping`
* **Verification / Tests:** `backend/tests/failure/test_failure_scenarios.py`

#### 1. Why This Matters
Stateful TCP connections in container networks can silently drop due to idle socket timeouts. Without pre-ping probes, requests fail on dead sockets.

#### 2. Nexus AI Implementation
[`backend/app/db/session.py`](file:///d:/NexusAI/backend/app/db/session.py) configures `pool_size=20`, `max_overflow=10`, and `pool_pre_ping=True` to health-check connections before executing queries.

---

### STORY-10: Database Idempotency Store (`ProcessingJob`)
* **Module:** Persistence & PostgreSQL
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-07
* **Leads To:** STORY-17, STORY-20
* **Primary Code Files:** [`backend/app/models/processing_job.py`](file:///d:/NexusAI/backend/app/models/processing_job.py), [`workers/processor/processor.py`](file:///d:/NexusAI/workers/processor/processor.py)
* **Concrete Symbols:** `ProcessingJob`, `idempotency_key`, `ix_processing_jobs_idempotency_key`
* **Verification / Tests:** `backend/tests/kafka/test_kafka_idempotency.py`

#### 1. Why This Matters
Kafka guarantees at-least-once delivery. Network retries or rebalances can deliver a message multiple times.

#### 2. Nexus AI Implementation
The worker creates `ProcessingJob(idempotency_key="proc:{event_id}")`. If a duplicate is replayed, the PostgreSQL `UNIQUE` index raises an `IntegrityError`, causing the worker to safely roll back and skip the duplicate.

---

# MODULE 4: High-Speed In-Memory State with Redis 7

---

### STORY-11: Async Redis Client & Connection Management
* **Module:** Redis State & Caching
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-01
* **Leads To:** STORY-12, STORY-13
* **Primary Code Files:** [`backend/app/redis/client.py`](file:///d:/NexusAI/backend/app/redis/client.py)
* **Concrete Symbols:** `get_redis()`, `RedisClient`, `ConnectionPool`
* **Verification / Tests:** `backend/tests/failure/test_failure_scenarios.py::test_failure_redis_outage_graceful_fallback`

#### 1. Why This Matters
Creating a new Redis TCP socket on every incoming edit causes massive latency overhead. A singleton async connection pool reuses persistent sockets across workers.

#### 2. Nexus AI Implementation
[`backend/app/redis/client.py`](file:///d:/NexusAI/backend/app/redis/client.py) manages a shared `ConnectionPool.from_url(settings.REDIS_URL)` with graceful reconnection handling.

---

### STORY-12: Rolling Sliding-Window Counters (Sorted Sets / ZSET)
* **Module:** Redis State & Caching
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-11
* **Leads To:** STORY-21
* **Primary Code Files:** [`backend/app/redis/client.py`](file:///d:/NexusAI/backend/app/redis/client.py), [`backend/app/redis/counters.py`](file:///d:/NexusAI/backend/app/redis/counters.py)
* **Concrete Symbols:** `record_article_activity()`, `get_activity_windows()`, `zadd`, `zcount`, `zremrangebyscore`
* **Verification / Tests:** `backend/tests/unit/test_spike_detector.py`

#### 1. Why This Matters
Running SQL `COUNT(*)` over rolling 1m, 5m, and 15m windows across thousands of articles causes heavy database lock contention.

#### 2. Nexus AI Implementation
`record_article_activity()` executes an atomic Redis pipeline:
1. `ZADD act:art:{article_id}:edits <now_epoch> <event_id>`
2. `ZREMRANGEBYSCORE act:art:{article_id}:edits -inf (now - 3600)`
3. `EXPIRE act:art:{article_id}:edits 3600`

---

### STORY-13: Distributed Mutex Locks (`SET NX EX`) & Release Mechanics
* **Module:** Redis State & Caching
* **Priority:** `[IMPORTANT]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-11
* **Leads To:** STORY-21
* **Primary Code Files:** [`backend/app/redis/lock.py`](file:///d:/NexusAI/backend/app/redis/lock.py)
* **Concrete Symbols:** `DistributedLock`, `acquire()`, `release()`, `token`
* **Verification / Tests:** `backend/tests/failure/test_failure_scenarios.py`

#### 1. Why This Matters
When an article experiences an intense burst of edits, multiple concurrent analytics workers could detect the spike simultaneously and emit duplicate trend alerts.

#### 2. Nexus AI Implementation
In [`backend/app/redis/lock.py`](file:///d:/NexusAI/backend/app/redis/lock.py), `acquire()` sets the key with a UUID token and TTL (`ex=15`). `release()` verifies `val == self.token` before deleting.

---

### STORY-14: Redis Cache-Aside KPI & Metrics Acceleration
* **Module:** Redis State & Caching
* **Priority:** `[IMPORTANT]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-11
* **Leads To:** STORY-28
* **Primary Code Files:** [`backend/app/redis/cache.py`](file:///d:/NexusAI/backend/app/redis/cache.py), [`backend/app/api/v1/metrics.py`](file:///d:/NexusAI/backend/app/api/v1/metrics.py)
* **Concrete Symbols:** `CacheService`, `get_cached_json()`, `set_cached_json()`
* **Verification / Tests:** `backend/tests/api/test_api_endpoints.py`

#### 1. Why This Matters
Dashboard telemetry polling queries can overwhelm database connections if not cached with short TTLs.

#### 2. Nexus AI Implementation
Global metrics are cached under `cache:metrics:global` with a 3-second TTL, serving dashboard polls in sub-millisecond time.

---

# MODULE 5: Event Streaming with Apache Kafka 3.7

---

### STORY-15: Kafka Broker KRaft Metadata Architecture
* **Module:** Apache Kafka Streaming
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-01
* **Leads To:** STORY-16, STORY-17
* **Primary Code Files:** [`docker-compose.yml`](file:///d:/NexusAI/docker-compose.yml), [`backend/app/kafka/topics.py`](file:///d:/NexusAI/backend/app/kafka/topics.py)
* **Concrete Symbols:** `apache/kafka:3.7.0`, `KAFKA_PROCESS_ROLES: broker,controller`, `KafkaAdminService`
* **Verification / Tests:** `backend/tests/kafka/test_confluent_kafka_integration.py::test_confluent_kafka_admin_service_instantiation`

#### 1. Why This Matters
KRaft mode eliminates the operational complexity and failure modes of ZooKeeper metadata clusters.

#### 2. Nexus AI Implementation
[`docker-compose.yml`](file:///d:/NexusAI/docker-compose.yml) configures Apache Kafka 3.7.0 in KRaft combined broker/controller mode with 3 partitions per topic.

---

### STORY-16: `librdkafka` C-Memory Producer & Key Partition Routing
* **Module:** Apache Kafka Streaming
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-15
* **Leads To:** STORY-19, STORY-20
* **Primary Code Files:** [`backend/app/kafka/producer.py`](file:///d:/NexusAI/backend/app/kafka/producer.py)
* **Concrete Symbols:** `KafkaProducerService`, `produce_event()`, `key = event.article_title`
* **Verification / Tests:** `backend/tests/kafka/test_confluent_kafka_integration.py`

#### 1. Why This Matters
Kafka guarantees ordering only within a partition. Routing edits by `article_title` ensures chronological revision ordering per article.

#### 2. Nexus AI Implementation
In [`backend/app/kafka/producer.py`](file:///d:/NexusAI/backend/app/kafka/producer.py), the producer encodes `key = event.article_title.encode('utf-8')` to route all edits for an article to the exact same partition.

---

### STORY-17: Kafka Manual Offset Management & At-Least-Once Semantics
* **Module:** Apache Kafka Streaming
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-15, STORY-16
* **Leads To:** STORY-18, STORY-20
* **Primary Code Files:** [`backend/app/kafka/consumer.py`](file:///d:/NexusAI/backend/app/kafka/consumer.py)
* **Concrete Symbols:** `KafkaConsumerService`, `enable.auto.commit: False`, `consumer.commit()`
* **Verification / Tests:** `backend/tests/kafka/test_confluent_kafka_integration.py`

#### 1. Why This Matters
Auto-committing offsets before processing causes silent data loss if the worker crashes mid-processing.

#### 2. Nexus AI Implementation
[`backend/app/kafka/consumer.py`](file:///d:/NexusAI/backend/app/kafka/consumer.py) sets `enable.auto.commit = False` and manually commits offsets strictly after database persistence succeeds.

---

### STORY-18: Dead Letter Queue (DLQ) & Poison Pill Isolation
* **Module:** Apache Kafka Streaming
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-16, STORY-17
* **Leads To:** STORY-20
* **Primary Code Files:** [`backend/app/kafka/dlq.py`](file:///d:/NexusAI/backend/app/kafka/dlq.py), [`workers/processor/processor.py`](file:///d:/NexusAI/workers/processor/processor.py)
* **Concrete Symbols:** `DeadLetterQueueService`, `route_to_dlq()`, `TOPIC_DLQ`
* **Verification / Tests:** `backend/tests/kafka/test_retry_dlq.py`

#### 1. Why This Matters
A single malformed message ("poison pill") that crashes deserialization will cause an infinite crash loop, blocking partition progress.

#### 2. Nexus AI Implementation
When deserialization fails, `route_to_dlq()` packages the raw bytes with error metadata, publishes to `wikimedia.dlq`, and commits the offset to unblock healthy messages.

---

# MODULE 6: Distributed Stream Processing Workers & Pipelines

---

### STORY-19: Stream Ingestor & Real-Time Event Normalization
* **Module:** Distributed Workers & Pipelines
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-02, STORY-16
* **Leads To:** STORY-20
* **Primary Code Files:** [`workers/stream_ingestor/ingestor.py`](file:///d:/NexusAI/workers/stream_ingestor/ingestor.py), [`workers/stream_ingestor/synthetic_generator.py`](file:///d:/NexusAI/workers/stream_ingestor/synthetic_generator.py)
* **Concrete Symbols:** `StreamIngestorService`, `normalize_event()`, `SyntheticEventGenerator`
* **Verification / Tests:** `backend/tests/integration/test_e2e_pipeline.py`

#### 1. Why This Matters
Raw Wikimedia streams contain non-standardized structures and missing fields. The ingestor normalizes events before publishing to Kafka.

#### 2. Nexus AI Implementation
`StreamIngestorService` connects to Wikimedia SSE or runs the synthetic generator, producing validated `IngestedEvent` messages to `wikimedia.recentchange`.

---

### STORY-20: Processor Worker & Relational Persistence Pipeline
* **Module:** Distributed Workers & Pipelines
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-08, STORY-10, STORY-12, STORY-17
* **Leads To:** STORY-21, STORY-22
* **Primary Code Files:** [`workers/processor/processor.py`](file:///d:/NexusAI/workers/processor/processor.py)
* **Concrete Symbols:** `EventProcessorWorker`, `handle_event()`, `TOPIC_ARTICLE_PROCESSED`
* **Verification / Tests:** `backend/tests/kafka/test_kafka_idempotency.py`

#### 1. Why This Matters
The processor worker acts as the central ingestion sink, bridging Kafka streams to PostgreSQL and Redis.

#### 2. Nexus AI Implementation
`EventProcessorWorker` consumes from `wikimedia.recentchange`, checks `ProcessingJob` idempotency, writes `Article` and `Edit` records, updates Redis sliding windows, and emits `wikimedia.article.processed`.

---

### STORY-21: Analytics Worker & Baseline Velocity Spike Detector
* **Module:** Distributed Workers & Pipelines
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-12, STORY-13, STORY-20
* **Leads To:** STORY-23
* **Primary Code Files:** [`workers/analytics/analytics.py`](file:///d:/NexusAI/workers/analytics/analytics.py), [`workers/analytics/detector.py`](file:///d:/NexusAI/workers/analytics/detector.py)
* **Concrete Symbols:** `AnalyticsWorker`, `SpikeDetector`, `evaluate_activity_spike()`
* **Verification / Tests:** `backend/tests/unit/test_spike_detector.py`

#### 1. Why This Matters
Surges in edit velocity indicate breaking news or coordinated editing events that warrant immediate intelligence synthesis.

#### 2. Nexus AI Implementation
`SpikeDetector` queries Redis rolling counts ($1\text{m}, 5\text{m}, 15\text{m}$) and triggers a trend alert when velocity multiplier $\ge 3.0\times$ baseline with $\ge 3$ unique editors.

---

### STORY-22: Embedding Worker & Dense Vectorization Pipeline
* **Module:** Distributed Workers & Pipelines
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-07, STORY-20
* **Leads To:** STORY-25
* **Primary Code Files:** [`workers/embedding/embedding_worker.py`](file:///d:/NexusAI/workers/embedding/embedding_worker.py), [`backend/app/search/embeddings.py`](file:///d:/NexusAI/backend/app/search/embeddings.py)
* **Concrete Symbols:** `EmbeddingWorker`, `handle_event()`, `embedding_service.get_embedding()`
* **Verification / Tests:** `backend/tests/integration/test_e2e_pipeline.py`

#### 1. Why This Matters
To support semantic search, revision text diffs must be transformed into dense vector embeddings asynchronously without slowing down edit ingestion.

#### 2. Nexus AI Implementation
`EmbeddingWorker` consumes from `wikimedia.article.processed`, generates a 384-dimensional vector, and stores a `KnowledgeChunk` record in PostgreSQL.

---

### STORY-23: AI Worker & Spike Intelligence Synthesis
* **Module:** Distributed Workers & Pipelines
* **Priority:** `[IMPORTANT]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-21, STORY-31
* **Leads To:** STORY-30
* **Primary Code Files:** [`workers/ai/ai_worker.py`](file:///d:/NexusAI/workers/ai/ai_worker.py), [`backend/app/services/ai_service.py`](file:///d:/NexusAI/backend/app/services/ai_service.py)
* **Concrete Symbols:** `AIWorker`, `AIService.explain_trend()`, `TOPIC_ANALYSIS_COMPLETED`
* **Verification / Tests:** `backend/tests/api/test_api_endpoints.py::test_trends_and_analysis_api`

#### 1. Why This Matters
When an activity spike is detected, users need an immediate plain-English explanation of why the article is trending.

#### 2. Nexus AI Implementation
`AIWorker` consumes from `wikimedia.trend.detected`, retrieves recent knowledge chunks for the article, and synthesizes an explanation via `LLMGateway`.

---

# MODULE 7: Lexical Full-Text Search (PostgreSQL GIN FTS)

---

### STORY-24: PostgreSQL GIN Full-Text Search with Stop-Word Filtering
* **Module:** Lexical Search
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-07
* **Leads To:** STORY-26, STORY-28
* **Primary Code Files:** [`backend/app/search/fts.py`](file:///d:/NexusAI/backend/app/search/fts.py)
* **Concrete Symbols:** `FullTextSearchService`, `search_keywords()`, `to_tsvector`, `plainto_tsquery`, `ts_rank_cd`
* **Verification / Tests:** `backend/tests/rag/test_hybrid_search.py`

#### 1. Why This Matters
Conversational stop-words in natural language queries dilute lexical matching if not filtered out before tsquery generation.

#### 2. Nexus AI Implementation
[`backend/app/search/fts.py`](file:///d:/NexusAI/backend/app/search/fts.py) filters conversational stop-words (`CONVERSATIONAL_STOP_WORDS`) and indexes `article_title` alongside `content` in `to_tsvector('english', ...)`.

---

# MODULE 8: Semantic Vector Embeddings & pgvector

---

### STORY-25: pgvector Cosine Distance Semantic Retrieval
* **Module:** Semantic Vector Search
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-07, STORY-22
* **Leads To:** STORY-26, STORY-28
* **Primary Code Files:** [`backend/app/search/vector.py`](file:///d:/NexusAI/backend/app/search/vector.py), [`backend/app/search/embeddings.py`](file:///d:/NexusAI/backend/app/search/embeddings.py)
* **Concrete Symbols:** `VectorSearchService`, `search_similar()`, `cosine_distance` (`<=>`)
* **Verification / Tests:** `backend/tests/rag/test_hybrid_search.py`

#### 1. Why This Matters
Keyword search fails when users search for concepts using synonyms. Dense vector embeddings capture conceptual similarity.

#### 2. Nexus AI Implementation
`VectorSearchService.search_similar()` executes pgvector `<=>` cosine distance retrieval over indexed `KnowledgeChunk.embedding` rows.

---

# MODULE 9: Hybrid Retrieval, Reciprocal Rank Fusion & Reranking

---

### STORY-26: Reciprocal Rank Fusion (RRF $k=60$) Algorithm
* **Module:** Hybrid Retrieval & Ranking
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-24, STORY-25
* **Leads To:** STORY-27, STORY-28
* **Primary Code Files:** [`backend/app/search/hybrid.py`](file:///d:/NexusAI/backend/app/search/hybrid.py)
* **Concrete Symbols:** `HybridSearchService`, `RRF_score(d) = Σ w_i / (k + rank_i + 1)`, `rrf_k = 60`
* **Verification / Tests:** `backend/tests/unit/test_rrf_math.py`

#### 1. Why This Matters
Dense cosine scores $[0, 1]$ and sparse BM25 scores $[0, \infty)$ have incompatible scales and cannot be directly summed.

#### 2. Nexus AI Implementation
In [`backend/app/search/hybrid.py`](file:///d:/NexusAI/backend/app/search/hybrid.py), RRF with constant $k=60$ merges the top candidates using 1-indexed rank scoring ($0.5 / (k + \text{rank} + 1)$).

---

### STORY-27: Candidate Reranker with Title & Keyword Boosting
* **Module:** Hybrid Retrieval & Ranking
* **Priority:** `[IMPORTANT]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-26
* **Leads To:** STORY-28
* **Primary Code Files:** [`backend/app/search/reranker.py`](file:///d:/NexusAI/backend/app/search/reranker.py)
* **Concrete Symbols:** `CandidateReranker`, `rerank()`, `title_bonus`, `exact_bonus`
* **Verification / Tests:** `backend/tests/unit/test_reranker.py`

#### 1. Why This Matters
RRF provides coarse rank combination; fine-grained title matching and exact phrase bonuses ensure target articles rank at top position.

#### 2. Nexus AI Implementation
[`backend/app/search/reranker.py`](file:///d:/NexusAI/backend/app/search/reranker.py) evaluates candidate chunks, applying title overlap bonuses and exact phrase matching.

---

### STORY-28: Hybrid Search Service Coordinator
* **Module:** Hybrid Retrieval & Ranking
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-24, STORY-25, STORY-26, STORY-27
* **Leads To:** STORY-29
* **Primary Code Files:** [`backend/app/search/hybrid.py`](file:///d:/NexusAI/backend/app/search/hybrid.py), [`backend/app/api/v1/search.py`](file:///d:/NexusAI/backend/app/api/v1/search.py)
* **Concrete Symbols:** `HybridSearchService.search()`, `SearchResponse`, `SearchResultItem`
* **Verification / Tests:** `backend/tests/rag/test_hybrid_search.py`

#### 1. Why This Matters
The search endpoint coordinates parallel dual-index queries, score fusion, and candidate reranking into a unified response schema.

#### 2. Nexus AI Implementation
[`backend/app/api/v1/search.py`](file:///d:/NexusAI/backend/app/api/v1/search.py) exposes `GET /api/v1/search`, executing `HybridSearchService.search()` with latency metrics tracking.

---

# MODULE 10: RAG Orchestration & Prompt Defense

---

### STORY-29: RAG Context Assembly & XML Isolation Barrier
* **Module:** RAG Orchestration
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-28, STORY-31
* **Leads To:** STORY-30
* **Primary Code Files:** [`backend/app/rag/context_builder.py`](file:///d:/NexusAI/backend/app/rag/context_builder.py), [`backend/app/core/security.py`](file:///d:/NexusAI/backend/app/core/security.py)
* **Concrete Symbols:** `RAGContextBuilder`, `build_qa_context()`, `<untrusted_wikipedia_content>`
* **Verification / Tests:** `backend/tests/security/test_security_hardening.py`, `backend/tests/unit/test_prompt_defense.py`

#### 1. Why This Matters
Untrusted Wikipedia edits can contain prompt injection attacks. Fencing retrieved text in XML barriers neutralizes injection attempts.

#### 2. Nexus AI Implementation
`RAGContextBuilder.build_qa_context()` wraps retrieved evidence chunks inside `<untrusted_wikipedia_content>` tags with explicit guardrail instructions.

---

### STORY-30: Citation Verification & Grounding Builder
* **Module:** RAG Orchestration
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-29
* **Leads To:** None
* **Primary Code Files:** [`backend/app/rag/citations.py`](file:///d:/NexusAI/backend/app/rag/citations.py), [`backend/app/services/ai_service.py`](file:///d:/NexusAI/backend/app/services/ai_service.py)
* **Concrete Symbols:** `CitationBuilder`, `build_citations_from_results()`, `Citation`
* **Verification / Tests:** `backend/tests/api/test_api_endpoints.py::test_rag_ai_ask_api`

#### 1. Why This Matters
AI answers without exact citations cannot be verified by human analysts.

#### 2. Nexus AI Implementation
`CitationBuilder` maps retrieved search items directly to `Citation` schemas with article title, revision ID, timestamp, and verified snippet.

---

# MODULE 11: LLM Gateway & Resilient AI Infrastructure

---

### STORY-31: Multi-Provider Fallback Cascade (Gemini $\to$ Ollama $\to$ Mock)
* **Module:** AI & LLM Infrastructure
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-01, STORY-02
* **Leads To:** STORY-32, STORY-33
* **Primary Code Files:** [`backend/app/llm/gateway.py`](file:///d:/NexusAI/backend/app/llm/gateway.py), [`backend/app/llm/providers/`](file:///d:/NexusAI/backend/app/llm/providers/)
* **Concrete Symbols:** `LLMGateway`, `analyze_structured()`, `GeminiProvider` (`gemini-1.5-flash`), `OllamaProvider` (`llama3.2:1b`), `MockProvider`
* **Verification / Tests:** `backend/tests/llm/test_gateway_fallback.py`

#### 1. Why This Matters
Relying on a single third-party cloud LLM API makes the entire application vulnerable to upstream outages, rate limits, and network latency spikes.

#### 2. Nexus AI Implementation
`LLMGateway` attempts Gemini 1.5 Flash first; if rate-limited or offline, it cascades to local Ollama (`llama3.2:1b`); if Ollama is unreachable, it falls back to a deterministic grounded mock provider.

---

### STORY-32: Circuit Breakers & Upstream Outage Isolation
* **Module:** AI & LLM Infrastructure
* **Priority:** `[IMPORTANT]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-31
* **Leads To:** None
* **Primary Code Files:** [`backend/app/llm/gateway.py`](file:///d:/NexusAI/backend/app/llm/gateway.py)
* **Concrete Symbols:** `LLMGateway`, `_circuit_open`, `_failure_counts`
* **Verification / Tests:** `backend/tests/failure/test_failure_scenarios.py::test_failure_llm_provider_timeout_cascading`

#### 1. Why This Matters
Repeatedly making failing network calls to an unavailable cloud provider wastes latency budget and exhausts client connection pools.

#### 2. Nexus AI Implementation
The gateway tracks consecutive failures; once threshold is reached, the circuit opens, immediately routing traffic to secondary providers without waiting for network timeouts.

---

### STORY-33: Structured JSON AI Output Validation
* **Module:** AI & LLM Infrastructure
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-02, STORY-31
* **Leads To:** STORY-30
* **Primary Code Files:** [`backend/app/schemas/ai.py`](file:///d:/NexusAI/backend/app/schemas/ai.py), [`backend/app/llm/providers/base.py`](file:///d:/NexusAI/backend/app/llm/providers/base.py)
* **Concrete Symbols:** `AIAnalysisOutput`, `AIAskResponse`, `BaseLLMProvider.generate_structured()`
* **Verification / Tests:** `backend/tests/unit/test_schemas.py::test_ai_structured_output_validation`

#### 1. Why This Matters
LLM responses returned as free-form strings cannot be parsed reliably by downstream workers or relational databases.

#### 2. Nexus AI Implementation
`AIAnalysisOutput` validates summary, importance, detected topic, change type, and evidence points matching strict Pydantic schemas.

---

### STORY-34: Deterministic Grounded Mock LLM Provider
* **Module:** AI & LLM Infrastructure
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-31, STORY-33
* **Leads To:** None
* **Primary Code Files:** [`backend/app/llm/providers/mock.py`](file:///d:/NexusAI/backend/app/llm/providers/mock.py)
* **Concrete Symbols:** `MockProvider`, `generate_structured()`
* **Verification / Tests:** `backend/tests/llm/test_gateway_fallback.py::test_llm_gateway_mock_provider`

#### 1. Why This Matters
Local development, unit tests, and CI/CD pipelines require zero-latency, deterministic AI responses without incurring cloud API costs.

#### 2. Nexus AI Implementation
`MockProvider` synthesizes evidence-grounded summaries from parsed prompt evidence blocks, returning valid `AIAnalysisOutput` structures with zero external network dependencies.

---

# MODULE 12: Backend Verification, Docker Topology & Horizontal Scaling

---

### STORY-35: Multi-Container Docker Compose Service Topology
* **Module:** Verification & Deployment
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-03, STORY-11, STORY-15, STORY-20
* **Leads To:** STORY-36
* **Primary Code Files:** [`docker-compose.yml`](file:///d:/NexusAI/docker-compose.yml), [`backend/Dockerfile`](file:///d:/NexusAI/backend/Dockerfile)
* **Concrete Symbols:** `wikipulse-postgres`, `wikipulse-redis`, `wikipulse-kafka`, `wikipulse-api`, `worker services`
* **Verification / Tests:** `docker compose ps`

#### 1. Why This Matters
Distributed architectures require reproducible container orchestration linking database, cache, broker, and worker services across isolated networks.

#### 2. Nexus AI Implementation
[`docker-compose.yml`](file:///d:/NexusAI/docker-compose.yml) orchestrates all 10 services with persistent volume bindings and healthcheck dependencies.

---

### STORY-36: Pytest Async Testing Suite & Fixtures
* **Module:** Verification & Deployment
* **Priority:** `[ESSENTIAL]`
* **Implementation Status:** `[CURRENT]`
* **Development Status:** `[COMPLETE]`
* **Prerequisites:** STORY-01, STORY-06, STORY-08
* **Leads To:** None
* **Primary Code Files:** [`backend/tests/conftest.py`](file:///d:/NexusAI/backend/tests/conftest.py), [`backend/tests/`](file:///d:/NexusAI/backend/tests/)
* **Concrete Symbols:** `client`, `db_session`, `pytest.mark.asyncio`, `pytest-asyncio`
* **Verification / Tests:** `pytest backend/tests -v`

#### 1. Why This Matters
Async backend systems require isolated test database sessions with rollback fixtures to prevent test pollution.

#### 2. Nexus AI Implementation
[`backend/tests/conftest.py`](file:///d:/NexusAI/backend/tests/conftest.py) provides `client` (via `httpx.AsyncClient`) and `db_session` fixtures running on in-memory SQLite with async rollback isolation.

---

### STORY-37: Kafka Distributed Partition Expansion
* **Module:** Future Horizontal Scaling
* **Priority:** `[ADVANCED]`
* **Implementation Status:** `[FUTURE]`
* **Development Status:** `[NOT IMPLEMENTED]`
* **Prerequisites:** STORY-15, STORY-16
* **Leads To:** None
* **Primary Code Files:** None (System Design Evolution)
* **Verification / Tests:** *No dedicated automated test currently exists.*

* **Current Architecture:** Single Kafka broker with 3 partitions per topic.
* **Scaling Trigger:** When measured consumer lag exceeds worker throughput capacity under sustained high ingestion rates.
* **Possible Evolution:** Expand partitions to 16+ per topic across a 3-broker cluster, enabling up to 16 concurrent worker instances per consumer group.

---

### STORY-38: PostgreSQL Read Replicas & CQRS Routing
* **Module:** Future Horizontal Scaling
* **Priority:** `[ADVANCED]`
* **Implementation Status:** `[FUTURE]`
* **Development Status:** `[NOT IMPLEMENTED]`
* **Prerequisites:** STORY-07, STORY-08
* **Leads To:** None
* **Primary Code Files:** None (System Design Evolution)
* **Verification / Tests:** *No dedicated automated test currently exists.*

* **Current Architecture:** Single PostgreSQL 16 instance handling both write transactions and hybrid search reads.
* **Scaling Trigger:** If hybrid-search read traffic saturates the primary database CPU and connection pool.
* **Possible Evolution:** Introduce read replicas using async replication, routing hybrid search queries to replicas while keeping writes on the primary.

---

### STORY-39: Redis Cluster Hash Slot Partitioning
* **Module:** Future Horizontal Scaling
* **Priority:** `[ADVANCED]`
* **Implementation Status:** `[FUTURE]`
* **Development Status:** `[NOT IMPLEMENTED]`
* **Prerequisites:** STORY-11, STORY-12
* **Leads To:** None
* **Primary Code Files:** None (System Design Evolution)
* **Verification / Tests:** *No dedicated automated test currently exists.*

* **Current Architecture:** Single Redis 7 instance storing sliding-window ZSETs.
* **Scaling Trigger:** When tracking active article windows exceeds available single-node memory.
* **Possible Evolution:** Migrate to Redis Cluster using hash tags (`{art:42}`) to co-locate related article keys on the same shard.

---

### STORY-40: Cross-Encoder Micro-Reranker Daemon
* **Module:** Future Horizontal Scaling
* **Priority:** `[ADVANCED]`
* **Implementation Status:** `[FUTURE]`
* **Development Status:** `[NOT IMPLEMENTED]`
* **Prerequisites:** STORY-26, STORY-27
* **Leads To:** None
* **Primary Code Files:** None (System Design Evolution)
* **Verification / Tests:** *No dedicated automated test currently exists.*

* **Current Architecture:** In-process heuristic candidate reranker using title matching and keyword overlap bonuses.
* **Scaling Trigger:** When query ambiguity requires deeper cross-attention contextual scoring.
* **Possible Evolution:** Deploy a dedicated ONNX / BGE-reranker microservice for deep passage scoring.
