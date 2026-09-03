# WikiPulse / NexusAI — 15-Minute System Design Presentation Script

A structured, conversational talk track designed to sound natural, authoritative, and technically grounded during a senior backend or system design interview.

---

### [0:00 – 2:00] Problem & Motivation
> "Hi everyone. Today I'd like to walk you through **WikiPulse**, a real-time knowledge change intelligence platform built to monitor, analyze, and synthesize Wikimedia's live edit stream.
>
> In real-time knowledge ecosystems, breaking events—like scientific discoveries, political elections, or natural disasters—trigger sudden bursts of edits across related Wikipedia articles. 
>
> The core engineering challenge is an **impedance mismatch**: Wikimedia emits bursts of up to hundreds of edits per second, but downstream operations—such as calculating multi-window velocity multipliers, generating 384-dimensional vector embeddings, and executing LLM summarization—take tens to hundreds of milliseconds. 
>
> If you process these synchronously on a REST API, your server immediately exhausts its worker pool and drops incoming traffic. 
> 
> To solve this, WikiPulse uses an event-driven architecture that decouples ingestion from compute-heavy downstream workers using Apache Kafka, tracks velocity in Redis Sorted Sets, stores hybrid vector and relational metadata in PostgreSQL with pgvector, and serves evidence-grounded AI summaries with strict citation attribution."

---

### [2:00 – 5:00] High-Level Architecture
> "Let's look at the high-level architecture. We separate the system into a **Data Plane** and a **Control Plane**.
>
> On the Data Plane, our `stream-ingestor` worker establishes an SSE connection to Wikimedia, validates the raw JSON payload with Pydantic, calculates byte difference deltas, and assigns a deterministic UUID `event_id`. 
>
> It publishes to Kafka under the topic `wikimedia.recentchange`. Crucially, we set the Kafka message partition key to `article_title`. This guarantees that all revisions for a specific article land on the same partition and are processed strictly in chronological order.
>
> Downstream, we have dedicated worker pools organized into independent consumer groups:
> 1. The **Processor Worker** normalizes and persists relational entities in PostgreSQL.
> 2. The **Analytics Worker** maintains rolling 1-minute, 5-minute, and 15-minute sliding windows in Redis to detect unusual velocity spikes.
> 3. The **Embedding Worker** generates 384-dimensional dense vectors using `SentenceTransformers` and indexes them into `pgvector`.
> 4. The **AI Worker** gathers contextual chunks upon detected spikes and generates structured summaries.
>
> On the Control Plane, a FastAPI application provides REST endpoints for hybrid knowledge retrieval, AI question-answering, health probes, and Server-Sent Events broadcasting to a live dashboard."

---

### [5:00 – 8:00] Kafka & Distributed Processing with confluent-kafka
> "Let's dive deeper into our Kafka client architecture. We migrated our entire pipeline to **`confluent-kafka`**, the official Python client backed by `librdkafka` in C.
>
> Because `confluent-kafka` is a synchronous C-extension, we had to carefully engineer the bridge to Python's `asyncio` event loop.
>
> On the producer side, `producer.produce()` is non-blocking—it enqueues the message directly into librdkafka's C-memory queue in roughly one microsecond. We configure `linger.ms: 5` to enable micro-batching without latency penalty, and call `producer.poll(0)` on loop ticks to dispatch delivery callbacks.
>
> On the consumer side, running `consumer.poll()` directly on the event loop would freeze our async worker coroutines. So we poll in worker threads via `await asyncio.to_thread(consumer.poll, 1.0)`.
>
> For delivery semantics, we enforce **strict at-least-once delivery**. We disable auto-commit (`enable.auto.commit = False`) and manually commit offsets via `consumer.commit(msg)` strictly after database persistence succeeds. If a worker container crashes mid-transaction, Kafka reassigns the uncommitted offset to a surviving replica during group rebalance."

---

### [8:00 – 10:00] Database, Idempotency & Failure Handling
> "Because at-least-once delivery means redeliveries can happen during rebalances or network blips, our database layer must guarantee **strict application-level idempotency**.
>
> In PostgreSQL 16, we maintain a `processing_jobs` table with a unique constraint on `idempotency_key = 'proc:{event_id}'`. 
>
> When the processor worker consumes an event, it attempts to insert this record. If two worker replicas process duplicate events concurrently, the second insert collides on the unique index and throws a database `IntegrityError`. Our code catches this, rolls back the session, and commits the Kafka offset as a safe no-op. We've verified this with concurrency tests running 50 simultaneous duplicate executions resulting in exactly 1 persisted record.
>
> Now, what about consistency across PostgreSQL and Redis? They are **separate consistency domains**. PostgreSQL is our authoritative system of record; Redis is transient derived aggregation state. If Redis experiences a transient blip, our `ActivityCounterService` degrades gracefully to an in-memory fallback without aborting the PostgreSQL transaction.
>
> For unparseable poison pills, our `RetryPolicy` executes exponential backoff for 3 attempts and then routes the corrupted payload to `wikimedia.dlq`, ensuring partition queues never stall."

---

### [10:00 – 12:00] Hybrid Search & Grounded RAG Pipeline
> "Next is our retrieval and AI synthesis pipeline. Pure vector search often fails on knowledge corpora because dense embeddings can dilute specific proper nouns or acronyms like 'JWST' or 'CRISPR'. Lexical full-text search guarantees exact keyword precision but misses semantic synonyms.
>
> We implement **Hybrid Search** using **Reciprocal Rank Fusion (RRF)**:
> 1. We run an ANN vector search in `pgvector` using an HNSW cosine distance index.
> 2. We run a full-text search in PostgreSQL using GIN inverted indexes.
> 3. We fuse their ranks using $RRF(d) = \sum \frac{1}{60 + \text{rank}_i(d)}$ and apply temporal recency reranking.
>
> In benchmarks, pure FTS runs in 1.93ms and pgvector runs in 11.1ms; our combined hybrid search pipeline executes in just **15.89ms (p95: 21.86ms)**.
>
> For RAG safety, we treat Wikipedia content strictly as untrusted data: we filter known prompt injection patterns, frame chunks inside `<untrusted_wikipedia_content>` tags, and enforce Pydantic schema validation on the LLM output with mandatory citations mapping to `[Article Title, Revision ID, Timestamp]'."

---

### [12:00 – 14:00] Performance & Horizontal Scalability
> "Let's talk performance and capacity sizing.
>
> In our measured benchmarks:
> - A single processor worker handles **11.81 events/sec** through full ACID writes and Redis updates.
> - With 3 worker replicas consuming across 3 Kafka partitions, throughput scales to **35–40 events/sec**.
> - Dense vectorization throughput on CPU achieves **1,312.87 chunks/sec**.
>
> To size for global English Wikipedia peaks ($200+\text{ edits/sec}$), our scaling formula shows:
> $$W = \left\lceil \frac{200\text{ ev/s}}{12.5\text{ ev/s}} \right\rceil = 16\text{ Workers}$$
>
> This requires partitioning our Kafka topic into at least 16 partitions, because Kafka assigns at most one active consumer per partition within a consumer group."

---

### [14:00 – 15:00] Tradeoffs & Production Evolution
> "To conclude, every technology in WikiPulse was chosen for architectural necessity:
> - We chose **PostgreSQL + pgvector** over a dedicated vector DB to keep relational metadata and vector embeddings ACID-consistent in a single database without dual-write lag.
> - We chose **Kafka** over RabbitMQ for disk-persisted commit logs and partition-level total ordering.
> - We chose **confluent-kafka** over pure-Python clients for native C-level buffering and protocol robustness.
>
> In a full cloud production deployment, the next evolution would be moving to Amazon MSK for multi-AZ Kafka replication, Aurora PostgreSQL with read replicas for vector queries, and using KEDA on Kubernetes to auto-scale worker pods based on Kafka consumer lag.
>
> Thank you, and I'd love to take your questions."
