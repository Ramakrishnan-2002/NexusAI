# ADR-001: Adoption of Apache Kafka for Ingestion Decoupling

## Status
**ACCEPTED**

## Context
WikiPulse consumes real-time Wikimedia recent-change streams with bursty event arrivals ($50 - 200+\text{ edits/sec}$). Downstream processing involves multi-window velocity aggregation, 384-dimensional dense vector embedding generation, and AI summarization, which require tens to hundreds of milliseconds per item. Coupling ingestion synchronously to downstream workers or using in-memory background tasks leads to thread starvation, memory exhaustion (OOM), and dropped events during traffic surges.

## Decision
Use Apache Kafka 3.7 as an immutable, disk-persisted distributed event commit log between stream ingestion and downstream worker services. Topics are partitioned by `article_title` to guarantee strict chronological ordering per Wikipedia article.

## Alternatives Considered
1. **FastAPI BackgroundTasks:** In-memory task queue in Python heap. Rejected because tasks are non-durable and lost on container restart.
2. **RabbitMQ:** Traditional message broker. Rejected because RabbitMQ deletes messages upon consumer ACK, preventing event replay and downstream multi-worker fanout.
3. **Redis Streams:** In-memory stream log. Rejected due to RAM capacity bounds during extended consumer worker outages.

## Consequences
- **Positive:** Ingestion is decoupled; worker groups scale independently; partition commit logs can be replayed on failure.
- **Tradeoff:** Operational overhead of running a Kafka cluster and managing topic partitions.
