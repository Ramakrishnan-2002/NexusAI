# NexusAI / WikiPulse — System Design & Engineering Defense

## 1. System Requirements

### 1.1 Functional Requirements
- **Stream Ingestion:** Ingest real-time Wikimedia recent changes via Server-Sent Events (SSE).
- **Asynchronous Processing:** Buffer and normalize events asynchronously into relational models.
- **Velocity Spikes:** Detect unusual editing velocity across 1m, 5m, and 15m sliding windows.
- **Hybrid Search:** Natural language search combining dense vector cosine similarity with lexical Full-Text Search via Reciprocal Rank Fusion ($k=60$).
- **Grounded AI Synthesis:** Summarize events using an LLM Gateway with deterministic fallback and exact citation attribution.
- **Live Broadcasting:** Stream detected spikes and edits to client dashboards via Server-Sent Events.

### 1.2 Non-Functional Requirements
- **Surge Buffering:** Kafka disk commit logs absorb bursts up to $200+\text{ events/sec}$ without dropping traffic.
- **Strict At-Least-Once Delivery:** Offsets are committed manually only after database persistence succeeds.
- **Durable Idempotency:** Duplicate messages redelivered during rebalances do not create duplicate business records.
- **Sub-50ms Search Latency:** Hybrid vector + FTS retrieval returns in $< 50\text{ms}$ (measured avg: 15.89ms).

---

## 2. Partitioning & Consumer Scaling Models

### 2.1 Partition Key Strategy
Messages on topic `wikimedia.recentchange` are keyed by `article_title`.
- **Guarantee:** All edit revisions for a specific Wikipedia article land on the same Kafka partition, ensuring strict chronological per-article processing order.
- **Tradeoff:** Breaking news on a single article concentrates traffic on one partition. Single-worker throughput for that specific key is $\approx 11.81\text{ events/sec}$.

### 2.2 Consumer Scaling Law
In Apache Kafka, **parallelism within a consumer group is strictly bounded by the number of topic partitions**:
$$\text{Active Consumers } W \le \text{Partition Count } P$$
Any consumer instances exceeding the partition count remain completely idle as standby replicas.

---

## 3. Capacity & Sizing Calculations

### 3.1 Global Wikipedia Sizing Model (Theoretical Sizing)
Global English Wikipedia averages $30 - 50\text{ edits/sec}$, with breaking news bursts peaking at $200+\text{ edits/sec}$.
- **Per-Worker Throughput:** $C = 12.5\text{ events/sec}$ (measured 11.81 ev/s).
- **Required Worker Count:**
  $$W = \left\lceil \frac{R_{\text{peak}}}{C} \right\rceil = \left\lceil \frac{200}{12.5} \right\rceil = 16\text{ Worker Replicas}$$
- **Required Topic Partitions:** $P \ge 16\text{ Partitions}$.

### 3.2 pgvector Memory Sizing for 10M Chunks (Theoretical Sizing)
- **Raw Float32 Vectors (384d):** $10,000,000 \times (384 \times 4\text{ bytes}) \approx 15\text{ GB}$.
- **HNSW Index Overhead ($M=16, \text{ef}=64$):** $\approx 4\text{ GB}$.
- **Relational Metadata:** $\approx 8\text{ GB}$.
- **Total PostgreSQL Host RAM Sizing:** $\approx 32\text{ GB RAM}$ (ensures full HNSW graph residency in buffer cache).

---

## 4. Scaling Bottleneck Roadmap

```text
[ 200 events/sec ] ──► Scaled via 16 worker replicas across 16 Kafka partitions on 1 PostgreSQL instance.
[ 2,000 events/sec ] ──► PostgreSQL write saturation; requires PgBouncer connection pooler & read replicas for hybrid search.
[ 20,000 events/sec ] ──► CPU vectorization bottleneck; requires dedicated GPU embedding worker clusters & multi-broker Kafka cluster.
```
