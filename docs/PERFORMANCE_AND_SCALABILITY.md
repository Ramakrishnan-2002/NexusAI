# WikiPulse / NexusAI — Performance, Scalability & Capacity Planning

This document provides the mathematical capacity models, horizontal scaling dynamics, partition sizing constraints, and bottleneck roadmaps for WikiPulse.

---

## 1. Capacity Sizing Mathematical Formulas

### 1.1 Worker Replicas Equation
To process incoming event streams without accumulating unbounded Kafka consumer lag:
$$W = \left\lceil \frac{R_{\text{peak}}}{C_{\text{worker}}} \right\rceil$$

Where:
- $R_{\text{peak}}$ = Peak incoming edit arrival rate ($\text{events/sec}$).
- $C_{\text{worker}}$ = Measured single-worker processing capacity ($11.81 \approx 12.5\text{ events/sec}$).
- $W$ = Required active worker replicas.

### 1.2 English Wikipedia Peak Capacity Sizing (Theoretical Sizing)
Global English Wikipedia averages $30 - 50\text{ edits/sec}$, with breaking news bursts peaking at $200+\text{ edits/sec}$:
$$W = \left\lceil \frac{200\text{ ev/s}}{12.5\text{ ev/s}} \right\rceil = 16\text{ Worker Replicas}$$

### 1.3 Kafka Partition Bound
In Apache Kafka, consumer group parallelism is strictly constrained by topic partitions:
$$\text{Active Consumers } W \le \text{Partition Count } P$$
$$\therefore \text{Required Kafka Partitions } P \ge 16\text{ Partitions}$$

---

## 2. Storage & Memory Growth Projections (Theoretical Sizing)

### 2.1 Daily Ingestion Projections ($50\text{ ev/s}$ Sustained)
- **Total Events / Day:** $50 \times 86,400 = 4,320,000\text{ events/day}$.
- **PostgreSQL Edit Records ($\approx 500\text{ bytes/row}$):** $\approx 2.16\text{ GB / day}$.
- **pgvector Knowledge Chunks ($384\text{ float32} \times 4\text{ bytes} = 1.536\text{ KB/chunk}$):** $\approx 6.63\text{ GB / day}$.
- **Monthly Database Growth:** $\approx 260\text{ GB / month}$.

### 2.2 pgvector 10M Chunks RAM Sizing (Theoretical Sizing)
- **Raw 384d Vectors:** $10,000,000 \times 1.536\text{ KB} \approx 15.36\text{ GB}$.
- **HNSW Graph Index ($M=16, \text{ef\_construction}=64$):** $\approx 4.2\text{ GB}$.
- **PostgreSQL OS Buffer Cache & Relational Metadata:** $\approx 12.44\text{ GB}$.
- **Recommended Database Host RAM:** $\mathbf{32\text{ GB RAM}}$ (ensures full HNSW graph residency in buffer memory, eliminating disk thrashing during approximate nearest neighbor search).

---

## 3. Scaling Bottleneck Roadmap

```text
┌─────────────────────────┬──────────────────────────────────┬──────────────────────────────────────────────────────────┐
│ Throughput Scale        │ Primary Bottleneck Area          │ Required Architectural Mitigation                        │
├─────────────────────────┼──────────────────────────────────┼──────────────────────────────────────────────────────────┤
│ 200 events / sec        │ Worker CPU & Partition Count     │ Scale to 16 workers across 16 Kafka partitions.          │
│ 2,000 events / sec      │ PostgreSQL Write Contention      │ Introduce PgBouncer & separate Read Replicas for search. │
│ 20,000 events / sec     │ Embedding CPU Vectorization      │ Deploy GPU-accelerated Triton Inference clusters.        │
└─────────────────────────┴──────────────────────────────────┴──────────────────────────────────────────────────────────┘
```
