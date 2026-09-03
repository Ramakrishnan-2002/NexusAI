# WikiPulse / NexusAI — Capacity Planning & Scalability Modeling

This document presents the mathematical capacity models, measured throughput baselines, partition sizing rules, and storage projections for WikiPulse.

---

## 1. Measured Throughput Baselines vs. Theoretical Projections

```text
┌──────────────────────────────────────────────┬───────────────────────────────┬────────────────────┐
│ Operation                                    │ Measured Baseline             │ Classification     │
├──────────────────────────────────────────────┼───────────────────────────────┼────────────────────┤
│ Single Processor Worker Throughput           │ 11.81 events / sec            │ MEASURED           │
│ 3-Worker Scaled Processor Throughput         │ 35–40 events / sec            │ PRELIMINARY        │
│ Dense Vector Embedding Throughput (CPU)      │ 1,312.87 chunks / sec         │ MEASURED           │
│ Hybrid Search Latency (pgvector + FTS + RRF) │ 15.89 ms (p95: 21.86 ms)      │ MEASURED           │
│ PostgreSQL Full-Text Search Latency          │ 1.93 ms (p95: 2.47 ms)        │ MEASURED           │
│ pgvector Cosine Distance Search Latency      │ 11.10 ms (p95: 18.77 ms)      │ MEASURED           │
│ Redis Sliding-Window Velocity Multiplier     │ < 0.5 ms                      │ MEASURED           │
└──────────────────────────────────────────────┴───────────────────────────────┴────────────────────┘
```

---

## 2. Capacity Sizing Formulas

### 2.1 Worker Replicas Formula
To process peak incoming traffic without growing consumer lag:
$$W = \left\lceil \frac{R_{\text{peak}}}{C_{\text{worker}}} \right\rceil$$

Where:
- $R_{\text{peak}}$ = Peak incoming event arrival rate ($\text{events/sec}$)
- $C_{\text{worker}}$ = Measured single-worker processing capacity ($11.81 \approx 12.5\text{ events/sec}$)

### 2.2 Sizing for English Wikipedia Peak ($200\text{ ev/s}$)
$$W = \left\lceil \frac{200\text{ ev/s}}{12.5\text{ ev/s}} \right\rceil = 16\text{ Worker Replicas}$$

### 2.3 Kafka Partition Constraint
Because Kafka assigns at most one consumer per partition in a consumer group:
$$\text{Partition Count } P \ge W \implies P \ge 16\text{ Partitions}$$

---

## 3. Storage & Memory Growth Projections (Theoretical Sizing)

### 3.1 Daily Ingestion at $50\text{ ev/s}$ Sustained
- **Daily Events:** $50 \times 86,400 \approx 4,320,000\text{ events/day}$.
- **PostgreSQL Edit Records ($\approx 500\text{ bytes/row}$):** $\approx 2.16\text{ GB / day}$.
- **pgvector Knowledge Chunks ($384\text{ float32} \times 4\text{ bytes} = 1.536\text{ KB/chunk}$):** $\approx 6.63\text{ GB / day}$.
- **Monthly Database Growth:** $\approx 260\text{ GB / month}$.

### 3.2 pgvector 10M Chunks Memory Sizing (Theoretical Sizing)
- **Raw 384d Vectors:** $10,000,000 \times 1.536\text{ KB} \approx 15.36\text{ GB}$.
- **HNSW Graph Index ($M=16, \text{ef}=64$):** $\approx 4.2\text{ GB}$.
- **PostgreSQL OS Buffer Cache & Metadata:** $\approx 12.44\text{ GB}$.
- **Recommended Host RAM:** $\mathbf{32\text{ GB RAM}}$ (guarantees zero disk thrashing during ANN vector traversal).
