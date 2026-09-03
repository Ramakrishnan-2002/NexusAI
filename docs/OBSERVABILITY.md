# NexusAI / WikiPulse — Observability, Health Probes & Metrics

This document details the telemetry metrics, health check probe semantics, logging structure, and recommended operational alerts for NexusAI.

---

## 1. Kubernetes-Style Health Check Probes

### 1.1 Liveness Probe (`GET /livez`)
- **Purpose:** Verifies that the FastAPI process is running and its asyncio event loop is responsive.
- **Critical Rule:** `/livez` **never performs database or Redis I/O**.
- **Rationale:** If `/livez` pinged the database and PostgreSQL experienced a transient connection spike, Kubernetes would mark all API pods dead and trigger simultaneous container restarts, worsening the outage.

### 1.2 Readiness Probe (`GET /readyz`)
- **Purpose:** Validates that downstream dependencies (PostgreSQL connection pool & Redis ping) are healthy before routing user traffic.
- **Failure Status:** Returns HTTP 503 (`Service Unavailable`) when dependencies fail, removing the pod from the load balancer rotation without killing the container.

---

## 2. Prometheus Telemetry Metrics

| Metric Name | Type | Emitted By | Purpose |
| :--- | :--- | :--- | :--- |
| `wikipulse_events_ingested_total` | Counter | `stream-ingestor` | Ingestion throughput from Wikimedia SSE stream. |
| `wikipulse_events_processed_total` | Counter | `processor-worker` | Database write throughput and idempotency checks. |
| `wikipulse_dlq_events_total` | Counter | `backend/app/kafka/dlq.py` | Number of unparseable poison pills routed to DLQ. |
| `wikipulse_rag_query_latency_seconds` | Histogram | `backend/app/api/v1/ai.py` | End-to-end RAG question answering latency distribution. |
| `wikipulse_llm_fallback_total` | Counter | `backend/app/llm/gateway.py` | Rate of provider fallback (Gemini $\to$ Ollama $\to$ Mock). |

---

## 3. Recommended Production Alerts

1. **Kafka Consumer Group Lag High:** `kafka_consumergroup_lag > 500` for 3 minutes $\implies$ Worker pool scaling required.
2. **DLQ Growth Rate Spiking:** `rate(wikipulse_dlq_events_total[5m]) > 5` $\implies$ Upstream schema breaking change or corruption.
3. **Database Connection Pool Saturation:** `db_pool_overflow_used >= 8` $\implies$ PostgreSQL connection starvation risk.
4. **API High Error Rate:** `rate(http_requests_total{status=~"5.."}[5m]) > 0.02` $\implies$ Service degraded.
5. **LLM Fallback Active:** `rate(wikipulse_llm_fallback_total[5m]) > 0` $\implies$ Primary frontier model (Gemini) rate limited or unreachable.
