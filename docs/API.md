# NexusAI / WikiPulse — REST & Streaming API Documentation

The FastAPI control plane exposes interactive OpenAPI documentation at `http://localhost:8000/docs` and raw OpenAPI JSON at `http://localhost:8000/openapi.json`.

---

## 1. Core Endpoints

### 1.1 Hybrid Knowledge Search
- **Endpoint:** `GET /api/v1/search`
- **Query Parameters:**
  - `q` (string, required): The search query.
  - `type` (string, optional): `hybrid` (default), `vector`, or `keyword`.
  - `limit` (integer, optional, default: 10): Maximum candidates to return.
- **Example Response:**
```json
{
  "query": "Quantum Computing",
  "search_type": "hybrid",
  "count": 5,
  "results": [
    {
      "article_id": 1,
      "article_title": "Quantum Computing",
      "revision_id": 120000206,
      "title": "Quantum Computing",
      "content": "Wikipedia article 'Quantum Computing' was edited...",
      "score": 0.67,
      "occurred_at": "2026-09-03T04:11:01+00:00"
    }
  ]
}
```

---

### 1.2 Grounded AI Question Answering (RAG)
- **Endpoint:** `POST /api/v1/ai/ask`
- **Request Body:**
```json
{
  "question": "What updates occurred on Quantum Computing?"
}
```
- **Example Response:**
```json
{
  "question": "What updates occurred on Quantum Computing?",
  "answer": "Activity detected regarding 'Quantum Computing Revision: 120000206'...",
  "structured_analysis": {
    "summary": "Activity detected regarding 'Quantum Computing'...",
    "importance": "medium",
    "detected_topic": "General Knowledge",
    "change_type": "expansion",
    "confidence": 0.85,
    "evidence_points": [
      "Wikipedia article 'Quantum Computing' was edited by user 'BioCurator' (+1449 bytes)"
    ],
    "citations": [
      {
        "article_title": "Quantum Computing",
        "revision_id": 120000206,
        "occurred_at": "2026-09-03T04:11:01+00:00",
        "snippet": "Wikipedia article 'Quantum Computing' was edited by user 'BioCurator' with byte difference of +1449 bytes.",
        "relevance_reason": "Relevance Score: 0.67"
      }
    ]
  },
  "provider_used": "mock",
  "retrieval_took_ms": 47.86,
  "total_took_ms": 48.68
}
```

---

### 1.3 Real-Time Server-Sent Events (SSE) Stream
- **Endpoint:** `GET /api/v1/stream/live`
- **Protocol:** Server-Sent Events (`text/event-stream`)
- **Event Format:**
```text
event: event
data: {"event_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3d0c24", "article_title": "Mars Sample Return", "byte_diff": 450}

: ping - 2026-09-03T04:15:00+00:00
```

---

### 1.4 Kubernetes Health Probes
- **Liveness Probe:** `GET /livez` $\implies$ `{"status": "healthy", "service": "WikiPulse Control Plane"}`
- **Readiness Probe:** `GET /readyz` $\implies$ `{"ready": true, "components": {"database": true, "redis": true}}`
