# NexusAI / WikiPulse — End-to-End Data Flow & Event Lifecycle

This document traces the complete step-by-step path of a single Wikimedia event from live SSE ingestion to hybrid retrieval, AI summarization, and client SSE streaming.

---

## 1. Step-by-Step Event Lifecycle

```text
[1] Wikimedia Public SSE Stream (Chunked HTTP)
         │
         ▼
[2] Stream Ingestor Service (workers/stream_ingestor/ingestor.py)
    • Parses raw JSON payload into IngestedEvent schema.
    • Computes byte delta difference (change_size, byte_diff).
    • Assigns a deterministic UUID event_id.
         │
         ▼
[3] Kafka Producer (backend/app/kafka/producer.py)
    • Enqueues message to librdkafka C-memory buffer (linger.ms: 5, acks: 1).
    • Partition key set to article_title.
    • Target topic: wikimedia.recentchange.
         │
         ▼
[4] Apache Kafka Broker (Partition Distribution)
    • Partitions: 0, 1, 2.
    • Message appended to commit log segment on disk.
         │
         ▼
[5] Processor Worker (workers/processor/processor.py)
    • Thread-isolated poll via asyncio.to_thread(consumer.poll, 1.0).
    • Queries ProcessingJob for idempotency_key = "proc:{event_id}".
    • If status == 'completed' -> Logs duplicate skip -> Commits offset.
    • If not found -> Inserts ProcessingJob(status='in_progress').
    • Concurrent duplicate collisions trigger IntegrityError -> Session rollback -> Safe skip.
         │
         ▼
[6] PostgreSQL 16 ACID Transaction
    • Upserts Article record (ArticleRepository).
    • Upserts Editor record (ArticleRepository).
    • Inserts Edit revision delta (EditRepository).
    • Updates ProcessingJob status = 'completed'.
    • Transaction COMMIT executed.
         │
         ▼
[7] Redis 7 Sliding Window Update (backend/app/redis/counters.py)
    • Pipeline: ZADD act:art:{id}:edits <timestamp> <event_id>.
    • ZREMRANGEBYSCORE prunes timestamps older than 15 minutes.
    • ZCARD computes rolling edit counts (1m, 5m, 15m).
         │
         ▼
[8] Downstream Kafka Event Emission & Manual Offset Commit
    • Produces ProcessedArticleEvent to topic: wikimedia.article.processed.
    • Commits Kafka offset via asyncio.to_thread(consumer.commit, msg).
         │
         ├──► [9A] Analytics Worker (workers/analytics/analytics.py)
         │         • Reads Redis sliding counters.
         │         • Evaluates velocity ratio (1m count / 15m baseline).
         │         • If multiplier >= 3.0x and edits >= 5 -> Emits trend.detected.
         │
         └──► [9B] Embedding Worker (workers/embedding/embedding_worker.py)
                   • Vectorizes edit summary into 384d float32 dense vector via SentenceTransformers.
                   • Inserts record into knowledge_chunks in PostgreSQL with pgvector HNSW index.
                           │
                           ▼
[10] User Hybrid Search Query (GET /api/v1/search?q=Quantum&type=hybrid)
    • Executes pgvector cosine distance search (<=>) (11.1ms).
    • Executes PostgreSQL Full-Text Search (to_tsvector @@ plainto_tsquery) (1.9ms).
    • Fuses candidate ranks via Reciprocal Rank Fusion: RRF(d) = 1/(60 + rank_vec) + 1/(60 + rank_fts).
    • Applies CandidateReranker (phrase match boost + recency decay).
    • Returns ranked knowledge chunks in 15.89ms (avg).
         │
         ▼
[11] AI Question Answering (POST /api/v1/ai/ask)
    • ContextBuilder frames top-5 chunks inside <untrusted_wikipedia_content> XML fences.
    • LLMGateway invokes Gemini Flash -> Local Ollama (llama3.2:1b) -> Mock Provider.
    • Pydantic validates AIAnalysisOutput schema and formats verified citations.
         │
         ▼
[12] Real-Time Dashboard Broadcasting (GET /api/v1/stream/live)
    • SSE publisher pushes JSON payload to client's bounded asyncio.Queue(maxsize=100).
    • Browser client receives formatted Server-Sent Event and renders live update.
```
