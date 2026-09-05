# NexusAI Active Recall & Technical Self-Assessment Drills

---

## 1. Core Technical Flashcards

### Flashcard 1: Async Python Mechanics
* **Prompt:** *What happens when synchronous `time.sleep()` is called inside an `async def` FastAPI endpoint?*
* **Answer:** *It blocks the underlying OS thread running the single-threaded asyncio event loop. No other coroutines or incoming network requests can execute until the sleep finishes, causing massive latency spikes across all concurrent users.*

---

### Flashcard 2: Kafka Consumer Group Rebalancing
* **Prompt:** *What triggers a Kafka partition rebalance, and how does it affect processing latency?*
* **Answer:** *A rebalance is triggered when a new consumer joins the group, an existing consumer crashes/leaves, or a consumer fails to send heartbeats within `max.poll.interval.ms`. During a rebalance, partition assignments are recalculated, temporarily pausing consumption.*

---

### Flashcard 3: Redis Sliding Window Mechanics
* **Prompt:** *How does `ZREMRANGEBYSCORE act:art:42:edits -inf (now - 3600)` prevent unbounded memory growth in Redis?*
* **Answer:** *It deletes all members whose score (epoch timestamp) is older than 1 hour. This ensures that only active edit timestamps are retained, bounding Redis RAM consumption to active rolling windows.*

---

### Flashcard 4: Reciprocal Rank Fusion Formula
* **Prompt:** *Write the Reciprocal Rank Fusion formula and explain the purpose of the constant $k=60$.*
* **Answer:** 
  $$\text{RRF}(d) = \sum_{m \in M} \frac{w_m}{k + \text{rank}_m(d)}$$
  *$k=60$ is the standard smoothing constant. It prevents rank #1 items in one list from dominating disproportionately over items that rank consistently well (e.g. rank #2 and #3) across multiple search systems.*

---

## 2. Failure Diagnosis Walkthroughs

### Scenario A: Kafka Consumer Lag Is Spiking Uncontrollably
1. **Observation:** Kafka UI (`http://localhost:8080`) shows consumer lag on `wikimedia.recentchange` growing by 500 msgs/sec.
2. **Root Cause Analysis:**
   - Check worker CPU and database connection pool utilization (`obs-db-pool`).
   - If worker is CPU-bound on embeddings: scale `embedding-worker` horizontally.
   - If database pool is exhausted: increase `DB_POOL_SIZE` or implement micro-batch database commits (`session.add_all()`).

---

### Scenario B: Cloud LLM Returns 429 Rate Limit Errors
1. **Observation:** Gemini API logs `google.api_core.exceptions.ResourceExhausted: 429`.
2. **Behavior Verification:**
   - The LLM Gateway catches the `ResourceExhausted` exception.
   - It marks Gemini as degraded and routes the prompt to the secondary provider (`http://ollama:11434` / `llama3.2:3b`).
   - If Ollama is offline, it falls back to `MockProvider`, returning a deterministic grounded summary with 0ms downtime.

---

## 3. Code Reconstruction Challenges

### Challenge 1: Implement a Pure Python RRF Merger
```python
def reciprocal_rank_fusion(dense_ranks, sparse_ranks, k=60, w_dense=0.5, w_sparse=0.5):
    scores = {}
    for rank, doc_id in enumerate(dense_ranks, 1):
        scores[doc_id] = scores.get(doc_id, 0.0) + (w_dense / (k + rank))
    for rank, doc_id in enumerate(sparse_ranks, 1):
        scores[doc_id] = scores.get(doc_id, 0.0) + (w_sparse / (k + rank))
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)
```
