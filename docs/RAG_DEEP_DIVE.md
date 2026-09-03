# WikiPulse — Retrieval-Augmented Generation (RAG) Deep Dive

This document details the knowledge chunking, hybrid retrieval algorithms, prompt-injection defense mechanisms, and multi-provider LLM fallback routing powering the WikiPulse RAG pipeline.

---

## 1. RAG Architecture Overview

```text
User Question
      │
      ├───────────────────────────────┬───────────────────────────────┐
      ▼                                                               ▼
1. Dense Vector Search                                    2. Lexical Full-Text Search
   • 384d all-MiniLM-L6-v2                                   • PostgreSQL to_tsvector('english')
   • pgvector HNSW Cosine Distance                           • GIN Index plainto_tsquery
   • Top 20 Semantic Candidates                              • Top 20 Exact Keyword Candidates
      │                                                               │
      └───────────────────────────────┬───────────────────────────────┘
                                      │
                                      ▼
                        3. Reciprocal Rank Fusion (RRF)
                           • k = 60 constant damping
                           • Fuses semantic & lexical rank
                                      │
                                      ▼
                        4. Cross-Scoring Reranker
                           • Exact phrase match boost
                           • Temporal recency decay
                           • Deduplication by Article ID
                                      │
                                      ▼
                        5. Untrusted Data Barrier
                           • sanitize_external_text()
                           • <untrusted_wikipedia_content> fences
                                      │
                                      ▼
                        6. LLM Gateway Routing
                           • Primary: Google Gemini Flash
                           • Secondary: Local Ollama (1B CPU)
                           • Tertiary: Deterministic Mock
                                      │
                                      ▼
                        7. Schema Validation & Citations
                           • Pydantic AIAnalysisOutput
                           • Explicit [Article, Revision, Time]
```

---

## 2. Hybrid Search & Reciprocal Rank Fusion (RRF)

### 2.1 Why Pure Semantic Vector Search Fails in Knowledge Systems
- **The Proper Noun & Acronym Blind Spot:** Vector embedding models frequently map uncommon technical acronyms (e.g. "JWST", "CRISPR-Cas9", "mRNA-1273") to generic cluster centroids, yielding low cosine similarity for exact query matches.
- **The Lexical FTS Advantage:** PostgreSQL Full-Text Search (`to_tsvector` + `plainto_tsquery`) guarantees exact lexical token matches with sub-2ms inverted index lookups.

### 2.2 Mathematical Formula for Reciprocal Rank Fusion
Given a set of documents $D$ retrieved across $M$ ranked retrieval channels, the RRF score is computed as:

$$\text{RRF\_Score}(d) = \sum_{m \in M} \frac{1}{k + \text{rank}_m(d)}$$

Where:
- $k = 60$ (standard dampening factor preventing top ranks from completely dominating scores).
- $\text{rank}_m(d) \in [1, N]$ is the 1-indexed rank of document $d$ in retrieval list $m$.

```python
# backend/app/search/hybrid.py
rrf_score = 0.0
if doc_id in vector_ranks:
    rrf_score += 1.0 / (k + vector_ranks[doc_id])
if doc_id in keyword_ranks:
    rrf_score += 1.0 / (k + keyword_ranks[doc_id])
```

---

## 3. Prompt Injection Defense & Untrusted Content Barriers

### 3.1 The Untrusted Data Principle
In WikiPulse, **external Wikipedia content is treated strictly as untrusted user data, NEVER as execution instructions**.

### 3.2 Multi-Layered Defense Architecture
1. **Regex Pattern Neutralization:** `sanitize_external_text()` in [`backend/app/core/security.py`](file:///d:/NexusAI/backend/app/core/security.py) replaces override strings (e.g. `IGNORE ALL PREVIOUS INSTRUCTIONS`, `You are now DAN`) with `[UNTRUSTED_INSTRUCTION_FILTERED]`.
2. **Context Boundary Fencing:** Chunks are framed inside explicit XML tags:
   ```xml
   <untrusted_wikipedia_content>
   [Chunk 1] Title: Quantum Computing | Content: ...
   </untrusted_wikipedia_content>
   ```
3. **Pydantic Schema Validation:** LLM responses are parsed and validated strictly into the `AIAnalysisOutput` schema, preventing arbitrary prompt instruction leakages.
4. **Insufficient Evidence Fallback:** If the retrieved chunks do not contain relevant facts for the query, the context builder instructs the model to return:
   > *"Insufficient evidence in current knowledge stream."*

---

## 4. LLM Gateway Multi-Provider Fallback Routing

```text
Incoming Request -> [LLMGateway.analyze_structured]
                           │
                 Is GEMINI_API_KEY present?
                 ├── YES ──► Invoke Google Gemini Flash (Timeout: 10s)
                 │              │
                 │              ├──► Success: Return structured analysis
                 │              │
                 │              └──► Rate Limited / Error: Cascade to Ollama
                 │
                 └── NO ──► Is Local Ollama Reachable?
                                │
                                ├──► YES ──► Invoke Ollama (llama3.2:1b) (Timeout: 15s)
                                │              │
                                │              └──► Success: Return structured analysis
                                │
                                └──► NO ───► Invoke Deterministic Mock Provider (0.20ms)
                                               │
                                               └──► Generates schema-compliant grounded response
```
