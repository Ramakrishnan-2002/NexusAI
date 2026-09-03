# ADR-005: Hybrid Knowledge Retrieval (pgvector + PostgreSQL FTS)

## Status
**ACCEPTED**

## Context
Pure vector search relies on dense semantic embeddings (e.g. `all-MiniLM-L6-v2`), which excel at conceptual similarity but frequently dilute uncommon proper nouns, acronyms ("JWST", "CRISPR-Cas9"), and numeric revision identifiers. Pure lexical full-text search guarantees exact keyword precision but misses semantic synonyms.

## Decision
Implement **Hybrid Knowledge Retrieval**:
1. Execute dense vector cosine distance search (`<=>`) in `pgvector` with HNSW indexing.
2. Execute lexical Full-Text Search in PostgreSQL using GIN inverted indexes (`to_tsvector @@ plainto_tsquery`).
3. Combine and rank candidate lists using Reciprocal Rank Fusion (RRF, $k=60$).

## Alternatives Considered
1. **Vector-Only Search:** High recall for conceptual queries, but poor precision for exact scientific acronyms and entity names.
2. **Dedicated Vector DB (Pinecone / Qdrant):** Creates dual-write synchronization lag and requires external network hops; `pgvector` allows single-database ACID joins with relational metadata.

## Consequences
- **Positive:** Superior retrieval quality across both conceptual queries and exact keyword/acronym searches in a single database.
- **Tradeoff:** Executes two queries per search request (~15.89ms combined latency vs ~1.93ms pure FTS).
