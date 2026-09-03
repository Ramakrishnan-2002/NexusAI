import time
from typing import Dict, List, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.knowledge_chunk import KnowledgeChunk
from app.schemas.search import SearchResultItem, SearchResponse
from app.search.vector import VectorSearchService
from app.search.fts import FullTextSearchService
from app.search.reranker import CandidateReranker
from app.core.config import settings
from app.core.metrics import RAG_RETRIEVAL_DURATION


class HybridSearchService:
    """
    Orchestrates Hybrid Retrieval:
      1. Vector semantic search (pgvector)
      2. Full-text search (PostgreSQL FTS)
      3. Reciprocal Rank Fusion (RRF) & score merging
      4. Reranking layer to produce top-K evidence
    """

    @staticmethod
    async def search(
        session: AsyncSession,
        query: str,
        search_type: str = "hybrid",
        top_k: int = 10,
        article_id: Optional[int] = None,
    ) -> SearchResponse:
        start_time = time.time()
        candidate_count = max(top_k * 2, settings.RERANK_CANDIDATE_COUNT)

        vector_results: List[Tuple[KnowledgeChunk, float]] = []
        keyword_results: List[Tuple[KnowledgeChunk, float]] = []

        if search_type in ("hybrid", "vector"):
            vector_results = await VectorSearchService.search_similar(
                session, query, top_k=candidate_count, article_id=article_id
            )

        if search_type in ("hybrid", "keyword"):
            keyword_results = await FullTextSearchService.search_keywords(
                session, query, top_k=candidate_count, article_id=article_id
            )

        # Merge results using Reciprocal Rank Fusion (RRF)
        # RRF formula: RRF_score(d) = sum_{m in models} (weight_m / (k + rank_m(d)))
        rrf_k = 60
        merged_scores: Dict[int, Dict[str, Any]] = {}

        # Process vector ranks
        for rank, (chunk, score) in enumerate(vector_results):
            cid = chunk.id
            if cid not in merged_scores:
                merged_scores[cid] = {"chunk": chunk, "score": 0.0, "vector_score": score, "keyword_score": None}
            rrf_add = settings.HYBRID_SEARCH_VECTOR_WEIGHT / (rrf_k + rank + 1)
            merged_scores[cid]["score"] += rrf_add

        # Process keyword ranks
        for rank, (chunk, score) in enumerate(keyword_results):
            cid = chunk.id
            if cid not in merged_scores:
                merged_scores[cid] = {"chunk": chunk, "score": 0.0, "vector_score": None, "keyword_score": score}
            else:
                merged_scores[cid]["keyword_score"] = score
            rrf_add = settings.HYBRID_SEARCH_FTS_WEIGHT / (rrf_k + rank + 1)
            merged_scores[cid]["score"] += rrf_add

        # Format candidate list for reranker
        candidates: List[Tuple[KnowledgeChunk, float]] = [
            (data["chunk"], data["score"] * 100.0) for data in merged_scores.values()
        ]

        # Apply reranking layer
        reranked = CandidateReranker.rerank(query=query, candidates=candidates, top_k=top_k)

        took_ms = round((time.time() - start_time) * 1000, 2)
        RAG_RETRIEVAL_DURATION.labels(search_type=search_type).observe(took_ms / 1000.0)

        # Build response items
        items: List[SearchResultItem] = []
        for chunk, final_score in reranked:
            meta = merged_scores.get(chunk.id, {})
            items.append(
                SearchResultItem(
                    chunk_id=chunk.id,
                    article_id=chunk.article_id,
                    article_title=chunk.article_title,
                    revision_id=chunk.revision_id,
                    title=chunk.title,
                    content=chunk.content,
                    score=round(final_score, 4),
                    vector_score=meta.get("vector_score"),
                    keyword_score=meta.get("keyword_score"),
                    occurred_at=chunk.occurred_at,
                    metadata=chunk.chunk_metadata,
                )
            )

        return SearchResponse(
            query=query,
            search_type=search_type,
            total_found=len(items),
            took_ms=took_ms,
            results=items,
        )
