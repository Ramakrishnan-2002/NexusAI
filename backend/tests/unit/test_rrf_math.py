import pytest
from app.schemas.search import SearchResultItem
from app.search.reranker import CandidateReranker
from app.search.hybrid import HybridSearchService
from app.models.knowledge_chunk import KnowledgeChunk
from datetime import datetime, timezone


def test_rrf_scoring_formula():
    """Verify mathematical properties of Reciprocal Rank Fusion"""
    rrf_k = 60
    vector_weight = 0.6
    fts_weight = 0.4

    # Rank 0 (1st item)
    v_score_0 = vector_weight / (rrf_k + 0 + 1)
    fts_score_0 = fts_weight / (rrf_k + 0 + 1)

    # Rank 1 (2nd item)
    v_score_1 = vector_weight / (rrf_k + 1 + 1)

    assert v_score_0 > v_score_1
    assert (v_score_0 + fts_score_0) > v_score_0
    assert v_score_0 == pytest.approx(0.6 / 61, rel=1e-4)


def test_reranker_deduplication():
    now = datetime.now(timezone.utc)
    chunk1 = KnowledgeChunk(
        id=101,
        article_id=5,
        article_title="Quantum Computing",
        title="Edit 1",
        content="Quantum error correction with surface codes.",
        occurred_at=now,
    )
    # Duplicate chunk ID
    candidates = [(chunk1, 0.8), (chunk1, 0.9)]
    reranked = CandidateReranker.rerank(query="quantum error", candidates=candidates, top_k=5)

    assert len(reranked) == 1
    assert reranked[0][0].id == 101
