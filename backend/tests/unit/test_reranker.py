from datetime import datetime, timezone
import pytest
from app.models.knowledge_chunk import KnowledgeChunk
from app.search.reranker import CandidateReranker


def test_candidate_reranking_phrase_match():
    chunk1 = KnowledgeChunk(
        id=1,
        article_id=10,
        article_title="Quantum Supremacy",
        title="Edit on Quantum Supremacy",
        content="Random minor grammar fix in introductory remarks.",
        occurred_at=datetime.now(timezone.utc),
    )
    chunk2 = KnowledgeChunk(
        id=2,
        article_id=11,
        article_title="James Webb Space Telescope",
        title="Edit on JWST",
        content="Detected exoplanet atmospheric water vapor signatures.",
        occurred_at=datetime.now(timezone.utc),
    )

    candidates = [(chunk1, 0.4), (chunk2, 0.3)]
    query = "exoplanet atmospheric water vapor"

    reranked = CandidateReranker.rerank(query=query, candidates=candidates, top_k=2)

    assert len(reranked) == 2
    # chunk2 has exact phrase match for the query and should be ranked #1
    assert reranked[0][0].id == 2
    assert reranked[0][1] > reranked[1][1]
