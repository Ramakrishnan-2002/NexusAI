from datetime import datetime, timezone
import pytest
from app.models.article import Article
from app.models.knowledge_chunk import KnowledgeChunk
from app.search.embeddings import embedding_service
from app.search.hybrid import HybridSearchService


@pytest.mark.asyncio
async def test_hybrid_search_scoring_and_retrieval(db_session):
    article = Article(title="Deep Space Telemetry", wiki="enwiki", namespace=0)
    db_session.add(article)
    await db_session.flush()

    content1 = "Deep Space Network antenna array received synchronized telemetry from Voyager 1."
    content2 = "Quantum microprocessor benchmark yielded high coherence fidelity."

    chunk1 = KnowledgeChunk(
        article_id=article.id,
        article_title="Deep Space Telemetry",
        title="Edit on Voyager Telemetry",
        content=content1,
        embedding=embedding_service.get_embedding(content1),
        chunk_metadata={"wiki": "enwiki"},
        occurred_at=datetime.now(timezone.utc),
    )
    chunk2 = KnowledgeChunk(
        article_id=article.id,
        article_title="Deep Space Telemetry",
        title="Edit on Quantum Benchmarks",
        content=content2,
        embedding=embedding_service.get_embedding(content2),
        chunk_metadata={"wiki": "enwiki"},
        occurred_at=datetime.now(timezone.utc),
    )

    db_session.add(chunk1)
    db_session.add(chunk2)
    await db_session.commit()

    # Search for Voyager Deep Space telemetry
    response = await HybridSearchService.search(
        session=db_session,
        query="Voyager antenna telemetry deep space",
        search_type="hybrid",
        top_k=2,
    )

    assert response.total_found >= 1
    assert response.results[0].chunk_id == chunk1.id
    assert response.results[0].score > 0
    assert "Voyager" in response.results[0].content
