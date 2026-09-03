import numpy as np
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.knowledge_chunk import KnowledgeChunk
from app.search.embeddings import embedding_service
from app.core.logging import logger


class VectorSearchService:
    """
    Executes semantic vector similarity retrieval on indexed KnowledgeChunks.
    Uses pgvector cosine distance on PostgreSQL, or vectorized numpy in fallback mode.
    """

    @staticmethod
    async def search_similar(
        session: AsyncSession,
        query_text: str,
        top_k: int = 10,
        article_id: Optional[int] = None,
    ) -> List[Tuple[KnowledgeChunk, float]]:
        query_embedding = embedding_service.get_embedding(query_text)
        is_postgres = "postgresql" in str(session.bind.url) if session.bind else False

        if is_postgres:
            try:
                # pgvector cosine distance operator is <=>
                # similarity = 1 - cosine_distance
                query = select(
                    KnowledgeChunk,
                    (1 - KnowledgeChunk.embedding.cosine_distance(query_embedding)).label("similarity")
                )
                if article_id:
                    query = query.where(KnowledgeChunk.article_id == article_id)

                query = query.order_by(text("similarity DESC")).limit(top_k)
                result = await session.execute(query)
                rows = result.all()
                return [(row[0], float(row[1])) for row in rows]
            except Exception as e:
                logger.warning(f"Native pgvector query error ({e}); falling back to python cosine similarity.")

        # Fallback in-memory vector matching over candidate chunks
        stmt = select(KnowledgeChunk)
        if article_id:
            stmt = stmt.where(KnowledgeChunk.article_id == article_id)
        stmt = stmt.order_by(KnowledgeChunk.occurred_at.desc()).limit(200)

        result = await session.execute(stmt)
        chunks = result.scalars().all()

        if not chunks:
            return []

        q_vec = np.array(query_embedding, dtype=np.float32)
        scored = []
        for chunk in chunks:
            if chunk.embedding:
                c_vec = np.array(chunk.embedding, dtype=np.float32)
                dot = np.dot(q_vec, c_vec)
                norm_c = np.linalg.norm(c_vec)
                norm_q = np.linalg.norm(q_vec)
                sim = float(dot / (norm_q * norm_c)) if (norm_q * norm_c) > 0 else 0.0
                scored.append((chunk, sim))

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:top_k]
