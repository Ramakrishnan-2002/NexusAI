import re
from typing import List, Optional, Tuple
from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.knowledge_chunk import KnowledgeChunk
from app.core.logging import logger


class FullTextSearchService:
    """
    PostgreSQL Full-Text Search (FTS) service using tsvector and ts_rank_cd,
    with word-token matching fallback for SQLite/non-PostgreSQL environments.
    """

    @staticmethod
    async def search_keywords(
        session: AsyncSession,
        query_text: str,
        top_k: int = 10,
        article_id: Optional[int] = None,
    ) -> List[Tuple[KnowledgeChunk, float]]:
        cleaned_query = re.sub(r"[^\w\s]", " ", query_text).strip()
        if not cleaned_query:
            return []

        is_postgres = "postgresql" in str(session.bind.url) if session.bind else False

        if is_postgres:
            try:
                # Format query tokens for plainto_tsquery or to_tsquery
                fts_query = select(
                    KnowledgeChunk,
                    func.ts_rank_cd(
                        func.to_tsvector("english", KnowledgeChunk.title + " " + KnowledgeChunk.content),
                        func.plainto_tsquery("english", cleaned_query)
                    ).label("rank")
                ).where(
                    func.to_tsvector("english", KnowledgeChunk.title + " " + KnowledgeChunk.content).op("@@")(
                        func.plainto_tsquery("english", cleaned_query)
                    )
                )

                if article_id:
                    fts_query = fts_query.where(KnowledgeChunk.article_id == article_id)

                fts_query = fts_query.order_by(text("rank DESC")).limit(top_k)
                result = await session.execute(fts_query)
                rows = result.all()
                return [(row[0], float(row[1])) for row in rows]
            except Exception as e:
                logger.warning(f"Native PostgreSQL FTS error: {e}; falling back to token ILIKE query.")

        # Fallback ILIKE / keyword matching
        words = cleaned_query.lower().split()
        conditions = []
        for word in words[:5]:
            conditions.append(KnowledgeChunk.title.ilike(f"%{word}%"))
            conditions.append(KnowledgeChunk.content.ilike(f"%{word}%"))

        stmt = select(KnowledgeChunk)
        if conditions:
            stmt = stmt.where(or_(*conditions))
        if article_id:
            stmt = stmt.where(KnowledgeChunk.article_id == article_id)

        stmt = stmt.order_by(KnowledgeChunk.occurred_at.desc()).limit(top_k * 2)
        result = await session.execute(stmt)
        chunks = result.scalars().all()

        scored = []
        for chunk in chunks:
            combined = (chunk.title + " " + chunk.content).lower()
            match_count = sum(1 for w in words if w in combined)
            score = match_count / max(1, len(words))
            scored.append((chunk, float(score)))

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:top_k]
