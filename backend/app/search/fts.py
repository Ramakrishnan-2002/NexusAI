import re
from typing import List, Optional, Tuple
from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.knowledge_chunk import KnowledgeChunk
from app.core.logging import logger

CONVERSATIONAL_STOP_WORDS = {
    "what", "is", "the", "recent", "updates", "update", "occurred", "happened",
    "on", "in", "and", "or", "a", "an", "tell", "me", "about", "latest", "status",
    "trend", "trends", "regarding", "why", "who", "which", "where", "how", "changes",
    "happening", "did", "do", "does", "any", "some"
}


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

        # Extract core subject keywords by filtering out conversational question stop words
        words = cleaned_query.split()
        core_keywords = [w for w in words if w.lower() not in CONVERSATIONAL_STOP_WORDS]
        effective_query = " ".join(core_keywords) if core_keywords else cleaned_query

        is_postgres = "postgresql" in str(session.bind.url) if session.bind else False

        if is_postgres:
            try:
                # Format query tokens using websearch_to_tsquery or plainto_tsquery
                ts_vector_expr = func.to_tsvector(
                    "english",
                    func.coalesce(KnowledgeChunk.article_title, "") + " " +
                    KnowledgeChunk.title + " " +
                    KnowledgeChunk.content
                )
                ts_query_expr = func.plainto_tsquery("english", effective_query)

                fts_query = select(
                    KnowledgeChunk,
                    func.ts_rank_cd(ts_vector_expr, ts_query_expr).label("rank")
                ).where(
                    ts_vector_expr.op("@@")(ts_query_expr)
                )

                if article_id:
                    fts_query = fts_query.where(KnowledgeChunk.article_id == article_id)

                fts_query = fts_query.order_by(text("rank DESC")).limit(top_k)
                result = await session.execute(fts_query)
                rows = result.all()
                if rows:
                    return [(row[0], float(row[1])) for row in rows]
            except Exception as e:
                logger.warning(f"Native PostgreSQL FTS error: {e}; falling back to token ILIKE query.")

        # Fallback ILIKE / keyword matching with title matching priority
        filter_words = core_keywords if core_keywords else words
        conditions = []
        for word in filter_words[:5]:
            conditions.append(KnowledgeChunk.article_title.ilike(f"%{word}%"))
            conditions.append(KnowledgeChunk.title.ilike(f"%{word}%"))
            conditions.append(KnowledgeChunk.content.ilike(f"%{word}%"))

        stmt = select(KnowledgeChunk)
        if conditions:
            stmt = stmt.where(or_(*conditions))
        if article_id:
            stmt = stmt.where(KnowledgeChunk.article_id == article_id)

        stmt = stmt.order_by(KnowledgeChunk.occurred_at.desc()).limit(top_k * 3)
        result = await session.execute(stmt)
        chunks = result.scalars().all()

        scored = []
        for chunk in chunks:
            title_text = (chunk.article_title or chunk.title or "").lower()
            combined = (title_text + " " + chunk.content).lower()
            match_count = sum(1 for w in filter_words if w.lower() in combined)
            # Extra weight if matched in article title
            title_match = sum(2 for w in filter_words if w.lower() in title_text)
            score = (match_count + title_match) / max(1, len(filter_words) * 2)
            scored.append((chunk, float(score)))

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:top_k]
