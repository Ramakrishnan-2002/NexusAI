from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from app.repositories.article_repo import ArticleRepository
from app.repositories.edit_repo import EditRepository
from app.schemas.article import ArticleDetail, ArticleSummary, EditItem
from app.redis.counters import ActivityCounterService


class ArticleService:
    """Service layer handling article timeline and detailed queries"""

    @staticmethod
    async def get_article_detail(session: AsyncSession, article_id: int) -> Optional[ArticleDetail]:
        article = await ArticleRepository.get_article_by_id(session, article_id)
        if not article:
            return None

        edits = await EditRepository.get_recent_edits_for_article(session, article_id, limit=30)
        edit_count, unique_editors = await ActivityCounterService.get_sliding_window_metrics(article_id, window_seconds=300)

        edit_items = [
            EditItem(
                id=e.id,
                event_id=e.event_id,
                editor_username=e.editor_username,
                is_bot=e.is_bot,
                is_minor=e.is_minor,
                revision_id=e.revision_id,
                change_size=e.change_size,
                byte_diff=e.byte_diff,
                comment=e.comment,
                occurred_at=e.occurred_at,
            )
            for e in edits
        ]

        return ArticleDetail(
            id=article.id,
            title=article.title,
            wiki=article.wiki,
            namespace=article.namespace,
            url=article.url,
            total_edits=len(article.edits) if article.edits else len(edits),
            created_at=article.created_at,
            updated_at=article.updated_at,
            recent_velocity=round(edit_count / 5.0, 2),  # edits/min
            unique_editors_count=unique_editors,
            edits=edit_items,
        )

    @staticmethod
    async def list_articles(session: AsyncSession, skip: int = 0, limit: int = 20) -> List[ArticleSummary]:
        articles = await ArticleRepository.list_articles(session, skip=skip, limit=limit)
        summaries = []
        for a in articles:
            summaries.append(
                ArticleSummary(
                    id=a.id,
                    title=a.title,
                    wiki=a.wiki,
                    namespace=a.namespace,
                    url=a.url,
                    total_edits=0,
                    created_at=a.created_at,
                    updated_at=a.updated_at,
                    recent_velocity=0.0,
                )
            )
        return summaries
