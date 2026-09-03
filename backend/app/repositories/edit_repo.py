from typing import Any, Dict, List, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.article import Article
from app.models.edit import Edit
from app.schemas.event import IngestedEvent


class EditRepository:
    """Repository for Wikipedia revision edits persistence and history queries"""

    @staticmethod
    async def create_edit(session: AsyncSession, event: IngestedEvent, article_id: int) -> Edit:
        # Check if already exists (idempotency)
        stmt = select(Edit).where(Edit.event_id == event.event_id)
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()
        if existing:
            return existing

        edit = Edit(
            event_id=event.event_id,
            article_id=article_id,
            editor_username=event.editor_username,
            is_bot=event.is_bot,
            is_minor=event.is_minor,
            revision_id=event.revision_id,
            parent_revision_id=event.parent_revision_id,
            change_size=event.change_size,
            byte_diff=event.byte_diff,
            comment=event.comment,
            occurred_at=event.occurred_at,
        )
        session.add(edit)
        await session.flush()
        return edit

    @staticmethod
    async def get_recent_edits_for_article(
        session: AsyncSession,
        article_id: int,
        limit: int = 50,
    ) -> List[Edit]:
        stmt = (
            select(Edit)
            .where(Edit.article_id == article_id)
            .order_by(Edit.occurred_at.desc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    @staticmethod
    async def get_recent_edits_global(
        session: AsyncSession,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        stmt = (
            select(Edit, Article.title)
            .outerjoin(Article, Edit.article_id == Article.id)
            .order_by(Edit.occurred_at.desc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        edits = []
        for edit, title in result.all():
            edits.append({
                "id": edit.id,
                "event_id": edit.event_id,
                "article_title": title or "Wikipedia Article",
                "editor_username": edit.editor_username,
                "is_bot": edit.is_bot,
                "is_minor": edit.is_minor,
                "revision_id": edit.revision_id,
                "change_size": edit.change_size,
                "byte_diff": edit.byte_diff,
                "comment": edit.comment,
                "occurred_at": edit.occurred_at,
            })
        return edits
