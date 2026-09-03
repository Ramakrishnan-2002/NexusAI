from datetime import datetime, timezone
from typing import List, Optional
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from app.models.article import Article
from app.models.edit import Edit
from app.models.editor import Editor


class ArticleRepository:
    """Repository for Article and Editor persistence and queries"""

    @staticmethod
    async def get_or_create_article(
        session: AsyncSession,
        title: str,
        wiki: str = "enwiki",
        namespace: int = 0,
        page_id: Optional[int] = None,
        url: Optional[str] = None,
    ) -> Article:
        stmt = select(Article).where(Article.wiki == wiki, Article.title == title)
        result = await session.execute(stmt)
        article = result.scalar_one_or_none()

        if article is None:
            article = Article(
                title=title,
                wiki=wiki,
                namespace=namespace,
                page_id=page_id,
                url=url,
            )
            session.add(article)
            await session.flush()
        else:
            article.updated_at = datetime.now(timezone.utc)
            if url and not article.url:
                article.url = url
            await session.flush()

        return article

    @staticmethod
    async def get_or_create_editor(
        session: AsyncSession,
        username: str,
        is_bot: bool = False,
    ) -> Editor:
        stmt = select(Editor).where(Editor.username == username)
        result = await session.execute(stmt)
        editor = result.scalar_one_or_none()

        if editor is None:
            editor = Editor(
                username=username,
                is_bot=is_bot,
                edit_count=1,
            )
            session.add(editor)
            await session.flush()
        else:
            editor.edit_count += 1
            editor.last_seen = datetime.now(timezone.utc)
            await session.flush()

        return editor

    @staticmethod
    async def get_article_by_id(session: AsyncSession, article_id: int) -> Optional[Article]:
        stmt = (
            select(Article)
            .options(selectinload(Article.edits))
            .where(Article.id == article_id)
        )
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    @staticmethod
    async def list_articles(
        session: AsyncSession,
        skip: int = 0,
        limit: int = 20,
        wiki: Optional[str] = None,
    ) -> List[Article]:
        stmt = select(Article).order_by(Article.updated_at.desc())
        if wiki:
            stmt = stmt.where(Article.wiki == wiki)
        stmt = stmt.offset(skip).limit(limit)
        result = await session.execute(stmt)
        return list(result.scalars().all())
