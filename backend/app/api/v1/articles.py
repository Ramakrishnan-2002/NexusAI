from typing import List
from fastapi import APIRouter, HTTPException, Path, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import DB
from app.services.article_service import ArticleService
from app.schemas.article import ArticleDetail, ArticleSummary

router = APIRouter(prefix="/articles", tags=["Articles"])


@router.get("", response_model=List[ArticleSummary])
async def list_articles(
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = DB,
):
    """List monitored Wikipedia articles sorted by recent activity."""
    return await ArticleService.list_articles(db, skip=skip, limit=limit)


@router.get("/{article_id}", response_model=ArticleDetail)
async def get_article_detail(
    article_id: int = Path(..., ge=1, description="Article ID"),
    db: AsyncSession = DB,
):
    """Get full article timeline, edit history, activity velocity, and editor stats."""
    article = await ArticleService.get_article_detail(db, article_id)
    if not article:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Article with ID {article_id} was not found.",
        )
    return article
