from fastapi import APIRouter, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import DB
from app.search.hybrid import HybridSearchService
from app.schemas.search import SearchRequest, SearchResponse

router = APIRouter(prefix="/search", tags=["Hybrid Knowledge Search"])


@router.post("", response_model=SearchResponse)
async def hybrid_search_post(
    body: SearchRequest,
    db: AsyncSession = DB,
):
    """
    Execute hybrid semantic vector + full-text search with reranking.
    """
    return await HybridSearchService.search(
        session=db,
        query=body.query,
        search_type=body.search_type,
        top_k=body.top_k,
    )


@router.get("", response_model=SearchResponse)
async def hybrid_search_get(
    q: str = Query(..., min_length=1, max_length=500, description="Search query"),
    type: str = Query("hybrid", description="'hybrid', 'vector', or 'keyword'"),
    top_k: int = Query(10, ge=1, le=50),
    db: AsyncSession = DB,
):
    """Convenience GET endpoint for hybrid search."""
    return await HybridSearchService.search(
        session=db,
        query=q,
        search_type=type,
        top_k=top_k,
    )
