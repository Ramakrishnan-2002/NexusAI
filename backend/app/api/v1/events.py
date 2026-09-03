from typing import List
from fastapi import APIRouter, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import DB
from app.repositories.edit_repo import EditRepository
from app.schemas.article import EditItem

router = APIRouter(prefix="/events", tags=["Recent Events"])


@router.get("", response_model=List[EditItem])
async def get_recent_events(
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = DB,
):
    """Retrieve the most recent Wikipedia edit events processed by the platform."""
    edits = await EditRepository.get_recent_edits_global(db, limit=limit)
    return [
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
