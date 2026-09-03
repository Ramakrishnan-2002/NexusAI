from fastapi import APIRouter
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.deps import DB
from app.services.ai_service import AIService
from app.schemas.ai import AIAskRequest, AIAskResponse

router = APIRouter(prefix="/ai", tags=["AI & RAG Intelligence"])


@router.post("/ask", response_model=AIAskResponse)
async def ask_knowledge_base(
    body: AIAskRequest,
    db: AsyncSession = DB,
):
    """
    Ask a natural language question about recent Wikipedia edits and trends.
    Uses Hybrid Search retrieval + RAG context builder + LLM Gateway.
    Returns factual answer with exact citations and confidence rating.
    """
    return await AIService.ask_question(db, body)
