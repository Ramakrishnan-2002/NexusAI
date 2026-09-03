import time
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from app.search.hybrid import HybridSearchService
from app.rag.context_builder import RAGContextBuilder
from app.rag.citations import CitationBuilder
from app.llm.gateway import llm_gateway
from app.schemas.ai import AIAskRequest, AIAskResponse, AIAnalysisOutput
from app.models.ai_analysis import AIAnalysis
from app.repositories.trend_repo import TrendRepository
from app.core.logging import logger


class AIService:
    """Service layer orchestrating RAG search, context construction, and LLM Gateway"""

    @staticmethod
    async def ask_question(session: AsyncSession, request: AIAskRequest) -> AIAskResponse:
        total_start = time.time()

        # 1. Retrieve top-K evidence chunks via Hybrid Search
        t_ret_start = time.time()
        search_res = await HybridSearchService.search(
            session=session,
            query=request.question,
            search_type="hybrid",
            top_k=request.top_k_evidence,
        )
        retrieval_took_ms = round((time.time() - t_ret_start) * 1000, 2)

        # 2. Build injection-safe RAG Context
        system_prompt, user_prompt = RAGContextBuilder.build_qa_context(
            query=request.question,
            evidence_items=search_res.results,
        )

        # 3. Invoke LLM Gateway with fallback chain
        analysis_out, provider_used, model_used, was_fallback, llm_took_ms = await llm_gateway.analyze_structured(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            task_type="complex_qa",
            preferred_provider=request.provider_override,
            model_override=request.model_override,
        )

        # 4. Attach verified source citations
        citations = CitationBuilder.build_citations_from_results(search_res.results)
        analysis_out.citations = citations

        total_took_ms = round((time.time() - total_start) * 1000, 2)

        return AIAskResponse(
            question=request.question,
            answer=analysis_out.summary,
            structured_analysis=analysis_out,
            citations=citations,
            provider_used=provider_used,
            model_used=model_used,
            was_fallback=was_fallback,
            retrieval_took_ms=retrieval_took_ms,
            llm_took_ms=llm_took_ms,
            total_took_ms=total_took_ms,
        )

    @staticmethod
    async def explain_trend(session: AsyncSession, trend_id: int) -> Optional[AIAnalysisOutput]:
        trend = await TrendRepository.get_trend_by_id(session, trend_id)
        if not trend:
            return None

        # Retrieve knowledge chunks for this article
        search_res = await HybridSearchService.search(
            session=session,
            query=trend.article_title,
            search_type="hybrid",
            top_k=5,
            article_id=trend.article_id,
        )

        system_prompt, user_prompt = RAGContextBuilder.build_qa_context(
            query=f"Explain why article '{trend.article_title}' is experiencing an activity spike (score {trend.activity_score:.1f}, {trend.edits_per_minute:.1f} edits/min).",
            evidence_items=search_res.results,
        )

        analysis_out, provider_used, model_used, was_fallback, latency_ms = await llm_gateway.analyze_structured(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            task_type="trend_explanation",
        )

        citations = CitationBuilder.build_citations_from_results(search_res.results)
        analysis_out.citations = citations

        # Persist analysis to database
        db_analysis = AIAnalysis(
            trend_id=trend.id,
            article_id=trend.article_id,
            title=f"Trend Analysis for {trend.article_title}",
            summary=analysis_out.summary,
            importance=analysis_out.importance,
            detected_topic=analysis_out.detected_topic,
            change_type=analysis_out.change_type,
            reasoning=analysis_out.reasoning,
            confidence=analysis_out.confidence,
            evidence_points=analysis_out.evidence_points,
            citations_json=[c.model_dump() for c in citations],
            provider=provider_used,
            model=model_used,
            was_fallback=was_fallback,
            latency_ms=latency_ms,
        )
        session.add(db_analysis)

        # Update trend record with summary
        trend.ai_summary = analysis_out.summary
        trend.ai_importance = analysis_out.importance
        await session.flush()

        return analysis_out
