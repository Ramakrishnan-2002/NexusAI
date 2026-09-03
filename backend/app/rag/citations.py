from typing import Any, Dict, List, Optional
from app.schemas.ai import Citation
from app.schemas.search import SearchResultItem


class CitationBuilder:
    """
    Builds and verifies citation objects linking LLM claims directly
    to source Wikipedia articles, revision IDs, and timestamps.
    """

    @staticmethod
    def build_citations_from_results(results: List[SearchResultItem]) -> List[Citation]:
        citations = []
        for item in results:
            citations.append(
                Citation(
                    article_title=item.article_title,
                    revision_id=item.revision_id,
                    occurred_at=item.occurred_at.isoformat() if item.occurred_at else None,
                    snippet=item.content[:200] + "..." if len(item.content) > 200 else item.content,
                    relevance_reason=f"Relevance Score: {item.score:.2f}",
                )
            )
        return citations
