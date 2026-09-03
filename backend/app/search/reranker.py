import re
from typing import Dict, List, Tuple
from app.models.knowledge_chunk import KnowledgeChunk


class CandidateReranker:
    """
    Reranks retrieved candidate chunks to prioritize high-relevance evidence,
    filter out noise, and ensure diverse, contextually grounded context for RAG.
    """

    @staticmethod
    def rerank(
        query: str,
        candidates: List[Tuple[KnowledgeChunk, float]],
        top_k: int = 5,
    ) -> List[Tuple[KnowledgeChunk, float]]:
        if not candidates:
            return []

        query_lower = query.lower()
        query_words = set(re.findall(r"\w+", query_lower))
        reranked = []

        for chunk, base_score in candidates:
            content_lower = (chunk.title + " " + chunk.content).lower()
            content_words = set(re.findall(r"\w+", content_lower))

            # 1. Word overlap score (Jaccard-like)
            overlap = len(query_words.intersection(content_words)) / max(1, len(query_words))

            # 2. Exact phrase match bonus
            exact_bonus = 0.2 if query_lower in content_lower else 0.0

            # 3. Recency factor (more recent changes slightly prioritized)
            recency_bonus = 0.05

            # Combined weighted score
            final_score = (base_score * 0.5) + (overlap * 0.3) + exact_bonus + recency_bonus
            reranked.append((chunk, min(1.0, float(final_score))))

        # Sort by reranked score descending
        reranked.sort(key=lambda x: x[1], reverse=True)

        # Deduplicate identical articles if multiple chunks exist
        seen_articles = set()
        deduped = []
        for chunk, score in reranked:
            if chunk.id not in seen_articles:
                deduped.append((chunk, score))
                seen_articles.add(chunk.id)

        return deduped[:top_k]
