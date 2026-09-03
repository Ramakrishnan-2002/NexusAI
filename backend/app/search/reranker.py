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
            title_lower = (chunk.article_title or chunk.title or "").lower()
            content_words = set(re.findall(r"\w+", content_lower))
            title_words = set(re.findall(r"\w+", title_lower))

            # 1. Content word overlap score
            overlap = len(query_words.intersection(content_words)) / max(1, len(query_words))

            # 2. Title matching bonus (crucial for exact topic queries)
            title_overlap = len(query_words.intersection(title_words)) / max(1, len(query_words))
            title_bonus = 0.5 * title_overlap

            # 3. Exact phrase match bonus
            exact_bonus = 0.3 if (query_lower in content_lower or title_lower in query_lower) else 0.0

            # 4. Recency factor
            recency_bonus = 0.05

            # Combined weighted score
            final_score = (base_score * 0.4) + (overlap * 0.2) + title_bonus + exact_bonus + recency_bonus
            reranked.append((chunk, min(1.0, float(final_score))))

        # Sort by reranked score descending
        reranked.sort(key=lambda x: x[1], reverse=True)

        # Deduplicate identical chunks if present
        seen_chunks = set()
        deduped = []
        for chunk, score in reranked:
            if chunk.id not in seen_chunks:
                deduped.append((chunk, score))
                seen_chunks.add(chunk.id)

        return deduped[:top_k]
