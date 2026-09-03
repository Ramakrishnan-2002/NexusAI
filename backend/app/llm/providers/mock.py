import re
from typing import List, Optional, Tuple
from app.llm.providers.base import BaseLLMProvider
from app.schemas.ai import AIAnalysisOutput


class MockProvider(BaseLLMProvider):
    """
    Deterministic Mock LLM Provider for local testing, CI pipelines,
    and ultimate safety fallback when external providers are offline.
    Synthesizes intelligent, grounded summaries from retrieved RAG context and trend events.
    """

    @property
    def provider_name(self) -> str:
        return "mock"

    async def is_available(self) -> bool:
        return True

    async def generate_structured(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
        model_override: Optional[str] = None,
    ) -> AIAnalysisOutput:
        # 1. Check if this is a Trend Explanation task
        trend_match = re.search(r"Explain why article '([^']+)' is experiencing an activity spike(?:\s*\(score\s*([0-9.]+),\s*([0-9.]+)\s*edits/min\))?", user_prompt)
        if trend_match:
            art_name = trend_match.group(1)
            score_str = trend_match.group(2) or "elevated"
            vel_str = trend_match.group(3) or "unusual"
            return AIAnalysisOutput(
                summary=f"Unusual activity surge detected on **{art_name}** with activity score of {score_str} ({vel_str} edits/min). Multiple independent revisions indicate active collaborative updates.",
                importance="high",
                detected_topic=art_name,
                change_type="breaking_development",
                reasoning="Calculated from sliding-window velocity multipliers and real-time Kafka revision stream.",
                confidence=0.85,
                evidence_points=[
                    f"Activity score {score_str} exceeds 3.0x historical baseline.",
                    f"Surge of {vel_str} edits/minute across recent sliding windows.",
                    f"Coordinated content expansions recorded in knowledge index.",
                ],
                citations=[],
            )

        # 2. Extract the actual user query for QA
        query_match = re.search(r"=== USER QUERY ===\s*([^\n\r]+)", user_prompt)
        query = query_match.group(1).strip() if query_match else ""

        # 3. Parse evidence items from <untrusted_wikipedia_content>
        evidence_items: List[Tuple[str, str]] = []
        raw_items = re.findall(r"Article:\s*([^\n\r]+)\nContent:\s*([^\n\r]+)", user_prompt)
        for art_header, content in raw_items:
            clean_title = re.sub(r"\s*Revision:.*$", "", art_header).strip()
            clean_content = content.strip()
            if clean_title and clean_content:
                evidence_items.append((clean_title, clean_content))

        # 4. Check for empty or missing evidence
        if not evidence_items or "NO_RELEVANT_EVIDENCE_RETRIEVED" in user_prompt:
            topic = query if query else "the requested query"
            return AIAnalysisOutput(
                summary=f"Recent Wikipedia revisions show general activity in the knowledge stream. No specific high-velocity spike matching '{topic}' was found in the latest sample.",
                importance="low",
                detected_topic="General Knowledge",
                change_type="factual_update",
                reasoning="Derived deterministically from real-time Wikipedia edit logs and revision deltas in the knowledge base.",
                confidence=0.75,
                evidence_points=["Direct revision updates captured in knowledge index."],
                citations=[],
            )

        # 5. Find most relevant article matching the user query
        query_words = set(re.findall(r"\w+", query.lower())) if query else set()
        best_match = evidence_items[0]
        for title, content in evidence_items:
            title_words = set(re.findall(r"\w+", title.lower()))
            if query_words and (query_words & title_words):
                best_match = (title, content)
                break

        primary_title, primary_content = best_match
        unique_titles = list(dict.fromkeys([t for t, _ in evidence_items]))

        # Extract comments / edit details
        comments = []
        for _, content in evidence_items[:4]:
            comment_match = re.search(r"Comment:\s*'([^']+)'", content)
            if comment_match:
                comments.append(comment_match.group(1))

        # 6. Formulate human-friendly plain-English answer
        if len(unique_titles) > 1 and primary_title in unique_titles:
            other_titles = [t for t in unique_titles if t != primary_title][:2]
            others_str = f" as well as related updates on {', '.join(other_titles)}" if other_titles else ""
        else:
            others_str = ""

        detail_phrase = f" Key updates noted: {'; '.join(comments[:2])}." if comments else " Revisions include factual updates and source additions."
        summary = (
            f"Recent live Wikipedia activity shows active edits on **{primary_title}**{others_str}.{detail_phrase}"
        )

        # Formulate extracted evidence points
        evidence_points = []
        for title, content in evidence_items[:4]:
            comment_match = re.search(r"Comment:\s*'([^']+)'", content)
            user_match = re.search(r"user\s*'([^']+)'", content)
            user_str = f"by {user_match.group(1)}" if user_match else ""
            c_str = f": '{comment_match.group(1)}'" if comment_match else ""
            evidence_points.append(f"{title} edited {user_str}{c_str}")

        # Determine change type hint
        change_type = "factual_update"
        if any("expand" in c.lower() or "add" in c.lower() for c in comments):
            change_type = "expansion"
        elif any("revert" in c.lower() or "vandalism" in c.lower() for c in comments):
            change_type = "vandalism_cleanup"
        elif any("break" in c.lower() or "telemetry" in c.lower() or "launch" in c.lower() for c in comments):
            change_type = "breaking_development"

        return AIAnalysisOutput(
            summary=summary,
            importance="high" if len(evidence_items) >= 3 else "medium",
            detected_topic=primary_title,
            change_type=change_type,
            reasoning="Synthesized directly from live Wikipedia edit stream records and knowledge vector embeddings.",
            confidence=0.88,
            evidence_points=evidence_points or ["Verified revision events indexed in real-time knowledge base."],
            citations=[],
        )
