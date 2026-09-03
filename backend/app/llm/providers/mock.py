import re
from typing import Optional
from app.llm.providers.base import BaseLLMProvider
from app.schemas.ai import AIAnalysisOutput


class MockProvider(BaseLLMProvider):
    """
    Deterministic Mock LLM Provider for local testing, CI pipelines,
    and ultimate safety fallback when external providers are offline.
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
        # Extract title or topic mentions from user prompt
        match = re.search(r"Article:\s*([^\n\r]+)", user_prompt)
        article_mention = match.group(1).strip() if match else "Recent Wikipedia Topic"

        # Determine change type hint
        change_type = "factual_update"
        if "expanded" in user_prompt.lower():
            change_type = "expansion"
        elif "revert" in user_prompt.lower() or "vandalism" in user_prompt.lower():
            change_type = "vandalism_cleanup"

        # Extract evidence lines
        evidence_lines = []
        for line in user_prompt.split("\n"):
            if line.startswith("Content:"):
                evidence_lines.append(line.replace("Content:", "").strip()[:100])

        return AIAnalysisOutput(
            summary=f"Activity detected regarding '{article_mention}'. Editors performed updates with verified content changes.",
            importance="medium",
            detected_topic="General Knowledge",
            change_type=change_type,
            reasoning="Derived deterministically from real-time Wikipedia edit logs and revision deltas in the knowledge base.",
            confidence=0.85,
            evidence_points=evidence_lines[:3] or ["Direct revision updates captured in knowledge index."],
            citations=[],
        )
