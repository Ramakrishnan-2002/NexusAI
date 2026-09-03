from typing import Optional
from app.core.config import settings


class LLMRouter:
    """
    Policy-based router selecting appropriate LLM provider based on:
      - Task complexity (classification/lightweight vs deep synthesis)
      - Privacy & local execution constraints
      - Model availability
    """

    @staticmethod
    def select_primary_provider(
        task_type: str = "general",
        preferred_provider: Optional[str] = None,
    ) -> str:
        if preferred_provider:
            return preferred_provider

        # Rule 1: Fast classification / lightweight summarization prefers Ollama if available
        if task_type in ("classify", "simple_summary"):
            return "ollama"

        # Rule 2: Deep multi-document reasoning / QA defaults to config
        return settings.DEFAULT_LLM_PROVIDER
