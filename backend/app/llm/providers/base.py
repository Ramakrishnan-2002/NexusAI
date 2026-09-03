from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from app.schemas.ai import AIAnalysisOutput


class BaseLLMProvider(ABC):
    """Abstract interface for LLM Providers (Gemini, Ollama, Mock)"""

    @property
    @abstractmethod
    def provider_name(self) -> str:
        pass

    @abstractmethod
    async def is_available(self) -> bool:
        """Check if provider endpoint and credentials are functioning"""
        pass

    @abstractmethod
    async def generate_structured(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
        model_override: Optional[str] = None,
    ) -> AIAnalysisOutput:
        """Generate validated structured JSON output conforming to AIAnalysisOutput"""
        pass
