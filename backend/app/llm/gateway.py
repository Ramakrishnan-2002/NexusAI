import time
from typing import Dict, List, Optional, Tuple
from app.core.config import settings
from app.core.logging import logger
from app.core.metrics import LLM_FAILURES, LLM_FALLBACKS, LLM_REQUEST_DURATION, metrics_collector
from app.llm.providers.base import BaseLLMProvider
from app.llm.providers.gemini import GeminiProvider
from app.llm.providers.ollama import OllamaProvider
from app.llm.providers.mock import MockProvider
from app.llm.router import LLMRouter
from app.schemas.ai import AIAnalysisOutput


class LLMGateway:
    """
    Unified LLM Gateway providing:
      - Provider abstraction (Gemini, Ollama, Mock)
      - Policy-based routing
      - Automatic cascading fallback chain on provider failure/timeout
      - Strict structured Pydantic output validation
      - Latency & failure metrics observability
    """

    def __init__(self):
        self._providers: Dict[str, BaseLLMProvider] = {
            "gemini": GeminiProvider(),
            "ollama": OllamaProvider(),
            "mock": MockProvider(),
        }

    async def analyze_structured(
        self,
        system_prompt: str,
        user_prompt: str,
        task_type: str = "general",
        preferred_provider: Optional[str] = None,
        model_override: Optional[str] = None,
    ) -> Tuple[AIAnalysisOutput, str, str, bool, float]:
        """
        Executes structured LLM generation through the fallback chain.
        Returns (result, provider_name, model_name, was_fallback, latency_ms)
        """
        primary = LLMRouter.select_primary_provider(task_type, preferred_provider)
        chain = [primary] + [p for p in settings.fallback_chain_list if p != primary]

        start_time = time.time()
        was_fallback = False
        last_error = None

        for idx, provider_name in enumerate(chain):
            provider = self._providers.get(provider_name)
            if not provider:
                continue

            # Check availability
            is_avail = await provider.is_available()
            if not is_avail:
                logger.debug(f"Provider {provider_name} unavailable, skipping to next in fallback chain.")
                continue

            try:
                t0 = time.time()
                result = await provider.generate_structured(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    model_override=model_override,
                )
                duration = time.time() - t0
                total_latency_ms = round((time.time() - start_time) * 1000, 2)

                model_name = model_override or getattr(provider, "model", provider_name)
                LLM_REQUEST_DURATION.labels(provider=provider_name, model=model_name).observe(duration)
                metrics_collector.llm_calls += 1

                if idx > 0:
                    was_fallback = True
                    metrics_collector.llm_fallbacks += 1
                    LLM_FALLBACKS.labels(from_provider=chain[0], to_provider=provider_name).inc()

                return result, provider_name, model_name, was_fallback, total_latency_ms

            except Exception as e:
                logger.warning(f"Provider {provider_name} failed: {e}")
                LLM_FAILURES.labels(provider=provider_name, error_type=type(e).__name__).inc()
                last_error = e

        # If all configured providers failed, use safe Mock fallback
        mock_provider = self._providers["mock"]
        result = await mock_provider.generate_structured(system_prompt, user_prompt)
        total_latency_ms = round((time.time() - start_time) * 1000, 2)
        return result, "mock", "deterministic-fallback", True, total_latency_ms


# Global singleton LLM Gateway
llm_gateway = LLMGateway()
