import json
import re
from typing import Any, Dict, Optional
import httpx
from app.core.config import settings
from app.core.logging import logger
from app.llm.providers.base import BaseLLMProvider
from app.schemas.ai import AIAnalysisOutput


class OllamaProvider(BaseLLMProvider):
    """
    Local Ollama Provider for privacy-preserving, zero-cloud-cost inference.
    """
    def __init__(self, base_url: Optional[str] = None, model: Optional[str] = None):
        self.base_url = (base_url or settings.OLLAMA_BASE_URL).rstrip("/")
        self.model = model or settings.OLLAMA_MODEL

    @property
    def provider_name(self) -> str:
        return "ollama"

    async def is_available(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                resp = await client.get(f"{self.base_url}/api/tags")
                return resp.status_code == 200
        except Exception:
            return False

    async def generate_structured(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
        model_override: Optional[str] = None,
    ) -> AIAnalysisOutput:
        model = model_override or self.model
        url = f"{self.base_url}/api/generate"

        prompt = f"{system_prompt}\n\n{user_prompt}"
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {
                "temperature": temperature,
            }
        }

        async with httpx.AsyncClient(timeout=settings.OLLAMA_TIMEOUT_SECONDS) as client:
            resp = await client.post(url, json=payload)
            if resp.status_code != 200:
                raise RuntimeError(f"Ollama returned {resp.status_code}: {resp.text}")

            data = resp.json()
            response_text = data.get("response", "{}")
            try:
                clean_json = re.sub(r"^```json\s*", "", response_text.strip())
                clean_json = re.sub(r"\s*```$", "", clean_json.strip())
                parsed = json.loads(clean_json)
                return AIAnalysisOutput(**parsed)
            except Exception as parse_err:
                logger.error(f"Failed parsing Ollama JSON response: {parse_err}")
                raise RuntimeError(f"Invalid structured JSON from Ollama: {parse_err}")
