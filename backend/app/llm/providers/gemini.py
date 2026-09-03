import json
import re
from typing import Any, Dict, Optional
import httpx
from app.core.config import settings
from app.core.logging import logger
from app.llm.providers.base import BaseLLMProvider
from app.schemas.ai import AIAnalysisOutput


class GeminiProvider(BaseLLMProvider):
    """
    Google Gemini API Provider for high-complexity synthesis and deep reasoning.
    """
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        self.api_key = api_key if api_key is not None else settings.GEMINI_API_KEY
        self.model = model or settings.GEMINI_MODEL

    @property
    def provider_name(self) -> str:
        return "gemini"

    async def is_available(self) -> bool:
        return bool(self.api_key and len(self.api_key) > 5)

    async def generate_structured(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
        model_override: Optional[str] = None,
    ) -> AIAnalysisOutput:
        if not await self.is_available():
            raise RuntimeError("Gemini API key is not configured or invalid.")

        model = model_override or self.model
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={self.api_key}"

        payload = {
            "contents": [
                {
                    "parts": [
                        {"text": f"{system_prompt}\n\n{user_prompt}"}
                    ]
                }
            ],
            "generationConfig": {
                "temperature": temperature,
                "response_mime_type": "application/json",
            }
        }

        async with httpx.AsyncClient(timeout=settings.GEMINI_TIMEOUT_SECONDS) as client:
            resp = await client.post(url, json=payload)
            if resp.status_code != 200:
                raise RuntimeError(f"Gemini API returned {resp.status_code}: {resp.text}")

            data = resp.json()
            try:
                content_text = data["candidates"][0]["content"]["parts"][0]["text"]
                # Clean any markdown json code blocks if returned
                clean_json = re.sub(r"^```json\s*", "", content_text.strip())
                clean_json = re.sub(r"\s*```$", "", clean_json.strip())
                parsed = json.loads(clean_json)
                return AIAnalysisOutput(**parsed)
            except Exception as parse_err:
                logger.error(f"Failed parsing Gemini JSON response: {parse_err}")
                raise RuntimeError(f"Invalid structured JSON from Gemini: {parse_err}")
