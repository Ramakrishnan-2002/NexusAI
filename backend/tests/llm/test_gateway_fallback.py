import pytest
from app.llm.gateway import llm_gateway
from app.schemas.ai import AIAnalysisOutput


@pytest.mark.asyncio
async def test_llm_gateway_mock_provider():
    system_prompt = "You are a knowledge assistant."
    user_prompt = "Article: Artificial Intelligence\nContent: Major benchmark improvements announced."

    result, provider, model, was_fallback, latency_ms = await llm_gateway.analyze_structured(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        preferred_provider="mock",
    )

    assert isinstance(result, AIAnalysisOutput)
    assert provider == "mock"
    assert "Artificial Intelligence" in result.summary
    assert result.confidence > 0.5
    assert latency_ms >= 0


@pytest.mark.asyncio
async def test_llm_gateway_cascading_fallback():
    # Attempting to call Gemini without API key should cascade to Mock provider
    system_prompt = "You are a knowledge assistant."
    user_prompt = "Article: Solar Energy Transition\nContent: Solar cell efficiency record broken."

    result, provider, model, was_fallback, latency_ms = await llm_gateway.analyze_structured(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        preferred_provider="gemini",
    )

    assert isinstance(result, AIAnalysisOutput)
    # Should safely complete with fallback provider
    assert provider in ("ollama", "mock")
    assert was_fallback is True
