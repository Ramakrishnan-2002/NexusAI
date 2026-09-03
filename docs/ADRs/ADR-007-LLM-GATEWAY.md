# ADR-007: Multi-Provider LLM Gateway with Cascading Fallback

## Status
**ACCEPTED**

## Context
Relying on a single external LLM provider introduces critical single-point-of-failure risks (API outages, rate-limiting, and network transit latencies). Local-only models require dedicated GPU hardware that may not be available across all deployment targets.

## Decision
Implement a unified `LLMGateway` with **automated cascading provider fallback**:
1. **Primary:** Google Gemini Flash (cloud frontier model, 10s timeout).
2. **Secondary:** Local Ollama `llama3.2:1b` (self-hosted local CPU/GPU inference, 15s timeout).
3. **Tertiary:** Deterministic Mock Provider (0.20ms, offline development and CI/CD testing).

All providers adhere strictly to the `AIAnalysisOutput` Pydantic schema and enforce citation attribution.

## Alternatives Considered
1. **Direct Cloud-Only SDK (google-genai):** Breaks in offline development environments, unit tests, and during external API outages.
2. **Local-Only Model (Ollama-Only):** Bounded by host CPU capabilities without GPU acceleration.

## Consequences
- **Positive:** High availability, seamless offline local development, and zero vendor lock-in.
- **Tradeoff:** Fallback models produce varying depths of contextual synthesis compared to frontier models.
