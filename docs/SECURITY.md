# NexusAI / WikiPulse — Security Architecture & Threat Mitigation

This document details the implemented security controls, prompt injection defenses, API protections, and recommended production hardening roadmap.

---

## 1. Implemented Security Controls

### 1.1 Layered Prompt-Injection Risk Mitigation
We do NOT claim that regex "solves prompt injection." NexusAI implements **layered risk mitigation**:
1. **Regex Pattern Neutralization:** `sanitize_external_text()` in [`backend/app/core/security.py`](file:///d:/NexusAI/backend/app/core/security.py) replaces override phrases (`IGNORE ALL PREVIOUS INSTRUCTIONS`, `You are now DAN`) with `[UNTRUSTED_INSTRUCTION_FILTERED]`.
2. **Context Boundary XML Fencing:** Retrieved chunks are framed inside explicit XML tags:
   ```xml
   <untrusted_wikipedia_content>
   [Chunk 1] Title: Quantum Computing | Content: ...
   </untrusted_wikipedia_content>
   ```
3. **Pydantic Schema Validation:** LLM responses are parsed and validated strictly into the `AIAnalysisOutput` schema, preventing arbitrary prompt instruction leakages.
4. **Insufficient Evidence Fallback:** If the retrieved chunks do not contain relevant facts for the query, the context builder instructs the model to return:
   > *"Insufficient evidence in current knowledge stream."*

### 1.2 API Security & Secret Management
- **Environment Separation:** Sensitive secrets are injected via environment variables (`.env`). No API keys or database passwords are hardcoded in source files.
- **SQL Injection Prevention:** All database operations utilize parameterized queries via SQLAlchemy 2.0 and `asyncpg` binary protocol execution.
- **Distributed Rate Limiting:** Redis token bucket rate limiting throttles API endpoints per client IP to prevent denial-of-service abuse.
- **CORS Middleware:** Configured in `backend/app/main.py` to restrict cross-origin access.

---

## 2. Production Security Hardening Roadmap

| Threat Vector | Implemented Mitigation | Recommended Future Production Hardening |
| :--- | :--- | :--- |
| **Kafka Wire Interception** | Local plaintext network in Docker | TLS mutual authentication (mTLS) & SASL/SCRAM |
| **API Abuse / DDoS** | Redis token bucket rate limiting | Cloudflare / AWS WAF edge rate limiting & DDoS shielding |
| **LLM Key Compromise** | Environment variables | AWS Secrets Manager / HashiCorp Vault with dynamic rotation |
| **Client Authentication** | Open API endpoints for demo | OAuth2 / OIDC with JWT bearer token validation |
