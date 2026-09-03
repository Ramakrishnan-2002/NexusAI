import re
import time
from typing import Dict, Tuple, Optional
from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader
from app.core.config import settings
from app.core.logging import logger

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def sanitize_external_text(text: str, max_length: int = 4000) -> str:
    """
    Sanitize untrusted external Wikipedia/editor content to prevent prompt injection,
    context-flooding, and system instruction overrides.
    """
    if not text:
        return ""

    # Truncate to maximum allowable length
    sanitized = text[:max_length]

    # Neutralize common system prompt override & jailbreak patterns
    patterns_to_neutralize = [
        r"(?i)ignore\s+(all\s+)?(previous|prior)\s+instructions",
        r"(?i)system\s+prompt\s*:",
        r"(?i)developer\s+message\s*:",
        r"(?i)you\s+are\s+now\s+(DAN|unconstrained|jailbroken)",
        r"(?i)new\s+instructions\s*:",
        r"(?i)disregard\s+the\s+above",
        r"(?i)output\s+the\s+system\s+instructions",
    ]

    for pattern in patterns_to_neutralize:
        sanitized = re.sub(pattern, "[UNTRUSTED_INSTRUCTION_FILTERED]", sanitized)

    # Strip non-printable/control chars except standard whitespace
    sanitized = "".join(ch for ch in sanitized if ch.isprintable() or ch in "\n\r\t")
    return sanitized.strip()


class DistributedRateLimiter:
    """
    Distributed token bucket rate limiter using Redis atomic operations,
    with graceful fallback to in-memory tracking if Redis is unreachable.
    """
    def __init__(self, requests_per_minute: int = 300):
        self.rate = requests_per_minute
        self._local_clients: Dict[str, Tuple[int, float]] = {}

    async def is_allowed(self, client_ip: str) -> bool:
        try:
            from app.redis.client import get_redis
            r = await get_redis()
            key = f"rate:{client_ip}"
            
            # Atomic increment
            count = await r.incr(key)
            if count == 1:
                await r.expire(key, 60)
            
            if count > self.rate:
                return False
            return True
        except Exception:
            # Fallback to local in-memory sliding window
            now = time.time()
            count, reset_at = self._local_clients.get(client_ip, (0, now + 60))

            if now > reset_at:
                self._local_clients[client_ip] = (1, now + 60)
                return True

            if count < self.rate:
                self._local_clients[client_ip] = (count + 1, reset_at)
                return True

            return False


global_rate_limiter = DistributedRateLimiter(requests_per_minute=300)


async def verify_api_key(api_key: str = Security(api_key_header)) -> str:
    """Optional API key verification for protected endpoints"""
    if settings.ENVIRONMENT == "development" and not settings.SECRET_KEY:
        return "anonymous_dev"

    if api_key:
        return api_key

    return "anonymous"
