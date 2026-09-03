from typing import Any, Dict, Optional
from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str = "1.0.0"
    timestamp: str


class ReadinessResponse(BaseModel):
    ready: bool
    components: Dict[str, bool]
    details: Dict[str, Any]
