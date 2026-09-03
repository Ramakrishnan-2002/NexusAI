from typing import List, Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="allow",
    )

    # Application
    PROJECT_NAME: str = "WikiPulse"
    ENVIRONMENT: str = "development"
    DEBUG: bool = True
    LOG_LEVEL: str = "INFO"
    API_V1_STR: str = "/api/v1"
    SECRET_KEY: str = "wikipulse_insecure_development_secret_key_change_in_production"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440

    # Database
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/wikipulse"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_DB: str = "wikipulse"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    DB_POOL_SIZE: int = 20
    DB_MAX_OVERFLOW: int = 10

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_CACHE_TTL_SECONDS: int = 300
    REDIS_HOT_COUNTER_WINDOW_SECONDS: int = 900

    # Apache Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_CONSUMER_GROUP_PREFIX: str = "wikipulse"
    KAFKA_AUTO_OFFSET_RESET: str = "latest"
    KAFKA_ENABLE_AUTO_COMMIT: bool = False
    KAFKA_MAX_POLL_INTERVAL_MS: int = 300000
    KAFKA_RETRIES: int = 3
    KAFKA_RETRY_BACKOFF_MS: int = 1000

    # Topics
    TOPIC_RECENT_CHANGE: str = "wikimedia.recentchange"
    TOPIC_ARTICLE_PROCESSED: str = "wikimedia.article.processed"
    TOPIC_TREND_DETECTED: str = "wikimedia.trend.detected"
    TOPIC_EMBEDDING_CREATED: str = "wikimedia.embedding.created"
    TOPIC_ANALYSIS_COMPLETED: str = "wikimedia.analysis.completed"
    TOPIC_DLQ: str = "wikimedia.dlq"

    # Ingestion Stream Settings
    STREAM_MODE: str = "synthetic"  # "wikimedia" or "synthetic"
    WIKIMEDIA_STREAM_URL: str = "https://stream.wikimedia.org/v2/stream/recentchange"
    WIKIMEDIA_USER_AGENT: str = "WikiPulse/1.0 (https://github.com/wikipulse/wikipulse; contact@wikipulse.local)"
    SYNTHETIC_EVENTS_PER_SECOND: int = 10
    FILTER_BOTS: bool = False
    FILTER_MIN_BYTE_DIFF: int = 10

    # AI / LLM Configuration
    DEFAULT_LLM_PROVIDER: str = "mock"  # "gemini", "ollama", "mock"
    LLM_FALLBACK_CHAIN: str = "gemini,ollama,mock"
    GEMINI_API_KEY: Optional[str] = ""
    GEMINI_MODEL: str = "gemini-1.5-flash"
    GEMINI_TIMEOUT_SECONDS: int = 15
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "llama3.2:1b"
    OLLAMA_EMBED_MODEL: str = "all-minilm"
    OLLAMA_TIMEOUT_SECONDS: int = 30

    # Embedding & Search
    EMBEDDING_DIMENSION: int = 384
    EMBEDDING_MODEL: str = "sentence-transformers/all-MiniLM-L6-v2"
    HYBRID_SEARCH_TOP_K: int = 10
    HYBRID_SEARCH_VECTOR_WEIGHT: float = 0.6
    HYBRID_SEARCH_FTS_WEIGHT: float = 0.4
    RERANK_CANDIDATE_COUNT: int = 20

    # Trend Detection Thresholds
    TREND_SPIKE_MULTIPLIER_THRESHOLD: float = 3.0
    TREND_MIN_EDITS_THRESHOLD: int = 5
    TREND_WINDOW_1M_WEIGHT: float = 0.5
    TREND_WINDOW_5M_WEIGHT: float = 0.3
    TREND_WINDOW_15M_WEIGHT: float = 0.2

    @property
    def fallback_chain_list(self) -> List[str]:
        return [p.strip() for p in self.LLM_FALLBACK_CHAIN.split(",") if p.strip()]


settings = Settings()
