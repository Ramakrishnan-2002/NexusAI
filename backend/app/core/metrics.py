import time
from typing import Any, Dict
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

# Prometheus Metrics Definitions
EVENTS_INGESTED = Counter(
    "wikipulse_events_ingested_total",
    "Total raw Wikimedia events received by ingestor",
    ["source", "status"]
)

EVENTS_PROCESSED = Counter(
    "wikipulse_events_processed_total",
    "Total events processed and persisted by processor worker",
    ["wiki", "namespace"]
)

TRENDS_DETECTED = Counter(
    "wikipulse_trends_detected_total",
    "Total unusual activity spikes and trends identified",
    ["topic"]
)

KAFKA_CONSUMER_LAG = Gauge(
    "wikipulse_kafka_consumer_lag",
    "Estimated Kafka consumer lag per partition/group",
    ["consumer_group", "topic"]
)

API_REQUEST_DURATION = Histogram(
    "wikipulse_api_request_duration_seconds",
    "HTTP API request duration in seconds",
    ["endpoint", "method", "status_code"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

RAG_RETRIEVAL_DURATION = Histogram(
    "wikipulse_rag_retrieval_duration_seconds",
    "RAG search and retrieval duration in seconds",
    ["search_type"]
)

LLM_REQUEST_DURATION = Histogram(
    "wikipulse_llm_request_duration_seconds",
    "LLM invocation latency in seconds",
    ["provider", "model"]
)

LLM_FAILURES = Counter(
    "wikipulse_llm_failures_total",
    "Total LLM invocation failures",
    ["provider", "error_type"]
)

LLM_FALLBACKS = Counter(
    "wikipulse_llm_fallbacks_total",
    "Total LLM fallback triggers",
    ["from_provider", "to_provider"]
)


class SystemMetricsCollector:
    """
    In-process thread-safe metrics collector for real-time dashboard and health queries.
    Captures live counters, processing rates, and p95 latencies.
    """
    def __init__(self):
        self.start_time = time.time()
        self.ingested_count = 0
        self.processed_count = 0
        self.trend_count = 0
        self.dlq_count = 0
        self.db_pool_in_use = 0
        self.db_pool_size = 20
        self.redis_hits = 0
        self.redis_misses = 0
        self.llm_calls = 0
        self.llm_fallbacks = 0
        self.recent_latencies = []

    def record_ingested(self, count: int = 1):
        self.ingested_count += count
        EVENTS_INGESTED.labels(source="stream", status="success").inc(count)

    def record_processed(self, wiki: str = "enwiki", namespace: str = "0"):
        self.processed_count += 1
        EVENTS_PROCESSED.labels(wiki=wiki, namespace=namespace).inc()

    def record_trend(self, topic: str = "general"):
        self.trend_count += 1
        TRENDS_DETECTED.labels(topic=topic).inc()

    def record_dlq(self):
        self.dlq_count += 1

    def record_redis_access(self, hit: bool):
        if hit:
            self.redis_hits += 1
        else:
            self.redis_misses += 1

    def record_api_latency(self, latency_ms: float):
        self.recent_latencies.append(latency_ms)
        if len(self.recent_latencies) > 1000:
            self.recent_latencies.pop(0)

    def get_snapshot(self) -> Dict[str, Any]:
        uptime_seconds = max(1.0, time.time() - self.start_time)
        events_per_sec = round(self.processed_count / uptime_seconds, 2)
        total_cache = self.redis_hits + self.redis_misses
        cache_hit_ratio = round((self.redis_hits / total_cache) * 100, 1) if total_cache > 0 else 100.0

        p95_latency = 0.0
        if self.recent_latencies:
            sorted_lat = sorted(self.recent_latencies)
            p95_idx = int(len(sorted_lat) * 0.95)
            p95_latency = round(sorted_lat[min(p95_idx, len(sorted_lat) - 1)], 2)

        return {
            "uptime_seconds": int(uptime_seconds),
            "ingested_count": self.ingested_count,
            "processed_count": self.processed_count,
            "trend_count": self.trend_count,
            "dlq_count": self.dlq_count,
            "events_per_sec": events_per_sec,
            "redis_hit_ratio_percent": cache_hit_ratio,
            "db_pool_in_use": self.db_pool_in_use,
            "db_pool_size": self.db_pool_size,
            "llm_total_calls": self.llm_calls,
            "llm_fallbacks": self.llm_fallbacks,
            "api_p95_latency_ms": p95_latency,
        }


metrics_collector = SystemMetricsCollector()
