import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
import numpy as np

# Ensure backend and workers are in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "backend")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

BENCH_DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "bench_wikipulse.db"))
BENCH_DB_URL = f"sqlite+aiosqlite:///{BENCH_DB_PATH}"

os.environ["DATABASE_URL"] = BENCH_DB_URL
os.environ["ENVIRONMENT"] = "testing"
os.environ["DEFAULT_LLM_PROVIDER"] = "mock"
os.environ["KAFKA_BOOTSTRAP_SERVERS"] = ""

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
import app.db.session as db_mod

bench_engine = create_async_engine(BENCH_DB_URL, echo=False)
BenchSession = async_sessionmaker(bind=bench_engine, class_=AsyncSession, expire_on_commit=False)
db_mod.engine = bench_engine
db_mod.AsyncSessionLocal = BenchSession

from app.models.base import Base
from app.models.article import Article
from app.models.knowledge_chunk import KnowledgeChunk
from app.search.embeddings import embedding_service
from app.search.hybrid import HybridSearchService
from app.search.vector import VectorSearchService
from app.search.fts import FullTextSearchService
from app.llm.gateway import llm_gateway
from app.schemas.event import IngestedEvent
from app.kafka.producer import event_producer
from workers.processor.processor import EventProcessorWorker
from workers.stream_ingestor.synthetic_generator import SyntheticWikimediaGenerator


async def run_benchmarks():
    print("================================================================================")
    print("                WIKIPULSE PERFORMANCE & LATENCY BENCHMARKS                     ")
    print("================================================================================")
    now_iso = datetime.now(timezone.utc).isoformat()
    print(f"Timestamp: {now_iso}")
    print(f"Python: {sys.version.split()[0]} | Platform: {sys.platform}")
    print("--------------------------------------------------------------------------------\n")

    # Force producer to in-memory mode for benchmark test isolation
    event_producer._use_fallback = True
    event_producer._started = True

    results_data = {
        "timestamp": now_iso,
        "platform": sys.platform,
        "python_version": sys.version.split()[0],
        "metrics": {},
    }

    async with bench_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    # 1. Benchmark Knowledge Ingestion & Vectorization
    print(">>> 1. BENCHMARKING KNOWLEDGE INGESTION & VECTOR INDEXING")
    sample_texts = [
        "NASA James Webb Space Telescope detected atmospheric methane on habitable-zone exoplanet.",
        "Superconductivity researchers reproduced ambient pressure zero-resistance anomalies.",
        "Global semiconductor manufacturing consortium announced sub-2nm node mass production timeline.",
        "Quantum computing error-correction threshold surpassed using surface-code lattice.",
        "International renewable energy grid capacity reached historic terawatt milestone.",
    ]

    async with BenchSession() as session:
        art = Article(title="Benchmark Knowledge Dataset", wiki="enwiki", namespace=0)
        session.add(art)
        await session.flush()

        t0 = time.time()
        num_chunks = 100
        for i in range(num_chunks):
            txt = sample_texts[i % len(sample_texts)] + f" (Variant #{i})"
            vec = embedding_service.get_embedding(txt)
            chunk = KnowledgeChunk(
                article_id=art.id,
                article_title=art.title,
                title=f"Chunk {i}",
                content=txt,
                embedding=vec,
                occurred_at=datetime.now(timezone.utc),
            )
            session.add(chunk)
        await session.commit()
        t_ingest = time.time() - t0
        ingest_rate = round(num_chunks / t_ingest, 2)
        print(f"  - Indexed {num_chunks} vector chunks in {t_ingest*1000:.1f}ms ({ingest_rate} chunks/sec)")

        results_data["metrics"]["vector_indexing"] = {
            "chunks_count": num_chunks,
            "total_ms": round(t_ingest * 1000, 2),
            "throughput_chunks_per_sec": ingest_rate,
        }

    # 2. Benchmark Retrieval: Vector vs FTS vs Hybrid
    print("\n>>> 2. BENCHMARKING RETRIEVAL LATENCIES (100 Iterations)")
    queries = [
        "exoplanet atmospheric methane Webb telescope",
        "superconductivity ambient pressure zero resistance",
        "semiconductor node fabrication capacity",
        "quantum error correction surface code",
    ]

    async with BenchSession() as session:
        # Vector Only
        latencies_vec = []
        for q in queries * 25:
            t_s = time.time()
            res = await VectorSearchService.search_similar(session, q, top_k=5)
            latencies_vec.append((time.time() - t_s) * 1000)

        # Keyword FTS Only
        latencies_fts = []
        for q in queries * 25:
            t_s = time.time()
            res = await FullTextSearchService.search_keywords(session, q, top_k=5)
            latencies_fts.append((time.time() - t_s) * 1000)

        # Hybrid Retrieval + RRF + Reranker
        latencies_hybrid = []
        for q in queries * 25:
            t_s = time.time()
            res = await HybridSearchService.search(session, q, search_type="hybrid", top_k=5)
            latencies_hybrid.append((time.time() - t_s) * 1000)

    print(f"  - Vector-Only Retrieval : Avg = {np.mean(latencies_vec):.2f}ms | p95 = {np.percentile(latencies_vec, 95):.2f}ms | p99 = {np.percentile(latencies_vec, 99):.2f}ms")
    print(f"  - Keyword-Only FTS      : Avg = {np.mean(latencies_fts):.2f}ms | p95 = {np.percentile(latencies_fts, 95):.2f}ms | p99 = {np.percentile(latencies_fts, 99):.2f}ms")
    print(f"  - Hybrid Search + RRF   : Avg = {np.mean(latencies_hybrid):.2f}ms | p95 = {np.percentile(latencies_hybrid, 95):.2f}ms | p99 = {np.percentile(latencies_hybrid, 99):.2f}ms")

    results_data["metrics"]["retrieval_latencies_ms"] = {
        "vector_only": {"avg": round(float(np.mean(latencies_vec)), 2), "p95": round(float(np.percentile(latencies_vec, 95)), 2), "p99": round(float(np.percentile(latencies_vec, 99)), 2)},
        "fts_keyword_only": {"avg": round(float(np.mean(latencies_fts)), 2), "p95": round(float(np.percentile(latencies_fts, 95)), 2), "p99": round(float(np.percentile(latencies_fts, 99)), 2)},
        "hybrid_rrf_search": {"avg": round(float(np.mean(latencies_hybrid)), 2), "p95": round(float(np.percentile(latencies_hybrid, 95)), 2), "p99": round(float(np.percentile(latencies_hybrid, 99)), 2)},
    }

    # 3. Benchmark Worker Pipeline Throughput
    print("\n>>> 3. BENCHMARKING PROCESSOR WORKER THROUGHPUT")
    processor = EventProcessorWorker()
    generator = SyntheticWikimediaGenerator(events_per_second=100)
    gen_stream = generator.event_stream()

    num_events = 50
    events_to_process = []
    for _ in range(num_events):
        raw = await gen_stream.__anext__()
        evt = IngestedEvent(
            event_id=f"bench-evt-{_}",
            occurred_at=datetime.now(timezone.utc),
            article_title=raw["title"],
            editor_username=raw["user"],
            change_size=raw["length"]["new"],
            byte_diff=raw["length"]["new"] - raw["length"]["old"],
            comment=raw["comment"],
        )
        events_to_process.append(evt.model_dump(mode="json"))

    t_worker_start = time.time()
    for evt_data in events_to_process:
        await processor.handle_event(evt_data)
    t_worker_total = time.time() - t_worker_start
    throughput = round(num_events / t_worker_total, 2)
    print(f"  - Processed {num_events} events in {t_worker_total*1000:.1f}ms")
    print(f"  - Worker End-to-End Throughput: {throughput} events/sec")

    results_data["metrics"]["worker_throughput"] = {
        "events_processed": num_events,
        "total_ms": round(t_worker_total * 1000, 2),
        "events_per_sec": throughput,
    }

    # 4. Benchmark LLM Gateway
    print("\n>>> 4. BENCHMARKING LLM GATEWAY INVOCATION")
    llm_latencies = []
    for _ in range(5):
        t_llm = time.time()
        analysis, provider, model, was_fb, took_ms = await llm_gateway.analyze_structured(
            system_prompt="You are WikiPulse Assistant.",
            user_prompt="Article: Benchmark AI\nContent: Major throughput milestone achieved.",
            preferred_provider="mock",
        )
        llm_latencies.append((time.time() - t_llm) * 1000)

    print(f"  - LLM Gateway Provider: {provider} ({model})")
    print(f"  - LLM Invocation Latency: Avg = {np.mean(llm_latencies):.2f}ms | Min = {min(llm_latencies):.2f}ms | Max = {max(llm_latencies):.2f}ms")

    results_data["metrics"]["llm_gateway"] = {
        "provider": provider,
        "model": model,
        "avg_ms": round(float(np.mean(llm_latencies)), 2),
        "min_ms": round(float(min(llm_latencies)), 2),
        "max_ms": round(float(max(llm_latencies)), 2),
    }

    # Save to benchmarks/results.json
    bench_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "benchmarks"))
    os.makedirs(bench_dir, exist_ok=True)
    results_path = os.path.join(bench_dir, "results.json")
    with open(results_path, "w") as f:
        json.dump(results_data, f, indent=2)

    print(f"\n[+] Saved machine-readable results to {results_path}")
    print("================================================================================")
    print("                        BENCHMARK SUMMARY COMPLETE                              ")
    print("================================================================================")

    if os.path.exists(BENCH_DB_PATH):
        try:
            os.remove(BENCH_DB_PATH)
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(run_benchmarks())
