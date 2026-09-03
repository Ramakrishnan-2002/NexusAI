from datetime import datetime, timezone
import pytest
from app.models.article import Article
from app.models.edit import Edit
from app.models.trend import TrendEvent


@pytest.mark.asyncio
async def test_health_and_readiness_endpoints(client):
    # Liveness
    resp = await client.get("/api/v1/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert "version" in data

    # Readiness
    resp_ready = await client.get("/api/v1/ready")
    assert resp_ready.status_code == 200
    ready_data = resp_ready.json()
    assert ready_data["ready"] is True


@pytest.mark.asyncio
async def test_articles_and_events_api(client, db_session):
    # Seed article and edit
    article = Article(title="Quantum Teleportation", wiki="enwiki", namespace=0)
    db_session.add(article)
    await db_session.flush()

    edit = Edit(
        event_id="test-evt-001",
        article_id=article.id,
        editor_username="Physicist99",
        is_bot=False,
        is_minor=False,
        change_size=15000,
        byte_diff=250,
        comment="Added photon entanglement section",
        occurred_at=datetime.now(timezone.utc),
    )
    db_session.add(edit)
    await db_session.commit()

    # List articles
    art_resp = await client.get("/api/v1/articles")
    assert art_resp.status_code == 200
    articles = art_resp.json()
    assert len(articles) >= 1
    assert articles[0]["title"] == "Quantum Teleportation"

    # Get article detail
    detail_resp = await client.get(f"/api/v1/articles/{article.id}")
    assert detail_resp.status_code == 200
    detail = detail_resp.json()
    assert detail["id"] == article.id
    assert len(detail["edits"]) == 1

    # List events
    evt_resp = await client.get("/api/v1/events")
    assert evt_resp.status_code == 200
    events = evt_resp.json()
    assert len(events) >= 1
    assert events[0]["event_id"] == "test-evt-001"


@pytest.mark.asyncio
async def test_trends_and_analysis_api(client, db_session):
    article = Article(title="Superconductivity Lab", wiki="enwiki", namespace=0)
    db_session.add(article)
    await db_session.flush()

    trend = TrendEvent(
        article_id=article.id,
        article_title="Superconductivity Lab",
        topic="Physics",
        activity_score=15.5,
        edits_per_minute=6.2,
        baseline_velocity=0.5,
        spike_multiplier=12.4,
        unique_editors=5,
        total_byte_delta=1200,
        status="active",
        first_detected_at=datetime.now(timezone.utc),
    )
    db_session.add(trend)
    await db_session.commit()

    # List trends
    trend_resp = await client.get("/api/v1/trends")
    assert trend_resp.status_code == 200
    trends_data = trend_resp.json()
    assert trends_data["total"] >= 1
    assert trends_data["trends"][0]["article_title"] == "Superconductivity Lab"

    # Analyze trend
    analysis_resp = await client.post(f"/api/v1/trends/{trend.id}/analyze")
    assert analysis_resp.status_code == 200
    analysis = analysis_resp.json()
    assert "summary" in analysis
    assert analysis["confidence"] > 0


@pytest.mark.asyncio
async def test_rag_ai_ask_api(client):
    req_body = {
        "question": "What unusual activity happened around quantum teleportation?",
        "top_k_evidence": 5,
        "provider_override": "mock",
    }
    resp = await client.post("/api/v1/ai/ask", json=req_body)
    assert resp.status_code == 200
    data = resp.json()
    assert data["provider_used"] == "mock"
    assert "answer" in data
    assert "structured_analysis" in data
