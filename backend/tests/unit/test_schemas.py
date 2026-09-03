from datetime import datetime, timezone
import pytest
from app.schemas.event import RawWikimediaEvent, IngestedEvent
from app.schemas.ai import AIAnalysisOutput, Citation


def test_raw_wikimedia_event_validation():
    payload = {
        "title": "Quantum Computing",
        "comment": "Added latest algorithmic complexity benchmarks.",
        "user": "AliceEngineer",
        "bot": False,
        "minor": False,
        "length": {"old": 1000, "new": 1400},
        "revision": {"old": 500, "new": 501},
        "wiki": "enwiki",
        "namespace": 0,
        "type": "edit",
    }
    raw = RawWikimediaEvent.model_validate(payload)
    assert raw.title == "Quantum Computing"
    assert raw.wiki == "enwiki"
    assert raw.user == "AliceEngineer"
    assert raw.length["new"] == 1400


def test_ingested_event_normalization():
    now = datetime.now(timezone.utc)
    event = IngestedEvent(
        event_id="evt-12345",
        occurred_at=now,
        article_title="James Webb Space Telescope",
        editor_username="AstroObserver",
        change_size=50000,
        byte_diff=450,
        comment="Updated infrared spectroscopy findings",
    )
    assert event.event_id == "evt-12345"
    assert event.is_bot is False
    assert event.byte_diff == 450
    assert event.schema_version == 1


def test_ai_structured_output_validation():
    citation = Citation(
        article_title="Fusion Power Research",
        revision_id=98765,
        snippet="Net energy gain sustained for 30 seconds in tokamak.",
    )
    analysis = AIAnalysisOutput(
        summary="Experimental breakthrough in magnetic confinement fusion.",
        importance="high",
        detected_topic="Physics",
        change_type="breaking_development",
        reasoning="Multiple independent edits confirming net energy gain reproduction.",
        confidence=0.92,
        evidence_points=["Tokamak run sustained for 30s", "Q-factor exceeded 1.25"],
        citations=[citation],
    )
    assert analysis.confidence == 0.92
    assert analysis.importance == "high"
    assert len(analysis.citations) == 1
    assert analysis.citations[0].revision_id == 98765
