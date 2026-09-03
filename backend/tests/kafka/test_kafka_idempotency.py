import asyncio
from datetime import datetime, timezone
import pytest
from sqlalchemy import select
from app.models.edit import Edit
from app.models.processing_job import ProcessingJob
from app.schemas.event import IngestedEvent
from workers.processor.processor import EventProcessorWorker


@pytest.mark.asyncio
async def test_processor_idempotency_duplicate_events(db_session):
    worker = EventProcessorWorker()
    now = datetime.now(timezone.utc)

    event_payload = IngestedEvent(
        event_id="unique-event-id-999",
        occurred_at=now,
        article_title="Mars Exploration Rover",
        editor_username="CuriosityFan",
        change_size=40000,
        byte_diff=150,
        comment="Updated rover wheel wear inspection logs",
    ).model_dump(mode="json")

    # First execution -> Should process and persist
    await worker.handle_event(event_payload)

    # Verify edit created
    stmt = select(Edit).where(Edit.event_id == "unique-event-id-999")
    result = await db_session.execute(stmt)
    edits = result.scalars().all()
    assert len(edits) == 1

    # Second execution with exact same event -> Should be skipped via idempotency check
    await worker.handle_event(event_payload)

    # Verify no duplicate edit was created
    result2 = await db_session.execute(stmt)
    edits2 = result2.scalars().all()
    assert len(edits2) == 1

    # Verify ProcessingJob status
    job_stmt = select(ProcessingJob).where(ProcessingJob.idempotency_key == "proc:unique-event-id-999")
    job_res = await db_session.execute(job_stmt)
    job = job_res.scalar_one_or_none()
    assert job is not None
    assert job.status == "completed"


@pytest.mark.asyncio
async def test_processor_concurrent_idempotency_race(db_session):
    """
    Verify concurrent duplicate processing race:
    50 concurrent worker executions for the same event_id.
    Exactly 1 record must be persisted; the other 49 must rollback safely via IntegrityError.
    """
    worker = EventProcessorWorker()
    now = datetime.now(timezone.utc)
    event_id = "concurrent-race-test-50"

    event_payload = IngestedEvent(
        event_id=event_id,
        occurred_at=now,
        article_title="James Webb Space Telescope",
        editor_username="AstroBot",
        change_size=52000,
        byte_diff=450,
        comment="Concurrent deduplication verification",
    ).model_dump(mode="json")

    # Launch 50 concurrent worker handle_event tasks
    tasks = [worker.handle_event(event_payload) for _ in range(50)]
    await asyncio.gather(*tasks)

    # Verify exactly 1 edit was persisted
    stmt = select(Edit).where(Edit.event_id == event_id)
    result = await db_session.execute(stmt)
    edits = result.scalars().all()
    assert len(edits) == 1

    # Verify exactly 1 ProcessingJob exists with status 'completed'
    job_stmt = select(ProcessingJob).where(ProcessingJob.idempotency_key == f"proc:{event_id}")
    job_res = await db_session.execute(job_stmt)
    jobs = job_res.scalars().all()
    assert len(jobs) == 1
    assert jobs[0].status == "completed"
