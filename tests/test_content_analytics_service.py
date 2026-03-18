from datetime import datetime, timedelta, timezone

import pytest

from src.models import ContentPipeline, PipelineGenerationBackend, PipelinePublishMode
from src.services.content_analytics_service import ContentAnalyticsService


async def _create_pipeline(db, name: str) -> int:
    return await db.repos.content_pipelines.add(
        ContentPipeline(
            name=name,
            prompt_template=f"{name} prompt",
            llm_model="gpt-test",
            image_model=None,
            publish_mode=PipelinePublishMode.MODERATED,
            generation_backend=PipelineGenerationBackend.CHAIN,
            is_active=True,
            last_generated_id=0,
            generate_interval_minutes=60,
        ),
        source_channel_ids=[],
        targets=[],
    )


async def _set_run_times(
    db,
    run_id: int,
    *,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    published_at: datetime | None = None,
) -> None:
    await db.execute(
        """
        UPDATE generation_runs
        SET created_at = COALESCE(?, created_at),
            updated_at = COALESCE(?, updated_at),
            published_at = COALESCE(?, published_at)
        WHERE id = ?
        """,
        (
            created_at.isoformat() if created_at else None,
            updated_at.isoformat() if updated_at else None,
            published_at.isoformat() if published_at else None,
            run_id,
        ),
    )
    await db.db.commit()


@pytest.mark.asyncio
async def test_content_analytics_summary_and_pipeline_stats(db):
    analytics = ContentAnalyticsService(db)
    pipeline_a = await _create_pipeline(db, "Pipeline A")
    pipeline_b = await _create_pipeline(db, "Pipeline B")

    published_run = await db.repos.generation_runs.create_run(pipeline_a, "published")
    await db.repos.generation_runs.save_result(published_run, "post")
    await db.repos.generation_runs.set_published_at(published_run)

    rejected_run = await db.repos.generation_runs.create_run(pipeline_a, "rejected")
    await db.repos.generation_runs.save_result(rejected_run, "post")
    await db.repos.generation_runs.set_moderation_status(rejected_run, "rejected")

    pending_run = await db.repos.generation_runs.create_run(pipeline_b, "pending")
    await db.repos.generation_runs.save_result(pending_run, "draft")

    summary = await analytics.get_summary()
    stats = await analytics.get_pipeline_stats()

    assert summary == {
        "total_generations": 3,
        "total_published": 1,
        "total_pending": 1,
        "total_rejected": 1,
        "pipelines_count": 2,
    }

    assert [(row.pipeline_id, row.total_generations, row.total_published) for row in stats] == [
        (pipeline_a, 2, 1),
        (pipeline_b, 1, 0),
    ]
    assert stats[0].total_rejected == 1
    assert stats[0].pending_moderation == 0
    assert stats[0].success_rate == 50.0
    assert stats[1].pending_moderation == 1
    assert stats[1].success_rate == 0.0


@pytest.mark.asyncio
async def test_content_analytics_daily_stats_aggregate_all_pipelines(db):
    analytics = ContentAnalyticsService(db)
    pipeline_a = await _create_pipeline(db, "Pipeline A")
    pipeline_b = await _create_pipeline(db, "Pipeline B")
    now = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
    day_one = now - timedelta(days=1)

    published_run = await db.repos.generation_runs.create_run(pipeline_a, "published")
    await db.repos.generation_runs.save_result(published_run, "post")
    await db.repos.generation_runs.set_published_at(published_run)
    await _set_run_times(
        db,
        published_run,
        created_at=day_one,
        updated_at=day_one + timedelta(hours=1),
        published_at=day_one + timedelta(hours=2),
    )

    rejected_run = await db.repos.generation_runs.create_run(pipeline_b, "rejected")
    await db.repos.generation_runs.save_result(rejected_run, "post")
    await db.repos.generation_runs.set_moderation_status(rejected_run, "rejected")
    await _set_run_times(
        db,
        rejected_run,
        created_at=now,
        updated_at=now + timedelta(hours=1),
    )

    stats = await analytics.get_daily_stats(days=2)

    assert [row.date for row in stats] == [
        day_one.strftime("%Y-%m-%d"),
        now.strftime("%Y-%m-%d"),
    ]
    assert stats[0].generations == 1
    assert stats[0].publications == 1
    assert stats[0].rejections == 0
    assert stats[1].generations == 1
    assert stats[1].publications == 0
    assert stats[1].rejections == 1
