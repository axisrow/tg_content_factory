import pytest


@pytest.mark.asyncio
async def test_generation_runs_repo(db):
    repo = db.repos.generation_runs
    run_id = await repo.create_run(42, "prompt-template")
    assert run_id > 0
    run = await repo.get(run_id)
    assert run is not None
    assert run.pipeline_id == 42
    assert run.status == "pending"

    await repo.set_status(run_id, "running")
    run = await repo.get(run_id)
    assert run.status == "running"

    await repo.save_result(run_id, "generated content", {"citations": []})
    run = await repo.get(run_id)
    assert run.status == "completed"
    assert run.generated_text == "generated content"

    rows = await repo.list_by_pipeline(42)
    assert any(r.id == run_id for r in rows)


@pytest.mark.asyncio
async def test_generation_runs_repo_hydrates_moderation_fields(db):
    repo = db.repos.generation_runs
    run_id = await repo.create_run(42, "prompt-template")

    await repo.set_status(run_id, "completed")
    await repo.set_moderation_status(run_id, "approved")
    await repo.set_published_at(run_id)

    run = await repo.get(run_id)
    assert run is not None
    assert run.moderation_status == "published"
    assert run.published_at is not None

    rows = await repo.list_by_pipeline(42)
    assert rows[0].moderation_status == "published"
    assert rows[0].published_at is not None


@pytest.mark.asyncio
async def test_list_pending_moderation_returns_runs(db):
    repo = db.repos.generation_runs
    pending_id = await repo.create_run(42, "pending-prompt")
    approved_id = await repo.create_run(42, "approved-prompt")

    await repo.set_moderation_status(approved_id, "approved")

    pending = await repo.list_pending_moderation(42)

    assert [run.id for run in pending] == [pending_id]
    assert pending[0].moderation_status == "pending"


@pytest.mark.asyncio
async def test_generation_runs_repo_hydrates_quality_fields(db):
    repo = db.repos.generation_runs
    run_id = await repo.create_run(42, "quality-prompt")

    await repo.set_quality_score(run_id, 0.82, ["too long", "weak ending"])

    run = await repo.get(run_id)
    assert run is not None
    assert run.quality_score == 0.82
    assert run.quality_issues == ["too long", "weak ending"]

    rows = await repo.list_by_pipeline(42)
    assert rows[0].quality_score == 0.82
    assert rows[0].quality_issues == ["too long", "weak ending"]
