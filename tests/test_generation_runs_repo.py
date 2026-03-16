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
