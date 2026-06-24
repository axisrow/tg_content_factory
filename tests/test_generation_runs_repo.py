from contextlib import contextmanager

import pytest


@contextmanager
def _patched(obj, attr, value):
    """Temporarily swap an attribute, restoring it on exit."""
    original = getattr(obj, attr)
    setattr(obj, attr, value)
    try:
        yield
    finally:
        setattr(obj, attr, original)


@pytest.mark.anyio
async def test_set_moderation_status_bulk_applies_all(db):
    """set_moderation_status_bulk flips every id in one call (issue #1041)."""
    repo = db.repos.generation_runs
    ids = [await repo.create_run(7, f"p-{i}") for i in range(3)]

    await repo.set_moderation_status_bulk(ids, "approved")

    for run_id in ids:
        run = await repo.get(run_id)
        assert run is not None
        assert run.moderation_status == "approved"


@pytest.mark.anyio
async def test_set_moderation_status_bulk_empty_is_noop(db):
    """An empty id list must not open a transaction or raise (issue #1041)."""
    repo = db.repos.generation_runs
    await repo.set_moderation_status_bulk([], "approved")


@pytest.mark.anyio
async def test_set_moderation_status_bulk_rolls_back_on_midbatch_failure(db):
    """RED→GREEN (#1041): a mid-batch failure leaves NO id half-applied.

    The pre-fix bulk tool looped one autocommit ``set_moderation_status`` per
    id, so a crash after committing ids 1..N-1 left those approved with no way
    back. The atomic version wraps the whole batch in one ``BEGIN IMMEDIATE``;
    we prove the rollback is real by letting the underlying ``executemany``
    apply the first row and then raise — the committed DB must still show every
    run as ``pending``.
    """
    repo = db.repos.generation_runs
    ids = [await repo.create_run(7, f"p-{i}") for i in range(3)]

    conn = db.db
    assert conn is not None
    real_executemany = conn.executemany

    async def exploding_executemany(sql, seq):
        seq = list(seq)
        # Apply the first row for real, then blow up mid-batch — exactly the
        # window where the old per-id loop would have left a partial commit.
        await real_executemany(sql, seq[:1])
        raise RuntimeError("simulated mid-batch crash")

    with pytest.raises(RuntimeError, match="mid-batch"):
        with _patched(conn, "executemany", exploding_executemany):
            await repo.set_moderation_status_bulk(ids, "approved")

    # The transaction must have rolled the first (real) write back.
    for run_id in ids:
        run = await repo.get(run_id)
        assert run is not None
        assert run.moderation_status == "pending"


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
async def test_list_by_pipeline_filters_status_before_offset(db):
    repo = db.repos.generation_runs
    failed_ids: list[int] = []
    for idx in range(5):
        completed_id = await repo.create_run(42, f"completed-{idx}")
        await repo.save_result(completed_id, "done", {})
        failed_id = await repo.create_run(42, f"failed-{idx}")
        await repo.set_status(failed_id, "failed")
        failed_ids.append(failed_id)

    rows = await repo.list_by_pipeline(42, limit=2, offset=1, status="failed")

    assert [run.id for run in rows] == list(reversed(failed_ids))[1:3]


@pytest.mark.anyio
async def test_list_by_pipeline_can_filter_moderation_status(db):
    repo = db.repos.generation_runs
    run_id = await repo.create_run(42, "approved-prompt")
    await repo.set_moderation_status(run_id, "approved")

    rows = await repo.list_by_pipeline(42, moderation_status="approved")

    assert [run.id for run in rows] == [run_id]


@pytest.mark.anyio
async def test_list_pending_moderation_returns_runs(db):
    repo = db.repos.generation_runs
    pending_id = await repo.create_run(42, "pending-prompt")
    approved_id = await repo.create_run(42, "approved-prompt")

    await repo.set_moderation_status(approved_id, "approved")

    pending = await repo.list_pending_moderation(42)

    ids = [run.id for run in pending]
    assert approved_id in ids
    assert pending_id in ids
    statuses = {run.id: run.moderation_status for run in pending}
    assert statuses[pending_id] == "pending"
    assert statuses[approved_id] == "approved"


@pytest.mark.anyio
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


@pytest.mark.anyio
async def test_generation_runs_repo_hydrates_variant_fields(db):
    repo = db.repos.generation_runs
    run_id = await repo.create_run(42, "variant-prompt")

    await repo.save_result(run_id, "base")
    await repo.set_variants(run_id, ["base", "variant 2"])
    await repo.select_variant(run_id, 1, "variant 2")

    run = await repo.get(run_id)
    assert run is not None
    assert run.variants == ["base", "variant 2"]
    assert run.selected_variant == 1

    rows = await repo.list_by_pipeline(42)
    assert rows[0].variants == ["base", "variant 2"]
    assert rows[0].selected_variant == 1


@pytest.mark.anyio
async def test_set_metadata_persists_without_changing_status(db):
    """set_metadata writes the metadata JSON but leaves status/published_at intact (issue #633)."""
    repo = db.repos.generation_runs
    run_id = await repo.create_run(42, "prompt-template")
    await repo.set_moderation_status(run_id, "approved")

    await repo.set_metadata(run_id, {"published_targets": ["+1:-1001"]})

    run = await repo.get(run_id)
    assert run is not None
    assert run.metadata["published_targets"] == ["+1:-1001"]
    # Status and publication state are untouched.
    assert run.moderation_status == "approved"
    assert run.published_at is None
