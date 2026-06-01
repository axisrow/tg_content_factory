"""Tests for GenerationRunsRepository (#633 bug #26)."""

from __future__ import annotations

import pytest


@pytest.mark.anyio
async def test_set_image_url_updates_row_and_timestamp(db):
    repo = db.repos.generation_runs
    run_id = await repo.create_run(42, "prompt-template")

    before = await repo.get(run_id)
    assert before is not None
    assert before.image_url is None

    await repo.set_image_url(run_id, "https://example.com/pic.png")

    after = await repo.get(run_id)
    assert after is not None
    assert after.image_url == "https://example.com/pic.png"
    # set_image_url must bump updated_at like the other setters.
    assert after.updated_at is not None
