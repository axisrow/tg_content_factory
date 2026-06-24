"""Tests for moderation routes."""

from __future__ import annotations

import base64
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from src.database import Database
from src.models import (
    ContentPipeline,
    PipelineGenerationBackend,
    PipelinePublishMode,
    PipelineTarget,
)
from src.services.publish_service import PublishResult


@pytest.fixture
async def client(base_app):
    app, db, pool_mock = base_app

    pool_mock.clients = {"+1234567890": MagicMock()}
    pool_mock.get_dialogs_for_phone = AsyncMock(return_value=[])

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        c._transport_app = app
        yield c



async def _create_pipeline(db: Database, *, publish_mode: PipelinePublishMode) -> int:
    pipeline = ContentPipeline(
        name="Moderation Pipeline",
        prompt_template="Write a summary",
        publish_mode=publish_mode,
        generation_backend=PipelineGenerationBackend.CHAIN,
    )
    return await db.repos.content_pipelines.add(
        pipeline,
        source_channel_ids=[100],
        targets=[
            PipelineTarget(
                pipeline_id=0,
                phone="+1234567890",
                dialog_id=200,
                title="Target Dialog",
                dialog_type="channel",
            )
        ],
    )


@pytest.mark.anyio
async def test_moderation_page_renders_empty_queue(client):
    resp = await client.get("/moderation/fragments/table")
    assert resp.status_code == 200
    assert "Нет черновиков на модерации." in resp.text
    assert "request.query_params.get" not in resp.text


@pytest.mark.anyio
async def test_moderation_page_is_skeleton(client):
    """Lazyload (#948): the page shell defers the queue table to the fragment."""
    resp = await client.get("/moderation/")
    assert resp.status_code == 200
    assert "/moderation/fragments/table" in resp.text
    assert 'hx-trigger="load"' in resp.text


@pytest.mark.anyio
async def test_publish_run_uses_publish_service(client, monkeypatch):
    db = client._transport_app.state.db
    pipeline_id = await _create_pipeline(db, publish_mode=PipelinePublishMode.MODERATED)
    run_id = await db.repos.generation_runs.create_run(pipeline_id, "prompt-template")
    await db.repos.generation_runs.save_result(run_id, "Generated post")
    await db.repos.generation_runs.set_moderation_status(run_id, "approved")

    resp = await client.post(f"/moderation/{run_id}/publish", follow_redirects=False)
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.anyio
async def test_publish_run_rejects_unapproved_run(client, monkeypatch):
    db = client._transport_app.state.db
    pipeline_id = await _create_pipeline(db, publish_mode=PipelinePublishMode.MODERATED)
    run_id = await db.repos.generation_runs.create_run(pipeline_id, "prompt-template")
    await db.repos.generation_runs.save_result(run_id, "Generated post")

    fake_publish = AsyncMock()

    class FakePublishService:
        def __init__(self, injected_db, pool):
            pass

        async def publish_run(self, run, pipeline):
            await fake_publish(run, pipeline)
            return [PublishResult(success=True, message_id=777)]

    monkeypatch.setattr("src.web.routes.moderation.PublishService", FakePublishService)

    resp = await client.post(f"/moderation/{run_id}/publish", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=run_not_approved" in resp.headers["location"]
    fake_publish.assert_not_awaited()


# === New tests ===


async def _create_pipeline_and_run(
    db: Database, publish_mode: PipelinePublishMode = PipelinePublishMode.MODERATED
) -> tuple[int, int]:
    """Create pipeline and run, return (pipeline_id, run_id)."""
    pipeline_id = await _create_pipeline(db, publish_mode=publish_mode)
    run_id = await db.repos.generation_runs.create_run(pipeline_id, "template")
    await db.repos.generation_runs.save_result(run_id, "Generated content")
    return pipeline_id, run_id


@pytest.mark.anyio
async def test_view_run_renders(client):
    """Test view run renders page."""
    db = client._transport_app.state.db
    _, run_id = await _create_pipeline_and_run(db)

    resp = await client.get(f"/moderation/{run_id}/view")
    assert resp.status_code == 200
    assert "Generated content" in resp.text


@pytest.mark.anyio
async def test_view_run_not_found(client):
    """Test view run with invalid ID redirects."""
    resp = await client.get("/moderation/999999/view", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=run_not_found" in resp.headers["location"]


@pytest.mark.anyio
async def test_approve_run(client):
    """Test approve run sets status."""
    db = client._transport_app.state.db
    _, run_id = await _create_pipeline_and_run(db)

    resp = await client.post(f"/moderation/{run_id}/approve", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=run_approved" in resp.headers["location"]

    run = await db.repos.generation_runs.get(run_id)
    assert run.moderation_status == "approved"


@pytest.mark.anyio
async def test_approve_run_not_found(client):
    """Test approve run with invalid ID redirects."""
    resp = await client.post("/moderation/999999/approve", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=run_not_found" in resp.headers["location"]


@pytest.mark.anyio
async def test_reject_run(client):
    """Test reject run sets status."""
    db = client._transport_app.state.db
    _, run_id = await _create_pipeline_and_run(db)

    resp = await client.post(f"/moderation/{run_id}/reject", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=run_rejected" in resp.headers["location"]

    run = await db.repos.generation_runs.get(run_id)
    assert run.moderation_status == "rejected"


@pytest.mark.anyio
async def test_reject_run_not_found(client):
    """Test reject run with invalid ID redirects."""
    resp = await client.post("/moderation/999999/reject", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=run_not_found" in resp.headers["location"]


@pytest.mark.anyio
async def test_bulk_approve(client):
    """Test bulk approve sets status for multiple runs."""
    db = client._transport_app.state.db
    _, run_id_1 = await _create_pipeline_and_run(db)
    _, run_id_2 = await _create_pipeline_and_run(db)

    resp = await client.post(
        "/moderation/bulk-approve",
        data={"run_ids": [str(run_id_1), str(run_id_2)]},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=runs_approved" in resp.headers["location"]

    run_1 = await db.repos.generation_runs.get(run_id_1)
    run_2 = await db.repos.generation_runs.get(run_id_2)
    assert run_1.moderation_status == "approved"
    assert run_2.moderation_status == "approved"


@pytest.mark.anyio
async def test_bulk_reject(client):
    """Test bulk reject sets status for multiple runs."""
    db = client._transport_app.state.db
    _, run_id_1 = await _create_pipeline_and_run(db)
    _, run_id_2 = await _create_pipeline_and_run(db)

    resp = await client.post(
        "/moderation/bulk-reject",
        data={"run_ids": [str(run_id_1), str(run_id_2)]},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=runs_rejected" in resp.headers["location"]

    run_1 = await db.repos.generation_runs.get(run_id_1)
    run_2 = await db.repos.generation_runs.get(run_id_2)
    assert run_1.moderation_status == "rejected"
    assert run_2.moderation_status == "rejected"


@pytest.mark.anyio
async def test_bulk_approve_is_atomic_on_midbatch_failure(client):
    """RED→GREEN (#1041): web bulk-approve must be all-or-nothing.

    Like the agent tool, the route used to loop one autocommit
    set_moderation_status per id. A crash partway through left earlier runs
    approved with no rollback. The route now routes survivors through the
    atomic set_moderation_status_bulk; if that raises mid-batch, NO run flips.
    """
    db = client._transport_app.state.db
    _, run_id_1 = await _create_pipeline_and_run(db)
    _, run_id_2 = await _create_pipeline_and_run(db)

    # Make the underlying executemany apply the first row, then explode.
    conn = db.db
    real_executemany = conn.executemany

    async def exploding_executemany(sql, seq):
        seq = list(seq)
        await real_executemany(sql, seq[:1])
        raise RuntimeError("simulated mid-batch crash")

    conn.executemany = exploding_executemany
    try:
        with pytest.raises(RuntimeError, match="mid-batch"):
            await client.post(
                "/moderation/bulk-approve",
                data={"run_ids": [str(run_id_1), str(run_id_2)]},
                follow_redirects=False,
            )
    finally:
        conn.executemany = real_executemany

    # Neither run may have been left approved.
    run_1 = await db.repos.generation_runs.get(run_id_1)
    run_2 = await db.repos.generation_runs.get(run_id_2)
    assert run_1.moderation_status == "pending"
    assert run_2.moderation_status == "pending"


@pytest.mark.anyio
async def test_bulk_approve_empty(client):
    """Test bulk approve with no IDs doesn't crash."""
    resp = await client.post("/moderation/bulk-approve", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=runs_approved" in resp.headers["location"]


@pytest.mark.anyio
async def test_moderation_page_with_pipeline_filter(client):
    """Test moderation page with pipeline filter."""
    resp = await client.get("/moderation/fragments/table?pipeline_id=1")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_moderation_page_empty_filter_params_return_200(client):
    """The filter form submits ?pipeline_id= (empty option value) — must not 422 (#779)."""
    resp = await client.get("/moderation/fragments/table?pipeline_id=&limit=&offset=")
    assert resp.status_code == 200
    assert "Очередь модерации" in resp.text


@pytest.mark.anyio
async def test_moderation_page_pipeline_id_none_string_returns_200(client):
    """Pagination links render pipeline_id=None when no pipeline selected — must not 422 (#779)."""
    resp = await client.get("/moderation/fragments/table?pipeline_id=None&limit=50&offset=50")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_moderation_page_empty_pipeline_id_shows_all_runs(client):
    """Empty pipeline_id means 'all pipelines' — pending runs stay visible (#779)."""
    db = client._transport_app.state.db
    _, run_id = await _create_pipeline_and_run(db)

    resp = await client.get("/moderation/fragments/table?pipeline_id=")
    assert resp.status_code == 200
    assert "Generated content" in resp.text
    assert f"/moderation/{run_id}/view" in resp.text


@pytest.mark.anyio
async def test_moderation_page_clamps_out_of_range_limit_and_offset(client):
    """limit is clamped to [1, 200] and offset to >= 0 instead of erroring (#779)."""
    resp = await client.get("/moderation/fragments/table?limit=100000&offset=-5")
    assert resp.status_code == 200
