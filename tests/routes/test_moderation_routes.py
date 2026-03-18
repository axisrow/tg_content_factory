"""Tests for moderation routes."""

from __future__ import annotations

import base64
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import AppConfig
from src.database import Database
from src.models import (
    Account,
    Channel,
    ContentPipeline,
    PipelineGenerationBackend,
    PipelinePublishMode,
    PipelineTarget,
)
from src.scheduler.manager import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.services.publish_service import PublishResult
from src.telegram.auth import TelegramAuth
from src.telegram.collector import Collector
from src.web.app import create_app


@pytest.fixture
async def client(tmp_path):
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    pool_mock = MagicMock()
    pool_mock.clients = {"+1234567890": MagicMock()}
    pool_mock.get_dialogs_for_phone = AsyncMock(return_value=[])
    app.state.pool = pool_mock

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None
    app.state.collector = Collector(pool_mock, db, config.scheduler)
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "test_secret_key"
    app.state.shutting_down = False

    await db.add_account(Account(phone="+1234567890", session_string="test_session"))
    await db.add_channel(Channel(channel_id=100, title="Test Channel"))

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        yield c

    await db.close()


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


@pytest.mark.asyncio
async def test_moderation_page_renders_empty_queue(client):
    resp = await client.get("/moderation/")
    assert resp.status_code == 200
    assert "Нет черновиков на модерации." in resp.text
    assert "request.query_params.get" not in resp.text


@pytest.mark.asyncio
async def test_publish_run_uses_publish_service(client, monkeypatch):
    db = client._transport.app.state.db
    pipeline_id = await _create_pipeline(db, publish_mode=PipelinePublishMode.MODERATED)
    run_id = await db.repos.generation_runs.create_run(pipeline_id, "prompt-template")
    await db.repos.generation_runs.save_result(run_id, "Generated post")
    await db.repos.generation_runs.set_moderation_status(run_id, "approved")

    observed: dict[str, int] = {}

    class FakePublishService:
        def __init__(self, injected_db, pool):
            assert injected_db is db
            assert pool is client._transport.app.state.pool

        async def publish_run(self, run, pipeline):
            observed["run_id"] = run.id
            observed["pipeline_id"] = pipeline.id
            return [PublishResult(success=True, message_id=777)]

    monkeypatch.setattr("src.web.routes.moderation.PublishService", FakePublishService)

    resp = await client.post(f"/moderation/{run_id}/publish", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=run_published" in resp.headers["location"]
    assert observed == {"run_id": run_id, "pipeline_id": pipeline_id}


@pytest.mark.asyncio
async def test_publish_run_rejects_unapproved_run(client, monkeypatch):
    db = client._transport.app.state.db
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
