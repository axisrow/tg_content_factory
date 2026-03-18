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
    GenerationRun,
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
    await db.repos.dialog_cache.replace_dialogs(
        "+1234567890",
        [{"channel_id": 200, "title": "Test Dialog", "channel_type": "channel"}],
    )

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        c.app = app
        yield c

    await db.close()


async def _create_pipeline_and_run(client: AsyncClient, *, moderation_status: str) -> int:
    db = client.app.state.db
    pipeline = ContentPipeline(
        name="Moderation Pipeline",
        prompt_template="Write a summary",
        publish_mode=PipelinePublishMode.MODERATED,
    )
    pipeline_id = await db.repos.content_pipelines.add(
        pipeline,
        [100],
        [
            PipelineTarget(
                pipeline_id=0,
                phone="+1234567890",
                dialog_id=200,
                title="Test Dialog",
                dialog_type="channel",
            )
        ],
    )

    run_id = await db.repos.generation_runs.create_run(pipeline_id, "Prompt")
    await db.repos.generation_runs.save_result(run_id, "Generated text")
    await db.repos.generation_runs.set_moderation_status(run_id, moderation_status)
    return run_id


@pytest.mark.asyncio
async def test_moderation_page_renders_empty_state_without_select_all_binding(client):
    resp = await client.get("/moderation/")

    assert resp.status_code == 200
    assert "Нет черновиков на модерации." in resp.text
    assert "const selectAll = document.getElementById('select-all');" in resp.text
    assert "if (selectAll)" in resp.text


@pytest.mark.asyncio
async def test_publish_run_uses_publish_service(monkeypatch, client):
    run_id = await _create_pipeline_and_run(client, moderation_status="approved")
    publish_run = AsyncMock(return_value=[PublishResult(success=True, message_id=42)])

    class FakePublishService:
        def __init__(self, db, pool):
            self.db = db
            self.pool = pool

        async def publish_run(self, run, pipeline):
            return await publish_run(run, pipeline)

    monkeypatch.setattr("src.web.routes.moderation.PublishService", FakePublishService)

    resp = await client.post(f"/moderation/{run_id}/publish", follow_redirects=False)

    assert resp.status_code == 303
    assert resp.headers["location"] == "/moderation?msg=run_published"
    publish_run.assert_awaited_once()
    run_arg, pipeline_arg = publish_run.await_args.args
    assert isinstance(run_arg, GenerationRun)
    assert run_arg.id == run_id
    assert pipeline_arg.id is not None


@pytest.mark.asyncio
async def test_publish_run_rejects_unapproved_draft(client):
    run_id = await _create_pipeline_and_run(client, moderation_status="pending")

    resp = await client.post(f"/moderation/{run_id}/publish", follow_redirects=False)

    assert resp.status_code == 303
    assert resp.headers["location"] == "/moderation?error=run_not_approved"
