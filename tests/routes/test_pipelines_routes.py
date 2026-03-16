"""Tests for pipelines routes."""
from __future__ import annotations

import base64
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import AppConfig
from src.database import Database
from src.models import Account, Channel
from src.scheduler.manager import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.telegram.auth import TelegramAuth
from src.telegram.collector import Collector
from src.web.app import create_app

_ADD_DATA = {
    "name": "Test Pipeline",
    "prompt_template": "Write a summary",
    "publish_mode": "moderated",
    "source_channel_ids": "100",
    "target_refs": "+1234567890|200",
    "llm_model": "",
    "image_model": "",
    "generation_backend": "chain",
    "generate_interval_minutes": "60",
}


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
    collector = Collector(pool_mock, db, config.scheduler)
    app.state.collector = collector
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
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}"},
    ) as c:
        yield c

    await db.close()


@pytest.mark.asyncio
async def test_pipelines_page_renders(client):
    resp = await client.get("/pipelines/")
    assert resp.status_code == 200
    assert "Пайплайны" in resp.text


@pytest.mark.asyncio
async def test_add_pipeline(client):
    resp = await client.post(
        "/pipelines/add",
        data=_ADD_DATA,
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=pipeline_added" in resp.headers["location"]


@pytest.mark.asyncio
async def test_pipelines_page_lists_pipeline(client):
    await client.post(
        "/pipelines/add",
        data={**_ADD_DATA, "name": "Listed Pipeline"},
    )
    resp = await client.get("/pipelines/")
    assert resp.status_code == 200
    assert "Listed Pipeline" in resp.text


@pytest.mark.asyncio
async def test_toggle_pipeline(client):
    await client.post("/pipelines/add", data=_ADD_DATA)
    resp = await client.post("/pipelines/1/toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=pipeline_toggled" in resp.headers["location"]


@pytest.mark.asyncio
async def test_delete_pipeline(client):
    await client.post("/pipelines/add", data=_ADD_DATA)
    resp = await client.post("/pipelines/1/delete", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=pipeline_deleted" in resp.headers["location"]


@pytest.mark.asyncio
async def test_edit_pipeline(client):
    await client.post("/pipelines/add", data=_ADD_DATA)
    resp = await client.post(
        "/pipelines/1/edit",
        data={**_ADD_DATA, "name": "Edited", "publish_mode": "auto", "llm_model": "gpt-4o"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=pipeline_edited" in resp.headers["location"]
