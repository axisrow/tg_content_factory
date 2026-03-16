"""Tests for pipelines routes."""
from __future__ import annotations

import base64
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import AppConfig
from src.database import Database
from src.models import Account
from src.scheduler.manager import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
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
    collector = Collector(pool_mock, db, config.scheduler)
    app.state.collector = collector
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "test_secret_key"
    app.state.shutting_down = False

    await db.add_account(Account(phone="+1234567890", session_string="test_session"))

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
        data={
            "name": "Test Pipeline",
            "phone": "+1234567890",
            "publish_mode": "draft",
            "prompt_template": "",
            "llm_model": "",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=pipeline_added" in resp.headers["location"]


@pytest.mark.asyncio
async def test_add_pipeline_invalid_phone(client):
    resp = await client.post(
        "/pipelines/add",
        data={
            "name": "Test",
            "phone": "+9999999999",
            "publish_mode": "draft",
            "prompt_template": "",
            "llm_model": "",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=invalid_account" in resp.headers["location"]


@pytest.mark.asyncio
async def test_pipelines_page_lists_pipeline(client):
    await client.post(
        "/pipelines/add",
        data={
            "name": "Listed Pipeline",
            "phone": "+1234567890",
            "publish_mode": "draft",
            "prompt_template": "",
            "llm_model": "",
        },
    )
    resp = await client.get("/pipelines/")
    assert resp.status_code == 200
    assert "Listed Pipeline" in resp.text


@pytest.mark.asyncio
async def test_toggle_pipeline(client):
    await client.post(
        "/pipelines/add",
        data={
            "name": "Toggle Test",
            "phone": "+1234567890",
            "publish_mode": "draft",
            "prompt_template": "",
            "llm_model": "",
        },
    )
    resp = await client.post("/pipelines/1/toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=pipeline_toggled" in resp.headers["location"]


@pytest.mark.asyncio
async def test_delete_pipeline(client):
    await client.post(
        "/pipelines/add",
        data={
            "name": "Delete Test",
            "phone": "+1234567890",
            "publish_mode": "draft",
            "prompt_template": "",
            "llm_model": "",
        },
    )
    resp = await client.post("/pipelines/1/delete", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=pipeline_deleted" in resp.headers["location"]


@pytest.mark.asyncio
async def test_edit_pipeline(client):
    await client.post(
        "/pipelines/add",
        data={
            "name": "Original",
            "phone": "+1234567890",
            "publish_mode": "draft",
            "prompt_template": "",
            "llm_model": "",
        },
    )
    resp = await client.post(
        "/pipelines/1/edit",
        data={
            "name": "Edited",
            "phone": "+1234567890",
            "publish_mode": "auto",
            "prompt_template": "new prompt",
            "llm_model": "gpt-4o",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=pipeline_edited" in resp.headers["location"]


@pytest.mark.asyncio
async def test_refresh_dialogs(client):
    resp = await client.post(
        "/pipelines/refresh",
        data={"phone": "+1234567890"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "/pipelines" in resp.headers["location"]


@pytest.mark.asyncio
async def test_no_nested_form_in_template(client):
    """Verify the nested form issue is fixed."""
    resp = await client.get("/pipelines/")
    text = resp.text
    assert 'id="refresh-dialogs-form"' in text
    # The refresh form should be outside the add form
    add_form_start = text.index('action="/pipelines/add"')
    add_form_end = text.index("</form>", add_form_start)
    refresh_form_pos = text.index('id="refresh-dialogs-form"')
    assert refresh_form_pos > add_form_end
