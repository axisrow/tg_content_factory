"""Production-like tests for accounts/agent routes in web runtime mode.

These tests exercise the real `build_web_container` wiring — i.e. the same
bootstrap path the production `src/main.py serve` entrypoint uses. No live
ClientPool / Collector / SchedulerManager are constructed; web code only
sees the snapshot shims (`SnapshotClientPool`, `SnapshotCollector`,
`SnapshotSchedulerManager`). Failures here indicate the web request path
is still reaching for runtime components it should not see.
"""
from __future__ import annotations

import base64
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import AppConfig, DatabaseConfig
from src.models import Account
from src.web.app import create_app
from src.web.bootstrap import build_container_with_templates
from src.web.log_handler import LogBuffer
from src.web.runtime_shims import SnapshotClientPool, SnapshotCollector, SnapshotSchedulerManager


@asynccontextmanager
async def _web_mode_client(tmp_path):
    config = AppConfig(database=DatabaseConfig(path=str(tmp_path / "test.db")))
    config.web.password = "testpass"

    log_buffer = LogBuffer()
    container = await build_container_with_templates(
        config,
        log_buffer=log_buffer,
        templates=None,
        runtime_mode="web",
    )
    await container.db.add_account(Account(phone="+1234567890", session_string="test_session"))

    app = create_app(config)
    app.state.db = container.db
    app.state.config = config
    app.state.pool = container.pool
    app.state.collector = container.collector
    app.state.scheduler = container.scheduler
    app.state.auth = container.auth
    app.state.notifier = container.notifier
    app.state.collection_queue = container.collection_queue
    app.state.unified_dispatcher = container.unified_dispatcher
    app.state.telegram_command_dispatcher = container.telegram_command_dispatcher
    app.state.agent_manager = container.agent_manager
    app.state.search_engine = container.search_engine
    app.state.ai_search = container.ai_search
    app.state.llm_provider_service = container.llm_provider_service
    app.state.runtime_mode = container.runtime_mode
    app.state.session_secret = "test_secret_key"
    app.state.shutting_down = False

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    try:
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            follow_redirects=False,
            headers={
                "Authorization": f"Basic {auth_header}",
                "Origin": "http://test",
            },
        ) as client:
            yield client, container
    finally:
        await container.db.close()


@pytest.mark.anyio
async def test_web_container_uses_snapshot_shims(tmp_path):
    """Verify runtime_mode="web" really produces the snapshot shims."""
    async with _web_mode_client(tmp_path) as (_, container):
        assert container.runtime_mode == "web"
        assert isinstance(container.pool, SnapshotClientPool)
        assert isinstance(container.collector, SnapshotCollector)
        assert isinstance(container.scheduler, SnapshotSchedulerManager)
        assert container.agent_manager is None
        assert container.telegram_command_dispatcher is None


@pytest.mark.anyio
async def test_account_toggle_in_web_mode_enqueues_command(tmp_path):
    """POST /settings/{id}/toggle in real web bootstrap: no live pool, just a queued command."""
    async with _web_mode_client(tmp_path) as (client, container):
        accounts = await container.db.get_accounts(active_only=False)
        acc = accounts[0]

        resp = await client.post(f"/settings/{acc.id}/toggle")
        assert resp.status_code == 303
        assert "account_toggle_queued" in resp.headers["location"]

        commands = await container.db.repos.telegram_commands.list_commands(limit=1)
        assert commands[0].command_type == "accounts.toggle"
        assert commands[0].payload == {"account_id": acc.id}


@pytest.mark.anyio
async def test_account_delete_in_web_mode_removes_row_and_enqueues_cleanup(tmp_path):
    """POST /settings/{id}/delete in real web bootstrap: DB row removed, cleanup command enqueued."""
    async with _web_mode_client(tmp_path) as (client, container):
        accounts = await container.db.get_accounts(active_only=False)
        acc = accounts[0]

        resp = await client.post(f"/settings/{acc.id}/delete")
        assert resp.status_code == 303
        assert "account_deleted" in resp.headers["location"]

        remaining = await container.db.get_accounts(active_only=False)
        assert not any(a.id == acc.id for a in remaining)

        commands = await container.db.repos.telegram_commands.list_commands(limit=1)
        assert commands[0].command_type == "accounts.delete"
        assert commands[0].payload == {"account_id": acc.id, "phone": acc.phone}


@pytest.mark.anyio
async def test_agent_chat_in_web_mode_returns_worker_only_error(tmp_path):
    """POST /agent/threads/{id}/chat in real web bootstrap returns a clear 503,
    not the old opaque 'AgentManager not initialized'."""
    async with _web_mode_client(tmp_path) as (client, container):
        thread_id = await container.db.create_agent_thread("Test thread")

        resp = await client.post(
            f"/agent/threads/{thread_id}/chat",
            json={"message": "hi"},
        )
        assert resp.status_code == 503
        detail = resp.json().get("detail", "")
        assert "worker" in detail.lower()


@pytest.mark.anyio
async def test_agent_chat_in_embedded_worker_mode_uses_worker_agent_manager(web_mode_app, web_mode_client):
    """Normal `serve` mode has a web container plus an embedded worker container.
    Agent chat should use the worker AgentManager instead of returning the split-mode 503."""
    app, container = web_mode_app
    thread_id = await container.db.create_agent_thread("Test thread")

    runtime = MagicMock()
    runtime.selected_backend = "deepagents"
    runtime.using_override = False
    runtime.error = None

    manager = MagicMock()
    manager.get_runtime_status = AsyncMock(return_value=runtime)
    manager.estimate_prompt_tokens = AsyncMock(return_value=100)

    async def _fake_stream(*args, **kwargs):
        yield 'data: {"delta": "ok"}\n\n'
        yield 'data: {"done": true, "full_text": "ok"}\n\n'

    manager.chat_stream = _fake_stream
    app.state.embedded_worker = SimpleNamespace(
        _ready_event=SimpleNamespace(is_set=lambda: True),
        container=SimpleNamespace(agent_manager=manager),
    )

    resp = await web_mode_client.post(
        f"/agent/threads/{thread_id}/chat",
        json={"message": "hi"},
    )

    assert resp.status_code == 200
    assert "Agent chat is only available in the worker process" not in resp.text
    assert '"full_text": "ok"' in resp.text


@pytest.mark.anyio
async def test_agent_page_with_old_thread_during_embedded_worker_startup_is_retryable(tmp_path):
    async with _web_mode_client(tmp_path) as (client, container):
        thread_id = await container.db.create_agent_thread("Old chat")
        await container.db.save_agent_message(thread_id, "user", "previous message")
        client._transport.app.state.embedded_worker = SimpleNamespace(
            agent_ready=False,
            startup_failed=False,
            container=SimpleNamespace(agent_manager=MagicMock()),
        )

        resp = await client.get(f"/agent?thread_id={thread_id}", follow_redirects=True)

        assert resp.status_code == 200
        assert "Old chat" in resp.text
        assert "previous message" in resp.text
        assert "Агент запускается." in resp.text
        assert "Чат недоступен в web-процессе" not in resp.text
        assert "Agent chat is only available in the worker process" not in resp.text


@pytest.mark.anyio
async def test_agent_chat_during_embedded_worker_startup_returns_retryable_503(tmp_path):
    async with _web_mode_client(tmp_path) as (client, container):
        thread_id = await container.db.create_agent_thread("Old chat")
        client._transport.app.state.embedded_worker = SimpleNamespace(
            agent_ready=False,
            startup_failed=False,
            container=SimpleNamespace(agent_manager=MagicMock()),
        )

        resp = await client.post(
            f"/agent/threads/{thread_id}/chat",
            json={"message": "hi"},
        )

        assert resp.status_code == 503
        assert resp.headers["retry-after"] == "3"
        body = resp.json()
        assert body["runtime_state"] == "starting"
        assert "worker process" not in body["detail"]
        assert await container.db.get_agent_messages(thread_id) == []


@pytest.mark.anyio
async def test_agent_permission_during_embedded_worker_startup_returns_retryable_503(tmp_path):
    async with _web_mode_client(tmp_path) as (client, container):
        thread_id = await container.db.create_agent_thread("Old chat")
        client._transport.app.state.embedded_worker = SimpleNamespace(
            agent_ready=False,
            startup_failed=False,
            container=SimpleNamespace(agent_manager=MagicMock()),
        )

        resp = await client.post(
            f"/agent/threads/{thread_id}/permission/abc-123",
            json={"choice": "once"},
        )

        assert resp.status_code == 503
        assert resp.headers["retry-after"] == "3"
        body = resp.json()
        assert body["runtime_state"] == "starting"
        assert "worker process" not in body["detail"]


@pytest.mark.anyio
async def test_agent_permission_in_web_mode_returns_worker_only_error(tmp_path):
    """POST /agent/threads/{id}/permission/{req_id} in web returns the same 503 contract."""
    async with _web_mode_client(tmp_path) as (client, container):
        thread_id = await container.db.create_agent_thread("Test thread")

        resp = await client.post(
            f"/agent/threads/{thread_id}/permission/abc-123",
            json={"choice": "once"},
        )
        assert resp.status_code == 503
        detail = resp.json().get("detail", "")
        assert "worker" in detail.lower()


@pytest.mark.anyio
async def test_agent_page_in_web_mode_renders_banner(tmp_path):
    """GET /agent in web mode renders the disabled banner but does not 500."""
    async with _web_mode_client(tmp_path) as (client, container):
        resp = await client.get("/agent", follow_redirects=True)
        assert resp.status_code == 200
        assert "worker" in resp.text.lower()
