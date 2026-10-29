"""Plugin discovery, lifecycle and observable agent/MCP/ACL behavior."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tg_agent import plugin_api
from tg_agent.agent.tools import build_agent_tool_registry, make_mcp_server, permissions
from tg_agent.config import AppConfig
from tg_agent.plugin_api import Plugin, start_plugins, text_response, tool

pytestmark = pytest.mark.anyio


async def mcp_tools(server, call=None):
    import anyio
    from mcp import ClientSession
    from mcp.shared.memory import create_client_server_memory_streams

    async with create_client_server_memory_streams() as (client, streams):
        async with anyio.create_task_group() as tasks:
            async def serve():
                await server.run(*streams, server.create_initialization_options())
            tasks.start_soon(serve)
            async with ClientSession(*client) as session:
                await session.initialize()
                listed = await session.list_tools()
                result = await session.call_tool(call, {}) if call else None
            tasks.cancel_scope.cancel()
    names = {item.name for item in listed.tools}
    return (names, result) if call else names


class Entry:
    def __init__(self, plugin, name=None):
        self.plugin = plugin
        self.name = name or plugin.id
        self.value = self.name + ':PLUGIN'
        self.loaded = False

    def load(self):
        self.loaded = True
        if isinstance(self.plugin, Exception):
            raise self.plugin
        return self.plugin


def operation(name='plugin_example', category='read', phone_bound=False):
    @tool(name, 'Example operation', {}, category=category, phone_bound=phone_bound)
    async def handler(args):
        return text_response('plugin result')
    return handler


@pytest.fixture
def entries(monkeypatch):
    entries = []
    monkeypatch.setattr(plugin_api, 'entry_points', lambda **kw: entries)
    return entries


@pytest.fixture
async def host(entries):
    db = SimpleNamespace(get_setting=AsyncMock(return_value=None))
    yield db
    if getattr(db, 'plugins', None):
        await db.plugins.close()


async def test_registry_mcp_permissions_and_cleanup(host, entries):
    events = []

    async def setup(ctx):
        events.append(('setup', ctx.db))

    async def shutdown():
        events.append(('shutdown', host))

    entries.append(Entry(Plugin('example', '1', 1, lambda ctx: [operation()], setup, shutdown)))
    runtime = await start_plugins(host, AppConfig())
    await asyncio.gather(runtime.start(), runtime.start())
    assert events == [('setup', host)]
    registry = build_agent_tool_registry(host, config=AppConfig())
    tool_obj = next(item for item in registry if item.name == 'plugin_example')
    assert await tool_obj.handler({}) == text_response('plugin result')
    assert permissions.TOOL_CATEGORIES['plugin_example'].value == 'read'
    assert permissions.MODULE_GROUPS['Plugin: example'] == ('plugin_example',)
    policy = await permissions.load_tool_access_policy(host)
    server = make_mcp_server(host, config=AppConfig(), access_policy=policy)['instance']
    assert 'plugin_example' in await mcp_tools(server)
    policy['plugin_example'] = permissions.ToolAccessState.DENIED
    denied = make_mcp_server(host, access_policy=policy)['instance']
    assert 'plugin_example' not in await mcp_tools(denied)
    await runtime.close()
    await runtime.close()
    assert events == [('setup', host), ('shutdown', host)]
    assert 'plugin_example' not in permissions.TOOL_CATEGORIES


async def test_disabled_not_imported_and_incompatible_not_setup(host, entries):
    setup = AsyncMock()
    disabled = Entry(Plugin('disabled', '1', 1, lambda ctx: [operation()], setup))
    entries.extend([disabled, Entry(Plugin('future', '1', 2, lambda ctx: [operation()], setup))])
    runtime = await start_plugins(host, AppConfig(plugins_disabled=['disabled']))
    assert not disabled.loaded
    setup.assert_not_awaited()
    assert runtime.tools == []


@pytest.mark.parametrize('failure', ['setup', 'register', 'collision', 'duplicate', 'metadata', 'id', 'import'])
async def test_failure_is_atomic_and_other_plugins_continue(host, entries, failure):
    shutdown = AsyncMock()

    async def setup(ctx):
        if failure == 'setup':
            raise RuntimeError('setup failed')

    def register(ctx):
        if failure == 'register':
            raise RuntimeError('register failed')
        if failure == 'collision':
            return [operation(), operation('list_channels')]
        if failure == 'duplicate':
            return [operation(), operation()]
        if failure == 'metadata':
            item = operation()
            del item.plugin_meta
            return [item]
        return [operation()]

    bad = Plugin('a_bad' if failure != 'id' else 'not-valid', '1', 1, register, setup, shutdown)
    entries.append(Entry(bad) if failure != 'import' else Entry(RuntimeError('import failed'), 'a_bad'))
    entries.append(Entry(Plugin('z_good', '1', 1, lambda ctx: [operation('healthy')])))
    runtime = await start_plugins(host, AppConfig())
    assert [item.name for item in runtime.tools] == ['healthy']
    assert 'plugin_example' not in permissions.TOOL_CATEGORIES
    if failure not in ('id', 'import'):
        shutdown.assert_awaited_once()


async def test_two_containers_keep_independent_context_and_metadata(host, entries):
    entries.append(Entry(Plugin('example', '1', 1, lambda ctx: [operation(phone_bound=True)])))
    other = SimpleNamespace()
    first = await start_plugins(host, AppConfig(), client_pool='web')
    second = await start_plugins(other, AppConfig(), client_pool='worker')
    try:
        assert first is not second
        assert first.client_pool == 'web'
        assert second.client_pool == 'worker'
        assert 'plugin_example' in permissions.PHONE_BINDED_TOOLS
        await first.close()
        assert 'plugin_example' in permissions.PHONE_BINDED_TOOLS
    finally:
        await second.close()
    assert 'plugin_example' not in permissions.PHONE_BINDED_TOOLS


async def test_cancelled_start_cleans_up(host, entries):
    shutdown = AsyncMock()
    entered = asyncio.Event()

    async def setup(ctx):
        entered.set()
        await asyncio.Event().wait()

    entries.append(Entry(Plugin('example', '1', 1, lambda ctx: [], setup, shutdown)))
    task = asyncio.create_task(start_plugins(host, AppConfig()))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    shutdown.assert_awaited_once()
    assert 'Plugin: example' not in permissions.MODULE_GROUPS


@pytest.mark.aiosqlite_serial
async def test_digest_real_discovery_db_mcp_and_ui(db, tmp_path, monkeypatch):
    from pathlib import Path

    # Model pip's installed metadata, using real importlib discovery rather than
    # mocking the loader. Pip installation itself is also smoke-tested separately.
    example = Path(__file__).parent.parent / "examples" / "plugin_digest"
    monkeypatch.syspath_prepend(str(example))
    distribution = tmp_path / "digest_example-0.1.0.dist-info"
    distribution.mkdir()
    (distribution / "METADATA").write_text("Name: digest-example\nVersion: 0.1.0\n")
    (distribution / "entry_points.txt").write_text(
        "[tg_agent.plugins]\nmy_digest = tg_plugin_digest:PLUGIN\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    await start_plugins(db, AppConfig())
    from datetime import UTC, datetime, timedelta

    from tg_agent.models import Message

    await db.repos.messages.insert_message(Message(
        channel_id=1, message_id=1, text="Проверка настоящего сообщения", date=datetime.now(UTC),
    ))
    await db.execute_write(
        "UPDATE messages SET collected_at = ?",
        ((datetime.now(UTC) - timedelta(hours=20)).strftime("%Y-%m-%d %H:%M:%S"),),
    )
    policy = await permissions.load_tool_access_policy(db)
    ui = permissions.build_template_context({"digest_today": True})
    group = next(g for g in ui["tool_permission_modules"] if g["name"] == "Plugin: my_digest")
    assert group["tools"][0]["name"] == "digest_today"
    server = make_mcp_server(db, access_policy=policy)["instance"]
    names, result = await mcp_tools(server, call="digest_today")
    assert "digest_today" in names
    assert result.content[0].text == "Проверка настоящего сообщения"
    runs = await db.execute_fetchall("SELECT ts FROM plugin_my_digest_runs")
    assert len(runs) == 1
    await db.close()
    assert "digest_today" not in permissions.TOOL_CATEGORIES


async def test_close_waits_for_activation_and_shutdown_failure_isolated(host, entries):
    entered, release = asyncio.Event(), asyncio.Event()
    stopped = []

    async def setup(ctx):
        entered.set()
        await release.wait()

    async def broken_shutdown():
        stopped.append("broken")
        raise RuntimeError("shutdown failed")

    async def good_shutdown():
        stopped.append("good")

    entries.extend([
        Entry(Plugin("a_good", "1", 1, lambda ctx: [operation("good")], shutdown=good_shutdown)),
        Entry(Plugin("z_broken", "1", 1, lambda ctx: [], setup, broken_shutdown)),
    ])
    activation = asyncio.create_task(start_plugins(host, AppConfig()))
    await entered.wait()
    closing = asyncio.create_task(host.plugins.close())
    assert not closing.done()
    release.set()
    await activation
    await closing
    assert stopped == ["broken", "good"]
    assert "good" not in permissions.TOOL_CATEGORIES


@pytest.mark.aiosqlite_serial
async def test_direct_agent_initializes_plugin_before_backend_registry(db, entries):
    from tg_agent.agent.manager import AgentManager

    async def setup(ctx):
        await ctx.db.execute_write("CREATE TABLE plugin_example_state (value TEXT)")
        await ctx.db.execute_write("INSERT INTO plugin_example_state VALUES ('ready')")

    def register(ctx):
        @tool("read_plugin_state", "Read plugin state", {}, category="read")
        async def read(args):
            rows = await ctx.db.execute_fetchall("SELECT value FROM plugin_example_state")
            return text_response(rows[0][0])
        return [read]

    entries.append(Entry(Plugin("example", "1", 1, register, setup)))
    manager = AgentManager(db, AppConfig())
    try:
        await manager.refresh_settings_cache()
        registry = build_agent_tool_registry(db)
        read = next(item for item in registry if item.name == "read_plugin_state")
        assert await read.handler({}) == text_response("ready")
    finally:
        await manager.close_all()


async def test_allow_list_cache_follows_plugin_lifecycle(host, entries):
    # get_all_allowed_tools() caches once; permission invalidation on plugin
    # register/close must reset it too (#325 review: stale allow-list otherwise).
    assert 'plugin_example' not in permissions.get_all_allowed_tools()
    entries.append(Entry(Plugin('example', '1', 1, lambda ctx: [operation()])))
    runtime = await start_plugins(host, AppConfig())
    assert 'mcp__telegram_db__plugin_example' in permissions.get_all_allowed_tools()
    await runtime.close()
    assert 'mcp__telegram_db__plugin_example' not in permissions.get_all_allowed_tools()


async def test_plugin_api_exports_confirmation_gate():
    from tg_agent.plugin_api import require_confirmation

    gate = require_confirmation('удалит кэш плагина', {})
    assert gate['content'][0]['text'] and 'confirm=true' in gate['content'][0]['text']
    assert require_confirmation('удалит кэш плагина', {'confirm': True}) is None


async def test_setup_timeout_skips_plugin_and_shuts_down(host, entries, monkeypatch):
    shutdown = AsyncMock()
    entered = asyncio.Event()

    async def hung_setup(ctx):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(plugin_api, '_LIFECYCLE_TIMEOUT', 0.05)
    entries.append(Entry(Plugin('example', '1', 1, lambda ctx: [], hung_setup, shutdown)))
    runtime = await start_plugins(host, AppConfig())
    assert runtime.tools == []
    shutdown.assert_awaited_once()


async def test_duplicate_plugin_id_keeps_first_registration(host, entries):
    entries.extend([
        Entry(Plugin("example", "1", 1, lambda ctx: [operation("first")])),
        Entry(Plugin("example", "2", 1, lambda ctx: [operation("second")])),
    ])
    runtime = await start_plugins(host, AppConfig())
    assert [item.name for item in runtime.tools] == ["first"]
    assert permissions.MODULE_GROUPS["Plugin: example"] == ("first",)
    assert "second" not in permissions.TOOL_CATEGORIES
