# Web-panel tests split out of tests/test_channel_stats.py (#1720 wave 3).

import pytest

from tg_agent.models import Channel, ChannelStats


class _FakeRouteStatsCollector:
    def __init__(
        self,
        *,
        result: ChannelStats | None = None,
        error: Exception | None = None,
    ):
        self.is_running = False
        self.is_stats_running = False
        self.calls: list[Channel] = []
        self._result = result
        self._error = error

    async def collect_channel_stats(self, channel: Channel) -> ChannelStats | None:
        self.calls.append(channel)
        if self._error is not None:
            raise self._error
        return self._result


async def _create_stats_web_test_context(tmp_path, collector):
    from tests.helpers import make_test_config
    from tg_agent.database import Database
    from tg_agent.scheduler.service import SchedulerManager
    from tg_agent.search.ai_search import AISearchEngine
    from tg_agent.search.engine import SearchEngine
    from tg_shared.auth import TelegramAuth
    from tg_webui.app import create_app

    config = make_test_config(tmp_path)
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    ch = Channel(channel_id=-100123, title="Test Channel", username="test")
    await db.add_channel(ch)
    channels = await db.get_channels()
    pk = channels[0].id

    async def _no_users(self):
        return []

    async def _resolve_channel(self, identifier):
        return {
            "channel_id": -100123,
            "title": "Test Channel",
            "username": "test",
            "channel_type": "channel",
        }

    async def _get_dialogs(self):
        return []

    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {},
            "get_users_info": _no_users,
            "resolve_channel": _resolve_channel,
            "get_dialogs": _get_dialogs,
        },
    )()

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None
    app.state.collector = collector
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "test_secret_key"
    return app, db, pk


@pytest.mark.anyio
async def test_stats_web_endpoint_enqueues_command(tmp_path):
    """Web route only enqueues a telegram command; worker executes it."""
    import base64

    from httpx import ASGITransport, AsyncClient

    collector = _FakeRouteStatsCollector(
        result=ChannelStats(channel_id=-100123, subscriber_count=999)
    )
    app, db, pk = await _create_stats_web_test_context(tmp_path, collector)

    try:
        transport = ASGITransport(app=app)
        auth_header = base64.b64encode(b":testpass").decode()
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
            follow_redirects=False,
            headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
        ) as c:
            resp = await c.post(f"/channels/{pk}/stats")
            assert resp.status_code == 303
            assert "stats_collection_queued" in resp.headers["location"]
            assert "command_id=" in resp.headers["location"]

        commands = await db.repos.telegram_commands.list_commands(limit=1)
        assert len(commands) == 1
        assert commands[0].command_type == "channels.collect_stats"
        assert commands[0].payload == {"channel_pk": pk}
        # Route must not have invoked the live collector directly.
        assert collector.calls == []
    finally:
        await db.close()
