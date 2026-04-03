import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from telethon.errors import FloodWaitError

from src.config import SchedulerConfig
from src.models import Channel, ChannelStats
from src.telegram.collector import Collector
from tests.helpers import make_mock_pool


@pytest.mark.asyncio
async def test_save_and_get_channel_stats(db):
    ch = Channel(channel_id=-100123, title="Test")
    await db.add_channel(ch)

    stats = ChannelStats(
        channel_id=-100123,
        subscriber_count=5000,
        avg_views=1200.5,
        avg_reactions=50.3,
        avg_forwards=10.0,
    )
    sid = await db.save_channel_stats(stats)
    assert sid > 0

    result = await db.get_channel_stats(-100123, limit=1)
    assert len(result) == 1
    assert result[0].subscriber_count == 5000
    assert result[0].avg_views == 1200.5
    assert result[0].avg_reactions == 50.3
    assert result[0].avg_forwards == 10.0
    assert result[0].collected_at is not None


@pytest.mark.asyncio
async def test_delete_channel_removes_stats(db):
    ch = Channel(channel_id=-100123, title="Test")
    await db.add_channel(ch)
    channels = await db.get_channels()
    pk = channels[0].id

    await db.save_channel_stats(ChannelStats(channel_id=-100123, subscriber_count=5000))
    assert len(await db.get_channel_stats(-100123)) == 1

    await db.delete_channel(pk)

    assert len(await db.get_channel_stats(-100123)) == 0


@pytest.mark.asyncio
async def test_get_latest_stats_for_all(db):
    ch1 = Channel(channel_id=-100111, title="Ch1")
    ch2 = Channel(channel_id=-100222, title="Ch2")
    await db.add_channel(ch1)
    await db.add_channel(ch2)

    await db.save_channel_stats(ChannelStats(channel_id=-100111, subscriber_count=100))
    await db.save_channel_stats(ChannelStats(channel_id=-100111, subscriber_count=200))
    await db.save_channel_stats(ChannelStats(channel_id=-100222, subscriber_count=300))

    latest = await db.get_latest_stats_for_all()
    assert len(latest) == 2
    assert latest[-100111].subscriber_count == 200
    assert latest[-100222].subscriber_count == 300


@pytest.mark.asyncio
async def test_get_previous_subscriber_counts(db):
    ch1 = Channel(channel_id=-100111, title="Ch1")
    ch2 = Channel(channel_id=-100222, title="Ch2")
    await db.add_channel(ch1)
    await db.add_channel(ch2)

    # ch1 has 2 entries — previous is 100, latest is 200
    await db.save_channel_stats(ChannelStats(channel_id=-100111, subscriber_count=100))
    await db.save_channel_stats(ChannelStats(channel_id=-100111, subscriber_count=200))
    # ch2 has only 1 entry — no previous
    await db.save_channel_stats(ChannelStats(channel_id=-100222, subscriber_count=300))

    prev = await db.get_previous_subscriber_counts()
    assert -100111 in prev
    assert prev[-100111] == 100
    # ch2 only has one entry, so it must not appear in previous counts
    assert -100222 not in prev


class _AsyncIterMessages:
    def __init__(self, messages):
        self._messages = list(messages)
        self._index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._index >= len(self._messages):
            raise StopAsyncIteration
        msg = self._messages[self._index]
        self._index += 1
        return msg


def _make_mock_msg(msg_id, views=100, forwards=5, reactions_count=10):
    reactions = (
        SimpleNamespace(results=[SimpleNamespace(count=reactions_count)])
        if reactions_count is not None
        else None
    )
    return SimpleNamespace(
        id=msg_id,
        views=views,
        forwards=forwards,
        reactions=reactions,
    )


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


async def _wait_for_task_status(db, task_id: int, status: str, *, timeout: float = 1.0):
    async def _poll():
        while True:
            task = await db.get_collection_task(task_id)
            if task is not None and task.status == status:
                return task
            await asyncio.sleep(0.01)

    return await asyncio.wait_for(_poll(), timeout=timeout)


async def _create_stats_web_test_context(tmp_path, collector):
    from src.database import Database
    from src.scheduler.service import SchedulerManager
    from src.search.ai_search import AISearchEngine
    from src.search.engine import SearchEngine
    from src.telegram.auth import TelegramAuth
    from src.web.app import create_app
    from tests.helpers import make_test_config

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


@pytest.mark.asyncio
async def test_collect_channel_stats_success(db):
    ch = Channel(channel_id=-100123, title="Test", username="test_chan")
    await db.add_channel(ch)

    mock_entity = SimpleNamespace()
    mock_full_chat = SimpleNamespace(participants_count=5000)
    mock_full = SimpleNamespace(full_chat=mock_full_chat)

    mock_messages = [_make_mock_msg(i) for i in range(1, 4)]

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=mock_entity)
    # await client(GetFullChannelRequest(...)) returns mock_full
    mock_client.return_value = mock_full
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterMessages(mock_messages))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    collector = Collector(pool, db, SchedulerConfig())
    stats = await collector.collect_channel_stats(ch)

    assert stats is not None
    assert stats.subscriber_count == 5000
    assert stats.avg_views == 100.0
    assert stats.avg_forwards == 5.0
    assert stats.avg_reactions == 10.0

    saved = await db.get_channel_stats(-100123)
    assert len(saved) == 1
    assert saved[0].subscriber_count == 5000
    assert mock_client.iter_messages.call_args.kwargs["wait_time"] == 1


@pytest.mark.asyncio
async def test_collect_channel_stats_rotates_on_flood_wait(db):
    ch = Channel(channel_id=-100321, title="Rotate", username="rotate_chan")
    await db.add_channel(ch)

    flood_err = FloodWaitError(request=None, capture=0)
    flood_err.seconds = 60

    client1 = AsyncMock()
    client1.get_entity = AsyncMock(side_effect=flood_err)

    mock_entity = SimpleNamespace()
    mock_full_chat = SimpleNamespace(participants_count=123)
    mock_full = SimpleNamespace(full_chat=mock_full_chat)
    client2 = AsyncMock()
    client2.get_entity = AsyncMock(return_value=mock_entity)
    client2.return_value = mock_full
    client2.iter_messages = MagicMock(return_value=_AsyncIterMessages([]))

    pool = make_mock_pool(
        get_available_client=AsyncMock(side_effect=[(client1, "+7001"), (client2, "+7002")])
    )

    collector = Collector(pool, db, SchedulerConfig())
    result = await collector.collect_channel_stats(ch)

    assert result is not None
    assert result.subscriber_count == 123
    pool.report_flood.assert_awaited_once_with("+7001", 60)
    client2.get_entity.assert_awaited_once_with("rotate_chan")


@pytest.mark.asyncio
async def test_collect_channel_stats_releases_client(db):
    ch = Channel(channel_id=-100123, title="Test", username="test_chan")
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(side_effect=ValueError("fail"))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    collector = Collector(pool, db, SchedulerConfig())
    with pytest.raises(ValueError):
        await collector.collect_channel_stats(ch)

    pool.release_client.assert_awaited_once_with("+7000")


@pytest.mark.asyncio
async def test_collect_channel_stats_no_client(db):
    ch = Channel(channel_id=-100123, title="Test")
    await db.add_channel(ch)

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=None))

    collector = Collector(pool, db, SchedulerConfig())
    result = await collector.collect_channel_stats(ch)
    assert result is None


@pytest.mark.asyncio
async def test_stats_web_endpoint(tmp_path):
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
            assert "msg=stats_collection_started" in resp.headers["location"]

        tasks = await db.get_collection_tasks()
        assert len(tasks) == 1
        assert tasks[0].channel_id == -100123
        assert tasks[0].channel_username == "test"
        assert tasks[0].id is not None

        task = await _wait_for_task_status(db, tasks[0].id, "completed")
        assert task.messages_collected == 1
        assert len(collector.calls) == 1
        assert collector.calls[0].channel_id == -100123
        assert collector.calls[0].username == "test"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_stats_web_endpoint_marks_task_failed(tmp_path):
    import base64

    from httpx import ASGITransport, AsyncClient

    collector = _FakeRouteStatsCollector(error=RuntimeError("stats route failed"))
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
            assert "msg=stats_collection_started" in resp.headers["location"]

        tasks = await db.get_collection_tasks()
        assert len(tasks) == 1
        assert tasks[0].id is not None

        task = await _wait_for_task_status(db, tasks[0].id, "failed")
        assert task.error == "stats route failed"
        assert task.messages_collected == 0
        assert len(collector.calls) == 1
        assert collector.calls[0].channel_id == -100123
        assert collector.calls[0].username == "test"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_collect_all_stats(db):
    ch1 = Channel(channel_id=-100111, title="Ch1", username="ch1")
    ch2 = Channel(channel_id=-100222, title="Ch2", username="ch2")
    await db.add_channel(ch1)
    await db.add_channel(ch2)

    mock_entity = SimpleNamespace()
    mock_full_chat = SimpleNamespace(participants_count=1000)
    mock_full = SimpleNamespace(full_chat=mock_full_chat)
    mock_messages = [_make_mock_msg(i) for i in range(1, 3)]

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=mock_entity)
    mock_client.return_value = mock_full
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterMessages(mock_messages))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(mock_client, "+7000")))

    collector = Collector(pool, db, SchedulerConfig())
    result = await collector.collect_all_stats()

    assert result["channels"] == 2
    assert result["errors"] == 0


def test_cli_channel_stats_no_args(capsys):
    """Calling `channel stats` without identifier or --all should not crash."""
    import argparse
    from unittest.mock import AsyncMock, MagicMock, patch

    from src.main import cmd_channel

    args = argparse.Namespace(
        config="config.yaml",
        channel_action="stats",
        identifier=None,
        all=False,
    )

    mock_db = AsyncMock()
    mock_db.close = AsyncMock()

    mock_pool = MagicMock()
    mock_pool.clients = {"phone": True}
    mock_pool.disconnect_all = AsyncMock()

    async def fake_init_db(config_path):
        from src.config import AppConfig

        return AppConfig(), mock_db

    async def fake_init_pool(config, db):
        return config, mock_pool

    with (
        patch("src.main._init_db", fake_init_db),
        patch("src.main._init_pool", fake_init_pool),
    ):
        cmd_channel(args)

    captured = capsys.readouterr()
    assert "Specify a channel identifier or use --all" in captured.out
