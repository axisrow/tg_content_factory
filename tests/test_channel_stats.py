from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

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
    reactions = SimpleNamespace(
        results=[SimpleNamespace(count=reactions_count)]
    ) if reactions_count is not None else None
    return SimpleNamespace(
        id=msg_id,
        views=views,
        forwards=forwards,
        reactions=reactions,
    )


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
    mock_client.return_value = mock_full
    mock_client.iter_messages = MagicMock(return_value=_AsyncIterMessages(mock_messages))

    pool = make_mock_pool(
        get_available_client=AsyncMock(return_value=(mock_client, "+7000"))
    )

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


@pytest.mark.asyncio
async def test_collect_channel_stats_releases_client(db):
    ch = Channel(channel_id=-100123, title="Test", username="test_chan")
    await db.add_channel(ch)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(side_effect=ValueError("fail"))

    pool = make_mock_pool(
        get_available_client=AsyncMock(return_value=(mock_client, "+7000"))
    )

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

    from src.config import AppConfig
    from src.database import Database
    from src.scheduler.manager import SchedulerManager
    from src.search.ai_search import AISearchEngine
    from src.search.engine import SearchEngine
    from src.web.app import create_app

    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
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

    from src.telegram.auth import TelegramAuth

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None

    collector = MagicMock()
    collector.collect_channel_stats = AsyncMock(
        return_value=ChannelStats(channel_id=-100123, subscriber_count=999)
    )
    collector.is_running = False
    app.state.collector = collector
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(
        Collector(app.state.pool, db, config.scheduler), config.scheduler
    )
    app.state.session_secret = "test_secret_key"

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=False,
        headers={"Authorization": f"Basic {auth_header}"},
    ) as c:
        resp = await c.post(f"/channels/{pk}/stats")
        assert resp.status_code == 303
        assert "msg=stats_collected" in resp.headers["location"]

    collector.collect_channel_stats.assert_awaited_once()
    await db.close()
