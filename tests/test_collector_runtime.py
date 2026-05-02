from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from telethon.errors import FloodWaitError

from src.config import SchedulerConfig
from src.models import Channel
from src.telegram.collector import Collector
from tests.helpers import AsyncIterMessages, FakeCliTelethonClient


def _resolved_channel_entity(channel_id: int, username: str | None = None):
    return SimpleNamespace(
        id=channel_id,
        username=username,
        title="Runtime Channel",
        broadcast=True,
        megagroup=False,
        gigagroup=False,
        forum=False,
        monoforum=False,
        scam=False,
        fake=False,
        restricted=False,
    )


def _message(msg_id: int, text: str = "msg"):
    return SimpleNamespace(
        id=msg_id,
        text=text,
        message=text,
        date=datetime.now(timezone.utc),
        media=None,
        sender=SimpleNamespace(first_name="First", last_name="Last"),
        sender_id=None,
        forwards=5,
        views=100,
        reactions=SimpleNamespace(results=[SimpleNamespace(count=10)]),
    )


@pytest.mark.anyio
async def test_collect_single_channel_retries_on_flood_with_second_account(
    db,
    real_pool_harness_factory,
):
    await db.add_channel(Channel(channel_id=123, title="Ch", username="ch"))
    channel = (await db.get_channels())[0]

    async def _flood_iter(*args, **kwargs):
        err = FloodWaitError(request=None, capture=0)
        err.seconds = 5
        raise err
        yield

    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone="+7001",
        client=FakeCliTelethonClient(
            entity_resolver=lambda _peer: _resolved_channel_entity(123, "ch"),
            iter_messages_factory=_flood_iter,
        ),
    )
    harness.queue_cli_client(
        phone="+7002",
        client=FakeCliTelethonClient(
            entity_resolver=lambda _peer: _resolved_channel_entity(123, "ch"),
            iter_messages_factory=lambda *args, **kwargs: AsyncIterMessages([_message(11)]),
        ),
    )
    await harness.add_account("+7001", session_string="session-a", is_primary=True)
    await harness.add_account("+7002", session_string="session-b")
    await harness.initialize_connected_accounts()

    collector = Collector(harness.pool, db, SchedulerConfig())
    collected = await collector.collect_single_channel(channel)

    assert collected == 1
    accounts = await db.get_accounts()
    flooded = next(account for account in accounts if account.phone == "+7001")
    assert flooded.flood_wait_until is not None


@pytest.mark.anyio
async def test_collect_single_channel_persists_sender_identity(db, real_pool_harness_factory):
    await db.add_channel(Channel(channel_id=123, title="Ch", username="ch"))
    channel = (await db.get_channels())[0]
    msg = _message(11, "identity")
    msg.sender_id = 77
    msg.sender = SimpleNamespace(
        id=77,
        first_name="Ivan",
        last_name="Petrov",
        username="@ivanp",
    )

    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(
            entity_resolver=lambda _peer: _resolved_channel_entity(123, "ch"),
            iter_messages_factory=lambda *args, **kwargs: AsyncIterMessages([msg]),
        ),
    )
    await harness.add_account("+7000", session_string="session-identity", is_primary=True)
    await harness.initialize_connected_accounts()

    collector = Collector(harness.pool, db, SchedulerConfig())
    collected = await collector.collect_single_channel(channel, force=True)

    assert collected == 1
    messages, total = await db.search_messages(channel_id=123)
    assert total == 1
    assert messages[0].sender_id == 77
    assert messages[0].sender_name == "Ivan Petrov"
    assert messages[0].sender_first_name == "Ivan"
    assert messages[0].sender_last_name == "Petrov"
    assert messages[0].sender_username == "ivanp"


@pytest.mark.anyio
async def test_collect_channel_stats_uses_transport_session_and_persists_stats(
    db,
    real_pool_harness_factory,
):
    await db.add_channel(Channel(channel_id=-100123, title="Test", username="test_chan"))
    channel = (await db.get_channels())[0]

    entity = _resolved_channel_entity(-100123, "test_chan")
    full = SimpleNamespace(full_chat=SimpleNamespace(participants_count=5000))
    messages = [_message(1), _message(2), _message(3)]

    def _invoke(request):
        return full

    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(
            entity_resolver=lambda _peer: entity,
            invoke_side_effect=_invoke,
            iter_messages_factory=lambda *args, **kwargs: AsyncIterMessages(messages),
        ),
    )
    await harness.add_account("+7000", session_string="session-stats", is_primary=True)
    await harness.initialize_connected_accounts()

    collector = Collector(harness.pool, db, SchedulerConfig())
    stats = await collector.collect_channel_stats(channel)

    assert stats is not None
    assert stats.subscriber_count == 5000
    assert stats.avg_views == 100.0
    assert stats.avg_forwards == 5.0
    assert stats.avg_reactions == 10.0

    saved = await db.get_channel_stats(-100123)
    assert len(saved) == 1
    assert saved[0].subscriber_count == 5000
