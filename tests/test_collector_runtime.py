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


def _resolved_bot_entity(user_id: int, username: str | None = None):
    """A Telegram User entity (bot) — no ``title`` attribute."""
    return SimpleNamespace(
        id=user_id,
        username=username,
        first_name="BotUser",
        last_name=None,
        bot=True,
    )


@pytest.mark.anyio
async def test_collect_channel_stats_deactivates_bot_channel(
    db,
    real_pool_harness_factory,
):
    """A channel whose username resolves to a bot (User entity without ``title``)
    must be deactivated and typed as ``'bot'`` instead of silently failing
    on every stats run forever.
    """
    await db.add_channel(Channel(channel_id=-100999, title="Bot Channel", username="some_bot"))
    channel = (await db.get_channels())[0]

    entity = _resolved_bot_entity(-100999, "some_bot")

    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(
            entity_resolver=lambda _peer: entity,
        ),
    )
    await harness.add_account("+7000", session_string="session-bot-test", is_primary=True)
    await harness.initialize_connected_accounts()

    collector = Collector(harness.pool, db, SchedulerConfig())
    result = await collector.collect_channel_stats(channel)

    # Stats must not be collected for a bot
    assert result is None

    # Channel must be deactivated — not left in eternal "no stats" limbo
    updated = (await db.get_channels())[0]
    assert updated.is_active is False

    # Channel type must be explicitly 'bot' for visibility in UI/CLI
    assert updated.channel_type == "bot"

    # No stats row should exist for a bot
    saved = await db.get_channel_stats(-100999)
    assert len(saved) == 0


@pytest.mark.anyio
async def test_collect_channel_stats_deactivates_dm_user_channel(
    db,
    real_pool_harness_factory,
):
    """A channel whose ID resolves to a regular user (not a bot) must be
    deactivated and typed as ``'dm'`` — whatever Telegram returns.
    """
    await db.add_channel(Channel(channel_id=-100998, title="DM Channel"))
    channel = (await db.get_channels())[0]

    user_entity = SimpleNamespace(
        id=-100998,
        username=None,
        first_name="Plain",
        last_name="User",
        bot=False,  # not a bot — just a regular user
    )

    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(
            entity_resolver=lambda _peer: user_entity,
        ),
    )
    await harness.add_account("+7000", session_string="session-dm-test", is_primary=True)
    await harness.initialize_connected_accounts()

    collector = Collector(harness.pool, db, SchedulerConfig())
    result = await collector.collect_channel_stats(channel)

    assert result is None

    updated = (await db.get_channels())[0]
    assert updated.is_active is False
    assert updated.channel_type == "dm"

    saved = await db.get_channel_stats(-100998)
    assert len(saved) == 0


@pytest.mark.anyio
async def test_collect_channel_stats_warms_cache_for_uncached_numeric_id(
    db,
    real_pool_harness_factory,
):
    """A channel without username whose numeric id is missing from the cold
    entity cache (StringSession lost it across restart) must warm the dialog
    cache once and retry — not fail forever on "Could not find the input
    entity" and keep the channel re-entering stats payloads (#794 regression).
    """
    await db.add_channel(Channel(channel_id=-100777, title="Uncached"))
    channel = (await db.get_channels())[0]

    entity = _resolved_channel_entity(-100777)
    full = SimpleNamespace(full_chat=SimpleNamespace(participants_count=4200))
    messages = [_message(1), _message(2)]

    # Cold cache: first resolve raises like Telethon does on a cache miss;
    # after warm_dialog_cache() (get_dialogs) the retry succeeds.
    calls = {"n": 0}

    def _resolver(_peer):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError(
                "Could not find the input entity for PeerChannel(channel_id=-100777)"
            )
        return entity

    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(
            entity_resolver=_resolver,
            invoke_side_effect=lambda request: full,
            iter_messages_factory=lambda *a, **k: AsyncIterMessages(messages),
        ),
    )
    await harness.add_account("+7000", session_string="session-warm", is_primary=True)
    await harness.initialize_connected_accounts()

    collector = Collector(harness.pool, db, SchedulerConfig())
    stats = await collector.collect_channel_stats(channel)

    # Warm-then-retry: resolve called exactly twice (1st raises on cache miss,
    # 2nd succeeds after warm_dialog_cache()).
    assert calls["n"] == 2
    assert stats is not None
    assert stats.subscriber_count == 4200

    updated = (await db.get_channels())[0]
    assert updated.is_active is True

    saved = await db.get_channel_stats(-100777)
    assert len(saved) == 1


@pytest.mark.anyio
async def test_collect_channel_stats_deactivates_unresolvable_numeric_id(
    db,
    real_pool_harness_factory,
):
    """A channel without username that cannot be resolved even after warming
    the dialog cache must be deactivated — not left active without stats,
    which makes the "Обновить фильтры" button loop forever (#794 regression).
    """
    await db.add_channel(Channel(channel_id=-100666, title="Ghost"))
    channel = (await db.get_channels())[0]

    def _resolver(_peer):
        raise ValueError(
            "Could not find the input entity for PeerChannel(channel_id=-100666)"
        )

    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(entity_resolver=_resolver),
    )
    await harness.add_account("+7000", session_string="session-ghost", is_primary=True)
    await harness.initialize_connected_accounts()

    collector = Collector(harness.pool, db, SchedulerConfig())
    result = await collector.collect_channel_stats(channel)

    assert result is None

    updated = (await db.get_channels())[0]
    assert updated.is_active is False

    saved = await db.get_channel_stats(-100666)
    assert len(saved) == 0


@pytest.mark.anyio
async def test_collect_channel_stats_deactivates_on_username_fallback_failure(
    db,
    real_pool_harness_factory,
):
    """When a channel's username is stale (UsernameNotOccupiedError) AND the
    numeric-id fallback also fails, the channel must be deactivated — not
    left in eternal "no stats" limbo (#794 regression, username-fallback path).
    """
    await db.add_channel(Channel(channel_id=-100555, title="Stale", username="old_name"))
    channel = (await db.get_channels())[0]

    def _resolver_username(_peer):
        # First call: resolve username → raise UsernameNotOccupiedError
        raise ValueError("No user has \"old_name\" as username")

    def _resolver_fallback(_peer):
        # Second call: resolve PeerChannel → also fail
        raise ValueError("Could not find the input entity for PeerChannel(channel_id=-100555)")

    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(
            entity_resolver=_resolver_username,
            input_entity_resolver=_resolver_fallback,
        ),
    )
    await harness.add_account("+7000", session_string="session-stale", is_primary=True)
    await harness.initialize_connected_accounts()

    collector = Collector(harness.pool, db, SchedulerConfig())
    result = await collector.collect_channel_stats(channel)

    assert result is None

    updated = (await db.get_channels())[0]
    assert updated.is_active is False

    saved = await db.get_channel_stats(-100555)
    assert len(saved) == 0


@pytest.mark.anyio
async def test_collect_channel_stats_transient_error_skips_without_deactivation(
    db,
    real_pool_harness_factory,
):
    """A transient failure (timeout, connection drop — anything that is not a
    ValueError/TypeError entity miss) must skip the stats run WITHOUT
    deactivating the channel (#815 review follow-up): the channel is valid,
    only this attempt failed.
    """
    await db.add_channel(Channel(channel_id=-100777, title="Flaky"))
    channel = (await db.get_channels())[0]

    def _resolver(_peer):
        raise RuntimeError("connection reset mid-request")

    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone="+7000",
        client=FakeCliTelethonClient(entity_resolver=_resolver),
    )
    await harness.add_account("+7000", session_string="session-flaky", is_primary=True)
    await harness.initialize_connected_accounts()

    collector = Collector(harness.pool, db, SchedulerConfig())
    result = await collector.collect_channel_stats(channel)

    assert result is None

    updated = (await db.get_channels())[0]
    assert updated.is_active is True

    saved = await db.get_channel_stats(-100777)
    assert len(saved) == 0
