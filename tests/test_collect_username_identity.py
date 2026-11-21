"""Collection must refuse a stored @username that now belongs to another peer.

The gateway collection path removed in #1868 verified the identity of a
username-resolved channel (#1769/#1779) and stamped channels nothing can
recover (#1783/#1805). The native path is the only one left, so it has to
refuse a reoccupied username itself instead of storing a foreign channel's
posts under the tracked channel id, and report unrecoverable rows as terminal
instead of spending a resolve on them every round.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from telethon.errors import UsernameNotOccupiedError
from telethon.tl.types import InputPeerChannel, InputPeerUser, PeerChannel

from tests.helpers import AsyncIterMessages, FakeCliTelethonClient, make_mock_pool
from tg_agent.collection_queue import CollectionQueue
from tg_agent.config import SchedulerConfig
from tg_agent.models import Account, Channel
from tg_agent.telegram.collector import Collector

STORED_ID = 123
FOREIGN_ID = 999
PHONE = "+7001"


def _message(msg_id: int, text: str):
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


def _resolver(username_owner, id_answer=None):
    """A session that resolves @ch live; the stored id answers ``id_answer``,
    by default the miss of a session that has never seen it."""

    def resolve(peer):
        if isinstance(peer, PeerChannel):
            return id_answer if id_answer is not None else ValueError("Could not find the input entity")
        return username_owner

    return resolve


async def _collect(
    db,
    real_pool_harness_factory,
    username_owner,
    *,
    username: str | None = "ch",
    dialogs_warm: bool = False,
    id_answer=None,
    idle_phone: str | None = None,
):
    await db.add_channel(Channel(channel_id=STORED_ID, title="Tracked", username=username))
    channel = (await db.get_channels())[0]
    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone=PHONE,
        client=FakeCliTelethonClient(
            entity_resolver=_resolver(username_owner, id_answer),
            iter_messages_factory=lambda *args, **kwargs: AsyncIterMessages([_message(11, "post")]),
        ),
    )
    await harness.add_account(PHONE, session_string="session-a", is_primary=True)
    await harness.initialize_connected_accounts()
    if idle_phone is not None:
        # Active, but not connected: it may well know the channel.
        await db.add_account(Account(phone=idle_phone, session_string="session-b"))
    if dialogs_warm:
        harness.pool.mark_dialogs_fetched(PHONE)
    collector = Collector(harness.pool, db, SchedulerConfig())
    try:
        collected = await collector.collect_single_channel(channel)
    except Exception as exc:  # noqa: BLE001 - the refusal is asserted by its code below
        return channel, collector, None, exc
    return channel, collector, collected, None


async def _stored(db) -> int:
    cursor = await db.execute("SELECT COUNT(*) FROM messages WHERE channel_id = ?", (STORED_ID,))
    row = await cursor.fetchone()
    return int(row[0])


def _vacant_as_telethon_reports_it() -> ValueError:
    """Telethon turns a vacant name into ValueError chained from the RPC error."""
    error = ValueError('No user has "ch" as username')
    error.__cause__ = UsernameNotOccupiedError(request=None)
    return error


# ---------------------------------------------------------------------------
# Identity of a username-resolved channel
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_username_owned_by_the_tracked_channel_is_collected(db, real_pool_harness_factory):
    owner = InputPeerChannel(channel_id=STORED_ID, access_hash=1)

    _, _, collected, refusal = await _collect(db, real_pool_harness_factory, owner)

    assert refusal is None
    assert collected == 1
    assert await _stored(db) == 1


@pytest.mark.anyio
async def test_username_reoccupied_by_another_channel_stores_nothing(db, real_pool_harness_factory):
    owner = InputPeerChannel(channel_id=FOREIGN_ID, access_hash=1)

    _, _, _, refusal = await _collect(db, real_pool_harness_factory, owner)

    stored = await _stored(db)
    assert stored == 0, f"{stored} foreign message(s) stored under channel {STORED_ID}"
    assert getattr(refusal, "code", None) == "UsernameChannelIdMismatch"


@pytest.mark.anyio
async def test_username_reoccupied_by_a_user_with_a_colliding_id_stores_nothing(db, real_pool_harness_factory):
    """Telegram id ranges overlap: a user can carry the stored channel's number."""
    owner = InputPeerUser(user_id=STORED_ID, access_hash=1)

    _, _, _, refusal = await _collect(db, real_pool_harness_factory, owner)

    stored = await _stored(db)
    assert stored == 0, f"{stored} message(s) of a user dialog stored under channel {STORED_ID}"
    assert getattr(refusal, "code", None) == "UsernameChannelIdMismatch"


@pytest.mark.anyio
async def test_refusal_stamps_the_channel_and_sends_it_to_review(db, real_pool_harness_factory):
    owner = InputPeerChannel(channel_id=FOREIGN_ID, access_hash=1)
    channel, collector, _, refusal = await _collect(db, real_pool_harness_factory, owner)
    assert refusal is not None
    task_id = await db.repos.tasks.create_collection_task_if_not_active(
        STORED_ID, "Tracked", channel_username="ch"
    )
    assert task_id is not None
    await db.repos.tasks.update_collection_task(task_id, "running")
    queue = CollectionQueue(collector, db)
    try:
        await queue._handle_collection_exception(
            refusal, task_id=task_id, channel=channel, force=False, full=False
        )

        row = await db.repos.channels.get_channel_by_pk(channel.id)
        assert row is not None
        assert row.terminal_code == "UsernameChannelIdMismatch"
        assert row.terminal_at is not None
        assert row.needs_review is True
        assert "UsernameChannelIdMismatch" in (row.review_reason or "")
        task = await db.get_collection_task(task_id)
        assert task is not None and task.status == "failed"
    finally:
        await queue.shutdown()


# ---------------------------------------------------------------------------
# Rows nothing can recover
# ---------------------------------------------------------------------------


@pytest.mark.anyio
@pytest.mark.parametrize(
    "vacant",
    [UsernameNotOccupiedError(request=None), _vacant_as_telethon_reports_it()],
    ids=["rpc-error", "telethon-valueerror"],
)
async def test_vacant_username_with_an_unknown_id_is_terminal(db, real_pool_harness_factory, vacant):
    _, _, _, refusal = await _collect(db, real_pool_harness_factory, vacant)

    assert getattr(refusal, "code", None) == "UsernameNotOccupiedError"
    assert await _stored(db) == 0


@pytest.mark.anyio
async def test_numeric_id_no_account_knows_is_terminal(db, real_pool_harness_factory):
    """No username to warm the entity from and the only account misses the id.

    The account's dialog cache is already warm, as in a long-lived worker: a cold
    account spends its one dialog sweep on the pre-warm, the resolver's own sweep
    is then deferred by the rate gate, and the verdict arrives on the retry.
    """
    _, _, _, refusal = await _collect(
        db,
        real_pool_harness_factory,
        ValueError("Could not find the input entity"),
        username=None,
        dialogs_warm=True,
    )

    assert getattr(refusal, "code", None) == "HistoryNumericUnresolved"
    assert await _stored(db) == 0


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("username", "username_owner"),
    [(None, ValueError("Could not find the input entity")), ("ch", _vacant_as_telethon_reports_it())],
    ids=["numeric-id", "vacant-username"],
)
async def test_unknown_id_is_not_terminal_while_an_account_is_not_connected(
    db, real_pool_harness_factory, username, username_owner
):
    """The account that knows the channel may be the one that failed to connect."""
    _, _, _, refusal = await _collect(
        db, real_pool_harness_factory, username_owner, username=username, dialogs_warm=True, idle_phone="+7002"
    )

    assert refusal is None
    assert await _stored(db) == 0


@pytest.mark.anyio
async def test_a_failing_request_is_not_a_terminal_miss(db, real_pool_harness_factory):
    """Telethon reports a request that kept failing (-500) as a ValueError too."""
    _, _, _, refusal = await _collect(
        db,
        real_pool_harness_factory,
        ValueError("Could not find the input entity"),
        username=None,
        dialogs_warm=True,
        id_answer=ValueError("Request was unsuccessful 6 time(s)"),
    )

    assert refusal is None


@pytest.mark.anyio
async def test_reoccupied_username_of_a_channel_whose_id_resolves_goes_to_review(db, real_pool_harness_factory):
    """The tracked channel lives on under a new name: the operator reviews the rename."""
    card = SimpleNamespace(id=STORED_ID, username="renamed", title="Tracked")

    channel, _, _, refusal = await _collect(
        db, real_pool_harness_factory, InputPeerChannel(channel_id=FOREIGN_ID, access_hash=1), id_answer=card
    )

    assert refusal is None
    assert await _stored(db) == 0
    row = await db.repos.channels.get_channel_by_pk(channel.id)
    assert row is not None and row.username == "renamed"


# ---------------------------------------------------------------------------
# Rediscovery verdict: only a peer miss is evidence about the channel
# ---------------------------------------------------------------------------


async def _verdict(get_client_by_phone, *, idle: tuple[str, ...] = ()) -> dict[str, bool]:
    pool = make_mock_pool()
    pool.get_client_by_phone = get_client_by_phone
    pool.connected_phones = MagicMock(return_value={PHONE, "+7002"})
    pool.is_dialogs_fetched = MagicMock(return_value=True)
    pool.release_client = AsyncMock()
    db = MagicMock()
    db.get_account_summaries = AsyncMock(return_value=[SimpleNamespace(phone=p) for p in (PHONE, "+7002", *idle)])
    collector = Collector(pool, db, SchedulerConfig())
    verdict: dict[str, bool] = {}
    assert await collector._discover_phone_for_channel(STORED_ID, PHONE, verdict=verdict) is None
    return verdict


def _missing_client(failure):
    client = AsyncMock()
    client.get_entity = AsyncMock(side_effect=failure)
    return AsyncMock(return_value=(client, "+7002"))


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("failure", "conclusive"),
    [
        (ValueError("no access"), True),
        (RuntimeError("network down"), False),
        (ValueError("Request was unsuccessful 6 time(s)"), False),
    ],
    ids=["peer-miss", "transient-error", "failing-request"],
)
async def test_discovery_verdict_counts_only_peer_misses(failure, conclusive):
    verdict = await _verdict(_missing_client(failure))

    assert verdict == {"conclusive": conclusive}


@pytest.mark.anyio
async def test_discovery_verdict_is_inconclusive_when_an_account_is_unavailable():
    verdict = await _verdict(AsyncMock(return_value=None))

    assert verdict == {"conclusive": False}


@pytest.mark.anyio
async def test_discovery_verdict_is_inconclusive_while_an_active_account_is_not_connected():
    verdict = await _verdict(_missing_client(ValueError("no access")), idle=("+7003",))

    assert verdict == {"conclusive": False}
