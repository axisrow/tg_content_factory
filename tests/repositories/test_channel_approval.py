"""Approval queue core and gate (#1313).

Search discovery is not operator approval: new channels found by search land
in the approval queue (``approval_state='pending'``) and are invisible to the
standard channel readers until the owner approves them.
"""

from __future__ import annotations

from datetime import datetime, timezone

from tg_agent.database.bundles import SearchBundle
from tg_agent.models import Channel, Message
from tg_agent.search.persistence import SearchPersistence
from tg_agent.services.collection_service import CollectionService


def _private_channel(channel_id: int) -> Channel:
    """A channel without a username — the private group case from owner decision #2."""
    return Channel(channel_id=channel_id, title="Private group")


def _msg(channel_id: int, message_id: int) -> Message:
    return Message(
        channel_id=channel_id,
        message_id=message_id,
        text="hit",
        date=datetime.now(timezone.utc),
    )


def _search_persistence(db) -> SearchPersistence:
    return SearchPersistence(SearchBundle.from_database(db), None)


async def test_search_does_not_autocollect_private_group(db):
    """A search-discovered private channel must not be collectable: it stays pending."""
    persistence = _search_persistence(db)
    await persistence.cache_search_results(
        {500: _private_channel(500)},
        [_msg(500, 1)],
        "+1",
        "query",
    )

    service = CollectionService(db, None)
    result = await service.enqueue_all_channels()
    assert result.total_candidates == 0
    assert result.queued_count == 0

    pending = await db.repos.channels.list_pending_approval()
    assert [c.channel_id for c in pending] == [500]
    assert all(c.approval_state == "pending" for c in pending)
    assert await db.repos.channels.get_channels() == []


async def test_search_does_not_downgrade_approved_channel(db):
    """An already-approved channel met again in search results stays approved."""
    await db.add_channel(Channel(channel_id=600, title="Approved", username="public"))
    persistence = _search_persistence(db)

    await persistence.cache_search_results(
        {600: Channel(channel_id=600, title="Approved", username="public")},
        [],
        "+1",
        "query",
    )

    channel = await db.repos.channels.get_channel_by_channel_id(600)
    assert channel is not None
    assert channel.approval_state == "approved"
    assert channel.is_active is True


async def test_add_channel_conflict_preserves_approval_state(db):
    """Upserting metadata for an existing channel never overwrites approval_state."""
    pk = await db.add_channel(
        Channel(channel_id=700, title="Pending", approval_state="pending")
    )

    await db.add_channel(Channel(channel_id=700, title="Refreshed"))

    channel = await db.repos.channels.get_channel_by_pk(pk)
    assert channel is not None
    assert channel.title == "Refreshed"
    assert channel.approval_state == "pending"


async def test_gate_hides_pending_from_channel_reads(db):
    """``get_channels*`` exclude pending unconditionally — no parameter, no opt-out."""
    await db.add_channel(Channel(channel_id=701, title="Live"))
    await db.add_channel(
        Channel(channel_id=702, title="Pending", approval_state="pending")
    )

    assert [c.channel_id for c in await db.repos.channels.get_channels()] == [701]
    counts = await db.repos.channels.get_channels_with_counts()
    assert [c.channel_id for c in counts] == [701]
    assert [c.channel_id for c in await db.repos.channels.list_pending_approval()] == [702]


async def test_approve_channel_sets_approved_and_human_origin(db):
    """Approval is the protected human decision: approval_state + active_origin in one write."""
    pk = await db.add_channel(
        Channel(channel_id=703, title="Queued", approval_state="pending")
    )

    assert await db.repos.channels.approve_channel(pk, actor="cli", reason="ok") == 1

    channel = await db.repos.channels.get_channel_by_pk(pk)
    assert channel is not None
    assert channel.approval_state == "approved"
    assert channel.active_origin == "human"
    assert [c.channel_id for c in await db.repos.channels.get_channels()] == [703]

    history = await db.repos.decisions.history("channel", 703, field="approval_state")
    assert [(d.new_value, d.origin, d.actor) for d in history] == [
        ("approved", "human", "cli")
    ]
    # Approving twice is a no-op: the channel is no longer pending.
    assert await db.repos.channels.approve_channel(pk, actor="cli") == 0


async def test_reject_channel_cleans_up(db):
    """Rejection removes the pending row together with its cached messages."""
    pk = await db.add_channel(
        Channel(channel_id=704, title="Spam", approval_state="pending")
    )
    await db.repos.messages.insert_message(_msg(704, 1))

    assert await db.repos.channels.reject_channel(pk, actor="cli", reason="spam") == 1

    assert await db.repos.channels.get_channel_by_pk(pk) is None
    assert await db.repos.channels.get_channel_by_channel_id(704) is None
    cur = await db.db.execute("SELECT COUNT(*) FROM messages WHERE channel_id = 704")
    assert (await cur.fetchone())[0] == 0

    history = await db.repos.decisions.history("channel", 704, field="approval_state")
    assert [(d.new_value, d.origin, d.actor) for d in history] == [
        ("rejected", "human", "cli")
    ]
    # Unknown pk → nothing to reject.
    assert await db.repos.channels.reject_channel(10**9, actor="cli") == 0
