"""Repo-level tests for forward_from_channel_id normalization and cross-channel citations."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.models import Channel, Message


async def _seed(db, channel_id: int, title: str = "Test", username: str | None = None) -> None:
    await db.add_channel(Channel(channel_id=channel_id, title=title, username=username))


async def _fwd_msg(
    db,
    target_ch: int,
    msg_id: int,
    fwd_from: int,
    text: str = "fwd",
    days_ago: int = 0,
) -> None:
    msg = Message(
        channel_id=target_ch,
        message_id=msg_id,
        text=text,
        date=datetime.now(timezone.utc) - timedelta(days=days_ago),
    )
    msg.forward_from_channel_id = fwd_from
    await db.insert_message(msg)


async def _plain_msg(db, target_ch: int, msg_id: int, text: str = "plain") -> None:
    await db.insert_message(
        Message(
            channel_id=target_ch,
            message_id=msg_id,
            text=text,
            date=datetime.now(timezone.utc),
        )
    )


@pytest.mark.asyncio
async def test_citation_positive_ids(db):
    """After fix, forward_from_channel_id stores positive values matching channels.channel_id."""
    await _seed(db, 100500, "Source Chan", "src")
    await _seed(db, 100123, "Target Chan")

    await _fwd_msg(db, 100123, 1, 100500, "fwd msg")

    rows = await db.repos.messages.get_cross_channel_citations(100123, days=30)
    assert len(rows) == 1
    assert rows[0]["source_channel_id"] == 100500
    assert rows[0]["source_title"] == "Source Chan"
    assert rows[0]["citation_count"] == 1


@pytest.mark.asyncio
async def test_citation_multiple_sources(db):
    """Multiple source channels are correctly identified."""
    await _seed(db, 100111, "Source A", "a")
    await _seed(db, 100222, "Source B", "b")
    await _seed(db, 100123, "Target")

    for i in range(5):
        await _fwd_msg(db, 100123, 100 + i, 100111, f"fwd a {i}")
    for i in range(2):
        await _fwd_msg(db, 100123, 200 + i, 100222, f"fwd b {i}")

    rows = await db.repos.messages.get_cross_channel_citations(100123, days=30)
    assert len(rows) == 2
    assert rows[0]["source_channel_id"] == 100111
    assert rows[0]["citation_count"] == 5
    assert rows[1]["source_channel_id"] == 100222
    assert rows[1]["citation_count"] == 2


@pytest.mark.asyncio
async def test_citation_null_source(db):
    """Messages without forward_from_channel_id are excluded."""
    await _seed(db, 100123, "Target")

    await _plain_msg(db, 100123, 1, "no forward")

    rows = await db.repos.messages.get_cross_channel_citations(100123, days=30)
    assert len(rows) == 0


@pytest.mark.asyncio
async def test_citation_unknown_source(db):
    """Forward from unknown channel returns citation with NULL title."""
    await _seed(db, 100123, "Target")

    await _fwd_msg(db, 100123, 1, 999999, "from unknown")

    rows = await db.repos.messages.get_cross_channel_citations(100123, days=30)
    assert len(rows) == 1
    assert rows[0]["source_channel_id"] == 999999
    assert rows[0]["source_title"] is None
    assert rows[0]["citation_count"] == 1


@pytest.mark.asyncio
async def test_citation_limit(db):
    """Limit parameter works correctly."""
    await _seed(db, 100123, "Target")

    for i in range(10):
        await _fwd_msg(db, 100123, 100 + i, 200 + i, f"fwd {i}")

    rows = await db.repos.messages.get_cross_channel_citations(100123, days=30, limit=3)
    assert len(rows) == 3


@pytest.mark.asyncio
async def test_citation_date_filtering(db):
    """Only messages within the date window are included."""
    await _seed(db, 100500, "Source")
    await _seed(db, 100123, "Target")

    # Recent forward (within 7 days)
    await _fwd_msg(db, 100123, 1, 100500, "recent", days_ago=1)
    # Old forward (outside 7 days)
    await _fwd_msg(db, 100123, 2, 100500, "old", days_ago=30)

    rows_7d = await db.repos.messages.get_cross_channel_citations(100123, days=7)
    assert len(rows_7d) == 1
    assert rows_7d[0]["citation_count"] == 1

    rows_90d = await db.repos.messages.get_cross_channel_citations(100123, days=90)
    assert len(rows_90d) == 1
    assert rows_90d[0]["citation_count"] == 2


@pytest.mark.asyncio
async def test_citation_ordering(db):
    """Results ordered by citation_count DESC."""
    await _seed(db, 100111, "Less")
    await _seed(db, 100222, "More")
    await _seed(db, 100123, "Target")

    for i in range(2):
        await _fwd_msg(db, 100123, 100 + i, 100111, f"few {i}")
    for i in range(5):
        await _fwd_msg(db, 100123, 200 + i, 100222, f"many {i}")

    rows = await db.repos.messages.get_cross_channel_citations(100123, days=30)
    assert rows[0]["source_channel_id"] == 100222
    assert rows[0]["citation_count"] == 5
    assert rows[1]["source_channel_id"] == 100111
    assert rows[1]["citation_count"] == 2
