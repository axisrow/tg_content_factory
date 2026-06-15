"""Tests for collector cancellation isolation + flood handling (audit #835/6, #835/16)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import SchedulerConfig
from src.telegram.collector import Collector
from src.telegram.flood_wait import FloodWaitInfo, HandledFloodWaitError


def _collector() -> Collector:
    return Collector(MagicMock(), MagicMock(), SchedulerConfig())


@pytest.mark.anyio
async def test_cancel_stats_does_not_cancel_channel_collection():
    """STATS_ALL cancel must not abort in-flight channel collection (audit #835/6)."""
    c = _collector()
    await c.cancel_stats()
    assert c._is_stats_cancelled() is True
    assert c._is_collection_cancelled() is False
    assert c.is_cancelled is False


@pytest.mark.anyio
async def test_global_cancel_stops_both_collection_and_stats():
    c = _collector()
    await c.cancel()
    assert c._is_collection_cancelled() is True
    assert c._is_stats_cancelled() is True
    assert c.is_cancelled is True


@pytest.mark.anyio
async def test_discover_phone_handles_handled_flood_wait():
    """resolve raises HandledFloodWaitError (not raw FloodWaitError) — the handler
    must catch it and move on, not crash on a dead branch (audit #835/16)."""
    c = _collector()
    pool = c._pool
    pool.connected_phones = MagicMock(return_value={"+1"})
    session = MagicMock()
    info = FloodWaitInfo(
        operation="resolve",
        phone="+1",
        wait_seconds=5,
        next_available_at_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
        detail="flood 5s",
    )
    session.resolve_entity = AsyncMock(side_effect=HandledFloodWaitError(info))
    pool.get_client_by_phone = AsyncMock(return_value=(session, "+1"))
    pool.is_dialogs_fetched = MagicMock(return_value=True)
    pool.release_client = AsyncMock()

    with patch("src.telegram.collector.adapt_transport_session", lambda s, **k: s):
        result = await c._discover_phone_for_channel(123, exclude="+2")

    assert result is None
    pool.release_client.assert_awaited()
