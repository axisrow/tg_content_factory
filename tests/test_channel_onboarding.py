"""Tests for channel onboarding helpers."""

from __future__ import annotations

import pytest

from tg_agent.models import Channel
from tg_agent.services.channel_onboarding import get_existing_channel


class SyncChannelStore:
    def __init__(self, channel: Channel | None):
        self.channel = channel

    def get_by_channel_id(self, channel_id: int) -> Channel | None:
        if self.channel and self.channel.channel_id == channel_id:
            return self.channel
        return None


@pytest.mark.anyio
async def test_get_existing_channel_accepts_sync_store_result():
    channel = Channel(channel_id=100, title="Existing")

    result = await get_existing_channel(SyncChannelStore(channel), 100)

    assert result == channel


@pytest.mark.anyio
async def test_get_existing_channel_accepts_sync_store_none():
    result = await get_existing_channel(SyncChannelStore(None), 100)

    assert result is None


@pytest.mark.anyio
async def test_fetch_channel_meta_silent_throttle_logs_warning(caplog):
    """#1769 review: a throttle skip without propagate_throttle must never be
    silent — the caller sees None and the operator sees WHY in the log."""
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from telethon_floodgate import TelegramRateLimitedError

    from tg_agent.services.channel_onboarding import fetch_channel_meta

    pool = SimpleNamespace(fetch_channel_meta=AsyncMock(
        side_effect=TelegramRateLimitedError("+70000000001", "dialogs", 17)
    ))
    with caplog.at_level("WARNING"):
        result = await fetch_channel_meta(pool, 700, "channel")

    assert result is None
    assert any(
        record.message.startswith("fetch_channel_meta throttled for 700") for record in caplog.records
    )
