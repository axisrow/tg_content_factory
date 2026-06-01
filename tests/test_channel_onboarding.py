"""Tests for channel onboarding helpers."""

from __future__ import annotations

import pytest

from src.models import Channel
from src.services.channel_onboarding import get_existing_channel


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
