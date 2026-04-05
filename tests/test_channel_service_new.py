"""Tests for ChannelService."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import Channel
from src.services.channel_service import ChannelService


def _make_channel(pk=1, channel_id=100, title="Test", is_active=True, username="testch"):
    return Channel(
        id=pk, channel_id=channel_id, title=title, is_active=is_active, username=username
    )


@pytest.mark.asyncio
async def test_list_for_page():
    ch = _make_channel()
    channels = MagicMock()
    channels.list_channels_with_counts = AsyncMock(return_value=[ch])
    channels.get_latest_and_previous_stats = AsyncMock(return_value=({}, {}))
    pool = MagicMock()

    svc = ChannelService(channels, pool, None)
    result = await svc.list_for_page()
    assert result[0] == [ch]
    assert result[1] == {}
    assert result[2] == {}


@pytest.mark.asyncio
async def test_add_by_identifier_success():
    channels = MagicMock()
    channels.add_channel = AsyncMock(return_value=1)
    pool = MagicMock()
    pool.resolve_channel = AsyncMock(return_value={
        "channel_id": -100123,
        "title": "Test Channel",
        "username": "testch",
        "channel_type": "channel",
    })
    pool.fetch_channel_meta = AsyncMock(return_value={
        "about": "Test about",
        "linked_chat_id": None,
        "has_comments": False,
    })

    svc = ChannelService(channels, pool, None)
    result = await svc.add_by_identifier("@testch")
    assert result is True
    channels.add_channel.assert_called_once()


@pytest.mark.asyncio
async def test_add_by_identifier_not_found():
    channels = MagicMock()
    pool = MagicMock()
    pool.resolve_channel = AsyncMock(return_value=None)

    svc = ChannelService(channels, pool, None)
    result = await svc.add_by_identifier("@notfound")
    assert result is False


@pytest.mark.asyncio
async def test_get_dialogs_with_added_flags():
    existing = [_make_channel(pk=1, channel_id=100)]
    channels = MagicMock()
    channels.list_channels = AsyncMock(return_value=existing)
    pool = MagicMock()
    pool.get_dialogs = AsyncMock(return_value=[
        {"channel_id": 100, "title": "Test", "username": "testch"},
        {"channel_id": 200, "title": "Other", "username": "other"},
    ])

    svc = ChannelService(channels, pool, None)
    result = await svc.get_dialogs_with_added_flags()
    assert len(result) == 2
    assert result[0]["already_added"] is True
    assert result[1]["already_added"] is False


@pytest.mark.asyncio
async def test_add_bulk_by_dialog_ids():
    channels = MagicMock()
    channels.add_channel = AsyncMock(return_value=1)
    pool = MagicMock()
    pool.get_dialogs = AsyncMock(return_value=[
        {"channel_id": 100, "title": "Test", "username": "testch", "channel_type": "channel"},
        {"channel_id": 200, "title": "Other", "username": "other", "channel_type": "channel"},
    ])

    svc = ChannelService(channels, pool, None)
    await svc.add_bulk_by_dialog_ids(["100"])
    channels.add_channel.assert_called_once()
    # Should only add the requested channel
    added = channels.add_channel.call_args[0][0]
    assert added.channel_id == 100


@pytest.mark.asyncio
async def test_get_my_dialogs():
    existing = [_make_channel(pk=1, channel_id=100)]
    channels = MagicMock()
    channels.list_channels = AsyncMock(return_value=existing)
    pool = MagicMock()
    pool.get_dialogs_for_phone = AsyncMock(return_value=[
        {"channel_id": 100, "title": "Test"},
        {"channel_id": 200, "title": "Other"},
    ])

    svc = ChannelService(channels, pool, None)
    result = await svc.get_my_dialogs("+1234567890")
    assert len(result) == 2
    assert result[0]["already_added"] is True
    assert result[1]["already_added"] is False


@pytest.mark.asyncio
async def test_toggle_deactivates():
    ch = _make_channel(is_active=True)
    channels = MagicMock()
    channels.get_by_pk = AsyncMock(return_value=ch)
    channels.set_active = AsyncMock()
    pool = MagicMock()

    svc = ChannelService(channels, pool, None)
    await svc.toggle(pk=1)
    channels.set_active.assert_called_once_with(1, False)


@pytest.mark.asyncio
async def test_toggle_activates():
    ch = _make_channel(is_active=False)
    channels = MagicMock()
    channels.get_by_pk = AsyncMock(return_value=ch)
    channels.set_active = AsyncMock()
    pool = MagicMock()

    svc = ChannelService(channels, pool, None)
    await svc.toggle(pk=1)
    channels.set_active.assert_called_once_with(1, True)


@pytest.mark.asyncio
async def test_toggle_not_found():
    channels = MagicMock()
    channels.get_by_pk = AsyncMock(return_value=None)
    pool = MagicMock()

    svc = ChannelService(channels, pool, None)
    await svc.toggle(pk=999)  # Should not raise


@pytest.mark.asyncio
async def test_delete_cancels_tasks():
    ch = _make_channel()
    channels = MagicMock()
    channels.get_by_pk = AsyncMock(return_value=ch)
    channels.get_active_collection_tasks_for_channel = AsyncMock(return_value=[])
    channels.delete_channel = AsyncMock()
    pool = MagicMock()
    queue = MagicMock()
    queue.cancel_task = AsyncMock()

    svc = ChannelService(channels, pool, queue)
    await svc.delete(pk=1)
    channels.delete_channel.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_get_by_pk_found():
    ch = _make_channel()
    channels = MagicMock()
    channels.get_by_pk = AsyncMock(return_value=ch)

    svc = ChannelService(channels, MagicMock(), None)
    result = await svc.get_by_pk(pk=1)
    assert result == ch


@pytest.mark.asyncio
async def test_get_by_pk_not_found():
    channels = MagicMock()
    channels.get_by_pk = AsyncMock(return_value=None)

    svc = ChannelService(channels, MagicMock(), None)
    result = await svc.get_by_pk(pk=999)
    assert result is None


@pytest.mark.asyncio
async def test_refresh_channel_meta_success():
    ch = _make_channel()
    channels = MagicMock()
    channels.get_by_pk = AsyncMock(return_value=ch)
    channels.update_channel_full_meta = AsyncMock()
    pool = MagicMock()
    pool.fetch_channel_meta = AsyncMock(return_value={
        "about": "New about",
        "linked_chat_id": 12345,
        "has_comments": True,
    })

    svc = ChannelService(channels, pool, None)
    result = await svc.refresh_channel_meta(pk=1)
    assert result is True
    channels.update_channel_full_meta.assert_called_once()


@pytest.mark.asyncio
async def test_refresh_channel_meta_not_found():
    channels = MagicMock()
    channels.get_by_pk = AsyncMock(return_value=None)

    pool = MagicMock()

    svc = ChannelService(channels, pool, None)
    result = await svc.refresh_channel_meta(pk=999)
    assert result is False


@pytest.mark.asyncio
async def test_refresh_channel_meta_no_meta():
    ch = _make_channel()
    channels = MagicMock()
    channels.get_by_pk = AsyncMock(return_value=ch)
    pool = MagicMock()
    pool.fetch_channel_meta = AsyncMock(return_value=None)

    svc = ChannelService(channels, pool, None)
    result = await svc.refresh_channel_meta(pk=1)
    assert result is False


@pytest.mark.asyncio
async def test_refresh_all_channel_meta():
    ch1 = _make_channel(pk=1, channel_id=100)
    ch2 = _make_channel(pk=2, channel_id=200)
    channels = MagicMock()
    channels.list_channels = AsyncMock(return_value=[ch1, ch2])
    channels.get_by_pk = AsyncMock(side_effect=[ch1, ch2])
    channels.update_channel_full_meta = AsyncMock()
    pool = MagicMock()
    pool.fetch_channel_meta = AsyncMock(return_value={
        "about": "Test",
        "linked_chat_id": None,
        "has_comments": False,
    })

    svc = ChannelService(channels, pool, None)
    ok, failed = await svc.refresh_all_channel_meta()
    assert ok == 2
    assert failed == 0


@pytest.mark.asyncio
async def test_leave_dialogs():
    pool = MagicMock()
    pool.leave_channels = AsyncMock(return_value={100: True, 200: False})
    channels = MagicMock()

    svc = ChannelService(channels, pool, None)
    result = await svc.leave_dialogs("+1234567890", [(100, "Test"), (200, "Other")])
    assert result == {100: True, 200: False}
