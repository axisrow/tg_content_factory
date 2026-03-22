"""Tests for FilterDeletionService."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.database import Database
from src.models import Channel, Message
from src.services.channel_service import ChannelService
from src.services.filter_deletion_service import FilterDeletionService, PurgeResult


@pytest.fixture
async def db(tmp_path):
    """Create in-memory database."""
    db = Database(":memory:")
    await db.initialize()
    yield db
    await db.close()


@pytest.fixture
def channel_service(db):
    """Create channel service mock."""
    service = MagicMock(spec=ChannelService)
    service._db = db
    return service


async def _add_filtered_channel(db: Database, channel_id: int, title: str | None) -> int:
    """Add a channel and mark it as filtered."""
    await db.add_channel(Channel(channel_id=channel_id, title=title))
    # Get the channel to find its pk
    channel = await db.get_channel_by_channel_id(channel_id)
    pk = channel.id
    # Mark as filtered using the repository method
    await db.repos.channels.set_channel_filtered(pk, filtered=True)
    return pk


@pytest.mark.asyncio
async def test_purge_channels_by_pks_empty_list(db):
    """Test purge with empty list returns empty result."""
    service = FilterDeletionService(db)
    result = await service.purge_channels_by_pks([])
    assert result.purged_count == 0
    assert result.skipped_count == 0
    assert result.purged_titles == []


@pytest.mark.asyncio
async def test_purge_channels_by_pks_channel_not_found(db):
    """Test purge skips non-existent channels."""
    service = FilterDeletionService(db)
    result = await service.purge_channels_by_pks([999])
    assert result.purged_count == 0
    assert result.skipped_count == 1


@pytest.mark.asyncio
async def test_purge_channels_by_pks_not_filtered(db):
    """Test purge skips channels that are not filtered."""
    await db.add_channel(Channel(channel_id=100, title="Test"))

    service = FilterDeletionService(db)
    result = await service.purge_channels_by_pks([1])
    assert result.purged_count == 0
    assert result.skipped_count == 1


@pytest.mark.asyncio
async def test_purge_channels_by_pks_success(db):
    """Test successful purge of filtered channel."""
    pk = await _add_filtered_channel(db, channel_id=100, title="Filtered Channel")
    # Add some messages
    await db.insert_message(
        Message(
            channel_id=100,
            message_id=1,
            text="test message",
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
    )

    service = FilterDeletionService(db)
    result = await service.purge_channels_by_pks([pk])
    assert result.purged_count == 1
    assert result.skipped_count == 0
    assert "Filtered Channel" in result.purged_titles
    assert result.total_messages_deleted == 1


@pytest.mark.asyncio
async def test_purge_channels_by_pks_no_title(db):
    """Test purge with channel that has no title."""
    pk = await _add_filtered_channel(db, channel_id=100, title=None)

    service = FilterDeletionService(db)
    result = await service.purge_channels_by_pks([pk])
    assert result.purged_count == 1
    assert f"pk={pk}" in result.purged_titles[0]


@pytest.mark.asyncio
async def test_purge_channels_by_pks_exception_handling(db):
    """Test purge handles exceptions gracefully."""
    # Create a mock db that raises exception
    mock_db = MagicMock(spec=Database)
    mock_db.get_channel_by_pk = AsyncMock(side_effect=Exception("DB error"))

    service = FilterDeletionService(mock_db)
    result = await service.purge_channels_by_pks([1])
    assert result.purged_count == 0
    assert result.skipped_count == 1


@pytest.mark.asyncio
async def test_purge_all_filtered_no_channels(db):
    """Test purge all when no filtered channels exist."""
    await db.add_channel(Channel(channel_id=100, title="Active"))

    service = FilterDeletionService(db)
    result = await service.purge_all_filtered()
    assert result.purged_count == 0


@pytest.mark.asyncio
async def test_purge_all_filtered_with_channels(db):
    """Test purge all with filtered channels."""
    pk1 = await _add_filtered_channel(db, channel_id=100, title="Filtered 1")
    pk2 = await _add_filtered_channel(db, channel_id=200, title="Filtered 2")
    await db.add_channel(Channel(channel_id=300, title="Not Filtered"))

    await db.insert_message(
        Message(
            channel_id=100,
            message_id=1,
            text="msg1",
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
    )
    await db.insert_message(
        Message(
            channel_id=200,
            message_id=2,
            text="msg2",
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
    )

    service = FilterDeletionService(db)
    result = await service.purge_all_filtered()
    assert result.purged_count == 2
    assert result.total_messages_deleted == 2


@pytest.mark.asyncio
async def test_hard_delete_requires_channel_service(db):
    """Test hard delete raises error without channel service."""
    service = FilterDeletionService(db, channel_service=None)

    with pytest.raises(RuntimeError, match="hard_delete requires channel_service"):
        await service.hard_delete_channels_by_pks([1])


@pytest.mark.asyncio
async def test_hard_delete_channel_not_found(db, channel_service):
    """Test hard delete skips non-existent channels."""
    channel_service.get_by_pk = AsyncMock(return_value=None)

    service = FilterDeletionService(db, channel_service=channel_service)
    result = await service.hard_delete_channels_by_pks([999])
    assert result.purged_count == 0
    assert result.skipped_count == 1


@pytest.mark.asyncio
async def test_hard_delete_not_filtered(db, channel_service):
    """Test hard delete skips channels that are not filtered."""
    channel = Channel(channel_id=100, title="Test", is_filtered=False)
    channel_service.get_by_pk = AsyncMock(return_value=channel)

    service = FilterDeletionService(db, channel_service=channel_service)
    result = await service.hard_delete_channels_by_pks([1])
    assert result.purged_count == 0
    assert result.skipped_count == 1


@pytest.mark.asyncio
async def test_hard_delete_success(db, channel_service):
    """Test successful hard delete of filtered channel."""
    channel = Channel(channel_id=100, title="To Delete", is_filtered=True)
    channel_service.get_by_pk = AsyncMock(return_value=channel)
    channel_service.delete = AsyncMock()

    service = FilterDeletionService(db, channel_service=channel_service)
    result = await service.hard_delete_channels_by_pks([1])
    assert result.purged_count == 1
    assert "To Delete" in result.purged_titles
    channel_service.delete.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_hard_delete_no_title(db, channel_service):
    """Test hard delete with channel that has no title."""
    channel = Channel(channel_id=100, title=None, is_filtered=True)
    channel_service.get_by_pk = AsyncMock(return_value=channel)
    channel_service.delete = AsyncMock()

    service = FilterDeletionService(db, channel_service=channel_service)
    result = await service.hard_delete_channels_by_pks([1])
    assert result.purged_count == 1
    assert "pk=1" in result.purged_titles[0]


@pytest.mark.asyncio
async def test_hard_delete_exception_handling(db, channel_service):
    """Test hard delete handles exceptions gracefully."""
    channel_service.get_by_pk = AsyncMock(side_effect=Exception("DB error"))

    service = FilterDeletionService(db, channel_service=channel_service)
    result = await service.hard_delete_channels_by_pks([1])
    assert result.purged_count == 0
    assert result.skipped_count == 1
