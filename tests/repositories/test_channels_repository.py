"""Tests for ChannelsRepository."""

from __future__ import annotations

import pytest

from src.database.repositories.channels import ChannelsRepository
from src.models import Channel



def make_channel(channel_id: int, title: str = "Test Channel", **kwargs) -> Channel:
    """Create a test Channel."""
    return Channel(channel_id=channel_id, title=title, **kwargs)


# add_channel tests


async def test_add_channel_insert(channels_repo):
    """Test inserting a new channel."""
    channel = make_channel(12345)
    pk = await channels_repo.add_channel(channel)
    assert pk > 0

    channels = await channels_repo.get_channels()
    assert len(channels) == 1
    assert channels[0].channel_id == 12345


async def test_add_channel_upsert_on_conflict(channels_repo):
    """Test that add_channel updates existing channel on channel_id conflict."""
    channel1 = make_channel(12345, title="Original Title", is_active=True)
    await channels_repo.add_channel(channel1)

    channel2 = make_channel(12345, title="Updated Title", is_active=False)
    await channels_repo.add_channel(channel2)

    channels = await channels_repo.get_channels()
    assert len(channels) == 1
    assert channels[0].title == "Updated Title"
    assert channels[0].is_active is False


async def test_add_channel_with_all_fields(channels_repo):
    """Test inserting channel with all optional fields."""
    channel = make_channel(
        12345,
        title="Full Channel",
        username="testchannel",
        channel_type="supergroup",
        is_active=False,
    )
    await channels_repo.add_channel(channel)

    channels = await channels_repo.get_channels()
    assert channels[0].username == "testchannel"
    assert channels[0].channel_type == "supergroup"
    assert channels[0].is_active is False


# get_channels tests


async def test_get_channels_empty(channels_repo):
    """Test getting channels when none exist."""
    channels = await channels_repo.get_channels()
    assert channels == []


async def test_get_channels_active_only(channels_repo):
    """Test filtering by active_only."""
    await channels_repo.add_channel(make_channel(1, is_active=True))
    await channels_repo.add_channel(make_channel(2, is_active=False))
    await channels_repo.add_channel(make_channel(3, is_active=True))

    all_channels = await channels_repo.get_channels()
    assert len(all_channels) == 3

    active_channels = await channels_repo.get_channels(active_only=True)
    assert len(active_channels) == 2
    ids = {c.channel_id for c in active_channels}
    assert ids == {1, 3}


async def test_get_channels_include_filtered(channels_repo):
    """Test filtering by include_filtered."""
    await channels_repo.add_channel(make_channel(1))
    await channels_repo.add_channel(make_channel(2))

    # Mark one as filtered
    channels = await channels_repo.get_channels()
    await channels_repo.set_channel_filtered(channels[0].id, True)

    # Default includes filtered
    all_channels = await channels_repo.get_channels()
    assert len(all_channels) == 2

    # Exclude filtered
    unfiltered = await channels_repo.get_channels(include_filtered=False)
    assert len(unfiltered) == 1
    assert unfiltered[0].channel_id == 2


async def test_get_channels_ordering(channels_repo):
    """Test that channels are ordered by id ASC."""
    await channels_repo.add_channel(make_channel(300))
    await channels_repo.add_channel(make_channel(100))
    await channels_repo.add_channel(make_channel(200))

    channels = await channels_repo.get_channels()
    assert channels[0].channel_id == 300  # First inserted = lowest id
    assert channels[1].channel_id == 100
    assert channels[2].channel_id == 200


# get_channel_by_pk tests


async def test_get_channel_by_pk_found(channels_repo):
    """Test getting channel by primary key."""
    channel = make_channel(12345, title="Test")
    pk = await channels_repo.add_channel(channel)

    result = await channels_repo.get_channel_by_pk(pk)
    assert result is not None
    assert result.channel_id == 12345
    assert result.title == "Test"


async def test_get_channel_by_pk_not_found(channels_repo):
    """Test getting non-existent channel by pk returns None."""
    result = await channels_repo.get_channel_by_pk(999)
    assert result is None


# get_channel_by_channel_id tests


async def test_get_channel_by_channel_id_found(channels_repo):
    """Test getting channel by channel_id."""
    await channels_repo.add_channel(make_channel(12345, title="Test"))

    result = await channels_repo.get_channel_by_channel_id(12345)
    assert result is not None
    assert result.title == "Test"


async def test_get_channel_by_channel_id_not_found(channels_repo):
    """Test getting non-existent channel by channel_id returns None."""
    result = await channels_repo.get_channel_by_channel_id(999)
    assert result is None


# get_channels_with_counts tests


async def test_get_channels_with_counts_empty(channels_repo):
    """Test getting channels with counts when none exist."""
    channels = await channels_repo.get_channels_with_counts()
    assert channels == []


async def test_get_channels_with_counts_no_messages(channels_repo):
    """Test getting channels with counts when no messages exist."""
    await channels_repo.add_channel(make_channel(1))
    await channels_repo.add_channel(make_channel(2))

    channels = await channels_repo.get_channels_with_counts()
    assert len(channels) == 2
    assert all(c.message_count == 0 for c in channels)


async def test_get_channels_with_counts_with_messages(channels_repo):
    """Test getting channels with message counts."""
    await channels_repo.add_channel(make_channel(1))
    await channels_repo.add_channel(make_channel(2))

    # Insert some messages
    await channels_repo._db.executemany(
        "INSERT INTO messages (channel_id, message_id, date) VALUES (?, ?, datetime('now'))",
        [(1, 100), (1, 101), (2, 200)],
    )
    await channels_repo._db.commit()

    channels = await channels_repo.get_channels_with_counts()
    assert len(channels) == 2
    counts = {c.channel_id: c.message_count for c in channels}
    assert counts[1] == 2
    assert counts[2] == 1


async def test_get_channels_with_counts_filters(channels_repo):
    """Test that active_only and include_filtered work with counts."""
    await channels_repo.add_channel(make_channel(1, is_active=True))
    await channels_repo.add_channel(make_channel(2, is_active=False))
    await channels_repo.add_channel(make_channel(3, is_active=True))

    # Mark channel 3 as filtered
    channels = await channels_repo.get_channels()
    await channels_repo.set_channel_filtered(next(c.id for c in channels if c.channel_id == 3), True)

    result = await channels_repo.get_channels_with_counts(active_only=True, include_filtered=False)
    assert len(result) == 1
    assert result[0].channel_id == 1


# update_channel_last_id tests


async def test_update_channel_last_id(channels_repo):
    """Test updating last_collected_id."""
    await channels_repo.add_channel(make_channel(12345))
    await channels_repo.update_channel_last_id(12345, 500)

    channel = await channels_repo.get_channel_by_channel_id(12345)
    assert channel.last_collected_id == 500


async def test_update_channel_last_id_overwrites(channels_repo):
    """Test that update_channel_last_id overwrites previous value."""
    await channels_repo.add_channel(make_channel(12345))
    await channels_repo.update_channel_last_id(12345, 100)
    await channels_repo.update_channel_last_id(12345, 200)

    channel = await channels_repo.get_channel_by_channel_id(12345)
    assert channel.last_collected_id == 200


# set_channel_active tests


async def test_set_channel_active_deactivate(channels_repo):
    """Test deactivating a channel."""
    await channels_repo.add_channel(make_channel(1, is_active=True))
    channels = await channels_repo.get_channels()
    pk = channels[0].id

    await channels_repo.set_channel_active(pk, False)

    channel = await channels_repo.get_channel_by_pk(pk)
    assert channel.is_active is False


async def test_set_channel_active_activate(channels_repo):
    """Test activating a channel."""
    await channels_repo.add_channel(make_channel(1, is_active=False))
    channels = await channels_repo.get_channels()
    pk = channels[0].id

    await channels_repo.set_channel_active(pk, True)

    channel = await channels_repo.get_channel_by_pk(pk)
    assert channel.is_active is True


# set_channel_filtered tests


async def test_set_channel_filtered_true(channels_repo):
    """Test marking channel as filtered."""
    await channels_repo.add_channel(make_channel(1))
    channels = await channels_repo.get_channels()
    pk = channels[0].id

    await channels_repo.set_channel_filtered(pk, True)

    channel = await channels_repo.get_channel_by_pk(pk)
    assert channel.is_filtered is True
    assert channel.filter_flags == "manual"


async def test_set_channel_filtered_false(channels_repo):
    """Test unmarking channel as filtered."""
    await channels_repo.add_channel(make_channel(1))
    channels = await channels_repo.get_channels()
    pk = channels[0].id

    await channels_repo.set_channel_filtered(pk, True)
    await channels_repo.set_channel_filtered(pk, False)

    channel = await channels_repo.get_channel_by_pk(pk)
    assert channel.is_filtered is False
    assert channel.filter_flags == ""


# set_filtered_bulk tests


async def test_set_filtered_bulk_empty(channels_repo):
    """Test set_filtered_bulk with empty list."""
    count = await channels_repo.set_filtered_bulk([])
    assert count == 0


async def test_set_filtered_bulk_multiple(channels_repo):
    """Test bulk updating filter flags."""
    await channels_repo.add_channel(make_channel(1))
    await channels_repo.add_channel(make_channel(2))
    await channels_repo.add_channel(make_channel(3))

    updates = [
        (1, "low_uniqueness"),
        (2, "cross_channel_spam,non_cyrillic"),
    ]
    count = await channels_repo.set_filtered_bulk(updates)
    assert count == 2

    c1 = await channels_repo.get_channel_by_channel_id(1)
    c2 = await channels_repo.get_channel_by_channel_id(2)
    c3 = await channels_repo.get_channel_by_channel_id(3)

    assert c1.is_filtered is True
    assert c1.filter_flags == "low_uniqueness"
    assert c2.is_filtered is True
    assert c2.filter_flags == "cross_channel_spam,non_cyrillic"
    assert c3.is_filtered is False


async def test_set_filtered_bulk_nonexistent_channel(channels_repo):
    """Test that nonexistent channels in bulk update don't affect count."""
    updates = [(999, "test_flag")]
    count = await channels_repo.set_filtered_bulk(updates)
    assert count == 0


# reset_all_filters tests


async def test_reset_all_filters(channels_repo):
    """Test resetting all channel filters."""
    await channels_repo.add_channel(make_channel(1))
    await channels_repo.add_channel(make_channel(2))

    # Set filters
    channels = await channels_repo.get_channels()
    for c in channels:
        await channels_repo.set_channel_filtered(c.id, True)

    count = await channels_repo.reset_all_filters()
    assert count == 2

    channels = await channels_repo.get_channels()
    assert all(c.is_filtered is False for c in channels)
    assert all(c.filter_flags == "" for c in channels)


async def test_reset_all_filters_empty(channels_repo):
    """Test resetting filters when no channels exist."""
    count = await channels_repo.reset_all_filters()
    assert count == 0


# update_channel_meta tests


async def test_update_channel_meta(channels_repo):
    """Test updating channel metadata."""
    await channels_repo.add_channel(make_channel(1, title="Old Title", username="old_name"))
    await channels_repo.update_channel_meta(1, username="new_name", title="New Title")

    channel = await channels_repo.get_channel_by_channel_id(1)
    assert channel.title == "New Title"
    assert channel.username == "new_name"


async def test_update_channel_meta_null_values(channels_repo):
    """Test updating channel metadata with None values."""
    await channels_repo.add_channel(make_channel(1, title="Title", username="username"))
    await channels_repo.update_channel_meta(1, username=None, title=None)

    channel = await channels_repo.get_channel_by_channel_id(1)
    assert channel.title is None
    assert channel.username is None


# update_channel_full_meta tests


async def test_update_channel_full_meta(channels_repo):
    """Test updating full channel metadata."""
    await channels_repo.add_channel(make_channel(1))
    await channels_repo.update_channel_full_meta(
        1,
        about="Test description",
        linked_chat_id=12345,
        has_comments=True,
    )

    channel = await channels_repo.get_channel_by_channel_id(1)
    assert channel.about == "Test description"
    assert channel.linked_chat_id == 12345
    assert channel.has_comments is True


async def test_update_channel_full_meta_null_values(channels_repo):
    """Test updating full channel metadata with None values."""
    await channels_repo.add_channel(make_channel(1, about="Old", linked_chat_id=999, has_comments=True))
    await channels_repo.update_channel_full_meta(1, about=None, linked_chat_id=None, has_comments=False)

    channel = await channels_repo.get_channel_by_channel_id(1)
    assert channel.about is None
    assert channel.linked_chat_id is None
    assert channel.has_comments is False


# add_channel_with_meta_fields tests


async def test_add_channel_with_meta_fields(channels_repo):
    """Test inserting channel with metadata fields."""
    channel = make_channel(
        12345,
        title="Full Channel",
        about="Channel description",
        linked_chat_id=54321,
        has_comments=True,
    )
    pk = await channels_repo.add_channel(channel)
    assert pk > 0

    result = await channels_repo.get_channel_by_pk(pk)
    assert result.about == "Channel description"
    assert result.linked_chat_id == 54321
    assert result.has_comments is True


async def test_add_channel_upsert_preserves_meta(channels_repo):
    """Test that upsert updates metadata on conflict."""
    ch1 = make_channel(
        12345,
        title="Original",
        about="Original description",
        linked_chat_id=111,
        has_comments=False,
    )
    await channels_repo.add_channel(ch1)

    ch2 = make_channel(
        12345,
        title="Updated",
        about="Updated description",
        linked_chat_id=222,
        has_comments=True,
    )
    await channels_repo.add_channel(ch2)

    result = await channels_repo.get_channel_by_channel_id(12345)
    assert result.title == "Updated"
    assert result.about == "Updated description"
    assert result.linked_chat_id == 222
    assert result.has_comments is True


async def test_map_channel_missing_meta_columns(channels_repo):
    """Test that _map_channel degrades gracefully when metadata columns are absent."""
    # Add a channel with full fields
    await channels_repo.add_channel(make_channel(1, title="Test"))
    # Simulate old row without metadata columns by selecting specific columns
    cur = await channels_repo._db.execute(
        "SELECT id, channel_id, title, username, channel_type, is_active,"
        " is_filtered, filter_flags, last_collected_id, added_at"
        " FROM channels WHERE channel_id = 1"
    )
    old_row = await cur.fetchone()

    # Should not raise, should use defaults for missing metadata columns
    channel = channels_repo._map_channel(old_row)
    assert channel.channel_id == 1
    assert channel.title == "Test"
    assert channel.about is None
    assert channel.linked_chat_id is None
    assert channel.has_comments is False


# get_forum_topics tests


async def test_get_forum_topics_empty(channels_repo):
    """Test getting topics when none exist."""
    await channels_repo.add_channel(make_channel(1))
    topics = await channels_repo.get_forum_topics(1)
    assert topics == []


async def test_get_forum_topics(channels_repo):
    """Test getting forum topics."""
    await channels_repo.add_channel(make_channel(1))
    await channels_repo._db.executemany(
        "INSERT INTO forum_topics (channel_id, topic_id, title) VALUES (?, ?, ?)",
        [(1, 101, "Topic 1"), (1, 102, "Topic 2")],
    )
    await channels_repo._db.commit()

    topics = await channels_repo.get_forum_topics(1)
    assert len(topics) == 2
    assert topics[0] == {"id": 101, "title": "Topic 1"}
    assert topics[1] == {"id": 102, "title": "Topic 2"}


# upsert_forum_topics tests


async def test_upsert_forum_topics_insert(channels_repo):
    """Test inserting forum topics."""
    await channels_repo.add_channel(make_channel(1))
    topics = [{"id": 101, "title": "Topic A"}, {"id": 102, "title": "Topic B"}]
    await channels_repo.upsert_forum_topics(1, topics)

    result = await channels_repo.get_forum_topics(1)
    assert len(result) == 2


async def test_upsert_forum_topics_replace(channels_repo):
    """Test that upsert replaces existing topics."""
    await channels_repo.add_channel(make_channel(1))

    # Insert initial topics
    await channels_repo.upsert_forum_topics(1, [{"id": 1, "title": "Old"}])
    assert len(await channels_repo.get_forum_topics(1)) == 1

    # Replace with new topics
    await channels_repo.upsert_forum_topics(1, [{"id": 2, "title": "New"}])
    topics = await channels_repo.get_forum_topics(1)
    assert len(topics) == 1
    assert topics[0]["id"] == 2


async def test_upsert_forum_topics_empty_list(channels_repo):
    """Test upserting empty list deletes all topics."""
    await channels_repo.add_channel(make_channel(1))
    await channels_repo._db.execute(
        "INSERT INTO forum_topics (channel_id, topic_id, title) VALUES (?, ?, ?)",
        (1, 1, "Topic"),
    )
    await channels_repo._db.commit()

    await channels_repo.upsert_forum_topics(1, [])
    topics = await channels_repo.get_forum_topics(1)
    assert topics == []


# delete_channel tests


async def test_delete_channel(channels_repo):
    """Test deleting a channel."""
    await channels_repo.add_channel(make_channel(1))
    channels = await channels_repo.get_channels()
    pk = channels[0].id

    await channels_repo.delete_channel(pk)

    result = await channels_repo.get_channel_by_pk(pk)
    assert result is None


async def test_delete_channel_cascades_messages(channels_repo):
    """Test that deleting channel also deletes messages."""
    await channels_repo.add_channel(make_channel(1))

    # Insert messages
    await channels_repo._db.executemany(
        "INSERT INTO messages (channel_id, message_id, date) VALUES (?, ?, datetime('now'))",
        [(1, 100), (1, 101)],
    )
    await channels_repo._db.commit()

    channels = await channels_repo.get_channels()
    pk = channels[0].id
    await channels_repo.delete_channel(pk)

    # Verify messages are deleted
    cur = await channels_repo._db.execute("SELECT COUNT(*) as cnt FROM messages WHERE channel_id = 1")
    row = await cur.fetchone()
    assert row["cnt"] == 0


async def test_delete_channel_cascades_stats(channels_repo):
    """Test that deleting channel also deletes channel_stats."""
    await channels_repo.add_channel(make_channel(1))

    # Insert stats
    await channels_repo._db.execute(
        "INSERT INTO channel_stats (channel_id) VALUES (1)",
    )
    await channels_repo._db.commit()

    channels = await channels_repo.get_channels()
    pk = channels[0].id
    await channels_repo.delete_channel(pk)

    cur = await channels_repo._db.execute("SELECT COUNT(*) as cnt FROM channel_stats WHERE channel_id = 1")
    row = await cur.fetchone()
    assert row["cnt"] == 0


async def test_delete_channel_cascades_forum_topics(channels_repo):
    """Test that deleting channel also deletes forum_topics."""
    await channels_repo.add_channel(make_channel(1))

    await channels_repo._db.execute(
        "INSERT INTO forum_topics (channel_id, topic_id, title) VALUES (1, 1, 'Topic')",
    )
    await channels_repo._db.commit()

    channels = await channels_repo.get_channels()
    pk = channels[0].id
    await channels_repo.delete_channel(pk)

    cur = await channels_repo._db.execute("SELECT COUNT(*) as cnt FROM forum_topics WHERE channel_id = 1")
    row = await cur.fetchone()
    assert row["cnt"] == 0


async def test_delete_channel_nonexistent(channels_repo):
    """Test deleting non-existent channel does not raise."""
    await channels_repo.delete_channel(999)  # Should not raise


# set_channel_type tests


async def test_set_channel_type(channels_repo):
    """Test updating channel type."""
    await channels_repo.add_channel(make_channel(1, channel_type="channel"))
    await channels_repo.set_channel_type(1, "supergroup")

    channel = await channels_repo.get_channel_by_channel_id(1)
    assert channel.channel_type == "supergroup"
