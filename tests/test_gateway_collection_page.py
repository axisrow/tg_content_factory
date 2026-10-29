"""The project commits collection data and its watermark in one transaction."""

import sqlite3
from datetime import datetime, timezone

import pytest

from tg_agent.models import Channel, Message

pytestmark = pytest.mark.aiosqlite_serial


async def test_collection_page_replay_does_not_inflate_statistics(db):
    await db.add_channel(Channel(channel_id=123, title="source"))
    message = Message(channel_id=123, message_id=7, text="hello", date=datetime.now(timezone.utc),
                      reactions_json='[{"emoji":"👍","count":2}]')
    assert await db.repos.messages.save_collected_page(123, [message], 7) == 1
    assert await db.repos.messages.save_collected_page(123, [message], 7) == 0
    async with db.db.execute("SELECT last_collected_id FROM channels WHERE channel_id=123") as cur:
        assert (await cur.fetchone())[0] == 7
    async with db.db.execute("SELECT total_all FROM channel_text_stats WHERE channel_id=123") as cur:
        assert (await cur.fetchone())[0] == 1


async def test_failed_cursor_update_rolls_back_messages_reactions_and_statistics(db):
    await db.add_channel(Channel(channel_id=123, title="source"))
    await db.execute_write("CREATE TRIGGER fail_cursor BEFORE UPDATE OF last_collected_id ON channels "
                           "BEGIN SELECT RAISE(ABORT,'injected cursor failure'); END")
    message = Message(channel_id=123, message_id=7, text="hello", date=datetime.now(timezone.utc),
                      reactions_json='[{"emoji":"👍","count":2}]')
    with pytest.raises(sqlite3.IntegrityError, match="injected"):
        await db.repos.messages.save_collected_page(123, [message], 7)
    for table in ("messages", "message_reactions", "channel_text_stats", "channel_prefix_stats"):
        async with db.db.execute(f"SELECT COUNT(*) FROM {table}") as cur:
            assert (await cur.fetchone())[0] == 0
    async with db.db.execute("SELECT last_collected_id FROM channels WHERE channel_id=123") as cur:
        assert (await cur.fetchone())[0] == 0
