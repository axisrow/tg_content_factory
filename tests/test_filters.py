from __future__ import annotations

import pytest

from src.filters.analyzer import ChannelAnalyzer
from src.filters.criteria import (
    check_chat_noise,
    check_cross_channel_dupes,
    check_low_uniqueness,
    check_non_cyrillic,
    check_subscriber_ratio,
)


async def _insert_channel(db, channel_id, title="Test", channel_type="channel"):
    await db.execute(
        "INSERT INTO channels (channel_id, title, channel_type, is_active) VALUES (?, ?, ?, 1)",
        (channel_id, title, channel_type),
    )
    await db.commit()


async def _insert_messages(db, channel_id, texts):
    for i, text in enumerate(texts, 1):
        await db.execute(
            "INSERT INTO messages (channel_id, message_id, text, date) "
            "VALUES (?, ?, ?, '2025-01-01')",
            (channel_id, i, text),
        )
    await db.commit()


async def _insert_stats(db, channel_id, subscriber_count):
    await db.execute(
        "INSERT INTO channel_stats (channel_id, subscriber_count) VALUES (?, ?)",
        (channel_id, subscriber_count),
    )
    await db.commit()


@pytest.fixture
async def raw_db(db):
    """Return the raw aiosqlite connection from the Database fixture."""
    return db.db


class TestLowUniqueness:
    async def test_high_uniqueness(self, raw_db):
        await _insert_channel(raw_db, 100)
        await _insert_messages(raw_db, 100, ["unique text 1", "unique text 2", "unique text 3"])
        pct, flagged = await check_low_uniqueness(raw_db, 100)
        assert pct == 100.0
        assert flagged is False

    async def test_low_uniqueness(self, raw_db):
        await _insert_channel(raw_db, 101)
        texts = ["same spam message"] * 10
        await _insert_messages(raw_db, 101, texts)
        pct, flagged = await check_low_uniqueness(raw_db, 101)
        assert pct == 10.0
        assert flagged is True

    async def test_no_messages(self, raw_db):
        await _insert_channel(raw_db, 102)
        pct, flagged = await check_low_uniqueness(raw_db, 102)
        assert pct is None
        assert flagged is False


class TestSubscriberRatio:
    async def test_healthy_ratio(self, raw_db):
        await _insert_channel(raw_db, 200)
        await _insert_messages(raw_db, 200, ["msg1", "msg2"])
        await _insert_stats(raw_db, 200, 1000)
        ratio, flagged = await check_subscriber_ratio(raw_db, 200)
        assert ratio == 500.0
        assert flagged is False

    async def test_low_ratio(self, raw_db):
        await _insert_channel(raw_db, 201)
        await _insert_messages(raw_db, 201, [f"msg{i}" for i in range(100)])
        await _insert_stats(raw_db, 201, 10)
        ratio, flagged = await check_subscriber_ratio(raw_db, 201)
        assert ratio == 0.1
        assert flagged is True

    async def test_no_stats(self, raw_db):
        await _insert_channel(raw_db, 202)
        await _insert_messages(raw_db, 202, ["msg"])
        ratio, flagged = await check_subscriber_ratio(raw_db, 202)
        assert ratio is None
        assert flagged is False


class TestCrossChannelDupes:
    async def test_no_dupes(self, raw_db):
        await _insert_channel(raw_db, 300)
        await _insert_channel(raw_db, 301, title="Other")
        await _insert_messages(raw_db, 300, ["unique content from channel A"])
        await _insert_messages(raw_db, 301, ["completely different text here"])
        pct, flagged = await check_cross_channel_dupes(raw_db, 300)
        assert flagged is False

    async def test_high_dupes(self, raw_db):
        await _insert_channel(raw_db, 302)
        await _insert_channel(raw_db, 303, title="Mirror")
        shared = ["this is a duplicated message across channels"]
        await _insert_messages(raw_db, 302, shared)
        await _insert_messages(raw_db, 303, shared)
        pct, flagged = await check_cross_channel_dupes(raw_db, 302)
        assert pct == 100.0
        assert flagged is True


class TestNonCyrillic:
    async def test_cyrillic_channel(self, raw_db):
        await _insert_channel(raw_db, 400)
        await _insert_messages(raw_db, 400, ["Привет мир", "Тест сообщение", "Ещё одно"])
        pct, flagged = await check_non_cyrillic(raw_db, 400)
        assert pct == 100.0
        assert flagged is False

    async def test_non_cyrillic_channel(self, raw_db):
        await _insert_channel(raw_db, 401)
        texts = ["hello world", "test message", "no cyrillic here"] + [
            "another eng text"
        ] * 7
        await _insert_messages(raw_db, 401, texts)
        pct, flagged = await check_non_cyrillic(raw_db, 401)
        assert pct == 0.0
        assert flagged is True


class TestChatNoise:
    async def test_not_a_group(self, raw_db):
        await _insert_channel(raw_db, 500, channel_type="channel")
        await _insert_messages(raw_db, 500, ["hi", "ok", "lol"])
        pct, flagged = await check_chat_noise(raw_db, 500)
        assert pct is None
        assert flagged is False

    async def test_noisy_group(self, raw_db):
        await _insert_channel(raw_db, 501, channel_type="group")
        texts = ["hi", "ok", "+", "lol", "da", "net", "yes"] + ["long enough message here"]
        await _insert_messages(raw_db, 501, texts)
        pct, flagged = await check_chat_noise(raw_db, 501)
        assert pct is not None
        assert flagged is True

    async def test_clean_group(self, raw_db):
        await _insert_channel(raw_db, 502, channel_type="group")
        await _insert_messages(
            raw_db, 502,
            ["This is a normal length message"] * 10,
        )
        pct, flagged = await check_chat_noise(raw_db, 502)
        assert pct == 0.0
        assert flagged is False


class TestChannelAnalyzer:
    async def test_analyze_all(self, raw_db):
        await _insert_channel(raw_db, 600)
        await _insert_messages(raw_db, 600, ["unique text"] * 3)
        analyzer = ChannelAnalyzer(raw_db)
        report = await analyzer.analyze_all()
        assert report.total_channels >= 1
        found = [r for r in report.results if r.channel_id == 600]
        assert len(found) == 1

    async def test_apply_filters(self, raw_db):
        await _insert_channel(raw_db, 700)
        # All same messages -> low uniqueness -> should be filtered
        await _insert_messages(raw_db, 700, ["spam"] * 20)
        analyzer = ChannelAnalyzer(raw_db)
        report = await analyzer.analyze_all()
        result = [r for r in report.results if r.channel_id == 700][0]
        assert result.is_filtered is True
        assert "low_uniqueness" in result.flags

        count = await analyzer.apply_filters(report)
        assert count >= 1

        cur = await raw_db.execute(
            "SELECT is_filtered FROM channels WHERE channel_id = 700"
        )
        row = await cur.fetchone()
        assert row["is_filtered"] == 1

    async def test_reset_filters(self, raw_db):
        await _insert_channel(raw_db, 800)
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 1 WHERE channel_id = 800"
        )
        await raw_db.commit()

        analyzer = ChannelAnalyzer(raw_db)
        await analyzer.reset_filters()

        cur = await raw_db.execute(
            "SELECT is_filtered FROM channels WHERE channel_id = 800"
        )
        row = await cur.fetchone()
        assert row["is_filtered"] == 0
