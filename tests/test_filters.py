from __future__ import annotations

import pytest

from src.filters.analyzer import ChannelAnalyzer
from src.filters.criteria import VALID_FLAGS, contains_cyrillic
from src.filters.models import ChannelFilterResult, FilterReport


async def _insert_channel(db, channel_id, title="Test", channel_type="channel", username=None):
    await db.execute(
        "INSERT INTO channels (channel_id, title, username, channel_type, is_active) "
        "VALUES (?, ?, ?, ?, 1)",
        (channel_id, title, username, channel_type),
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


class TestContainsCyrillic:
    def test_cyrillic(self):
        assert contains_cyrillic("Привет") is True

    def test_no_cyrillic(self):
        assert contains_cyrillic("hello") is False

    def test_mixed(self):
        assert contains_cyrillic("hello Мир") is True


class TestValidFlags:
    def test_all_flags_present(self):
        assert "low_uniqueness" in VALID_FLAGS
        assert "low_subscriber_ratio" in VALID_FLAGS
        assert "low_subscriber_manual" in VALID_FLAGS
        assert "manual" in VALID_FLAGS
        assert "cross_channel_spam" in VALID_FLAGS
        assert "non_cyrillic" in VALID_FLAGS
        assert "chat_noise" in VALID_FLAGS
        assert "username_changed" in VALID_FLAGS
        assert "title_changed" in VALID_FLAGS
        assert "suspicious_username" in VALID_FLAGS
        assert len(VALID_FLAGS) == 10


class TestAnalyzerLowUniqueness:
    async def test_high_uniqueness(self, db, raw_db):
        await _insert_channel(raw_db, 100)
        await _insert_messages(raw_db, 100, ["unique text 1", "unique text 2", "unique text 3"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(100)
        assert result.uniqueness_pct == 100.0
        assert "low_uniqueness" not in result.flags

    async def test_low_uniqueness(self, db, raw_db):
        await _insert_channel(raw_db, 101)
        texts = ["same spam message"] * 10
        await _insert_messages(raw_db, 101, texts)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(101)
        assert result.uniqueness_pct == 10.0
        assert "low_uniqueness" in result.flags

    async def test_no_messages(self, db, raw_db):
        await _insert_channel(raw_db, 102)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(102)
        assert result.uniqueness_pct is None
        assert "low_uniqueness" not in result.flags

    async def test_boundary_exactly_at_threshold(self, db, raw_db):
        await _insert_channel(raw_db, 103)
        # 4 unique / 10 total = 40% > 30% threshold -> NOT flagged
        texts = ["unique A", "unique B", "unique C"] + ["same"] * 7
        await _insert_messages(raw_db, 103, texts)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(103)
        assert "low_uniqueness" not in result.flags


class TestAnalyzerSubscriberRatio:
    async def test_healthy_ratio(self, db, raw_db):
        await _insert_channel(raw_db, 200)
        await _insert_messages(raw_db, 200, ["msg1", "msg2"])
        await _insert_stats(raw_db, 200, 1000)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(200)
        assert result.subscriber_ratio == 500.0
        assert "low_subscriber_ratio" not in result.flags

    async def test_low_ratio(self, db, raw_db):
        await _insert_channel(raw_db, 201)
        await _insert_messages(raw_db, 201, [f"msg{i}" for i in range(100)])
        await _insert_stats(raw_db, 201, 10)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(201)
        assert result.subscriber_ratio == 0.1
        assert "low_subscriber_ratio" in result.flags

    async def test_no_stats(self, db, raw_db):
        await _insert_channel(raw_db, 202)
        await _insert_messages(raw_db, 202, ["msg"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(202)
        assert result.subscriber_ratio is None
        assert "low_subscriber_ratio" not in result.flags


class TestAnalyzerCrossChannelDupes:
    async def test_no_dupes(self, db, raw_db):
        await _insert_channel(raw_db, 300)
        await _insert_channel(raw_db, 301, title="Other")
        await _insert_messages(raw_db, 300, ["unique content from channel A"])
        await _insert_messages(raw_db, 301, ["completely different text here"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(300)
        assert "cross_channel_spam" not in result.flags

    async def test_high_dupes(self, db, raw_db):
        await _insert_channel(raw_db, 302)
        await _insert_channel(raw_db, 303, title="Mirror")
        shared = ["this is a duplicated message across channels"]
        await _insert_messages(raw_db, 302, shared)
        await _insert_messages(raw_db, 303, shared)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(302)
        assert result.cross_dupe_pct == 100.0
        assert "cross_channel_spam" in result.flags


class TestAnalyzerNonCyrillic:
    async def test_cyrillic_channel(self, db, raw_db):
        await _insert_channel(raw_db, 400)
        await _insert_messages(raw_db, 400, ["Привет мир", "Тест сообщение", "Ещё одно"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(400)
        assert result.cyrillic_pct == 100.0
        assert "non_cyrillic" not in result.flags

    async def test_non_cyrillic_channel(self, db, raw_db):
        await _insert_channel(raw_db, 401)
        texts = ["hello world", "test message", "no cyrillic here"] + ["another eng text"] * 7
        await _insert_messages(raw_db, 401, texts)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(401)
        assert result.cyrillic_pct == 0.0
        assert "non_cyrillic" in result.flags

    async def test_mixed_content_channel(self, db, raw_db):
        await _insert_channel(raw_db, 402)
        await _insert_messages(raw_db, 402, ["hello world", "Привет мир", "ещё один", "test"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(402)
        assert result.cyrillic_pct == 50.0
        assert "non_cyrillic" not in result.flags


class TestAnalyzerChatNoise:
    async def test_not_a_group(self, db, raw_db):
        await _insert_channel(raw_db, 500, channel_type="channel")
        await _insert_messages(raw_db, 500, ["hi", "ok", "lol"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(500)
        assert result.short_msg_pct is None
        assert "chat_noise" not in result.flags

    async def test_noisy_group(self, db, raw_db):
        await _insert_channel(raw_db, 501, channel_type="group")
        texts = ["hi", "ok", "+", "lol", "da", "net", "yes"] + ["long enough message here"]
        await _insert_messages(raw_db, 501, texts)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(501)
        assert result.short_msg_pct is not None
        assert "chat_noise" in result.flags

    async def test_clean_group(self, db, raw_db):
        await _insert_channel(raw_db, 502, channel_type="group")
        await _insert_messages(
            raw_db,
            502,
            ["This is a normal length message"] * 10,
        )
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(502)
        assert result.short_msg_pct == 0.0
        assert "chat_noise" not in result.flags

    async def test_media_only_not_counted_as_short(self, db, raw_db):
        await _insert_channel(raw_db, 503, channel_type="group")
        # Insert messages with NULL text (media-only)
        for i in range(1, 9):
            await raw_db.execute(
                "INSERT INTO messages (channel_id, message_id, text, date) "
                "VALUES (?, ?, NULL, '2025-01-01')",
                (503, i),
            )
        # Insert 2 long text messages
        await raw_db.execute(
            "INSERT INTO messages (channel_id, message_id, text, date) "
            "VALUES (503, 9, 'This is a long enough text message', '2025-01-01')"
        )
        await raw_db.execute(
            "INSERT INTO messages (channel_id, message_id, text, date) "
            "VALUES (503, 10, 'Another long enough text message here', '2025-01-01')"
        )
        await raw_db.commit()
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(503)
        # 8 NULL + 2 long = 0 short out of 10 -> 0%
        assert result.short_msg_pct == 0.0
        assert "chat_noise" not in result.flags


class TestAnalyzerSuspiciousUsername:
    async def test_random_alnum_username_flagged(self, db, raw_db):
        await _insert_channel(raw_db, 700, username="S0IMD1EDUAW")
        await _insert_messages(raw_db, 700, ["any"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(700)
        assert "suspicious_username" in result.flags

    async def test_random_alnum_username_longer_flagged(self, db, raw_db):
        await _insert_channel(raw_db, 701, username="EXF74CHE3RZ1")
        await _insert_messages(raw_db, 701, ["any"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(701)
        assert "suspicious_username" in result.flags

    async def test_normal_lowercase_username_ok(self, db, raw_db):
        await _insert_channel(raw_db, 702, username="durov")
        await _insert_messages(raw_db, 702, ["any"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(702)
        assert "suspicious_username" not in result.flags

    async def test_mixed_case_username_ok(self, db, raw_db):
        await _insert_channel(raw_db, 703, username="PublicChannel42")
        await _insert_messages(raw_db, 703, ["any"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(703)
        assert "suspicious_username" not in result.flags

    async def test_all_caps_no_digits_ok(self, db, raw_db):
        # "NASDAQNEWS" — all caps but no digits → not flagged.
        await _insert_channel(raw_db, 704, username="NASDAQNEWS")
        await _insert_messages(raw_db, 704, ["any"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(704)
        assert "suspicious_username" not in result.flags

    async def test_short_alnum_ok(self, db, raw_db):
        # "BITCOIN24" — 9 chars, under length threshold of 10 → not flagged.
        await _insert_channel(raw_db, 705, username="BITCOIN24")
        await _insert_messages(raw_db, 705, ["any"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(705)
        assert "suspicious_username" not in result.flags

    async def test_null_username_ok(self, db, raw_db):
        await _insert_channel(raw_db, 706, username=None)
        await _insert_messages(raw_db, 706, ["any"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(706)
        assert "suspicious_username" not in result.flags


class TestAnalyzerStickyFlags:
    async def test_username_changed_preserved_after_apply(self, db, raw_db):
        await _insert_channel(raw_db, 710, username="durov")
        await _insert_messages(raw_db, 710, ["unique a", "unique b", "unique c"])
        # Simulate collector marking the channel with username_changed.
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 1, filter_flags = 'username_changed' "
            "WHERE channel_id = ?",
            (710,),
        )
        await raw_db.commit()

        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        await analyzer.apply_filters(report)

        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = ?", (710,)
        )
        row = await cur.fetchone()
        assert row["is_filtered"] == 1
        assert "username_changed" in row["filter_flags"]

    async def test_title_changed_preserved_after_apply(self, db, raw_db):
        await _insert_channel(raw_db, 711, username="durov")
        await _insert_messages(raw_db, 711, ["unique a", "unique b", "unique c"])
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 1, filter_flags = 'title_changed' "
            "WHERE channel_id = ?",
            (711,),
        )
        await raw_db.commit()

        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        await analyzer.apply_filters(report)

        cur = await raw_db.execute(
            "SELECT filter_flags FROM channels WHERE channel_id = ?", (711,)
        )
        row = await cur.fetchone()
        assert "title_changed" in row["filter_flags"]


class TestChannelAnalyzer:
    async def test_analyze_all(self, db, raw_db):
        await _insert_channel(raw_db, 600)
        await _insert_messages(raw_db, 600, ["unique text"] * 3)
        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        assert report.total_channels >= 1
        found = [r for r in report.results if r.channel_id == 600]
        assert len(found) == 1

    async def test_apply_filters(self, db, raw_db):
        await _insert_channel(raw_db, 700)
        # All same messages -> low uniqueness -> should be filtered
        await _insert_messages(raw_db, 700, ["spam"] * 20)
        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        result = [r for r in report.results if r.channel_id == 700][0]
        assert result.is_filtered is True
        assert "low_uniqueness" in result.flags

        count = await analyzer.apply_filters(report)
        assert count >= 1

        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 700"
        )
        row = await cur.fetchone()
        assert row["is_filtered"] == 1
        assert "low_uniqueness" in row["filter_flags"]

    async def test_analyze_all_invalid_min_subscribers_setting_falls_back_to_zero(self, db, raw_db):
        await db.set_setting("min_subscribers_filter", "broken")
        await _insert_channel(raw_db, 601)
        await _insert_messages(raw_db, 601, ["hello world", "hello again"])

        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()

        found = [result for result in report.results if result.channel_id == 601]
        assert len(found) == 1
        assert "low_subscriber_manual" not in found[0].flags

    async def test_apply_filters_resets_stale(self, db, raw_db):
        # Channel 710 was previously filtered
        await _insert_channel(raw_db, 710, title="Previously Filtered")
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 1, filter_flags = 'low_uniqueness'"
            " WHERE channel_id = 710"
        )
        await _insert_messages(raw_db, 710, [f"уникальное сообщение {i}" for i in range(20)])

        # Channel 711 is genuinely spammy
        await _insert_channel(raw_db, 711, title="Spammy")
        await _insert_messages(raw_db, 711, ["spam"] * 20)
        await raw_db.commit()

        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        await analyzer.apply_filters(report)

        # Channel 710 should be clean now
        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 710"
        )
        row = await cur.fetchone()
        assert row["is_filtered"] == 0

        # Channel 711 should still be filtered
        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 711"
        )
        row = await cur.fetchone()
        assert row["is_filtered"] == 1

    async def test_apply_filters_deduplicates_same_channel(self, db, raw_db):
        await _insert_channel(raw_db, 712, title="Dupes")
        analyzer = ChannelAnalyzer(db)
        report = FilterReport(
            results=[
                ChannelFilterResult(
                    channel_id=712,
                    flags=["low_uniqueness"],
                    is_filtered=True,
                ),
                ChannelFilterResult(
                    channel_id=712,
                    flags=["non_cyrillic"],
                    is_filtered=True,
                ),
            ],
            total_channels=2,
            filtered_count=2,
        )

        count = await analyzer.apply_filters(report)
        assert count == 1

        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 712"
        )
        row = await cur.fetchone()
        assert row["is_filtered"] == 1
        assert row["filter_flags"] == "low_uniqueness,non_cyrillic"

    async def test_apply_filters_is_atomic_on_error(self, db, raw_db, monkeypatch):
        await _insert_channel(raw_db, 713, title="Atomic")
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 1, filter_flags = 'legacy_flag'"
            " WHERE channel_id = 713"
        )
        await raw_db.commit()

        analyzer = ChannelAnalyzer(db)
        report = FilterReport(
            results=[
                ChannelFilterResult(
                    channel_id=713,
                    flags=["low_uniqueness"],
                    is_filtered=True,
                )
            ],
            total_channels=1,
            filtered_count=1,
        )

        async def _boom(updates, *, commit=True):
            raise RuntimeError("boom")

        monkeypatch.setattr(db, "set_channels_filtered_bulk", _boom)

        with pytest.raises(RuntimeError, match="boom"):
            await analyzer.apply_filters(report)

        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 713"
        )
        row = await cur.fetchone()
        assert row["is_filtered"] == 1
        assert row["filter_flags"] == "legacy_flag"

    async def test_apply_filters_recovers_from_stale_transaction(self, db, raw_db):
        """BEGIN succeeds even if a prior operation left an uncommitted txn."""
        await _insert_channel(raw_db, 714, title="Stale")
        await raw_db.commit()
        # Simulate stale transaction left by an interrupted operation
        await db.db.execute("BEGIN")
        analyzer = ChannelAnalyzer(db)
        report = FilterReport(results=[], total_channels=0, filtered_count=0)
        # Should not raise OperationalError
        count = await analyzer.apply_filters(report)
        assert count == 0

    async def test_reset_filters(self, db, raw_db):
        await _insert_channel(raw_db, 800)
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 1, filter_flags = 'low_uniqueness,non_cyrillic'"
            " WHERE channel_id = 800"
        )
        await raw_db.commit()

        analyzer = ChannelAnalyzer(db)
        await analyzer.reset_filters()

        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 800"
        )
        row = await cur.fetchone()
        assert row["is_filtered"] == 0
        assert row["filter_flags"] == ""

    async def test_precheck_broadcast_low_ratio(self, db, raw_db):
        # broadcast channel: subscriber_count=10, message_count=20 -> 0.5 < 1.0 -> filtered
        await raw_db.execute(
            "INSERT INTO channels (channel_id, title, channel_type, is_active) VALUES (?, ?, ?, 1)",
            (1001, "Spam Broadcast", "channel"),
        )
        await raw_db.commit()
        await _insert_messages(raw_db, 1001, [f"msg {i}" for i in range(20)])
        await _insert_stats(raw_db, 1001, 10)
        analyzer = ChannelAnalyzer(db)
        count = await analyzer.precheck_subscriber_ratio()
        assert count == 1
        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 1001"
        )
        row = await cur.fetchone()
        assert row["is_filtered"] == 1
        assert row["filter_flags"] == "low_subscriber_ratio"

    async def test_precheck_supergroup_low_ratio(self, db, raw_db):
        # supergroup: subscriber_count=1, message_count=100 -> 0.01 < 0.02 -> filtered
        await raw_db.execute(
            "INSERT INTO channels (channel_id, title, channel_type, is_active) VALUES (?, ?, ?, 1)",
            (1002, "Spam Supergroup", "supergroup"),
        )
        await raw_db.commit()
        await _insert_messages(raw_db, 1002, [f"msg {i}" for i in range(100)])
        await _insert_stats(raw_db, 1002, 1)
        analyzer = ChannelAnalyzer(db)
        count = await analyzer.precheck_subscriber_ratio()
        assert count == 1

    async def test_precheck_supergroup_healthy_ratio(self, db, raw_db):
        # @PattayaVse: subscriber_count=7039, message_count=100 -> 70.39 > 0.02 -> NOT filtered
        await raw_db.execute(
            "INSERT INTO channels (channel_id, title, channel_type, is_active) VALUES (?, ?, ?, 1)",
            (1003, "PattayaVse", "supergroup"),
        )
        await raw_db.commit()
        await _insert_messages(raw_db, 1003, [f"msg {i}" for i in range(100)])
        await _insert_stats(raw_db, 1003, 7039)
        analyzer = ChannelAnalyzer(db)
        count = await analyzer.precheck_subscriber_ratio()
        assert count == 0
        cur = await raw_db.execute("SELECT is_filtered FROM channels WHERE channel_id = 1003")
        row = await cur.fetchone()
        assert row["is_filtered"] == 0

    async def test_precheck_no_stats_skipped(self, db, raw_db):
        # No stats -> skip
        await raw_db.execute(
            "INSERT INTO channels (channel_id, title, channel_type, is_active) VALUES (?, ?, ?, 1)",
            (1004, "No Stats", "channel"),
        )
        await raw_db.commit()
        await _insert_messages(raw_db, 1004, [f"msg {i}" for i in range(50)])
        analyzer = ChannelAnalyzer(db)
        count = await analyzer.precheck_subscriber_ratio()
        assert count == 0

    async def test_precheck_zero_message_count_skipped(self, db, raw_db):
        # No messages in DB -> message_count=0 -> skip
        await raw_db.execute(
            "INSERT INTO channels (channel_id, title, channel_type, is_active) VALUES (?, ?, ?, 1)",
            (1005, "No Collected", "channel"),
        )
        await raw_db.commit()
        await _insert_stats(raw_db, 1005, 500)
        analyzer = ChannelAnalyzer(db)
        count = await analyzer.precheck_subscriber_ratio()
        assert count == 0

    async def test_bulk_cross_channel_dupes_flag(self, db, raw_db):
        await _insert_channel(raw_db, 900, title="A")
        await _insert_channel(raw_db, 901, title="B")
        await _insert_channel(raw_db, 902, title="C")

        await _insert_messages(
            raw_db,
            900,
            [
                "shared duplicated content alpha beta gamma",
                "shared duplicated content delta epsilon zeta",
                "unique channel 900 payload long enough",
            ],
        )
        await _insert_messages(
            raw_db,
            901,
            [
                "shared duplicated content alpha beta gamma",
                "shared duplicated content delta epsilon zeta",
            ],
        )
        await _insert_messages(raw_db, 902, ["independent long content for channel 902 only"])

        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        ch900 = next(r for r in report.results if r.channel_id == 900)
        assert ch900.cross_dupe_pct == 66.7
        assert "cross_channel_spam" in ch900.flags
