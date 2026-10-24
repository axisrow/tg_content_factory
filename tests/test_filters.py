from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tg_agent.filters.analyzer import ChannelAnalyzer
from tg_agent.filters.criteria import (
    AUTHOR_CONTENT_TAG,
    LABEL_ONLY_FLAGS,
    VALID_FLAGS,
    WHITELIST_FLAG,
    contains_cyrillic,
)
from tg_agent.filters.models import ChannelFilterResult, FilterReport


async def _insert_channel(
    db,
    channel_id,
    title="Test",
    channel_type: str | None = "channel",
    username=None,
    *,
    filtered_origin: str = "auto",
):
    await db.execute(
        "INSERT INTO channels (channel_id, title, username, channel_type, is_active, filtered_origin) "
        "VALUES (?, ?, ?, ?, 1, ?)",
        (channel_id, title, username, channel_type, filtered_origin),
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


async def _insert_messages_with_views(db, channel_id, views):
    """Уникальные длинные посты без ссылок с заданными views (None допустим)."""
    for i, views_count in enumerate(views, 1):
        text = f"уникальный авторский пост номер {i} " + "подробный разбор без ссылок " * 5
        await db.execute(
            "INSERT INTO messages (channel_id, message_id, text, views, date) "
            "VALUES (?, ?, ?, ?, '2025-01-01')",
            (channel_id, i, text, views_count),
        )
    await db.commit()


async def _insert_forward_mix(db, channel_id, forwards, plains, *, base=1000, textless_forwards=False):
    """forwards постов-форвардов (fwd=999) + plains обычных; id не пересекаются."""
    for i in range(forwards):
        if textless_forwards:
            await db.execute(
                "INSERT INTO messages (channel_id, message_id, forward_from_channel_id, date) "
                "VALUES (?, ?, 999, '2025-01-01')",
                (channel_id, base + i),
            )
        else:
            await db.execute(
                "INSERT INTO messages (channel_id, message_id, text, forward_from_channel_id, date) "
                "VALUES (?, ?, ?, 999, '2025-01-01')",
                (channel_id, base + i, f"пересланный чужой пост номер {i}, длинный текст целиком"),
            )
    for i in range(plains):
        await db.execute(
            "INSERT INTO messages (channel_id, message_id, text, date) VALUES (?, ?, ?, '2025-01-01')",
            (channel_id, base + 500 + i, f"уникальный авторский пост номер {i}, длинный и цельный"),
        )
    await db.commit()


@pytest.fixture
async def raw_db(db):
    """Return the raw aiosqlite connection from the Database fixture."""
    return db.db


_AUTO_FEED_TEMPLATE = "Exploit Slug #{i} | RCE in Product | PoC https://example.com/poc/{i}"


def _author_texts(count: int) -> list[str]:
    return [
        f"Long-form analysis part {i}: " + ("подробный разбор методологии исследования " * 6) + f"выводы {i}"
        for i in range(count)
    ]


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
        assert "auto_feed" in VALID_FLAGS
        assert "low_engagement" in VALID_FLAGS
        assert "forwarder" in VALID_FLAGS
        assert "no_author_content" in VALID_FLAGS
        assert "high_volume" in VALID_FLAGS
        assert len(VALID_FLAGS) == 15


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

    async def test_forwards_not_counted_in_uniqueness(self, db, raw_db):
        # «20 уникальных постов + 80 форвардов одного текста»: форварды —
        # атрибуция, в метрику оригинальности не входят (решение владельца
        # 16.10.26). Старый код считал их: 21/100 = 21% < 30% → флаг.
        await _insert_channel(raw_db, 104)
        await _insert_messages(raw_db, 104, [f"уникальный авторский пост {i}" for i in range(20)])
        for i in range(80):
            await raw_db.execute(
                "INSERT INTO messages (channel_id, message_id, text, forward_from_channel_id, date) "
                "VALUES (104, ?, ?, 999, '2025-01-01')",
                (100 + i, "пересланный чужой пост, один и тот же текст"),
            )
        await raw_db.commit()
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(104)
        assert result.uniqueness_pct == 100.0
        assert "low_uniqueness" not in result.flags


class TestSavedThresholdSettings:
    """#1596: пороги из `settings filter-criteria` (filter_*) обязаны влиять на analyze.

    Дефолт при отсутствии настройки — константы criteria.py; сохранённый порог
    заменяет константу; мусор/вне диапазона в БД — откат к дефолту.
    """

    async def test_saved_uniqueness_threshold_used(self, db, raw_db):
        # 40% уникальности (4 distinct / 10): выше дефолта 30 (чисто),
        # ниже сохранённого 45 → флаг.
        await _insert_channel(raw_db, 110)
        texts = ["уникальный пост раз", "уникальный пост два", "уникальный пост три"] + [
            "одно и то же сообщение"
        ] * 7
        await _insert_messages(raw_db, 110, texts)
        await db.set_setting("filter_min_uniqueness", "45")
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(110)
        assert result.uniqueness_pct == 40.0
        assert "low_uniqueness" in result.flags

    async def test_saved_cross_dupe_threshold_used(self, db, raw_db):
        # 10% кросс-дупликатов: ниже дефолта 50 (чисто), выше сохранённого 5 → флаг.
        await _insert_channel(raw_db, 310, title="Source")
        await _insert_channel(raw_db, 311, title="Copier")
        shared = "this is a duplicated message across channels"
        await _insert_messages(raw_db, 310, [shared] + [f"source own {i}" for i in range(9)])
        await _insert_messages(raw_db, 311, [f"copier own {i}" for i in range(9)])
        await raw_db.execute(
            "INSERT INTO messages (channel_id, message_id, text, date) VALUES (311, 100, ?, '2025-01-05')",
            (shared,),
        )
        await raw_db.commit()
        await db.set_setting("filter_max_cross_dupe_pct", "5")
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(311)
        assert result.cross_dupe_pct == 10.0
        assert "cross_channel_spam" in result.flags

    async def test_defaults_preserved_without_settings(self, db, raw_db):
        # Настроек нет — работают дефолты criteria.py: 10% < 50 → чисто.
        await _insert_channel(raw_db, 310, title="Source")
        await _insert_channel(raw_db, 312, title="Copier")
        shared = "this is a duplicated message across channels"
        await _insert_messages(raw_db, 310, [shared] + [f"source own {i}" for i in range(9)])
        await _insert_messages(raw_db, 312, [f"copier own {i}" for i in range(9)])
        await raw_db.execute(
            "INSERT INTO messages (channel_id, message_id, text, date) VALUES (312, 100, ?, '2025-01-05')",
            (shared,),
        )
        await raw_db.commit()
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(312)
        assert result.cross_dupe_pct == 10.0
        assert "cross_channel_spam" not in result.flags

    async def test_saved_subscriber_ratio_threshold_used(self, db, raw_db):
        # ratio 2.5: выше дефолта 1.0 (чисто), ниже сохранённого 3.0 → флаг.
        await _insert_channel(raw_db, 320)
        await _insert_messages(raw_db, 320, ["post one", "post two"])
        await _insert_stats(raw_db, 320, 5)
        await db.set_setting("filter_min_subscriber_ratio", "3.0")
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(320)
        assert result.subscriber_ratio == 2.5
        assert "low_subscriber_ratio" in result.flags

    async def test_saved_cyrillic_threshold_used(self, db, raw_db):
        # 50% кириллицы: выше дефолта 10 (чисто), ниже сохранённых 60 → non_cyrillic.
        await _insert_channel(raw_db, 330)
        texts = [
            "привет мир раз", "привет мир два", "привет мир три", "привет мир четыре", "привет мир пять",
            "hello world one", "hello world two", "hello world three", "hello world four", "hello world five",
        ]
        await _insert_messages(raw_db, 330, texts)
        await db.set_setting("filter_min_cyrillic_pct", "60")
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(330)
        assert result.cyrillic_pct == 50.0
        assert "non_cyrillic" in result.flags

    async def test_out_of_range_saved_threshold_falls_back_to_default(self, db, raw_db):
        # Вне-диапазонное значение в БД (напр. до появления валидации записи)
        # не крэшит и не глушит флаг: откат к дефолту criteria.py.
        await _insert_channel(raw_db, 340)
        texts = ["уникальный пост раз", "уникальный пост два", "уникальный пост три"] + [
            "одно и то же сообщение"
        ] * 7
        await _insert_messages(raw_db, 340, texts)
        await db.set_setting("filter_min_uniqueness", "150")
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(340)
        assert result.uniqueness_pct == 40.0
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

    async def test_null_type_broadcast_threshold(self, db, raw_db):
        # NULL-тип судится по broadcast-порогу (0.1 < 1.0 → флаг), а не по
        # chat-порогу 0.02. Реальный кейс: ферма ai_for_dev, ratio 0.30 —
        # со старым whitelist-гейтом она молча проходила.
        await _insert_channel(raw_db, 203, channel_type=None)
        await _insert_messages(raw_db, 203, [f"msg{i}" for i in range(100)])
        await _insert_stats(raw_db, 203, 10)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(203)
        assert "low_subscriber_ratio" in result.flags


class TestAnalyzerCrossChannelDupes:
    async def test_no_dupes(self, db, raw_db):
        await _insert_channel(raw_db, 300)
        await _insert_channel(raw_db, 301, title="Other")
        await _insert_messages(raw_db, 300, ["unique content from channel A"])
        await _insert_messages(raw_db, 301, ["completely different text here"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(300)
        assert "cross_channel_spam" not in result.flags

    async def test_copy_later_flagged_source_clean(self, db, raw_db):
        # Асимметричная семантика (фидбек владельца 15.10.26): дупликат — это
        # текст, опубликованный каналом ПОЗЖЕ, чем он появился в другом канале.
        # 302 — источник, 306 — скопировал без атрибуции: флаг у копии, у
        # источника чисто.
        await _insert_channel(raw_db, 302, title="Source")
        await _insert_channel(raw_db, 306, title="Copier")
        shared = "this is a duplicated message across channels"
        await raw_db.execute(
            "INSERT INTO messages (channel_id, message_id, text, date) VALUES (302, 1, ?, '2025-01-01')",
            (shared,),
        )
        await raw_db.execute(
            "INSERT INTO messages (channel_id, message_id, text, date) VALUES (306, 1, ?, '2025-01-05')",
            (shared,),
        )
        await raw_db.commit()
        analyzer = ChannelAnalyzer(db)
        source = await analyzer.analyze_channel(302)
        assert source.cross_dupe_pct == 0.0
        assert "cross_channel_spam" not in source.flags
        copier = await analyzer.analyze_channel(306)
        assert copier.cross_dupe_pct == 100.0
        assert "cross_channel_spam" in copier.flags

    async def test_same_moment_flagged_both(self, db, raw_db):
        # Равные даты (одновременная публикация) — дупли для всех участников
        # повтора (#1676, решение владельца 24.10.26): сеть близнецов, постящая
        # копии в одну секунду, больше не проходит из-за тай-брейка по секунде.
        await _insert_channel(raw_db, 302)
        await _insert_channel(raw_db, 303, title="Mirror")
        shared = ["this is a duplicated message across channels"]
        await _insert_messages(raw_db, 302, shared)
        await _insert_messages(raw_db, 303, shared)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(302)
        assert result.cross_dupe_pct == 100.0
        assert "cross_channel_spam" in result.flags
        mirror = await analyzer.analyze_channel(303)
        assert mirror.cross_dupe_pct == 100.0
        assert "cross_channel_spam" in mirror.flags

    async def test_forward_repost_not_flagged(self, db, raw_db):
        # Репост с атрибуцией (forward_from_channel_id) не считается дупликатом:
        # честная пересылка — не кросс-канальный спам.
        await _insert_channel(raw_db, 307, title="Reposter")
        await _insert_channel(raw_db, 308, title="Origin")
        await raw_db.execute(
            "INSERT INTO messages (channel_id, message_id, text, date) "
            "VALUES (308, 1, 'оригинальный пост чужого канала целиком', '2025-01-01')"
        )
        await raw_db.execute(
            "INSERT INTO messages (channel_id, message_id, text, forward_from_channel_id, date) "
            "VALUES (307, 1, 'оригинальный пост чужого канала целиком', 999, '2025-01-03')"
        )
        await raw_db.commit()
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(307)
        assert "cross_channel_spam" not in result.flags


class TestAnalyzerAutoFeed:
    """«Автогенерённая лента»: короткие посты (<200 симв.) со ссылкой наружу."""

    async def test_templated_link_feed_flagged(self, db, raw_db):
        await _insert_channel(raw_db, 400)
        await _insert_messages(raw_db, 400, [_AUTO_FEED_TEMPLATE.format(i=i) for i in range(40)])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(400)
        assert result.auto_feed_pct == 100.0
        assert "auto_feed" in result.flags

    async def test_author_content_not_flagged(self, db, raw_db):
        await _insert_channel(raw_db, 401)
        await _insert_messages(raw_db, 401, _author_texts(40))
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(401)
        assert result.auto_feed_pct == 0.0
        assert "auto_feed" not in result.flags

    async def test_mixed_below_threshold_not_flagged(self, db, raw_db):
        await _insert_channel(raw_db, 402)
        texts = [_AUTO_FEED_TEMPLATE.format(i=i) for i in range(20)]
        texts += _author_texts(20)
        await _insert_messages(raw_db, 402, texts)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(402)
        assert result.auto_feed_pct == 50.0
        assert "auto_feed" not in result.flags

    async def test_too_few_messages_not_flagged(self, db, raw_db):
        await _insert_channel(raw_db, 403)
        await _insert_messages(raw_db, 403, [_AUTO_FEED_TEMPLATE.format(i=i) for i in range(10)])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(403)
        assert result.auto_feed_pct == 100.0
        assert "auto_feed" not in result.flags


class TestAnalyzeAllQuickMode:
    """quick=True skips the cross-dupe map — the heaviest query on live DBs (#774)."""

    async def test_analyze_all_quick_omits_cross_dupe_flags(self, db, raw_db):
        await _insert_channel(raw_db, 500)
        await _insert_channel(raw_db, 501, title="Mirror")
        shared = ["this is a duplicated message across channels"]
        await _insert_messages(raw_db, 500, shared)
        # 501 скопировал позже: при равных датах дупликата нет (см. ту же
        # семантику в TestAnalyzerCrossChannelDupes).
        await raw_db.execute(
            "INSERT INTO messages (channel_id, message_id, text, date) VALUES (501, 1, ?, '2025-01-05')",
            shared,
        )
        await raw_db.commit()
        analyzer = ChannelAnalyzer(db)

        full = await analyzer.analyze_all()
        assert any("cross_channel_spam" in r.flags for r in full.results)

        quick = await analyzer.analyze_all(quick=True)
        assert quick.results
        assert all("cross_channel_spam" not in r.flags for r in quick.results)
        assert all(r.cross_dupe_pct is None for r in quick.results)
        assert any(r.uniqueness_pct is not None for r in quick.results)

    async def test_analyze_all_quick_parallel_path_skips_cross_dupe(self, tmp_path):
        from tg_agent.database import Database

        file_db = Database(str(tmp_path / "quick.db"))
        await file_db.initialize()
        try:
            conn = file_db.db
            await _insert_channel(conn, 600)
            await _insert_channel(conn, 601, title="Mirror")
            shared = ["duplicated message body across channels"]
            await _insert_messages(conn, 600, shared)
            await _insert_messages(conn, 601, shared)

            repo = file_db.filter_repo
            assert repo._can_parallel()

            _, _, _, full_cross_map, _, _, _, _, _ = await repo.fetch_maps_parallel(None)
            assert full_cross_map

            _, _, _, quick_cross_map, _, _, _, _, _ = await repo.fetch_maps_parallel(None, include_cross_dupe=False)
            assert quick_cross_map == {}

            quick = await ChannelAnalyzer(file_db).analyze_all(quick=True)
            assert quick.results
            assert all("cross_channel_spam" not in r.flags for r in quick.results)
        finally:
            await file_db.close()

    async def test_analyze_all_quick_computes_auto_feed(self, db, raw_db):
        """auto_feed, в отличие от cross-dupe, считается и в quick (sampled-путь)."""
        await _insert_channel(raw_db, 502)
        await _insert_messages(raw_db, 502, [_AUTO_FEED_TEMPLATE.format(i=i) for i in range(40)])
        analyzer = ChannelAnalyzer(db)

        quick = await analyzer.analyze_all(quick=True, sample_size=50)
        result = next(r for r in quick.results if r.channel_id == 502)
        assert result.auto_feed_pct == 100.0
        assert "auto_feed" in result.flags


class TestAnalyzeAllQuickSampling:
    """quick=True samples the last N messages/channel for text metrics (#1138).

    A full analyze scans the whole history; quick samples only the most recent
    ``sample_size`` messages. We build channels whose recent slice disagrees with
    their full history to prove the LIMIT-N sampling is actually applied (not just
    a cross-dupe skip).
    """

    async def test_quick_sampling_uses_recent_window_for_uniqueness(self, db, raw_db):
        # 50 identical spam messages (ids 1..50), then 10 unique recent ones (51..60).
        # Full history: 10 unique / 60 = 16.7% < 30% -> low_uniqueness.
        # Sampled last 10: 10 unique / 10 = 100% -> NOT flagged.
        await _insert_channel(raw_db, 800)
        spam = ["same spam body"] * 50
        recent = [f"fresh unique post number {i}" for i in range(10)]
        await _insert_messages(raw_db, 800, spam + recent)
        analyzer = ChannelAnalyzer(db)

        full = await analyzer.analyze_all()
        full_800 = next(r for r in full.results if r.channel_id == 800)
        assert "low_uniqueness" in full_800.flags

        quick = await analyzer.analyze_all(quick=True, sample_size=10)
        quick_800 = next(r for r in quick.results if r.channel_id == 800)
        assert "low_uniqueness" not in quick_800.flags
        # Sampled total is the window size, not the full history.
        assert quick_800.uniqueness_pct == 100.0

    async def test_quick_sampling_recent_window_for_cyrillic(self, db, raw_db):
        # Old history is all English; the recent window is all Russian.
        # Full: 0% cyrillic -> non_cyrillic. Sampled last 5: 100% -> NOT flagged.
        await _insert_channel(raw_db, 801)
        # 50 english + 5 russian -> 5/55 = 9.1% < 10% threshold in the full history.
        english = [f"english message number {i}" for i in range(50)]
        russian = [f"русское сообщение номер {i}" for i in range(5)]
        await _insert_messages(raw_db, 801, english + russian)
        analyzer = ChannelAnalyzer(db)

        full = await analyzer.analyze_all()
        full_801 = next(r for r in full.results if r.channel_id == 801)
        assert "non_cyrillic" in full_801.flags

        quick = await analyzer.analyze_all(quick=True, sample_size=5)
        quick_801 = next(r for r in quick.results if r.channel_id == 801)
        assert "non_cyrillic" not in quick_801.flags

    async def test_quick_default_sample_size_is_300(self, db, raw_db):
        from tg_agent.filters.criteria import DEFAULT_QUICK_SAMPLE_SIZE

        assert DEFAULT_QUICK_SAMPLE_SIZE == 300
        # With <300 messages the default window covers the whole history, so the
        # quick verdict matches the full one for uniqueness here.
        await _insert_channel(raw_db, 802)
        await _insert_messages(raw_db, 802, ["spam"] * 20)
        analyzer = ChannelAnalyzer(db)
        quick = await analyzer.analyze_all(quick=True)  # no explicit sample_size
        quick_802 = next(r for r in quick.results if r.channel_id == 802)
        assert "low_uniqueness" in quick_802.flags

    async def test_quick_nonpositive_sample_size_falls_back_to_default(self, db, raw_db):
        # Codex review #1138: sample_size=0 -> LIMIT 0 -> every text total is 0 ->
        # the `if total > 0` guards suppress all flags -> a spammy channel would look
        # clean. analyze_all must clamp non-positive sizes to the calibrated default.
        await _insert_channel(raw_db, 805)
        await _insert_messages(raw_db, 805, ["spam"] * 20)  # low_uniqueness in any sane window
        analyzer = ChannelAnalyzer(db)

        for bad in (0, -5):
            report = await analyzer.analyze_all(quick=True, sample_size=bad)
            ch = next(r for r in report.results if r.channel_id == 805)
            assert "low_uniqueness" in ch.flags, f"sample_size={bad} silently produced a clean verdict"
            assert ch.uniqueness_pct is not None

    async def test_full_analyze_never_samples(self, db, raw_db):
        # Explicit sample_size is ignored unless quick=True: full path scans all.
        await _insert_channel(raw_db, 803)
        spam = ["same spam body"] * 50
        recent = [f"fresh unique post {i}" for i in range(10)]
        await _insert_messages(raw_db, 803, spam + recent)
        analyzer = ChannelAnalyzer(db)
        # sample_size passed but quick=False -> must scan full history -> flagged.
        report = await analyzer.analyze_all(quick=False, sample_size=10)
        ch = next(r for r in report.results if r.channel_id == 803)
        assert "low_uniqueness" in ch.flags

    async def test_sampled_sql_limit_in_repo_uniqueness_map(self, db, raw_db):
        # Direct repo-level check that sample_size flips to the LIMIT-N query.
        await _insert_channel(raw_db, 804)
        await _insert_messages(raw_db, 804, ["dup"] * 40 + [f"u{i}" for i in range(5)])
        repo = db.filter_repo
        full_map = await repo.fetch_uniqueness_map(804)
        # full: 6 distinct (dup + u0..u4) / 45
        assert full_map[804] == (45, 6)
        sampled_map = await repo.fetch_uniqueness_map(804, sample_size=5)
        # last 5 ids = u0..u4 -> 5 total, 5 unique
        assert sampled_map[804] == (5, 5)

    async def test_quick_sampling_parallel_path(self, tmp_path):
        from tg_agent.database import Database

        file_db = Database(str(tmp_path / "quick_sample.db"))
        await file_db.initialize()
        try:
            conn = file_db.db
            await _insert_channel(conn, 810)
            spam = ["same spam body"] * 50
            recent = [f"fresh unique post number {i}" for i in range(10)]
            await _insert_messages(conn, 810, spam + recent)

            repo = file_db.filter_repo
            assert repo._can_parallel()

            # Parallel sampled uniqueness map must apply the window.
            # Full: 60 msgs, 11 distinct prefixes (1 spam + 10 recent unique).
            u_full, *_ = await repo.fetch_maps_parallel(None)
            assert u_full[810] == (60, 11)
            # Sampled last 10: the 10 recent unique posts -> 10 total, 10 distinct.
            u_sampled, *_ = await repo.fetch_maps_parallel(None, sample_size=10)
            assert u_sampled[810] == (10, 10)

            analyzer = ChannelAnalyzer(file_db)
            quick = await analyzer.analyze_all(quick=True, sample_size=10)
            ch = next(r for r in quick.results if r.channel_id == 810)
            assert "low_uniqueness" not in ch.flags
        finally:
            await file_db.close()


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
        # non_cyrillic — флаг-метка (решение владельца 06.10.26): не блокирует сбор.
        assert result.is_filtered is False

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

    async def test_noisy_gigagroup_flagged(self, db, raw_db):
        # Regression #971: gigagroup is a real chat type and noisy gigagroups
        # must be flagged just like groups/supergroups.
        await _insert_channel(raw_db, 5011, channel_type="gigagroup")
        texts = ["hi", "ok", "+", "lol", "da", "net", "yes"] + ["long enough message here"]
        await _insert_messages(raw_db, 5011, texts)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(5011)
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

    @pytest.mark.parametrize(
        "username",
        [
            "UB1JH44UM",
            "RRE1MQD0A",
            "EO4ULAEW",
            "HIFP7WAU",
            "EZ7BNQ8ZL",
            "K66CING9V",
            "C81WJ13F",
            "F0ZJJD3Y",
            "OYO8R9JT",
        ],
    )
    async def test_short_farm_username_flagged(self, db, raw_db, username):
        # Ферма 09.10.26 (#1510): 8-9 символов ALL-CAPS с цифрой ВНУТРИ имени —
        # под старый порог Формы 1 {10,} не попадали (10/10 -> clean).
        await _insert_channel(raw_db, 750, username=username)
        await _insert_messages(raw_db, 750, ["any"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(750)
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
        # "BITCOIN24" — 9 chars (short-farm shape), but its only digits sit in
        # the tail (positions 7-8, outside the first-6 window) → not flagged.
        await _insert_channel(raw_db, 705, username="BITCOIN24")
        await _insert_messages(raw_db, 705, ["any"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(705)
        assert "suspicious_username" not in result.flags

    async def test_short_farm_digit_window_boundary_ok(self, db, raw_db):
        # Boundary of the short-farm digit window: a digit exactly at index 6
        # (7th position) is outside the first-6 window → not flagged. Pins
        # {0,5} in SUSPICIOUS_USERNAME_RE — under a {0,6} mutation this name
        # would be flagged.
        await _insert_channel(raw_db, 715, username="QWERTY6UI")
        await _insert_messages(raw_db, 715, ["any"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(715)
        assert "suspicious_username" not in result.flags

    async def test_null_username_ok(self, db, raw_db):
        await _insert_channel(raw_db, 706, username=None)
        await _insert_messages(raw_db, 706, ["any"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(706)
        assert "suspicious_username" not in result.flags

    # CamelCase + digit-tail farm shape (#1482): >=2 caps, >=2 lowercase,
    # >=8 letters, >=3 trailing digits.

    async def test_camelcase_digit_tail_flagged(self, db, raw_db):
        await _insert_channel(raw_db, 707, username="NexusTrend847")
        await _insert_messages(raw_db, 707, ["any"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(707)
        assert "suspicious_username" in result.flags

    async def test_camelcase_long_digit_tail_flagged(self, db, raw_db):
        await _insert_channel(raw_db, 708, username="VegaStream392")
        await _insert_messages(raw_db, 708, ["any"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(708)
        assert "suspicious_username" in result.flags

    async def test_name_year_tail_ok(self, db, raw_db):
        # "JohnDoe1990" — only 7 letters before the year tail → not flagged.
        await _insert_channel(raw_db, 709, username="JohnDoe1990")
        await _insert_messages(raw_db, 709, ["any"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(709)
        assert "suspicious_username" not in result.flags

    async def test_lowercase_only_digit_tail_ok(self, db, raw_db):
        # lowercase-only farms are deliberately out of scope: too many legit
        # usernames share the shape ("haberkanal724").
        await _insert_channel(raw_db, 710, username="haberkanal724")
        await _insert_messages(raw_db, 710, ["any"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(710)
        assert "suspicious_username" not in result.flags

    async def test_single_cap_digit_tail_ok(self, db, raw_db):
        # "Readingzone221" — one capital, three digits → not flagged.
        await _insert_channel(raw_db, 711, username="Readingzone221")
        await _insert_messages(raw_db, 711, ["any"])
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(711)
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

    async def test_manual_flag_not_sticky_without_human_origin(self, db, raw_db):
        # Этап 4 #1312: sticky «manual» уволен. Человеческий вердикт выражается
        # filtered_origin='human' (protected writer-ы); auto-строка с флагом
        # manual больше не переживает цикл reset → apply — канал переоценивается.
        await _insert_channel(raw_db, 712, username="durov")
        await _insert_messages(raw_db, 712, ["unique a", "unique b", "unique c"])
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 1, filter_flags = 'low_uniqueness,manual' "
            "WHERE channel_id = 712"
        )
        await raw_db.commit()

        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        await analyzer.apply_filters(report)

        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = ?", (712,)
        )
        row = await cur.fetchone()
        assert row["is_filtered"] == 0
        assert "manual" not in (row["filter_flags"] or "")


class TestProvenanceHumanExclusion:
    """Этап 4 #1312: человеческие каналы для анализатора не существуют —
    ни метрик, ни вердиктов, ни предложений «а может отфильтруем»."""

    async def test_approved_channel_not_analyzed(self, db, raw_db):
        # channel_id человеческого канала отсутствует в выборке анализатора:
        # доказывает «не трогаем и не считаем», а не просто «не фильтруем».
        await _insert_channel(raw_db, 720, username=None, filtered_origin="human")
        await _insert_messages(raw_db, 720, ["spam"] * 20)
        await _insert_channel(raw_db, 721, username="spammer")
        await _insert_messages(raw_db, 721, ["spam"] * 20)
        await raw_db.commit()

        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()

        assert 720 not in [r.channel_id for r in report.results]
        found = [r for r in report.results if r.channel_id == 721]
        assert len(found) == 1
        assert "low_uniqueness" in found[0].flags

    async def test_fetch_channels_for_analysis_excludes_human(self, db, raw_db):
        await _insert_channel(raw_db, 730, filtered_origin="human")
        await _insert_channel(raw_db, 731)
        await raw_db.commit()

        rows = await db.filter_repo.fetch_channels_for_analysis()
        ids = {row["channel_id"] for row in rows}
        assert 730 not in ids
        assert 731 in ids

        # Точечный запрос человеческого канала тоже пуст — метрики не считаются.
        assert await db.filter_repo.fetch_channels_for_analysis(730) == []
        single_auto = await db.filter_repo.fetch_channels_for_analysis(731)
        assert [row["channel_id"] for row in single_auto] == [731]

    async def test_analyzer_cycle_preserves_human_unfilter(self, db, raw_db):
        # «802»: человек снял авто-фильтр — цикл analyze+apply не возвращает его.
        await _insert_channel(raw_db, 740, username=None, filtered_origin="human")
        await _insert_messages(raw_db, 740, ["spam"] * 20)
        await raw_db.commit()

        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        await analyzer.apply_filters(report)

        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags, filtered_origin FROM channels WHERE channel_id = 740"
        )
        row = await cur.fetchone()
        assert row["is_filtered"] == 0
        assert row["filtered_origin"] == "human"

    async def test_analyzer_cycle_preserves_human_filter_verdict(self, db, raw_db):
        # Человеческий вердикт «фильтровать» цикл анализатора не сбрасывает.
        await _insert_channel(raw_db, 741, username=None, filtered_origin="human")
        await _insert_messages(raw_db, 741, ["spam"] * 20)
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 1, filter_flags = 'low_subscriber_manual' "
            "WHERE channel_id = 741"
        )
        await raw_db.commit()

        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        await analyzer.apply_filters(report)

        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags, filtered_origin FROM channels WHERE channel_id = 741"
        )
        row = await cur.fetchone()
        assert row["is_filtered"] == 1
        assert row["filtered_origin"] == "human"
        assert row["filter_flags"] == "low_subscriber_manual"

    async def test_precheck_subscriber_ratio_skips_human_channel(self, db, raw_db):
        # Пречек соотношения подписчики/сообщения фильтрует auto-канал,
        # человеческий (одинаковые данные) не трогает вовсе.
        await _insert_channel(raw_db, 750, username=None, filtered_origin="human")
        await _insert_messages(raw_db, 750, ["post"] * 100)
        await _insert_stats(raw_db, 750, 3)
        await _insert_channel(raw_db, 751, username="lowsub")
        await _insert_messages(raw_db, 751, ["post"] * 100)
        await _insert_stats(raw_db, 751, 3)
        await raw_db.commit()

        analyzer = ChannelAnalyzer(db)
        applied = await analyzer.precheck_subscriber_ratio()
        assert applied == 1

        human = await db.get_channel_by_channel_id(750)
        assert human.is_filtered is False
        assert human.filtered_origin == "human"
        auto = await db.get_channel_by_channel_id(751)
        assert auto.is_filtered is True
        assert "low_subscriber_ratio" in auto.filter_flags


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

    async def test_precheck_does_not_count_human_unfiltered_channel(self, db, raw_db):
        await raw_db.execute(
            "INSERT INTO channels (channel_id, title, channel_type, is_active) VALUES (?, ?, ?, 1)",
            (1006, "Human Unfiltered", "channel"),
        )
        await raw_db.commit()
        channel = await db.get_channel_by_channel_id(1006)
        assert channel is not None and channel.id is not None
        await db.set_channel_filtered(channel.id, False, origin="human")
        await _insert_messages(raw_db, 1006, [f"msg {i}" for i in range(20)])
        await _insert_stats(raw_db, 1006, 10)

        analyzer = ChannelAnalyzer(db)
        assert await analyzer.precheck_subscriber_ratio() == 0

        cur = await raw_db.execute(
            "SELECT is_filtered, filtered_origin FROM channels WHERE channel_id = 1006"
        )
        row = await cur.fetchone()
        assert row["is_filtered"] == 0
        assert row["filtered_origin"] == "human"

    async def test_analyzer_cycle_preserves_human_unfilter(self, db, raw_db):
        """An automatic apply cycle must not re-filter an owner-unfiltered channel."""
        await _insert_channel(raw_db, 1311, title="802", channel_type="channel")
        await _insert_messages(raw_db, 1311, [f"message {i}" for i in range(100)])
        await _insert_stats(raw_db, 1311, 10)
        await db.set_setting("min_subscribers_filter", "20")
        channel = await db.get_channel_by_channel_id(1311)
        assert channel is not None and channel.id is not None
        await db.set_channel_filtered(channel.id, True, origin="auto")
        await db.set_channel_filtered(channel.id, False, origin="human")

        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        # Stage 4 (#1312): the human channel is not analyzed at all — no
        # metrics, no report row (pinned by TestProvenanceHumanExclusion).
        assert all(r.channel_id != 1311 for r in report.results)
        await analyzer.apply_filters(report)

        cur = await raw_db.execute(
            "SELECT is_filtered, filtered_origin FROM channels WHERE channel_id = 1311"
        )
        row = await cur.fetchone()
        assert (row["is_filtered"], row["filtered_origin"]) == (0, "human")

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
        # 901 скопировал оба текста позже — флаг у копии, источник чист.
        for i, text in enumerate(
            [
                "shared duplicated content alpha beta gamma",
                "shared duplicated content delta epsilon zeta",
            ],
            1,
        ):
            await raw_db.execute(
                "INSERT INTO messages (channel_id, message_id, text, date) VALUES (901, ?, ?, '2025-01-05')",
                (i, text),
            )
        await raw_db.commit()
        await _insert_messages(raw_db, 902, ["independent long content for channel 902 only"])

        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        ch901 = next(r for r in report.results if r.channel_id == 901)
        assert ch901.cross_dupe_pct == 100.0
        assert "cross_channel_spam" in ch901.flags
        ch900 = next(r for r in report.results if r.channel_id == 900)
        assert "cross_channel_spam" not in ch900.flags


class TestApplyFiltersKeepTag:
    async def _tag_channel(self, raw_db, channel_id: int, tag: str) -> None:
        await raw_db.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (tag,))
        await raw_db.execute(
            "INSERT INTO channel_tags (channel_pk, tag_id) "
            "SELECT c.id, t.id FROM channels c, tags t "
            "WHERE c.channel_id = ? AND t.name = ?",
            (channel_id, tag),
        )
        await raw_db.commit()

    async def test_keep_tag_escrow_blocks_autofilter(self, db, raw_db):
        from tg_agent.filters.criteria import KEEP_TAG

        # 900: keep-канал со спам-username — флаг ставится меткой, сбор не блокирует.
        await _insert_channel(raw_db, 900, username="S0IMD1EDUAW")
        await _insert_messages(raw_db, 900, ["hello world", "another text"])
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 1, filter_flags = 'low_subscriber_ratio' "
            "WHERE channel_id = 900"
        )
        await self._tag_channel(raw_db, 900, KEEP_TAG)
        # 901: тот же спам-username без тега — обычный авто-фильтр.
        await _insert_channel(raw_db, 901, username="EXF74CHE3RZ1")
        await _insert_messages(raw_db, 901, ["hello world", "another text"])

        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        await analyzer.apply_filters(report)

        cur = await raw_db.execute(
            "SELECT channel_id, is_filtered, filter_flags FROM channels "
            "WHERE channel_id IN (900, 901)"
        )
        rows = {r[0]: (r[1], r[2]) for r in await cur.fetchall()}

        filtered, flags_901 = rows[901]
        assert filtered == 1
        assert "suspicious_username" in flags_901

        unfiltered, flags_900 = rows[900]
        assert unfiltered == 0
        # старые метки не потеряны + свежая метка доложена
        assert "low_subscriber_ratio" in flags_900
        assert "suspicious_username" in flags_900

    async def test_keep_tag_channel_without_flags_gets_no_flags(self, db, raw_db):
        from tg_agent.filters.criteria import KEEP_TAG

        # Чистый keep-канал: эскроу не должен вешать флаги из воздуха.
        await _insert_channel(raw_db, 910)
        await _insert_messages(raw_db, 910, ["clean unique post", "one more clean post"])
        await self._tag_channel(raw_db, 910, KEEP_TAG)

        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        await analyzer.apply_filters(report)

        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 910"
        )
        is_filtered, filter_flags = await cur.fetchone()
        assert is_filtered == 0
        # Эскроу не вешает блокирующих флагов: только метки (тут — non_cyrillic).
        from tg_agent.filters.criteria import LABEL_ONLY_FLAGS

        assert set((filter_flags or "").split(",")) - {""} <= LABEL_ONLY_FLAGS


_ENGLISH_TEXTS = ["hello world", "test message", "no cyrillic here"] + ["another eng text"] * 7


async def _tag_channel(raw_db, channel_id: int, tag: str) -> None:
    await raw_db.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (tag,))
    await raw_db.execute(
        "INSERT INTO channel_tags (channel_pk, tag_id) "
        "SELECT c.id, t.id FROM channels c, tags t "
        "WHERE c.channel_id = ? AND t.name = ?",
        (channel_id, tag),
    )
    await raw_db.commit()


class TestApplyFiltersLabelOnly:
    """#1517: метки (LABEL_ONLY_FLAGS) должны переживать apply и у обычных
    каналов — apply сохраняет флаги независимо от решения блокировать."""

    async def test_label_only_flag_survives_apply(self, db, raw_db):
        from tg_agent.filters.criteria import LABEL_ONLY_FLAGS

        await _insert_channel(raw_db, 920)
        await _insert_messages(raw_db, 920, _ENGLISH_TEXTS)

        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        ch920 = next(r for r in report.results if r.channel_id == 920)
        assert ch920.flags == ["non_cyrillic"]
        assert ch920.is_filtered is False

        await analyzer.apply_filters(report)

        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 920"
        )
        is_filtered, filter_flags = await cur.fetchone()
        assert is_filtered == 0
        # forwarder — label-only, но каналу без форвардов не проставляется:
        # ассерт по фактически заработанным меткам + контракт #1517 (метки
        # выживают, блокирующих флагов нет).
        flags = set((filter_flags or "").split(",")) - {""}
        assert flags == {"non_cyrillic"}
        assert flags <= set(LABEL_ONLY_FLAGS)

    async def test_label_only_apply_idempotent(self, db, raw_db):
        from tg_agent.filters.criteria import LABEL_ONLY_FLAGS

        await _insert_channel(raw_db, 921)
        await _insert_messages(raw_db, 921, _ENGLISH_TEXTS)

        analyzer = ChannelAnalyzer(db)
        for _ in range(2):
            report = await analyzer.analyze_all()
            await analyzer.apply_filters(report)

        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 921"
        )
        is_filtered, filter_flags = await cur.fetchone()
        assert is_filtered == 0
        # См. тест выше: forwarder в фикстуре не зарабатывается.
        flags = set((filter_flags or "").split(",")) - {""}
        assert flags == {"non_cyrillic"}
        assert flags <= set(LABEL_ONLY_FLAGS)

    async def test_label_only_with_blocking_flag_filters(self, db, raw_db):
        # non_cyrillic + блокирующий suspicious_username → блокировка и оба флага.
        await _insert_channel(raw_db, 922, username="S0IMD1EDUAW")
        await _insert_messages(raw_db, 922, _ENGLISH_TEXTS)

        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        ch922 = next(r for r in report.results if r.channel_id == 922)
        assert ch922.is_filtered is True

        await analyzer.apply_filters(report)

        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 922"
        )
        is_filtered, filter_flags = await cur.fetchone()
        assert is_filtered == 1
        assert set((filter_flags or "").split(",")) == {"non_cyrillic", "suspicious_username"}

    async def test_label_only_respects_human_decision(self, db, raw_db):
        # Human-блокировка не сносится label-only апдейтом (#1517, criterion 5).
        await _insert_channel(raw_db, 923)
        await _insert_messages(raw_db, 923, _ENGLISH_TEXTS)
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 1, filtered_origin = 'human', "
            "filter_flags = '' WHERE channel_id = 923"
        )
        await raw_db.commit()

        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        await analyzer.apply_filters(report)

        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 923"
        )
        is_filtered, filter_flags = await cur.fetchone()
        assert is_filtered == 1
        assert (filter_flags or "") == ""


class TestPrecheckKeepTag:
    """#1516: precheck_subscriber_ratio соблюдает контракт keep-тега."""

    async def test_precheck_skips_keep_channel(self, db, raw_db):
        from tg_agent.filters.criteria import KEEP_TAG

        # Repro #1516: broadcast-канал с keep, 1 подписчик, 3 сообщения.
        await _insert_channel(raw_db, 930)
        await _insert_messages(raw_db, 930, ["msg 1", "msg 2", "msg 3"])
        await _insert_stats(raw_db, 930, 1)
        await _tag_channel(raw_db, 930, KEEP_TAG)
        # Контраст: те же данные без тега → фильтруется как раньше.
        await _insert_channel(raw_db, 931)
        await _insert_messages(raw_db, 931, ["msg 1", "msg 2", "msg 3"])
        await _insert_stats(raw_db, 931, 1)

        analyzer = ChannelAnalyzer(db)
        count = await analyzer.precheck_subscriber_ratio()
        assert count == 1

        cur = await raw_db.execute(
            "SELECT channel_id, is_filtered, filter_flags FROM channels "
            "WHERE channel_id IN (930, 931)"
        )
        rows = {r[0]: (r[1], r[2]) for r in await cur.fetchall()}
        assert rows[930] == (0, "")
        assert rows[931][0] == 1
        assert "low_subscriber_ratio" in rows[931][1]

    async def test_precheck_min_subs_skips_keep_channel(self, db, raw_db):
        from tg_agent.filters.criteria import KEEP_TAG

        await _insert_channel(raw_db, 932)
        await _insert_messages(raw_db, 932, ["msg 1", "msg 2", "msg 3"])
        await _insert_stats(raw_db, 932, 5)
        await _tag_channel(raw_db, 932, KEEP_TAG)
        await _insert_channel(raw_db, 933)
        await _insert_messages(raw_db, 933, ["msg 1", "msg 2", "msg 3"])
        await _insert_stats(raw_db, 933, 5)
        await db.repos.settings.set_setting("min_subscribers_filter", "100")

        analyzer = ChannelAnalyzer(db)
        count = await analyzer.precheck_subscriber_ratio()
        assert count == 1

        cur = await raw_db.execute(
            "SELECT channel_id, is_filtered, filter_flags FROM channels "
            "WHERE channel_id IN (932, 933)"
        )
        rows = {r[0]: (r[1], r[2]) for r in await cur.fetchall()}
        assert rows[932] == (0, "")
        assert rows[933][0] == 1
        assert "low_subscriber_manual" in rows[933][1]


class TestAnalyzerLowEngagement:
    """«Мёртвое вовлечение»: views/подписчик < 10% в окне последних 100 постов.

    Калибровка 15.10.26 на живой БД: ферма ai_for_dev — 7.8% (flag), зомби с
    живым ratio — 0.1% (flag), живые каналы — 23–76% (чист). Порог и окно — в
    criteria.py (LOW_ENGAGEMENT_*). Решение владельца: конъюнкции с
    subs/посты НЕТ — одиночная views-метрика ловит и ферму, и зомби-когорту.
    """

    async def test_farm_profile_flagged(self, db, raw_db):
        # 941 подписчик, 40 постов по 70 просмотров: 70/941 = 7.4% < 10%.
        # subs/посты = 23.5 — «живой»: флаг ставит только views-метрика.
        await _insert_channel(raw_db, 4200)
        await _insert_messages_with_views(raw_db, 4200, [70] * 40)
        await _insert_stats(raw_db, 4200, 941)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(4200)
        assert result.views_ratio_pct == 7.4
        assert "low_engagement" in result.flags
        assert result.is_filtered is True

    async def test_engaged_channel_not_flagged(self, db, raw_db):
        await _insert_channel(raw_db, 4201)
        await _insert_messages_with_views(raw_db, 4201, [500] * 40)
        await _insert_stats(raw_db, 4201, 1000)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(4201)
        assert result.views_ratio_pct == 50.0
        assert "low_engagement" not in result.flags

    async def test_high_ratio_dead_views_still_flagged(self, db, raw_db):
        # Зомби-канал: живой subs/посты (5000/40 = 125), мёртвые просмотры.
        # Пин решения владельца: без конъюнкции — флаг по одной views-метрике.
        await _insert_channel(raw_db, 4202)
        await _insert_messages_with_views(raw_db, 4202, [10] * 40)
        await _insert_stats(raw_db, 4202, 5000)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(4202)
        assert result.views_ratio_pct == 0.2
        assert "low_engagement" in result.flags

    async def test_boundary_views_ratio_not_flagged(self, db, raw_db):
        # Ровно 10.0% — порог строгий (<).
        await _insert_channel(raw_db, 4203)
        await _insert_messages_with_views(raw_db, 4203, [10] * 40)
        await _insert_stats(raw_db, 4203, 100)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(4203)
        assert result.views_ratio_pct == 10.0
        assert "low_engagement" not in result.flags

    async def test_no_views_skip(self, db, raw_db):
        # Ни у одного поста в окне нет views — статистики нет, метки нет.
        await _insert_channel(raw_db, 4204)
        await _insert_messages_with_views(raw_db, 4204, [None] * 40)
        await _insert_stats(raw_db, 4204, 100)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(4204)
        assert result.views_ratio_pct is None
        assert "low_engagement" not in result.flags

    async def test_few_viewed_messages_skip(self, db, raw_db):
        # Просмотренных < LOW_ENGAGEMENT_MIN_VIEWED_MESSAGES — данных мало.
        await _insert_channel(raw_db, 4205)
        await _insert_messages_with_views(raw_db, 4205, [1] * 10)
        await _insert_stats(raw_db, 4205, 100)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(4205)
        assert result.views_ratio_pct is None
        assert "low_engagement" not in result.flags

    async def test_no_stats_skip(self, db, raw_db):
        await _insert_channel(raw_db, 4206)
        await _insert_messages_with_views(raw_db, 4206, [1] * 40)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(4206)
        assert result.views_ratio_pct is None
        assert "low_engagement" not in result.flags

    async def test_chat_excluded(self, db, raw_db):
        # Чаты (group/supergroup/...) не проверяются — только broadcast.
        await _insert_channel(raw_db, 4207, channel_type="supergroup")
        await _insert_messages_with_views(raw_db, 4207, [1] * 40)
        await _insert_stats(raw_db, 4207, 100)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(4207)
        assert result.views_ratio_pct is None
        assert "low_engagement" not in result.flags

    async def test_apply_sets_flag_and_filters(self, db, raw_db):
        await _insert_channel(raw_db, 4208)
        await _insert_messages_with_views(raw_db, 4208, [70] * 40)
        await _insert_stats(raw_db, 4208, 941)
        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        await analyzer.apply_filters(report)
        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 4208"
        )
        row = await cur.fetchone()
        assert row["is_filtered"] == 1
        assert "low_engagement" in row["filter_flags"]

    async def test_quick_mode_computes_engagement(self, db, raw_db):
        # Окно engagement-метрики — всегда последние 100 постов, независимо от
        # quick sample_size (метрика «мёртвое сейчас», не «среднее за жизнь»).
        await _insert_channel(raw_db, 4209)
        await _insert_messages_with_views(raw_db, 4209, [70] * 40)
        await _insert_stats(raw_db, 4209, 941)
        analyzer = ChannelAnalyzer(db)
        quick = await analyzer.analyze_all(quick=True, sample_size=50)
        result = next(r for r in quick.results if r.channel_id == 4209)
        assert "low_engagement" in result.flags

    async def test_null_channel_type_still_checked(self, db, raw_db):
        # NULL channel_type (legacy-импорт, 354 канала на живой БД 15.10.26):
        # судим по данным, а не по типу — гейт «не чат», а не «в broadcast».
        # Реальный кейс: ферма ai_for_dev имеет channel_type IS NULL.
        await _insert_channel(raw_db, 4210, channel_type=None)
        await _insert_messages_with_views(raw_db, 4210, [70] * 40)
        await _insert_stats(raw_db, 4210, 941)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(4210)
        assert result.views_ratio_pct == 7.4
        assert "low_engagement" in result.flags


class TestAnalyzerForwardShare:
    """Метка `forwarder`: ≥50% форвардов в окне последних 100 постов.

    Label-only (решение владельца 16.10.26): пересылочный канал — не спам,
    метка для кураторства, сбор не блокирует.
    """

    async def test_majority_forwards_labeled(self, db, raw_db):
        await _insert_channel(raw_db, 4300)
        await _insert_forward_mix(raw_db, 4300, forwards=40, plains=10)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(4300)
        assert result.forward_pct == 80.0
        assert "forwarder" in result.flags
        # Label-only: метка не блокирует сбор.
        assert result.is_filtered is False

    async def test_minority_forwards_not_labeled(self, db, raw_db):
        await _insert_channel(raw_db, 4301)
        await _insert_forward_mix(raw_db, 4301, forwards=10, plains=40)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(4301)
        assert result.forward_pct == 20.0
        assert "forwarder" not in result.flags

    async def test_boundary_forward_share_labeled(self, db, raw_db):
        # Ровно 50.0% — порог нестрогий (>=): «наполовину пересылочный» метится.
        await _insert_channel(raw_db, 4305)
        await _insert_forward_mix(raw_db, 4305, forwards=20, plains=20)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(4305)
        assert result.forward_pct == 50.0
        assert "forwarder" in result.flags

    async def test_few_messages_skip(self, db, raw_db):
        # Меньше FORWARD_SHARE_MIN_MESSAGES — статистики нет.
        await _insert_channel(raw_db, 4302)
        await _insert_forward_mix(raw_db, 4302, forwards=10, plains=5)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(4302)
        assert result.forward_pct is None
        assert "forwarder" not in result.flags

    async def test_textless_forwards_counted(self, db, raw_db):
        # Форвард-медиа без текста тоже форвард.
        await _insert_channel(raw_db, 4303)
        await _insert_forward_mix(raw_db, 4303, forwards=40, plains=10, textless_forwards=True)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(4303)
        assert result.forward_pct == 80.0
        assert "forwarder" in result.flags

    async def test_quick_mode_computes_forward_share(self, db, raw_db):
        # Окно метки — всегда последние 100 постов, независимо от quick
        # sample_size.
        await _insert_channel(raw_db, 4304)
        await _insert_forward_mix(raw_db, 4304, forwards=40, plains=10)
        analyzer = ChannelAnalyzer(db)
        quick = await analyzer.analyze_all(quick=True, sample_size=50)
        result = next(r for r in quick.results if r.channel_id == 4304)
        assert "forwarder" in result.flags


class TestApplyWhitelist:
    """Whitelist-режим #1490 (MVP): доверие по тегу «авторский контент»."""

    async def test_untagged_channels_filtered_with_flag(self, db, raw_db):
        await _insert_channel(raw_db, 5001)
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 1, filter_flags = 'low_uniqueness' "
            "WHERE channel_id = 5001"
        )
        await _insert_channel(raw_db, 5002)
        await raw_db.commit()

        analyzer = ChannelAnalyzer(db)
        count = await analyzer.apply_whitelist()

        assert count == 2
        cur = await raw_db.execute(
            "SELECT channel_id, is_filtered, filter_flags FROM channels "
            "WHERE channel_id IN (5001, 5002) ORDER BY channel_id"
        )
        rows = {r[0]: (r[1], r[2]) for r in await cur.fetchall()}
        # Старые флаги сохраняются как метки, whitelist-флаг доложен.
        assert rows[5001] == (1, "low_uniqueness,no_author_content")
        assert rows[5002] == (1, WHITELIST_FLAG)

    async def test_tagged_channel_trusted_despite_flags(self, db, raw_db):
        await _insert_channel(raw_db, 5011, username="S0IMD1EDUAW")
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 1, "
            "filter_flags = 'low_uniqueness,suspicious_username' WHERE channel_id = 5011"
        )
        await _tag_channel(raw_db, 5011, AUTHOR_CONTENT_TAG)

        analyzer = ChannelAnalyzer(db)
        count = await analyzer.apply_whitelist()

        assert count == 0
        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 5011"
        )
        is_filtered, flags = await cur.fetchone()
        assert is_filtered == 0
        # Флаги остаются метками (их видит filter suspects), доверие сильнее флагов.
        assert "low_uniqueness" in flags
        assert "suspicious_username" in flags

    async def test_human_decisions_untouched_both_ways(self, db, raw_db):
        # 5021: human-вердикт «фильтровать» — whitelist не переигрывает и не
        # доложает флаг; 5022: human «не фильтровать» — не фильтруется;
        # 5023: тегированный, но human отфильтровал — доверие не сильнее человека.
        await _insert_channel(raw_db, 5021)
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 1, filtered_origin = 'human', "
            "filter_flags = 'manual' WHERE channel_id = 5021"
        )
        await _insert_channel(raw_db, 5022)
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 0, filtered_origin = 'human', "
            "filter_flags = '' WHERE channel_id = 5022"
        )
        await _insert_channel(raw_db, 5023)
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 1, filtered_origin = 'human', "
            "filter_flags = 'manual' WHERE channel_id = 5023"
        )
        await _tag_channel(raw_db, 5023, AUTHOR_CONTENT_TAG)
        await raw_db.commit()

        analyzer = ChannelAnalyzer(db)
        await analyzer.apply_whitelist()

        cur = await raw_db.execute(
            "SELECT channel_id, is_filtered, filter_flags FROM channels "
            "WHERE channel_id IN (5021, 5022, 5023) ORDER BY channel_id"
        )
        rows = {r[0]: (r[1], r[2]) for r in await cur.fetchall()}
        assert rows[5021] == (1, "manual")
        assert rows[5022] == (0, "")
        assert rows[5023] == (1, "manual")

    async def test_keep_tag_trusted(self, db, raw_db):
        from tg_agent.filters.criteria import KEEP_TAG

        await _insert_channel(raw_db, 5031)
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 1, filter_flags = 'low_subscriber_ratio' "
            "WHERE channel_id = 5031"
        )
        await _tag_channel(raw_db, 5031, KEEP_TAG)

        analyzer = ChannelAnalyzer(db)
        count = await analyzer.apply_whitelist()

        assert count == 0
        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 5031"
        )
        is_filtered, flags = await cur.fetchone()
        assert is_filtered == 0
        assert "low_subscriber_ratio" in flags
        assert WHITELIST_FLAG not in flags

    async def test_idempotent_second_run(self, db, raw_db):
        await _insert_channel(raw_db, 5041)
        await _insert_channel(raw_db, 5042)
        await _tag_channel(raw_db, 5042, AUTHOR_CONTENT_TAG)
        await raw_db.commit()

        analyzer = ChannelAnalyzer(db)
        first = await analyzer.apply_whitelist()
        second = await analyzer.apply_whitelist()

        assert first == 1
        assert second == 1
        cur = await raw_db.execute(
            "SELECT channel_id, is_filtered, filter_flags FROM channels "
            "WHERE channel_id IN (5041, 5042) ORDER BY channel_id"
        )
        rows = {r[0]: (r[1], r[2]) for r in await cur.fetchall()}
        assert rows[5041] == (1, WHITELIST_FLAG)
        assert rows[5042][0] == 0
        assert WHITELIST_FLAG not in (rows[5042][1] or "")

    async def test_stale_whitelist_flag_removed_after_tagging(self, db, raw_db):
        # Сценарий ревью #1573: первый прогон фильтрует (no_author_content),
        # затем канал тегируют — второй прогон снимает фильтр И stale-метку
        # «канал без тега», иначе она лжёт куратору навсегда.
        await _insert_channel(raw_db, 5061)
        analyzer = ChannelAnalyzer(db)
        await analyzer.apply_whitelist()

        await _tag_channel(raw_db, 5061, AUTHOR_CONTENT_TAG)
        await analyzer.apply_whitelist()

        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 5061"
        )
        is_filtered, flags = await cur.fetchone()
        assert is_filtered == 0
        assert WHITELIST_FLAG not in (flags or "")

    async def test_regular_apply_clears_whitelist_flag(self, db, raw_db):
        # Whitelist — явный режим: обычный blacklist-цикл (reset→apply) сносит
        # no_author_content и возвращает канал под авто-вердикты (семантика
        # зафиксирована сознательно — открытый вопрос Q4 из #1490).
        await _insert_channel(raw_db, 5051)
        analyzer = ChannelAnalyzer(db)
        await analyzer.apply_whitelist()
        report = await analyzer.analyze_all()
        await analyzer.apply_filters(report)

        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 5051"
        )
        is_filtered, flags = await cur.fetchone()
        assert is_filtered == 0
        assert WHITELIST_FLAG not in (flags or "")


class TestMergeFilterFlag:
    """Операторские sticky-флаги #1499: merge_filter_flag."""

    async def _pk(self, raw_db, channel_id: int) -> int:
        cur = await raw_db.execute("SELECT id FROM channels WHERE channel_id = ?", (channel_id,))
        return (await cur.fetchone())[0]

    async def test_set_marks_durable_human_verdict(self, db, raw_db):
        await _insert_channel(raw_db, 6001, username="durov")
        await raw_db.execute(
            "UPDATE channels SET filter_flags = 'username_changed' WHERE channel_id = 6001"
        )
        await raw_db.commit()
        pk = await self._pk(raw_db, 6001)

        applied = await db.repos.channels.merge_filter_flag(pk, "manual", add=True)

        assert applied is True
        cur = await raw_db.execute(
            "SELECT is_filtered, filtered_origin, filter_flags FROM channels WHERE id = ?",
            (pk,),
        )
        is_filtered, origin, flags = await cur.fetchone()
        assert (is_filtered, origin, flags) == (1, "human", "manual,username_changed")

    async def test_unset_returns_to_auto_and_keeps_other_flags(self, db, raw_db):
        await _insert_channel(raw_db, 6002)
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 1, filtered_origin = 'human', "
            "filter_flags = 'manual,title_changed' WHERE channel_id = 6002"
        )
        await raw_db.commit()
        pk = await self._pk(raw_db, 6002)

        applied = await db.repos.channels.merge_filter_flag(pk, "manual", add=False)

        assert applied is True
        cur = await raw_db.execute(
            "SELECT is_filtered, filtered_origin, filter_flags FROM channels WHERE id = ?",
            (pk,),
        )
        is_filtered, origin, flags = await cur.fetchone()
        # is_filtered не меняется — его пересчитает следующий apply.
        assert (is_filtered, origin, flags) == (1, "auto", "title_changed")

    async def test_unset_absent_flag_is_noop(self, db, raw_db):
        # Холостой unset не должен снимать чужое human-решение «не фильтровать»;
        # caller'у сообщается no-op (None), чтобы CLI не рапортовал ложное изменение.
        await _insert_channel(raw_db, 6003)
        await raw_db.execute(
            "UPDATE channels SET is_filtered = 0, filtered_origin = 'human', "
            "filter_flags = '' WHERE channel_id = 6003"
        )
        await raw_db.commit()
        pk = await self._pk(raw_db, 6003)

        applied = await db.repos.channels.merge_filter_flag(pk, "manual", add=False)

        assert applied is None
        cur = await raw_db.execute(
            "SELECT is_filtered, filtered_origin, filter_flags FROM channels WHERE id = ?",
            (pk,),
        )
        is_filtered, origin, flags = await cur.fetchone()
        assert (is_filtered, origin, flags) == (0, "human", "")

    async def test_missing_pk_returns_false(self, db):
        applied = await db.repos.channels.merge_filter_flag(424242, "manual", add=True)
        assert applied is False

    async def test_manual_verdict_survives_apply_filters(self, db, raw_db):
        # Инвариант #1499: вердикт через merge_filter_flag переживает полный
        # цикл analyze + apply (reset не сносит human-origin).
        await _insert_channel(raw_db, 6005)
        pk = await self._pk(raw_db, 6005)
        await db.repos.channels.merge_filter_flag(pk, "manual", add=True)

        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        await analyzer.apply_filters(report)

        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE id = ?", (pk,)
        )
        is_filtered, flags = await cur.fetchone()
        assert is_filtered == 1
        assert "manual" in flags


def _recent_iso(minutes_ago: int = 0) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).isoformat()


async def _insert_window_messages(db, channel_id, count, *, minutes_ago=0, base=9000):
    """count сообщений в окне 24ч (minutes_ago=0) или вне его (minutes_ago > 1440)."""
    date = _recent_iso(minutes_ago)
    for i in range(count):
        await db.execute(
            "INSERT INTO messages (channel_id, message_id, text, date) VALUES (?, ?, ?, ?)",
            (channel_id, base + i, f"ленточный пост номер {i}", date),
        )
    await db.commit()


class TestAnalyzerHighVolume:
    """«Массовый постинг» (high_volume): count_24h >= max(P95, HIGH_VOLUME_MIN_24H).

    Метрика — число сообщений канала за последние 24ч; порог — P95 распределения
    объёма по всем каналам отчёта с абсолютным полом HIGH_VOLUME_MIN_24H (иначе
    плоская выборка флагает ~5% каналов по построению). Label-only до калибровки
    на живой БД: флаг — метка для кураторства, сбор не блокирует (#1571).
    """

    async def test_feed_channel_flagged(self, db, raw_db):
        # Триггер из #1571: 123 сообщения за 24ч против 9 «тихих» по 5.
        await _insert_channel(raw_db, 5200)
        await _insert_window_messages(raw_db, 5200, 123)
        for i in range(9):
            await _insert_channel(raw_db, 5201 + i)
            await _insert_window_messages(raw_db, 5201 + i, 5, base=9100 + i * 100)
        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        by_id = {r.channel_id: r for r in report.results}
        assert by_id[5200].volume_24h == 123
        assert "high_volume" in by_id[5200].flags
        for i in range(9):
            assert by_id[5201 + i].volume_24h == 5
            assert "high_volume" not in by_id[5201 + i].flags

    async def test_relative_threshold_bites_above_floor(self, db, raw_db):
        # P95 > пола: пять каналов по 110 и один 200 → p95=177.5, флагает
        # только 200, хотя 110 выше абсолютного пола. Относительная часть
        # порога работает, а не только пол.
        for i in range(5):
            await _insert_channel(raw_db, 5300 + i)
            await _insert_window_messages(raw_db, 5300 + i, 110, base=9300 + i * 200)
        await _insert_channel(raw_db, 5305)
        await _insert_window_messages(raw_db, 5305, 200, base=9400)
        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        by_id = {r.channel_id: r for r in report.results}
        assert "high_volume" in by_id[5305].flags
        for i in range(5):
            assert "high_volume" not in by_id[5300 + i].flags

    async def test_floor_on_flat_distribution(self, db, raw_db):
        # Плоская выборка: у всех по 5 сообщений за 24ч, p95=5 — без пола
        # флагался бы каждый пятый канал по построению. Пол защищает.
        for i in range(20):
            await _insert_channel(raw_db, 5400 + i)
            await _insert_window_messages(raw_db, 5400 + i, 5, base=9500 + i * 100)
        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        assert all("high_volume" not in r.flags for r in report.results)

    async def test_boundary_exactly_at_threshold(self, db, raw_db):
        # Ровно 100 (= HIGH_VOLUME_MIN_24H, p95≈57 ниже) → порог нестрогий (>=).
        await _insert_channel(raw_db, 5500)
        await _insert_window_messages(raw_db, 5500, 100)
        for i in range(9):
            await _insert_channel(raw_db, 5501 + i)
            await _insert_window_messages(raw_db, 5501 + i, 5, base=9700 + i * 100)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(5500)
        assert result.volume_24h == 100
        assert "high_volume" in result.flags

    async def test_old_messages_outside_window(self, db, raw_db):
        # 24ч-окно: старые сообщения не считаются, свежие — считаются.
        await _insert_channel(raw_db, 5600)
        await _insert_window_messages(raw_db, 5600, 123, minutes_ago=2 * 24 * 60)
        await _insert_channel(raw_db, 5601)
        await _insert_window_messages(raw_db, 5601, 123)
        for i in range(9):
            await _insert_channel(raw_db, 5602 + i)
            await _insert_window_messages(raw_db, 5602 + i, 5, base=9800 + i * 100)
        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        by_id = {r.channel_id: r for r in report.results}
        assert by_id[5600].volume_24h == 0
        assert "high_volume" not in by_id[5600].flags
        assert by_id[5601].volume_24h == 123
        assert "high_volume" in by_id[5601].flags

    async def test_single_channel_analyze_applies_floor(self, db, raw_db):
        # Одиночный analyze_channel: p95 вырождается в собственный count,
        # решает пол — 50/24ч не флагается, 123/24ч флагается.
        await _insert_channel(raw_db, 5700)
        await _insert_window_messages(raw_db, 5700, 50, base=9950)
        analyzer = ChannelAnalyzer(db)
        result = await analyzer.analyze_channel(5700)
        assert result.volume_24h == 50
        assert "high_volume" not in result.flags

        await _insert_channel(raw_db, 5701)
        await _insert_window_messages(raw_db, 5701, 123, base=9999)
        result = await analyzer.analyze_channel(5701)
        assert result.volume_24h == 123
        assert "high_volume" in result.flags

    async def test_apply_is_blocking(self, db, raw_db):
        # Блокирующий (#1676): high_volume ставит is_filtered=1, флаг остаётся
        # в filter_flags как метка для куратора.
        await _insert_channel(raw_db, 5800)
        await _insert_window_messages(raw_db, 5800, 123)
        for i in range(9):
            await _insert_channel(raw_db, 5801 + i)
            await _insert_window_messages(raw_db, 5801 + i, 5, base=10200 + i * 100)
        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        await analyzer.apply_filters(report)
        cur = await raw_db.execute(
            "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = 5800"
        )
        row = await cur.fetchone()
        assert row["is_filtered"] == 1
        assert "high_volume" in row["filter_flags"]
        assert "high_volume" not in LABEL_ONLY_FLAGS

    async def test_quick_mode_computes_volume(self, db, raw_db):
        # 24ч-окно не зависит от quick-семпла: sample_size=10 меньше объёма
        # канала, метрика всё равно по календарным суткам.
        await _insert_channel(raw_db, 5900)
        await _insert_window_messages(raw_db, 5900, 123, base=10400)
        for i in range(9):
            await _insert_channel(raw_db, 5901 + i)
            await _insert_window_messages(raw_db, 5901 + i, 5, base=10500 + i * 100)
        analyzer = ChannelAnalyzer(db)
        quick = await analyzer.analyze_all(quick=True, sample_size=10)
        result = next(r for r in quick.results if r.channel_id == 5900)
        assert result.volume_24h == 123
        assert "high_volume" in result.flags

    async def test_volume_map_counts_window_and_zero(self, db, raw_db):
        # Репо-метод: каналы без свежих сообщений входят в карту с нулём —
        # они участвуют в P95-распределении.
        await _insert_channel(raw_db, 5950)
        await _insert_window_messages(raw_db, 5950, 123, minutes_ago=2 * 24 * 60, base=10700)
        await _insert_channel(raw_db, 5951)
        await _insert_window_messages(raw_db, 5951, 2, base=10900)
        volume_map = await db.filter_repo.fetch_volume_map()
        assert volume_map[5950] == 0
        assert volume_map[5951] == 2
        single = await db.filter_repo.fetch_volume_map(channel_id=5951)
        assert single == {5951: 2}

    async def test_percentile_helper_linear_interpolation(self):
        # Утилита перцентиля (линейная интерполяция, как numpy.percentile).
        from tg_agent.filters.analyzer import _percentile

        assert _percentile([], 95.0) is None
        assert _percentile([7], 95.0) == 7.0
        assert _percentile([5] * 9 + [123], 95.0) == pytest.approx(69.9)
        assert _percentile([110] * 5 + [200], 95.0) == pytest.approx(177.5)
