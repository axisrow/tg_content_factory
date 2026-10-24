"""Инкрементальный справочник текст-статистики фильтратора (#1679).

Контракт: дельты при вставке повторяют условия полных SQL-шаблонов filters.py
в точности, быстрый путь анализатора даёт те же карты, что живой SQL на той
же БД, дельты и rebuild сходятся на граничных данных.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from tg_agent.filters.analyzer import ChannelAnalyzer
from tg_agent.models import Channel, Message

_BASE = datetime(2026, 10, 24, 12, 0, 0, tzinfo=UTC)


def _msg(
    channel_id: int,
    message_id: int,
    text: str | None,
    *,
    minutes: int = 0,
    forward_from: int | None = None,
    entities_json: str | None = None,
) -> Message:
    return Message(
        channel_id=channel_id,
        message_id=message_id,
        text=text,
        date=_BASE + timedelta(minutes=minutes),
        forward_from_channel_id=forward_from,
        entities_json=entities_json,
    )


async def _add_channels(db, *channel_ids: int) -> None:
    cur = await db.db.execute(
        "INSERT INTO channels (channel_id, title, channel_type) VALUES "
        + ", ".join(f"({cid}, 'C{cid}', 'channel')" for cid in channel_ids)
    )
    await cur.close()


async def _stats_row(db, channel_id: int) -> tuple:
    cur = await db.db.execute(
        "SELECT total_all, total_text, total_text_no_fwd, short, cyr, short_link "
        "FROM channel_text_stats WHERE channel_id = ?",
        (channel_id,),
    )
    return await cur.fetchone()


class TestInsertDeltas:
    async def test_counters_and_prefixes(self, db, filters_repo):
        """Дельты повторяют условия полных шаблонов: пустая строка считается
        только в short, не-NULL без текста — только в total_all, ссылка —
        регистронезависимо, форвард — вне префиксов и uniqueness."""
        msgs = [
            _msg(700, 1, None),  # только total_all
            _msg(700, 2, ""),  # total_all + short (len 0 ≤ 10)
            _msg(700, 3, "короткий", minutes=1),  # text+no_fwd+short+cyr
            _msg(700, 4, "LONG ENOUGH LATIN TEXT WITHOUT CYRILLIC " * 3, minutes=2),
            _msg(700, 5, "текст со ссылкой HTTPS://EXAMPLE.COM", minutes=3),
            _msg(700, 6, "форвард с текстом", minutes=4, forward_from=999),
            _msg(700, 7, "текст с entity-ссылкой", minutes=5, entities_json='[{"url":"http://x"}]'),
        ]
        await db.repos.messages.insert_messages_batch(msgs)

        row = await _stats_row(db, 700)
        # total_all=7; total_text=5 (непустые); no_fwd=4 (плюс без форварда);
        # short: "" и "короткий"; cyr — все 4 кириллических текста;
        # short_link: HTTPS (lower) и entity-ссылка.
        assert tuple(row) == (7, 5, 4, 2, 4, 2)
        cur = await db.db.execute(
            "SELECT prefix, first_date, cnt FROM channel_prefix_stats "
            "WHERE channel_id = 700 ORDER BY prefix"
        )
        prefixes = await cur.fetchall()
        # Префиксы — только не-форвардный непустой текст: 4 строки.
        assert len(prefixes) == 4
        assert all(row["cnt"] == 1 for row in prefixes)

    async def test_duplicate_prefix_keeps_min_date(self, db, filters_repo):
        await db.repos.messages.insert_messages_batch(
            [
                _msg(701, 1, "одинаковый префикс двух постов", minutes=10),
                _msg(701, 2, "одинаковый префикс двух постов", minutes=5),
            ]
        )
        cur = await db.db.execute(
            "SELECT first_date, cnt FROM channel_prefix_stats WHERE channel_id = 701"
        )
        row = await cur.fetchone()
        assert row["cnt"] == 2
        assert row["first_date"] == (_BASE + timedelta(minutes=5)).isoformat()

    async def test_existing_message_no_delta(self, db, filters_repo):
        msgs = [_msg(702, 1, "текст существующего сообщения")]
        await db.repos.messages.insert_messages_batch(msgs)
        # Повторная вставка той же пары (channel_id, message_id) — дельты нет.
        await db.repos.messages.insert_messages_batch(msgs)
        # cyr=1: текст кириллический.
        assert tuple(await _stats_row(db, 702)) == (1, 1, 1, 0, 1, 0)

    async def test_insert_message_single_path(self, db, filters_repo):
        await db.repos.messages.insert_message(_msg(703, 1, "одиночная вставка текста"))
        assert tuple(await _stats_row(db, 703)) == (1, 1, 1, 0, 1, 0)

    async def test_premium_purge_rebuilds_affected_channels(self, db, filters_repo):
        """Частичное удаление (premium-кэш) — точечный пересчёт канала (#1679)."""
        await db.repos.messages.insert_messages_batch(
            [
                _msg(704, 1, "обычный собранный текст канала"),
                _msg(704, 2, "сообщение из премиум-поиска"),
            ]
        )
        # Пометить второе как premium-кэш и удалить только его.
        await db.db.execute(
            "UPDATE messages SET premium_search_query = 'тест' "
            "WHERE channel_id = 704 AND message_id = 2"
        )
        await db.db.commit()
        deleted = await db.repos.messages.delete_premium_search_results("тест")
        assert deleted == 1
        # Счётчики пересчитаны из оставшихся сообщений, а не обнулены и не
        # оставлены завышенными.
        assert tuple(await _stats_row(db, 704)) == (1, 1, 1, 0, 1, 0)
        cur = await db.db.execute(
            "SELECT COUNT(*) AS n FROM channel_prefix_stats WHERE channel_id = 704"
        )
        assert (await cur.fetchone())["n"] == 1


class TestDeleteResetsStats:
    async def test_purge_resets_sidecar(self, db, filters_repo):
        await db.repos.messages.insert_messages_batch([_msg(710, 1, "текст для purge-теста")])
        assert await _stats_row(db, 710) is not None
        deleted = await db.repos.messages.delete_messages_for_channel(710)
        assert deleted == 1
        assert await _stats_row(db, 710) is None
        cur = await db.db.execute(
            "SELECT COUNT(*) AS n FROM channel_prefix_stats WHERE channel_id = 710"
        )
        assert (await cur.fetchone())["n"] == 0

    async def test_hard_delete_removes_sidecar(self, db, filters_repo, channels_repo):
        pk = await channels_repo.add_channel(
            Channel(channel_id=711, title="del", channel_type="channel")
        )
        await db.repos.messages.insert_messages_batch([_msg(711, 1, "текст для hard-delete")])
        await channels_repo.delete_channel(pk)
        assert await _stats_row(db, 711) is None


class TestFastPathParity:
    async def _seed(self, db):
        """Три канала: копия позже, одновременные копии, уникальный."""
        msgs = [
            # 720 — источник, 721 — скопировал позже.
            _msg(720, 1, "общий текст длиннее десяти символов", minutes=0),
            _msg(721, 1, "общий текст длиннее десяти символов", minutes=30),
            _msg(720, 2, "уникальный текст канала-источника тут", minutes=1),
            # 722/723 — одновременные копии.
            _msg(722, 1, "одновременный пост двух близнецов сразу", minutes=2),
            _msg(723, 1, "одновременный пост двух близнецов сразу", minutes=2),
            _msg(722, 2, "свой текст второго близнеца уникальный", minutes=3),
            _msg(723, 2, "свой текст первого близнеца уникальный", minutes=3),
        ]
        await db.repos.messages.insert_messages_batch(msgs)
        await _add_channels(db, 720, 721, 722, 723)

    async def test_text_maps_match_live_sql(self, db, filters_repo):
        await self._seed(db)
        await filters_repo.rebuild_text_stats()
        fast = await filters_repo.fetch_text_stats_maps_fast()
        live_uniq = await filters_repo.fetch_uniqueness_map()
        live_short = await filters_repo.fetch_short_message_map()
        live_cyr = await filters_repo.fetch_cyrillic_map()
        live_auto = await filters_repo.fetch_auto_feed_map()
        assert fast[0] == live_uniq
        assert fast[1] == live_short
        assert fast[2] == live_cyr
        assert fast[3] == live_auto

    async def test_analyzer_fast_path_same_flags(self, db, filters_repo):
        """Полный analyze по справочнику = analyze по живому SQL (флаги/метрики)."""
        await self._seed(db)

        slow = await ChannelAnalyzer(db).analyze_all()
        await filters_repo.rebuild_text_stats()
        await filters_repo.set_stats_populated(True)
        fast = await ChannelAnalyzer(db).analyze_all()

        slow_by_id = {r.channel_id: r for r in slow.results}
        for result in fast.results:
            slow_r = slow_by_id[result.channel_id]
            assert result.flags == slow_r.flags, result.channel_id
            assert result.is_filtered == slow_r.is_filtered
            assert result.cross_dupe_pct == slow_r.cross_dupe_pct
            assert result.uniqueness_pct == slow_r.uniqueness_pct
            assert result.volume_24h == slow_r.volume_24h

    async def test_flag_off_falls_back_to_live_sql(self, db, filters_repo):
        await self._seed(db)
        # Справочник не заполнен и флаг не ставился — анализатор идёт по живому SQL.
        assert await filters_repo.stats_populated() is False
        report = await ChannelAnalyzer(db).analyze_all()
        assert report.results


class TestDeltaRebuildEquivalence:
    async def test_deltas_match_rebuild_on_boundaries(self, db, filters_repo):
        """Дельты при вставке ≡ rebuild из истории на граничных случаях.

        Единственная защита от рассинхрона трёх копий условий (дельты-Python,
        полные шаблоны, rebuild-SQL): если правка критерия коснётся одной
        копии и не тронет другую — этот тест красный.
        """
        boundary = [
            _msg(740, 1, None),
            _msg(740, 2, ""),  # пустая строка: только total_all + short
            _msg(740, 3, "x" * 10, minutes=1),  # граница short (<=10)
            _msg(740, 4, "x" * 11, minutes=2),  # сразу после границы
            _msg(740, 5, "кириллица со ссылкой http://a", minutes=3),
            _msg(740, 6, "z" * 200 + " http://a", minutes=4),  # ссылка при len>=200
            _msg(740, 7, "пересланный пост http://a", minutes=5, forward_from=555),
            _msg(740, 8, "текст с entity-ссылкой", minutes=6, entities_json='{"u":"http://a"}'),
        ]
        await db.repos.messages.insert_messages_batch(boundary)

        async def _snapshot() -> tuple[list, list]:
            cur = await db.db.execute(
                "SELECT * FROM channel_text_stats ORDER BY channel_id"
            )
            stats = await cur.fetchall()
            cur = await db.db.execute(
                "SELECT * FROM channel_prefix_stats ORDER BY channel_id, prefix"
            )
            prefixes = await cur.fetchall()
            return [tuple(r) for r in stats], [tuple(r) for r in prefixes]

        before = await _snapshot()
        await filters_repo.rebuild_text_stats()
        after = await _snapshot()
        assert before == after

        # И точечный пересчёт канала согласован с полным.
        await db.repos.messages.insert_messages_batch(
            [_msg(741, 1, "второй канал для точечного пересчёта")]
        )
        await db.repos.messages.delete_premium_search_results("нет такого запроса")
        await filters_repo.rebuild_channel_text_stats([740])
        await filters_repo.rebuild_text_stats()
        assert await _snapshot() == await _snapshot()


class TestRebuild:
    async def test_rebuild_replaces_stale_stats(self, db, filters_repo):
        await db.repos.messages.insert_messages_batch([_msg(730, 1, "текст первого прогона")])
        await filters_repo.rebuild_text_stats()
        # Мусорная строка «прошлой жизни» должна исчезнуть при rebuild.
        await db.db.execute(
            "INSERT INTO channel_prefix_stats (channel_id, prefix, first_date, cnt) "
            "VALUES (999, 'мусорный префикс прошлой жизни', '2020-01-01', 5)"
        )
        await db.db.commit()
        await filters_repo.rebuild_text_stats()
        cur = await db.db.execute(
            "SELECT COUNT(*) AS n FROM channel_prefix_stats WHERE channel_id = 999"
        )
        assert (await cur.fetchone())["n"] == 0

    async def test_rebuild_survives_delta_between_delete_and_insert(self, db, filters_repo):
        """Гонка rebuild ↔ дельта воркера (ревью #1680).

        Дельта, закоммиченная воркером в окне между DELETE и агрегатным
        INSERT...SELECT (инструкции rebuild — отдельные короткие write-lock'и),
        переживает DELETE; до upsert-а она роняла агрегат по UNIQUE-PK.
        """
        await db.repos.messages.insert_messages_batch([_msg(732, 1, "текст до гонки")])

        orig_execute_write = db.execute_write
        raced = False

        async def racing_execute_write(sql, params=()):
            nonlocal raced
            await orig_execute_write(sql, params)
            if not raced and sql.lstrip() == "DELETE FROM channel_text_stats":
                raced = True
                # Воркер коммитит дельту ровно в окне DELETE → INSERT.
                await db.repos.messages.insert_messages_batch(
                    [_msg(732, 2, "текст, собранный посреди rebuild", minutes=1)]
                )

        db.execute_write = racing_execute_write
        try:
            await filters_repo.rebuild_text_stats()
        finally:
            db.execute_write = orig_execute_write
        assert raced
        # Точный агрегат заменил пережившую дельту: обе строки в счётчиках
        # (кириллица, длина > 10, без ссылок и форвардов).
        assert tuple(await _stats_row(db, 732)) == (2, 2, 2, 0, 2, 0)

        # Итог гонки ≡ чистый rebuild на той же истории.

        async def _snapshot() -> tuple[list, list]:
            cur = await db.db.execute("SELECT * FROM channel_text_stats ORDER BY channel_id")
            stats = await cur.fetchall()
            cur = await db.db.execute(
                "SELECT * FROM channel_prefix_stats ORDER BY channel_id, prefix"
            )
            prefixes = await cur.fetchall()
            return [tuple(r) for r in stats], [tuple(r) for r in prefixes]

        after_race = await _snapshot()
        await filters_repo.rebuild_text_stats()
        assert await _snapshot() == after_race

    async def test_rebuild_enables_fast_path_flag(self, db, filters_repo):
        await db.repos.messages.insert_messages_batch([_msg(731, 1, "текст для флага")])
        assert await filters_repo.stats_populated() is False
        await filters_repo.rebuild_text_stats()
        await filters_repo.set_stats_populated(True)
        assert await filters_repo.stats_populated() is True
        await filters_repo.set_stats_populated(False)
        assert await filters_repo.stats_populated() is False
