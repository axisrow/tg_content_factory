"""Репозиторий аггрегатных метрик каналов для фильтрации качества (ChannelAnalyzer)."""

from __future__ import annotations

import asyncio
import logging
import re
from typing import TYPE_CHECKING, cast

import aiosqlite

from tg_agent.database.pool import ReadConnection

# Intentionally duplicated from tg_agent/filters/criteria.py — UDF layer must not
# depend on the filters package to keep DB initialisation self-contained.
_CYRILLIC_RE = re.compile(r"[а-яА-ЯёЁ]")

logger = logging.getLogger(__name__)


def _has_cyrillic_udf(text: str | None) -> int:
    if not text:
        return 0
    return 1 if _CYRILLIC_RE.search(text) else 0


# ── SQL templates ─────────────────────────────────────────────────────────
# Used by both the sequential fetch_* methods and the parallel
# _fetch_*_conn helpers below.

_SQL_CHANNELS = """
    SELECT
        c.channel_id,
        c.title,
        c.username,
        c.channel_type,
        COALESCE(cnt.total, 0) AS message_count
    FROM channels c
    LEFT JOIN (
        SELECT channel_id, COUNT(*) AS total
        FROM messages
        GROUP BY channel_id
    ) cnt ON c.channel_id = cnt.channel_id
    WHERE c.filtered_origin != 'human'
"""

_SQL_UNIQUENESS = """
    SELECT
        channel_id,
        COUNT(*) AS total,
        COUNT(DISTINCT substr(text, 1,100)) AS uniq
    FROM messages
    WHERE text IS NOT NULL AND text != ''
        AND forward_from_channel_id IS NULL
"""

_SQL_SUBSCRIBER_BASE = """
    SELECT channel_id, subscriber_count
    FROM (
        SELECT
            channel_id,
            subscriber_count,
            ROW_NUMBER() OVER (
                PARTITION BY channel_id
                ORDER BY collected_at DESC, id DESC
            ) AS rn
        FROM channel_stats
        WHERE subscriber_count IS NOT NULL
"""

_SQL_SHORT_MESSAGE = """
    SELECT
        channel_id,
        COUNT(*) AS total,
        SUM(CASE WHEN text IS NOT NULL AND length(text) <= 10
            THEN 1 ELSE 0 END) AS short
    FROM messages
"""

# Кросс-канальные дубли, асимметрично (фидбек владельца 15.10.26, @prog_ai;
# уточнено #1676, решение владельца 24.10.26): дупль — канал не является
# уникально-первым для префикса. Строго-поздняя копия — дупль (как было);
# одновременные копии с равной самой ранней датой — дупль у всех участников
# тай-я: сеть близнецов больше не спасается секундой публикации. Уникально-
# первый источник чист. Репосты с атрибуцией (forward_from_channel_id)
# исключены целиком: честная пересылка — не спам. Порог флага —
# CROSS_DUPE_THRESHOLD в criteria.py (анализатор).
_SQL_CROSS_DUPE = """
    WITH channel_prefixes AS (
        SELECT
            channel_id,
            substr(text, 1, 100) AS prefix,
            MIN(date) AS first_date
        FROM messages
        WHERE text IS NOT NULL AND length(text) > 10
            AND forward_from_channel_id IS NULL
        GROUP BY channel_id, substr(text, 1, 100)
    ),
    first_seen AS (
        SELECT prefix, MIN(first_date) AS global_first
        FROM channel_prefixes
        GROUP BY prefix
    ),
    earliest_tie AS (
        SELECT cp.prefix, COUNT(*) AS n_earliest
        FROM channel_prefixes cp
        JOIN first_seen fs ON fs.prefix = cp.prefix AND cp.first_date = fs.global_first
        GROUP BY cp.prefix
    )
    SELECT
        cp.channel_id,
        COUNT(*) AS uniq_total,
        SUM(CASE WHEN cp.first_date > fs.global_first
                   OR (cp.first_date = fs.global_first AND et.n_earliest > 1)
                 THEN 1 ELSE 0 END) AS duped
    FROM channel_prefixes cp
    JOIN first_seen fs ON fs.prefix = cp.prefix
    JOIN earliest_tie et ON et.prefix = cp.prefix
"""

_SQL_CYRILLIC = """
    SELECT
        channel_id,
        COUNT(*) AS total,
        SUM(has_cyrillic(text)) AS cyr
    FROM messages
    WHERE text IS NOT NULL AND text != ''
"""

# «Автогенерённая лента»: доля коротких (<200 симв.) постов со ссылкой наружу.
# Ссылка считается и в entities_json (#1492): замаскированная гиперссылка
# (MessageEntityTextUrl) хранит url только там — в text остаётся лишь надпись.
# LIKE по NULL даёт 0, обычные посты без сущностей не меняют счёт.
# Порог флага — в criteria.py (AUTO_FEED_*), литерал 200 здесь сознательно
# дублирован: SQL-слой не импортирует пакет filters (см. комментарий к UDF выше).
_SQL_AUTO_FEED = """
    SELECT
        channel_id,
        COUNT(*) AS total,
        SUM(CASE WHEN length(text) < 200
                AND (text LIKE '%http%' OR text LIKE '%t.me%'
                     OR entities_json LIKE '%http%' OR entities_json LIKE '%t.me%')
            THEN 1 ELSE 0 END) AS short_link
    FROM messages
    WHERE text IS NOT NULL AND text != ''
"""

# ── Sampled SQL templates (quick mode, #1138) ───────────────────────────────
# Instead of scanning the whole messages table, sample the last N messages per
# channel. The leading table is `channels` (~1.4k rows); for each one a
# correlated subquery does an index seek + LIMIT N on
# sqlite_autoindex_messages_1 (channel_id, message_id) and aggregates the slice.
#
# Why not ROW_NUMBER() OVER (PARTITION BY channel_id ORDER BY message_id DESC)?
# It reads cleaner, but SQLite cannot push the `rn <= N` filter into the index —
# it full-scans all messages and sorts in a TEMP B-TREE. Measured on the 25.6M-row
# production DB that is ~267s vs ~5s for the channels-driven form below (#1138).
# Each template takes the sample size twice (one bind per correlated subquery).
# N=300 is the calibrated default: binary verdict matches the full-history
# baseline on 99.65% of channels (#1138 stage 2).

_SQL_UNIQUENESS_SAMPLED = """
    SELECT
        ch.channel_id AS channel_id,
        (SELECT COUNT(*) FROM (
            SELECT text FROM messages m
            WHERE m.channel_id = ch.channel_id
                AND m.forward_from_channel_id IS NULL
            ORDER BY m.message_id DESC LIMIT ?
         ) s WHERE s.text IS NOT NULL AND s.text != '') AS total,
        (SELECT COUNT(DISTINCT substr(text, 1, 100)) FROM (
            SELECT text FROM messages m
            WHERE m.channel_id = ch.channel_id
                AND m.forward_from_channel_id IS NULL
            ORDER BY m.message_id DESC LIMIT ?
         ) s WHERE s.text IS NOT NULL AND s.text != '') AS uniq
    FROM channels ch
"""

_SQL_SHORT_MESSAGE_SAMPLED = """
    SELECT
        ch.channel_id AS channel_id,
        (SELECT COUNT(*) FROM (
            SELECT text FROM messages m
            WHERE m.channel_id = ch.channel_id
            ORDER BY m.message_id DESC LIMIT ?
         ) s) AS total,
        (SELECT SUM(CASE WHEN text IS NOT NULL AND length(text) <= 10
                    THEN 1 ELSE 0 END) FROM (
            SELECT text FROM messages m
            WHERE m.channel_id = ch.channel_id
            ORDER BY m.message_id DESC LIMIT ?
         ) s) AS short
    FROM channels ch
"""

_SQL_CYRILLIC_SAMPLED = """
    SELECT
        ch.channel_id AS channel_id,
        (SELECT COUNT(*) FROM (
            SELECT text FROM messages m
            WHERE m.channel_id = ch.channel_id
            ORDER BY m.message_id DESC LIMIT ?
         ) s WHERE s.text IS NOT NULL AND s.text != '') AS total,
        (SELECT SUM(has_cyrillic(text)) FROM (
            SELECT text FROM messages m
            WHERE m.channel_id = ch.channel_id
            ORDER BY m.message_id DESC LIMIT ?
         ) s WHERE s.text IS NOT NULL AND s.text != '') AS cyr
    FROM channels ch
"""

_SQL_AUTO_FEED_SAMPLED = """
    SELECT
        ch.channel_id AS channel_id,
        (SELECT COUNT(*) FROM (
            SELECT text FROM messages m
            WHERE m.channel_id = ch.channel_id
            ORDER BY m.message_id DESC LIMIT ?
         ) s WHERE s.text IS NOT NULL AND s.text != '') AS total,
        (SELECT SUM(CASE WHEN length(text) < 200
                    AND (text LIKE '%http%' OR text LIKE '%t.me%'
                         OR entities_json LIKE '%http%' OR entities_json LIKE '%t.me%')
                THEN 1 ELSE 0 END) FROM (
            SELECT text, entities_json FROM messages m
            WHERE m.channel_id = ch.channel_id
            ORDER BY m.message_id DESC LIMIT ?
         ) s WHERE s.text IS NOT NULL AND s.text != '') AS short_link
    FROM channels ch
"""

# «Мёртвое вовлечение»: (просмотрено, сумма views) в окне последних N постов.
# Окно применяется ВСЕГДА (и в full, и в quick) — метрика «мёртвое сейчас», не
# «среднее за жизнь» (см. LOW_ENGAGEMENT_* в criteria.py). NULL-views исключаются
# из обоих агрегатов; порог и guard'ы — в анализаторе.
_SQL_ENGAGEMENT_SAMPLED = """
    SELECT
        ch.channel_id AS channel_id,
        (SELECT COUNT(views) FROM (
            SELECT views FROM messages m
            WHERE m.channel_id = ch.channel_id
            ORDER BY m.message_id DESC LIMIT ?
         ) s WHERE s.views IS NOT NULL) AS viewed,
        (SELECT SUM(views) FROM (
            SELECT views FROM messages m
            WHERE m.channel_id = ch.channel_id
            ORDER BY m.message_id DESC LIMIT ?
         ) s WHERE s.views IS NOT NULL) AS views_sum
    FROM channels ch
"""

# «Пересылочный канал»: (всего сообщений, из них форвардов) в окне последних N.
# Окно применяется всегда; порог и guard — в criteria.py (FORWARD_SHARE_*).
# Форварды без текста считаются (медиа-пересылка — тоже пересылка).
_SQL_FORWARD_SHARE_SAMPLED = """
    SELECT
        ch.channel_id AS channel_id,
        (SELECT COUNT(*) FROM (
            SELECT forward_from_channel_id FROM messages m
            WHERE m.channel_id = ch.channel_id
            ORDER BY m.message_id DESC LIMIT ?
         ) s) AS total,
        (SELECT SUM(CASE WHEN forward_from_channel_id IS NOT NULL
                    THEN 1 ELSE 0 END) FROM (
            SELECT forward_from_channel_id FROM messages m
            WHERE m.channel_id = ch.channel_id
            ORDER BY m.message_id DESC LIMIT ?
         ) s) AS fwd
    FROM channels ch
"""

# «Массовый постинг» (#1571): count сообщений за последние 24ч. Channels-driven
# — в карту входят и каналы с нулём за сутки: они участвуют в P95-распределении.
# Окно — strftime-литерал (дубль паттерна get_err24_data): messages.date хранится
# как ISO с '+00:00', строка datetime('now') с пробелом сравнивалась бы неверно.
_SQL_VOLUME_24H = """
    SELECT
        ch.channel_id AS channel_id,
        (SELECT COUNT(*) FROM messages m
         WHERE m.channel_id = ch.channel_id
           AND m.date >= strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now', '-1 day')) AS cnt
    FROM channels ch
"""

if TYPE_CHECKING:
    from tg_agent.database.facade import Database


class FilterRepository:
    """Аггрегатные выборки для `ChannelAnalyzer`: метрики качества каналов.

    Считает по сообщениям/статистике карты признаков (уникальность, доля
    коротких сообщений, кросс-канальные дубли, доля кириллицы, подписчики),
    которыми анализатор отсеивает спам/низкокачественные каналы. Тяжёлые карты
    можно считать параллельно на отдельных read-only соединениях
    (`fetch_maps_parallel`) — это безопасно только для файловой БД, не `:memory:`.
    Кириллица определяется через SQLite UDF `has_cyrillic`, регистрируемый лениво.
    """

    def __init__(
        self,
        db: ReadConnection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database
        self._udf_registered = False

    async def _ensure_udf(self) -> None:
        if not self._udf_registered:
            await self._db.create_function("has_cyrillic", 1, _has_cyrillic_udf, deterministic=True)
            self._udf_registered = True

    # ── Sequential fetch methods (original, used for single-channel) ──────

    async def fetch_channels_for_analysis(
        self, channel_id: int | None = None
    ) -> list[aiosqlite.Row]:
        """Базовые поля каналов + счётчик сообщений (все или один по channel_id)."""
        sql = _SQL_CHANNELS
        params: tuple = ()
        if channel_id is not None:
            sql += " AND c.channel_id = ?"
            params = (channel_id,)
        sql += " ORDER BY c.id ASC"
        cur = await self._db.execute(sql, params)
        return await cur.fetchall()

    async def fetch_uniqueness_map(
        self, channel_id: int | None = None, *, sample_size: int | None = None
    ) -> dict[int, tuple[int, int]]:
        """Карта `channel_id → (всего сообщений, уникальных по префиксу текста)`.

        Низкая доля уникальных — признак канала-репостера/спама. С ``sample_size``
        метрика считается по последним N сообщениям канала (quick-режим, #1138).
        """
        if sample_size is not None:
            sql = _SQL_UNIQUENESS_SAMPLED
            params: tuple = (sample_size, sample_size)
            if channel_id is not None:
                sql += " WHERE ch.channel_id = ?"
                params = (sample_size, sample_size, channel_id)
        else:
            sql = _SQL_UNIQUENESS
            params = ()
            if channel_id is not None:
                sql += " AND channel_id = ?"
                params = (channel_id,)
            sql += " GROUP BY channel_id"
        cur = await self._db.execute(sql, params)
        rows = await cur.fetchall()
        return {row["channel_id"]: (row["total"], row["uniq"]) for row in rows}

    async def fetch_subscriber_map(self, channel_id: int | None = None) -> dict[int, int]:
        """Карта `channel_id → последнее известное число подписчиков` (по свежей записи stats)."""
        sql = _SQL_SUBSCRIBER_BASE
        params: tuple = ()
        if channel_id is not None:
            sql += " AND channel_id = ?"
            params = (channel_id,)
        sql += """
            )
            WHERE rn = 1
        """
        cur = await self._db.execute(sql, params)
        rows = await cur.fetchall()
        return {row["channel_id"]: row["subscriber_count"] for row in rows}

    async def fetch_short_message_map(
        self, channel_id: int | None = None, *, sample_size: int | None = None
    ) -> dict[int, tuple[int, int]]:
        """Карта `channel_id → (всего сообщений, из них коротких ≤10 символов)`.

        Высокая доля коротких сообщений — признак чата-шума, а не контент-канала.
        С ``sample_size`` считается по последним N сообщениям канала (#1138).
        """
        if sample_size is not None:
            sql = _SQL_SHORT_MESSAGE_SAMPLED
            params: tuple = (sample_size, sample_size)
            if channel_id is not None:
                sql += " WHERE ch.channel_id = ?"
                params = (sample_size, sample_size, channel_id)
        else:
            sql = _SQL_SHORT_MESSAGE
            params = ()
            if channel_id is not None:
                sql += " WHERE channel_id = ?"
                params = (channel_id,)
            sql += " GROUP BY channel_id"
        cur = await self._db.execute(sql, params)
        rows = await cur.fetchall()
        return {row["channel_id"]: (row["total"], row["short"] or 0) for row in rows}

    async def count_matching_prefixes_in_other_channels(
        self, channel_id: int, prefixes: list[str]
    ) -> int:
        """Сколько из переданных prefixes уже есть в сообщениях других каналов.

        Форварды (forward_from_channel_id) не считаются: атрибуция — не кочевание
        текста (решение владельца 16.10.26).
        """
        if not prefixes:
            return 0
        placeholders = ",".join("?" * len(prefixes))
        sql = f"""
            SELECT COUNT(DISTINCT substr(text, 1, 100))
            FROM messages
            WHERE channel_id != ?
              AND text IS NOT NULL
              AND forward_from_channel_id IS NULL
              AND substr(text, 1, 100) IN ({placeholders})
        """
        cur = await self._db.execute(sql, (channel_id, *prefixes))
        row = await cur.fetchone()
        return row[0] if row else 0

    async def fetch_cross_dupe_map(
        self, channel_id: int | None = None
    ) -> dict[int, tuple[int, int]]:
        """Карта `channel_id → (уникальных префиксов, из них не уникально-первых)`.

        Асимметричная метрика: дупликат — канал не уникально-первый для текста:
        строго-поздняя копия или участник тай-я на самой ранней дате
        (одновременные копии, #1676); уникально-первый источник чист. Репосты с
        forward_from_channel_id исключены. Это самый
        тяжёлый запрос (self-join по префиксам).
        """
        sql = _SQL_CROSS_DUPE
        params: tuple = ()
        if channel_id is not None:
            sql += " WHERE cp.channel_id = ?"
            params = (channel_id,)
        sql += " GROUP BY cp.channel_id"
        cur = await self._db.execute(sql, params)
        rows = await cur.fetchall()
        return {row["channel_id"]: (row["uniq_total"], row["duped"] or 0) for row in rows}

    async def fetch_cyrillic_map(
        self, channel_id: int | None = None, *, sample_size: int | None = None
    ) -> dict[int, tuple[int, int]]:
        """Карта `channel_id → (всего сообщений, из них с кириллицей)` через UDF `has_cyrillic`.

        Низкая доля кириллицы отсеивает иноязычные каналы для русскоязычного сбора.
        С ``sample_size`` считается по последним N сообщениям канала (#1138).
        """
        await self._ensure_udf()
        if sample_size is not None:
            sql = _SQL_CYRILLIC_SAMPLED
            params: tuple = (sample_size, sample_size)
            if channel_id is not None:
                sql += " WHERE ch.channel_id = ?"
                params = (sample_size, sample_size, channel_id)
        else:
            sql = _SQL_CYRILLIC
            params = ()
            if channel_id is not None:
                sql += " AND channel_id = ?"
                params = (channel_id,)
            sql += " GROUP BY channel_id"
        cur = await self._db.execute(sql, params)
        rows = await cur.fetchall()
        return {row["channel_id"]: (row["total"], row["cyr"] or 0) for row in rows}

    async def fetch_auto_feed_map(
        self, channel_id: int | None = None, *, sample_size: int | None = None
    ) -> dict[int, tuple[int, int]]:
        """Карта `channel_id → (всего сообщений, из них коротких <200 симв. со ссылкой)`.

        Высокая доля коротких постов-ссылок — признак автогенерённой ленты
        (слаг-фиды, автопостинг); порог флага в criteria.py. С ``sample_size``
        считается по последним N сообщениям канала (#1138).
        """
        if sample_size is not None:
            sql = _SQL_AUTO_FEED_SAMPLED
            params: tuple = (sample_size, sample_size)
            if channel_id is not None:
                sql += " WHERE ch.channel_id = ?"
                params = (sample_size, sample_size, channel_id)
        else:
            sql = _SQL_AUTO_FEED
            params = ()
            if channel_id is not None:
                sql += " AND channel_id = ?"
                params = (channel_id,)
            sql += " GROUP BY channel_id"
        cur = await self._db.execute(sql, params)
        rows = await cur.fetchall()
        return {row["channel_id"]: (row["total"], row["short_link"] or 0) for row in rows}

    async def fetch_engagement_map(
        self, channel_id: int | None = None, *, window: int = 100
    ) -> dict[int, tuple[int, int]]:
        """Карта `channel_id → (просмотрено постов, сумма views)` в окне последних N.

        Доля просмотров от подписчиков ниже порога — «мёртвое вовлечение»
        (нейрослоп-фермы с автопостингом, зомби-каналы); порог и guard'ы в
        criteria.py (LOW_ENGAGEMENT_*). Окно применяется всегда: метрика
        «мёртвое сейчас», полная история завышает среднее накопленными
        просмотрами старых постов. Литерал 100 — дубль LOW_ENGAGEMENT_WINDOW:
        SQL-слой не импортирует пакет filters (см. комментарий к UDF выше).
        """
        sql, params = self._windowed_sql(_SQL_ENGAGEMENT_SAMPLED, channel_id=channel_id, window=window)
        cur = await self._db.execute(sql, params)
        rows = await cur.fetchall()
        return {row["channel_id"]: (row["viewed"], row["views_sum"] or 0) for row in rows}

    async def fetch_forward_share_map(
        self, channel_id: int | None = None, *, window: int = 100
    ) -> dict[int, tuple[int, int]]:
        """Карта `channel_id → (всего сообщений, из них форвардов)` в окне последних N.

        Метка `forwarder` (label-only): доля форвардов >= порога — пересылочный
        канал. Окно применяется всегда; литерал 100 — дубль
        FORWARD_SHARE_WINDOW: SQL-слой не импортирует пакет filters.
        """
        sql, params = self._windowed_sql(_SQL_FORWARD_SHARE_SAMPLED, channel_id=channel_id, window=window)
        cur = await self._db.execute(sql, params)
        rows = await cur.fetchall()
        return {row["channel_id"]: (row["total"], row["fwd"] or 0) for row in rows}

    async def fetch_recent_post_samples(
        self,
        channel_id: int | None = None,
        *,
        sample_size: int,
    ) -> dict[int, list[tuple[str, bool]]]:
        """Карта `channel_id → [(text, is_forward), …]` — сэмпл последних постов.

        Сырьё для контент-рейтинга «полезное vs вода» (filters/usefulness.py) И
        для сэмпла LLM-судьи (channel_analysis_service._sample_posts — единая
        популяция постов, иначе калибровка порогов пойдёт не по тому
        распределению): тексты + флаг форварда, последние N на канал.

        Перканальный цикл мелких запросов (index seek по
        sqlite_autoindex_messages_1 (channel_id, message_id) + LIMIT —
        стоимость ∝ каналы × N): ROW_NUMBER() OVER здесь запрещён замером
        #1138 (~267s против ~5s, см. комментарий над _SQL_UNIQUENESS_SAMPLED),
        а FROM-подзапрос в SQLite не коррелирует с внешним channels (опыт
        22.10.26: «no such column: ch.channel_id»).

        Срез-потом-фильтр, как у соседних _*_SAMPLED-шаблонов: LIMIT берёт
        последние N СЫРЫХ строк (чистый index walk), текст/message_kind
        фильтруются в Python. Фильтр внутри среза — неограниченный проход
        назад через медиа/сервисные сообщения канала: замерено на боевой БД
        22.10.26 — 2:39 на отчёт против секунд после выноса.

        Каналы делятся между несколькими read-only соединениями (тот же приём,
        что fetch_maps_parallel): 1722 перканальных запроса последовательным
        циклом ≈ 2:49 на боевой БД, параллельно — вчетверо меньше. `:memory:`
        идёт по прямому пути на соединении БД.
        """
        list_sql = "SELECT channel_id FROM channels"
        params: tuple = ()
        if channel_id is not None:
            list_sql += " WHERE channel_id = ?"
            params = (channel_id,)
        cur = await self._db.execute(list_sql, params)
        targets = [row["channel_id"] for row in await cur.fetchall()]

        post_sql = (
            "SELECT text, forward_from_channel_id, message_kind FROM messages m "
            "WHERE m.channel_id = ? ORDER BY m.message_id DESC LIMIT ?"
        )

        async def sample_on(
            conn: "ReadConnection | aiosqlite.Connection", cids: list[int]
        ) -> dict:
            out: dict[int, list[tuple[str, bool]]] = {}
            for cid in cids:
                cur = await conn.execute(post_sql, (cid, sample_size))
                rows = await cur.fetchall()
                texts = [
                    (row["text"], row["forward_from_channel_id"] is not None)
                    for row in rows
                    if row["text"]
                    and (row["message_kind"] == "regular" or row["message_kind"] is None)
                ]
                if texts:
                    out[cid] = texts
            return out

        workers = min(4, len(targets))
        if not (self._can_parallel() and workers > 1):
            return await sample_on(self._db, targets)

        conns = [await self._open_readonly_conn() for _ in range(workers)]
        try:
            chunks = [targets[i::workers] for i in range(workers)]
            parts = await asyncio.gather(
                *(sample_on(conns[i], chunks[i]) for i in range(workers))
            )
        finally:
            for conn in conns:
                await conn.close()
        samples: dict[int, list[tuple[str, bool]]] = {}
        for part in parts:
            samples.update(part)
        return samples

    async def fetch_volume_map(self, channel_id: int | None = None) -> dict[int, int]:
        """Карта `channel_id → сообщений за последние 24ч` (#1571, high_volume).

        Channels-driven: каналы без свежих сообщений входят с нулём — P95-порог
        считается по распределению всех каналов отчёта, а не только активных.
        """
        sql = _SQL_VOLUME_24H
        params: tuple = ()
        if channel_id is not None:
            sql += " WHERE ch.channel_id = ?"
            params = (channel_id,)
        cur = await self._db.execute(sql, params)
        rows = await cur.fetchall()
        return {row["channel_id"]: row["cnt"] for row in rows}

    # ── Parallel fetch: separate read-only connections ─────────────────────

    def _can_parallel(self) -> bool:
        """True if we have a file-based DB (not :memory:) and a Database reference."""
        if self._database is None:
            return False
        db_path = getattr(self._database, "_db_path", None)
        return db_path is not None and db_path != ":memory:"

    async def _open_readonly_conn(self) -> aiosqlite.Connection:
        """Open a temporary read-only connection for parallel queries."""
        assert self._database is not None, (
            "FilterRepository._open_readonly_conn requires a Database reference"
        )
        db_path = self._database._db_path  # noqa: SLF001
        conn = await aiosqlite.connect(
            f"file:{db_path}?mode=ro",
            uri=True,
            timeout=10.0,
        )
        conn.row_factory = aiosqlite.Row
        await conn.create_function("has_cyrillic", 1, _has_cyrillic_udf, deterministic=True)
        return conn

    @staticmethod
    async def _run_on_conn(
        conn: aiosqlite.Connection,
        sql: str,
        params: tuple = (),
    ) -> list[aiosqlite.Row]:
        cur = await conn.execute(sql, params)
        # aiosqlite stubs declare fetchall() as Iterable[Row]; at runtime it is a list.
        return cast("list[aiosqlite.Row]", await cur.fetchall())

    @staticmethod
    def _text_map_sql(
        template_full: str,
        template_sampled: str,
        *,
        channel_id: int | None,
        sample_size: int | None,
        full_where_join: str,
    ) -> tuple[str, tuple]:
        """(sql, params) для text-карты: sampled-окно последних N или полный скан.

        Sampled-шаблон channels-driven и берёт размер окна дважды; полный
        группируется по channel_id (full_where_join — " AND " или " WHERE ",
        смотря есть ли WHERE в шаблоне).
        """
        if sample_size is not None:
            sql = template_sampled + (" WHERE ch.channel_id = ?" if channel_id is not None else "")
            params: tuple = (
                (sample_size, sample_size, channel_id) if channel_id is not None else (sample_size, sample_size)
            )
            return sql, params
        sql = template_full + (
            (full_where_join + "channel_id = ?" if channel_id is not None else "") + " GROUP BY channel_id"
        )
        return sql, ((channel_id,) if channel_id is not None else ())

    @staticmethod
    def _windowed_sql(template: str, *, channel_id: int | None, window: int) -> tuple[str, tuple]:
        """(sql, params) для всегда-оконной channels-driven карты."""
        sql = template + (" WHERE ch.channel_id = ?" if channel_id is not None else "")
        params = (window, window, channel_id) if channel_id is not None else (window, window)
        return sql, params

    async def fetch_maps_parallel(
        self,
        channel_id: int | None = None,
        *,
        include_cross_dupe: bool = True,
        sample_size: int | None = None,
    ) -> tuple[
        dict[int, tuple[int, int]],  # uniqueness_map
        dict[int, int],               # subscriber_map
        dict[int, tuple[int, int]],  # short_map
        dict[int, tuple[int, int]],  # cross_dupe_map
        dict[int, tuple[int, int]],  # cyrillic_map
        dict[int, tuple[int, int]],  # auto_feed_map
        dict[int, tuple[int, int]],  # engagement_map
        dict[int, tuple[int, int]],  # forward_map
        dict[int, int],              # volume_map (24h)
    ]:
        """Run the map queries in parallel on separate read-only connections.

        include_cross_dupe=False skips the cross-channel duplicate self-join —
        by far the heaviest query on large DBs (#774) — returning an empty map.
        sample_size routes the four text-based maps
        (uniqueness/short/cyrillic/auto_feed) to the last-N-per-channel sampled
        SQL (#1138); subscriber stays full (it is count/stats-based, not
        text-based).
        """
        # Build parameterised SQL for each query. The text maps switch to the
        # channels-driven sampled templates when sample_size is given.
        u_sql, u_params = self._text_map_sql(
            _SQL_UNIQUENESS, _SQL_UNIQUENESS_SAMPLED,
            channel_id=channel_id, sample_size=sample_size, full_where_join=" AND ",
        )
        sm_sql, sm_params = self._text_map_sql(
            _SQL_SHORT_MESSAGE, _SQL_SHORT_MESSAGE_SAMPLED,
            channel_id=channel_id, sample_size=sample_size, full_where_join=" WHERE ",
        )
        cy_sql, cy_params = self._text_map_sql(
            _SQL_CYRILLIC, _SQL_CYRILLIC_SAMPLED,
            channel_id=channel_id, sample_size=sample_size, full_where_join=" AND ",
        )
        af_sql, af_params = self._text_map_sql(
            _SQL_AUTO_FEED, _SQL_AUTO_FEED_SAMPLED,
            channel_id=channel_id, sample_size=sample_size, full_where_join=" AND ",
        )
        # engagement/forward: окно всегда (метрики «сейчас», не «за жизнь»);
        # литерал 100 — дубль *_WINDOW, SQL-слой не импортирует пакет filters.
        e_sql, e_params = self._windowed_sql(_SQL_ENGAGEMENT_SAMPLED, channel_id=channel_id, window=100)
        f_sql, f_params = self._windowed_sql(_SQL_FORWARD_SHARE_SAMPLED, channel_id=channel_id, window=100)

        s_sql = (
            _SQL_SUBSCRIBER_BASE
            + (" AND channel_id = ?" if channel_id is not None else "")
            + "\n)\nWHERE rn = 1"
        )
        s_params: tuple = (channel_id,) if channel_id is not None else ()

        cd_sql = (
            _SQL_CROSS_DUPE
            + (" WHERE cp.channel_id = ?" if channel_id is not None else "")
            + " GROUP BY cp.channel_id"
        )
        cd_params: tuple = (channel_id,) if channel_id is not None else ()

        v_sql = _SQL_VOLUME_24H
        v_params: tuple = ()
        if channel_id is not None:
            v_sql += " WHERE ch.channel_id = ?"
            v_params = (channel_id,)

        async def _no_rows() -> list[aiosqlite.Row]:
            return []

        conns = [await self._open_readonly_conn() for _ in range(8 + int(include_cross_dupe))]
        try:
            rows_u, rows_s, rows_sm, rows_cy, rows_af, rows_e, rows_f, rows_v, rows_cd = await asyncio.gather(
                self._run_on_conn(conns[0], u_sql, u_params),
                self._run_on_conn(conns[1], s_sql, s_params),
                self._run_on_conn(conns[2], sm_sql, sm_params),
                self._run_on_conn(conns[3], cy_sql, cy_params),
                self._run_on_conn(conns[4], af_sql, af_params),
                self._run_on_conn(conns[5], e_sql, e_params),
                self._run_on_conn(conns[6], f_sql, f_params),
                self._run_on_conn(conns[7], v_sql, v_params),
                self._run_on_conn(conns[8], cd_sql, cd_params) if include_cross_dupe else _no_rows(),
            )
        finally:
            for conn in conns:
                await conn.close()

        return (
            {r["channel_id"]: (r["total"], r["uniq"]) for r in rows_u},
            {r["channel_id"]: r["subscriber_count"] for r in rows_s},
            {r["channel_id"]: (r["total"], r["short"] or 0) for r in rows_sm},
            {r["channel_id"]: (r["uniq_total"], r["duped"] or 0) for r in rows_cd},
            {r["channel_id"]: (r["total"], r["cyr"] or 0) for r in rows_cy},
            {r["channel_id"]: (r["total"], r["short_link"] or 0) for r in rows_af},
            {r["channel_id"]: (r["viewed"], r["views_sum"] or 0) for r in rows_e},
            {r["channel_id"]: (r["total"], r["fwd"] or 0) for r in rows_f},
            {r["channel_id"]: r["cnt"] for r in rows_v},
        )
