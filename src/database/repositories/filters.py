"""Репозиторий аггрегатных метрик каналов для фильтрации качества (ChannelAnalyzer)."""

from __future__ import annotations

import asyncio
import logging
import re
from typing import TYPE_CHECKING

import aiosqlite

# Intentionally duplicated from src/filters/criteria.py — UDF layer must not
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
"""

_SQL_UNIQUENESS = """
    SELECT
        channel_id,
        COUNT(*) AS total,
        COUNT(DISTINCT substr(text, 1,100)) AS uniq
    FROM messages
    WHERE text IS NOT NULL AND text != ''
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

_SQL_CROSS_DUPE = """
    WITH channel_prefixes AS (
        SELECT channel_id, substr(text, 1, 100) AS prefix
        FROM messages
        WHERE text IS NOT NULL AND length(text) > 10
        GROUP BY channel_id, prefix
    ),
    prefix_channel_counts AS (
        SELECT prefix, COUNT(*) AS channel_count
        FROM channel_prefixes
        GROUP BY prefix
    )
    SELECT
        cp.channel_id,
        COUNT(*) AS uniq_total,
        SUM(CASE WHEN pcc.channel_count > 1 THEN 1 ELSE 0 END) AS duped
    FROM channel_prefixes cp
    JOIN prefix_channel_counts pcc ON pcc.prefix = cp.prefix
"""

_SQL_CYRILLIC = """
    SELECT
        channel_id,
        COUNT(*) AS total,
        SUM(has_cyrillic(text)) AS cyr
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
            ORDER BY m.message_id DESC LIMIT ?
         ) s WHERE s.text IS NOT NULL AND s.text != '') AS total,
        (SELECT COUNT(DISTINCT substr(text, 1, 100)) FROM (
            SELECT text FROM messages m
            WHERE m.channel_id = ch.channel_id
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

if TYPE_CHECKING:
    from src.database.facade import Database


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
        db: aiosqlite.Connection,
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
            sql += " WHERE c.channel_id = ?"
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
        """Сколько из переданных prefixes уже есть в сообщениях других каналов."""
        if not prefixes:
            return 0
        placeholders = ",".join("?" * len(prefixes))
        sql = f"""
            SELECT COUNT(DISTINCT substr(text, 1, 100))
            FROM messages
            WHERE channel_id != ?
              AND text IS NOT NULL
              AND substr(text, 1, 100) IN ({placeholders})
        """
        cur = await self._db.execute(sql, (channel_id, *prefixes))
        row = await cur.fetchone()
        return row[0] if row else 0

    async def fetch_cross_dupe_map(
        self, channel_id: int | None = None
    ) -> dict[int, tuple[int, int]]:
        """Карта `channel_id → (уникальных префиксов, из них встречаются в других каналах)`.

        Доля кросс-канальных дублей выявляет агрегаторы/сетки, перепечатывающие
        чужой контент. Это самый тяжёлый запрос (self-join по префиксам).
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
        return await cur.fetchall()

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
    ]:
        """Run the map queries in parallel on separate read-only connections.

        include_cross_dupe=False skips the cross-channel duplicate self-join —
        by far the heaviest query on large DBs (#774) — returning an empty map.
        sample_size routes the three text-based maps (uniqueness/short/cyrillic)
        to the last-N-per-channel sampled SQL (#1138); subscriber stays full
        (it is count/stats-based, not text-based).
        """
        sampled = sample_size is not None
        # Build parameterised SQL for each query. The text maps switch to the
        # channels-driven sampled templates when sample_size is given.
        if sampled:
            u_sql = _SQL_UNIQUENESS_SAMPLED + (" WHERE ch.channel_id = ?" if channel_id is not None else "")
            u_params: tuple = (
                (sample_size, sample_size, channel_id) if channel_id is not None else (sample_size, sample_size)
            )
            sm_sql = _SQL_SHORT_MESSAGE_SAMPLED + (" WHERE ch.channel_id = ?" if channel_id is not None else "")
            sm_params: tuple = (
                (sample_size, sample_size, channel_id) if channel_id is not None else (sample_size, sample_size)
            )
            cy_sql = _SQL_CYRILLIC_SAMPLED + (" WHERE ch.channel_id = ?" if channel_id is not None else "")
            cy_params: tuple = (
                (sample_size, sample_size, channel_id) if channel_id is not None else (sample_size, sample_size)
            )
        else:
            u_sql = (
                _SQL_UNIQUENESS + (" AND channel_id = ?" if channel_id is not None else "") + " GROUP BY channel_id"
            )
            u_params = (channel_id,) if channel_id is not None else ()
            sm_sql = (
                _SQL_SHORT_MESSAGE
                + (" WHERE channel_id = ?" if channel_id is not None else "")
                + " GROUP BY channel_id"
            )
            sm_params = (channel_id,) if channel_id is not None else ()
            cy_sql = (
                _SQL_CYRILLIC
                + (" AND channel_id = ?" if channel_id is not None else "")
                + " GROUP BY channel_id"
            )
            cy_params = (channel_id,) if channel_id is not None else ()

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

        async def _no_rows() -> list[aiosqlite.Row]:
            return []

        conns = [await self._open_readonly_conn() for _ in range(4 + int(include_cross_dupe))]
        try:
            rows_u, rows_s, rows_sm, rows_cy, rows_cd = await asyncio.gather(
                self._run_on_conn(conns[0], u_sql, u_params),
                self._run_on_conn(conns[1], s_sql, s_params),
                self._run_on_conn(conns[2], sm_sql, sm_params),
                self._run_on_conn(conns[3], cy_sql, cy_params),
                self._run_on_conn(conns[4], cd_sql, cd_params) if include_cross_dupe else _no_rows(),
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
        )
