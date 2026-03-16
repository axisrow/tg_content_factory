from __future__ import annotations

import re

import aiosqlite

# Intentionally duplicated from src/filters/criteria.py — UDF layer must not
# depend on the filters package to keep DB initialisation self-contained.
_CYRILLIC_RE = re.compile(r"[а-яА-ЯёЁ]")


def _has_cyrillic_udf(text: str | None) -> int:
    if not text:
        return 0
    return 1 if _CYRILLIC_RE.search(text) else 0


class FilterRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db
        self._udf_registered = False

    async def _ensure_udf(self) -> None:
        if not self._udf_registered:
            await self._db.create_function("has_cyrillic", 1, _has_cyrillic_udf, deterministic=True)
            self._udf_registered = True

    async def fetch_channels_for_analysis(
        self, channel_id: int | None = None
    ) -> list[aiosqlite.Row]:
        sql = """
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
        params: tuple = ()
        if channel_id is not None:
            sql += " WHERE c.channel_id = ?"
            params = (channel_id,)
        sql += " ORDER BY c.id ASC"
        cur = await self._db.execute(sql, params)
        return await cur.fetchall()

    async def fetch_uniqueness_map(
        self, channel_id: int | None = None
    ) -> dict[int, tuple[int, int]]:
        sql = """
            SELECT
                channel_id,
                COUNT(*) AS total,
                COUNT(DISTINCT substr(text, 1, 100)) AS uniq
            FROM messages
            WHERE text IS NOT NULL AND text != ''
        """
        params: tuple = ()
        if channel_id is not None:
            sql += " AND channel_id = ?"
            params = (channel_id,)
        sql += " GROUP BY channel_id"
        cur = await self._db.execute(sql, params)
        rows = await cur.fetchall()
        return {row["channel_id"]: (row["total"], row["uniq"]) for row in rows}

    async def fetch_subscriber_map(self, channel_id: int | None = None) -> dict[int, int]:
        sql = """
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
        self, channel_id: int | None = None
    ) -> dict[int, tuple[int, int]]:
        sql = """
            SELECT
                channel_id,
                COUNT(*) AS total,
                SUM(CASE WHEN text IS NOT NULL AND length(text) <= 10
                    THEN 1 ELSE 0 END) AS short
            FROM messages
        """
        params: tuple = ()
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
        sql = """
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
        params: tuple = ()
        if channel_id is not None:
            sql += " WHERE cp.channel_id = ?"
            params = (channel_id,)
        sql += " GROUP BY cp.channel_id"
        cur = await self._db.execute(sql, params)
        rows = await cur.fetchall()
        return {row["channel_id"]: (row["uniq_total"], row["duped"] or 0) for row in rows}

    async def fetch_cyrillic_map(self, channel_id: int | None = None) -> dict[int, tuple[int, int]]:
        await self._ensure_udf()
        sql = """
            SELECT
                channel_id,
                COUNT(*) AS total,
                SUM(has_cyrillic(text)) AS cyr
            FROM messages
            WHERE text IS NOT NULL AND text != ''
        """
        params: tuple = ()
        if channel_id is not None:
            sql += " AND channel_id = ?"
            params = (channel_id,)
        sql += " GROUP BY channel_id"
        cur = await self._db.execute(sql, params)
        rows = await cur.fetchall()
        return {row["channel_id"]: (row["total"], row["cyr"] or 0) for row in rows}
