from __future__ import annotations

import logging

import aiosqlite

from src.database import Database
from src.filters.criteria import contains_cyrillic
from src.filters.models import ChannelFilterResult, FilterReport

logger = logging.getLogger(__name__)


LOW_UNIQUENESS_THRESHOLD = 30.0
LOW_SUBSCRIBER_RATIO_THRESHOLD = 1.0
CROSS_DUPE_THRESHOLD = 50.0
NON_CYRILLIC_THRESHOLD = 10.0
CHAT_NOISE_THRESHOLD = 70.0


class ChannelAnalyzer:
    def __init__(self, db: Database):
        assert db.db is not None
        self._database = db
        self._db = db.db

    async def _fetch_channels_for_analysis(
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

    async def _fetch_uniqueness_map(self) -> dict[int, tuple[int, int]]:
        cur = await self._db.execute(
            """
            SELECT
                channel_id,
                COUNT(*) AS total,
                COUNT(DISTINCT substr(text, 1, 100)) AS uniq
            FROM messages
            WHERE text IS NOT NULL AND text != ''
            GROUP BY channel_id
            """
        )
        rows = await cur.fetchall()
        return {row["channel_id"]: (row["total"], row["uniq"]) for row in rows}

    async def _fetch_latest_subscriber_map(self) -> dict[int, int]:
        cur = await self._db.execute(
            """
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
            )
            WHERE rn = 1
            """
        )
        rows = await cur.fetchall()
        return {row["channel_id"]: row["subscriber_count"] for row in rows}

    async def _fetch_short_message_map(self) -> dict[int, tuple[int, int]]:
        cur = await self._db.execute(
            """
            SELECT
                channel_id,
                COUNT(*) AS total,
                SUM(CASE WHEN length(COALESCE(text, '')) <= 10 THEN 1 ELSE 0 END) AS short
            FROM messages
            GROUP BY channel_id
            """
        )
        rows = await cur.fetchall()
        return {row["channel_id"]: (row["total"], row["short"] or 0) for row in rows}

    async def _fetch_cross_dupe_map(self) -> dict[int, tuple[int, int]]:
        cur = await self._db.execute(
            """
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
            GROUP BY cp.channel_id
            """
        )
        rows = await cur.fetchall()
        return {row["channel_id"]: (row["uniq_total"], row["duped"] or 0) for row in rows}

    async def _fetch_cyrillic_map(self) -> dict[int, tuple[int, int]]:
        cur = await self._db.execute(
            """
            SELECT channel_id, text
            FROM messages
            WHERE text IS NOT NULL AND text != ''
            """
        )
        totals: dict[int, int] = {}
        cyrillic_totals: dict[int, int] = {}
        while True:
            rows = await cur.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                channel_id = row["channel_id"]
                text = row["text"]
                if not text:
                    continue
                totals[channel_id] = totals.get(channel_id, 0) + 1
                if contains_cyrillic(text):
                    cyrillic_totals[channel_id] = cyrillic_totals.get(channel_id, 0) + 1
        return {
            channel_id: (total, cyrillic_totals.get(channel_id, 0))
            for channel_id, total in totals.items()
        }

    async def _build_report(self, channel_id: int | None = None) -> FilterReport:
        channels = await self._fetch_channels_for_analysis(channel_id)
        if not channels:
            return FilterReport()

        uniqueness_map = await self._fetch_uniqueness_map()
        subscriber_map = await self._fetch_latest_subscriber_map()
        short_map = await self._fetch_short_message_map()
        cross_dupe_map = await self._fetch_cross_dupe_map()
        cyrillic_map = await self._fetch_cyrillic_map()

        results: list[ChannelFilterResult] = []
        for channel in channels:
            channel_id_value = channel["channel_id"]
            message_count = int(channel["message_count"] or 0)
            flags: list[str] = []

            uniqueness_pct: float | None = None
            low_uniqueness = False
            if channel_id_value in uniqueness_map:
                total, uniq = uniqueness_map[channel_id_value]
                raw_uniqueness = uniq / total * 100
                uniqueness_pct = round(raw_uniqueness, 1)
                low_uniqueness = raw_uniqueness < LOW_UNIQUENESS_THRESHOLD
            if low_uniqueness:
                flags.append("low_uniqueness")

            subscriber_ratio: float | None = None
            low_subscriber = False
            subscriber_count = subscriber_map.get(channel_id_value)
            if subscriber_count is not None and message_count > 0:
                raw_ratio = subscriber_count / message_count
                subscriber_ratio = round(raw_ratio, 2)
                low_subscriber = raw_ratio < LOW_SUBSCRIBER_RATIO_THRESHOLD
            if low_subscriber:
                flags.append("low_subscriber_ratio")

            cross_dupe_pct: float | None = None
            cross_dupe = False
            if channel_id_value in cross_dupe_map:
                uniq_total, duped = cross_dupe_map[channel_id_value]
                if uniq_total > 0:
                    raw_cross_pct = duped / uniq_total * 100
                    cross_dupe_pct = round(raw_cross_pct, 1)
                    cross_dupe = raw_cross_pct > CROSS_DUPE_THRESHOLD
            if cross_dupe:
                flags.append("cross_channel_spam")

            cyrillic_pct: float | None = None
            non_cyrillic = False
            if channel_id_value in cyrillic_map:
                cyr_total, cyr_count = cyrillic_map[channel_id_value]
                if cyr_total > 0:
                    raw_cyr_pct = cyr_count / cyr_total * 100
                    cyrillic_pct = round(raw_cyr_pct, 1)
                    non_cyrillic = raw_cyr_pct < NON_CYRILLIC_THRESHOLD
            if non_cyrillic:
                flags.append("non_cyrillic")

            short_msg_pct: float | None = None
            noisy_chat = False
            if channel["channel_type"] == "group" and channel_id_value in short_map:
                short_total, short_count = short_map[channel_id_value]
                if short_total > 0:
                    raw_short_pct = short_count / short_total * 100
                    short_msg_pct = round(raw_short_pct, 1)
                    noisy_chat = raw_short_pct > CHAT_NOISE_THRESHOLD
            if noisy_chat:
                flags.append("chat_noise")

            results.append(
                ChannelFilterResult(
                    channel_id=channel_id_value,
                    title=channel["title"],
                    username=channel["username"],
                    message_count=message_count,
                    flags=flags,
                    uniqueness_pct=uniqueness_pct,
                    subscriber_ratio=subscriber_ratio,
                    cyrillic_pct=cyrillic_pct,
                    short_msg_pct=short_msg_pct,
                    cross_dupe_pct=cross_dupe_pct,
                    is_filtered=bool(flags),
                )
            )

        filtered_count = sum(1 for result in results if result.is_filtered)
        return FilterReport(
            results=results,
            total_channels=len(results),
            filtered_count=filtered_count,
        )

    async def analyze_channel(self, channel_id: int) -> ChannelFilterResult:
        report = await self._build_report(channel_id=channel_id)
        if report.results:
            return report.results[0]
        return ChannelFilterResult(channel_id=channel_id)

    async def analyze_all(self) -> FilterReport:
        return await self._build_report()

    async def apply_filters(self, report: FilterReport) -> int:
        updates = [
            (result.channel_id, ",".join(result.flags))
            for result in report.results
            if result.is_filtered
        ]
        if not updates:
            return 0
        return await self._database.set_channels_filtered_bulk(updates)

    async def reset_filters(self) -> None:
        await self._database.reset_all_channel_filters()
