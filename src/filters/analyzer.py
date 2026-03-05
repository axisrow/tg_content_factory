from __future__ import annotations

import logging

import aiosqlite

from src.filters.criteria import (
    check_chat_noise,
    check_cross_channel_dupes,
    check_low_uniqueness,
    check_non_cyrillic,
    check_subscriber_ratio,
)
from src.filters.models import ChannelFilterResult, FilterReport

logger = logging.getLogger(__name__)


class ChannelAnalyzer:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    async def analyze_channel(self, channel_id: int) -> ChannelFilterResult:
        cur = await self._db.execute(
            "SELECT channel_id, title, username FROM channels WHERE channel_id = ?",
            (channel_id,),
        )
        ch_row = await cur.fetchone()
        title = ch_row["title"] if ch_row else None
        username = ch_row["username"] if ch_row else None

        cur = await self._db.execute(
            "SELECT COUNT(*) AS cnt FROM messages WHERE channel_id = ?",
            (channel_id,),
        )
        msg_count = (await cur.fetchone())["cnt"]

        flags: list[str] = []

        uniqueness_pct, low_uniq = await check_low_uniqueness(self._db, channel_id)
        if low_uniq:
            flags.append("low_uniqueness")

        sub_ratio, low_sub = await check_subscriber_ratio(self._db, channel_id)
        if low_sub:
            flags.append("low_subscriber_ratio")

        cross_pct, cross_dup = await check_cross_channel_dupes(self._db, channel_id)
        if cross_dup:
            flags.append("cross_channel_spam")

        cyr_pct, non_cyr = await check_non_cyrillic(self._db, channel_id)
        if non_cyr:
            flags.append("non_cyrillic")

        short_pct, noisy = await check_chat_noise(self._db, channel_id)
        if noisy:
            flags.append("chat_noise")

        return ChannelFilterResult(
            channel_id=channel_id,
            title=title,
            username=username,
            message_count=msg_count,
            flags=flags,
            uniqueness_pct=uniqueness_pct,
            subscriber_ratio=sub_ratio,
            cyrillic_pct=cyr_pct,
            short_msg_pct=short_pct,
            cross_dupe_pct=cross_pct,
            is_filtered=len(flags) > 0,
        )

    async def analyze_all(self) -> FilterReport:
        cur = await self._db.execute("SELECT channel_id FROM channels ORDER BY id")
        rows = await cur.fetchall()

        results: list[ChannelFilterResult] = []
        for row in rows:
            result = await self.analyze_channel(row["channel_id"])
            results.append(result)

        filtered_count = sum(1 for r in results if r.is_filtered)
        return FilterReport(
            results=results,
            total_channels=len(results),
            filtered_count=filtered_count,
        )

    async def apply_filters(self, report: FilterReport) -> int:
        count = 0
        for result in report.results:
            if result.is_filtered:
                await self._db.execute(
                    "UPDATE channels SET is_filtered = 1 WHERE channel_id = ?",
                    (result.channel_id,),
                )
                count += 1
        await self._db.commit()
        return count

    async def reset_filters(self) -> None:
        await self._db.execute("UPDATE channels SET is_filtered = 0")
        await self._db.commit()
