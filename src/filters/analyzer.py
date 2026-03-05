from __future__ import annotations

import logging

from src.database import Database
from src.filters.criteria import (
    CHAT_NOISE_THRESHOLD,
    CROSS_DUPE_THRESHOLD,
    LOW_SUBSCRIBER_RATIO_THRESHOLD,
    LOW_UNIQUENESS_THRESHOLD,
    NON_CYRILLIC_THRESHOLD,
)
from src.filters.models import ChannelFilterResult, FilterReport

logger = logging.getLogger(__name__)


class ChannelAnalyzer:
    def __init__(self, db: Database):
        if db.db is None:
            raise RuntimeError("Database not initialized")
        self._database = db
        self._repo = db.filter_repo

    async def _build_report(self, channel_id: int | None = None) -> FilterReport:
        channels = await self._repo.fetch_channels_for_analysis(channel_id)
        if not channels:
            return FilterReport()

        uniqueness_map = await self._repo.fetch_uniqueness_map(channel_id)
        subscriber_map = await self._repo.fetch_subscriber_map(channel_id)
        short_map = await self._repo.fetch_short_message_map(channel_id)
        cross_dupe_map = await self._repo.fetch_cross_dupe_map(channel_id)
        cyrillic_map = await self._repo.fetch_cyrillic_map(channel_id)

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
        # Dedupe by channel_id (last value wins) to avoid double updates/count inflation.
        deduped: dict[int, str] = {}
        for result in report.results:
            if result.is_filtered:
                deduped[result.channel_id] = ",".join(result.flags)
        updates = list(deduped.items())

        conn = self._database.db
        assert conn is not None
        await conn.execute("BEGIN")
        try:
            await self._database.reset_all_channel_filters(commit=False)
            count = 0
            if updates:
                count = await self._database.set_channels_filtered_bulk(
                    updates, commit=False
                )
            await conn.commit()
            return count
        except Exception:
            await conn.rollback()
            raise

    async def reset_filters(self) -> None:
        await self._database.reset_all_channel_filters()
