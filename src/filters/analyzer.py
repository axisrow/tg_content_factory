from __future__ import annotations

import logging

from src.database import Database
from src.filters.criteria import (
    CHAT_NOISE_THRESHOLD,
    CROSS_DUPE_THRESHOLD,
    LOW_SUBSCRIBER_RATIO_CHAT_THRESHOLD,
    LOW_SUBSCRIBER_RATIO_THRESHOLD,
    LOW_UNIQUENESS_THRESHOLD,
    NON_CYRILLIC_THRESHOLD,
)
from src.filters.models import ChannelFilterResult, FilterReport
from src.settings_utils import parse_int_setting

logger = logging.getLogger(__name__)


class ChannelAnalyzer:
    def __init__(self, db: Database):
        if db.db is None:
            raise RuntimeError("Database not initialized")
        self._database = db
        self._repo = db.filter_repo

    async def _load_min_subscribers_filter(self) -> int:
        return parse_int_setting(
            await self._database.get_setting("min_subscribers_filter"),
            setting_name="min_subscribers_filter",
            default=0,
            logger=logger,
        )

    async def _build_report(self, channel_id: int | None = None) -> FilterReport:
        channels = await self._repo.fetch_channels_for_analysis(channel_id)
        if not channels:
            return FilterReport()

        uniqueness_map = await self._repo.fetch_uniqueness_map(channel_id)
        subscriber_map = await self._repo.fetch_subscriber_map(channel_id)
        short_map = await self._repo.fetch_short_message_map(channel_id)
        cross_dupe_map = await self._repo.fetch_cross_dupe_map(channel_id)
        cyrillic_map = await self._repo.fetch_cyrillic_map(channel_id)

        min_subs = await self._load_min_subscribers_filter()

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
                is_broadcast = channel["channel_type"] in ("channel", "monoforum")
                threshold = (
                    LOW_SUBSCRIBER_RATIO_THRESHOLD
                    if is_broadcast
                    else LOW_SUBSCRIBER_RATIO_CHAT_THRESHOLD
                )
                low_subscriber = raw_ratio < threshold
            manual_subs_flagged = False
            if (
                min_subs > 0
                and subscriber_count is not None
                and subscriber_count < min_subs
            ):
                flags.append("low_subscriber_manual")
                manual_subs_flagged = True

            if low_subscriber and not manual_subs_flagged:
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
            is_chat = channel["channel_type"] in ("group", "supergroup", "forum")
            if is_chat and channel_id_value in short_map:
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
        # Dedupe by channel_id (merge flags via set union) to avoid double updates/count inflation.
        deduped: dict[int, set[str]] = {}
        for result in report.results:
            if result.is_filtered:
                existing = deduped.get(result.channel_id, set())
                existing.update(result.flags)
                deduped[result.channel_id] = existing
        updates = [(cid, ",".join(sorted(flags))) for cid, flags in deduped.items()]
        conn = self._database.db
        assert conn is not None
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

    async def precheck_subscriber_ratio(self) -> int:
        """Filter channels by subscriber_count/message_count without Telegram.
        Returns count of newly filtered channels."""
        channels = await self._database.get_channels_with_counts(
            active_only=True, include_filtered=False,
        )
        stats_map = await self._database.get_latest_stats_for_all()
        to_filter: list[tuple[int, str]] = []
        for channel in channels:
            stats = stats_map.get(channel.channel_id)
            subscriber_count = stats.subscriber_count if stats else None
            if not subscriber_count or not channel.message_count:
                continue
            is_broadcast = channel.channel_type in ("channel", "monoforum")
            threshold = (
                LOW_SUBSCRIBER_RATIO_THRESHOLD if is_broadcast
                else LOW_SUBSCRIBER_RATIO_CHAT_THRESHOLD
            )
            if subscriber_count / channel.message_count < threshold:
                to_filter.append((channel.channel_id, "low_subscriber_ratio"))

        # Применить ручной порог min_subscribers_filter
        min_subs = await self._load_min_subscribers_filter()
        if min_subs > 0:
            already = {cid for cid, _ in to_filter}
            for channel in channels:
                if channel.channel_id in already:
                    continue
                stats = stats_map.get(channel.channel_id)
                subs = stats.subscriber_count if stats else None
                if subs is not None and subs < min_subs:
                    to_filter.append((channel.channel_id, "low_subscriber_manual"))

        if to_filter:
            await self._database.set_channels_filtered_bulk(to_filter)
        return len(to_filter)

    async def reset_filters(self) -> None:
        await self._database.reset_all_channel_filters()
