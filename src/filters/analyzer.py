from __future__ import annotations

import logging
import time

from src.database import Database
from src.filters.criteria import (
    CHAT_NOISE_THRESHOLD,
    CROSS_DUPE_THRESHOLD,
    DEFAULT_QUICK_SAMPLE_SIZE,
    LOW_SUBSCRIBER_RATIO_CHAT_THRESHOLD,
    LOW_SUBSCRIBER_RATIO_THRESHOLD,
    LOW_UNIQUENESS_THRESHOLD,
    NON_CYRILLIC_THRESHOLD,
    SUSPICIOUS_USERNAME_RE,
)
from src.filters.models import ChannelFilterResult, FilterReport
from src.settings_utils import parse_int_setting

logger = logging.getLogger(__name__)

# Channel-type groupings shared across the analyzer's broadcast/chat branches so a
# new Telegram type is added in one place (e.g. gigagroup, #971).
BROADCAST_CHANNEL_TYPES = ("channel", "monoforum")
CHAT_CHANNEL_TYPES = ("group", "supergroup", "gigagroup", "forum")


async def _timed_fetch(label: str, coro):
    """Await coro, log elapsed time and row count, return result."""
    logger.info("filter/analyze: %s — started", label)
    t = time.monotonic()
    result = await coro
    logger.info("filter/analyze: %s (%d) in %.1fs", label, len(result), time.monotonic() - t)
    return result


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

    async def _fetch_channels_for_report(self, channel_id: int | None, started_at: float) -> list:
        channels = await self._repo.fetch_channels_for_analysis(channel_id)
        logger.info("filter/analyze: fetched %d channels in %.1fs", len(channels), time.monotonic() - started_at)
        return channels

    async def _fetch_analysis_maps(
        self,
        channel_id: int | None = None,
        *,
        skip_cross_dupe: bool = False,
        sample_size: int | None = None,
    ) -> tuple:
        if self._repo._can_parallel():
            logger.info(
                "filter/analyze: all maps — started (parallel%s)",
                f", sample={sample_size}" if sample_size is not None else "",
            )
            t_map = time.monotonic()
            uniqueness_map, subscriber_map, short_map, cross_dupe_map, cyrillic_map = (
                await self._repo.fetch_maps_parallel(
                    channel_id, include_cross_dupe=not skip_cross_dupe, sample_size=sample_size
                )
            )
            logger.info(
                "filter/analyze: all maps in %.1fs (parallel) — %d channels",
                time.monotonic() - t_map,
                len(uniqueness_map),
            )
        else:
            uniqueness_map = await _timed_fetch(
                "uniqueness map", self._repo.fetch_uniqueness_map(channel_id, sample_size=sample_size)
            )
            subscriber_map = await _timed_fetch("subscriber map", self._repo.fetch_subscriber_map(channel_id))
            short_map = await _timed_fetch(
                "short-message map", self._repo.fetch_short_message_map(channel_id, sample_size=sample_size)
            )
            cross_dupe_map = (
                {}
                if skip_cross_dupe
                else await _timed_fetch("cross-dupe map", self._repo.fetch_cross_dupe_map(channel_id))
            )
            cyrillic_map = await _timed_fetch(
                "cyrillic map", self._repo.fetch_cyrillic_map(channel_id, sample_size=sample_size)
            )
        return uniqueness_map, subscriber_map, short_map, cross_dupe_map, cyrillic_map

    def _append_uniqueness_flag(self, flags: list[str], channel_id_value: int, uniqueness_map: dict) -> float | None:
        uniqueness_pct: float | None = None
        low_uniqueness = False
        if channel_id_value in uniqueness_map:
            total, uniq = uniqueness_map[channel_id_value]
            if total > 0:
                raw_uniqueness = uniq / total * 100
                uniqueness_pct = round(raw_uniqueness, 1)
                low_uniqueness = raw_uniqueness < LOW_UNIQUENESS_THRESHOLD
        if low_uniqueness:
            flags.append("low_uniqueness")
        return uniqueness_pct

    def _append_subscriber_flags(
        self,
        flags: list[str],
        channel,
        message_count: int,
        subscriber_count: int | None,
        min_subs: int,
    ) -> float | None:
        subscriber_ratio: float | None = None
        low_subscriber = False
        if subscriber_count is not None and message_count > 0:
            raw_ratio = subscriber_count / message_count
            subscriber_ratio = round(raw_ratio, 2)
            is_broadcast = channel["channel_type"] in BROADCAST_CHANNEL_TYPES
            threshold = (
                LOW_SUBSCRIBER_RATIO_THRESHOLD
                if is_broadcast
                else LOW_SUBSCRIBER_RATIO_CHAT_THRESHOLD
            )
            low_subscriber = raw_ratio < threshold
        manual_subs_flagged = False
        if min_subs > 0 and subscriber_count is not None and subscriber_count < min_subs:
            flags.append("low_subscriber_manual")
            manual_subs_flagged = True

        if low_subscriber and not manual_subs_flagged:
            flags.append("low_subscriber_ratio")
        return subscriber_ratio

    def _append_cross_dupe_flag(self, flags: list[str], channel_id_value: int, cross_dupe_map: dict) -> float | None:
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
        return cross_dupe_pct

    def _append_cyrillic_flag(self, flags: list[str], channel_id_value: int, cyrillic_map: dict) -> float | None:
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
        return cyrillic_pct

    def _append_chat_noise_flag(
        self,
        flags: list[str],
        channel,
        channel_id_value: int,
        short_map: dict,
    ) -> float | None:
        short_msg_pct: float | None = None
        noisy_chat = False
        is_chat = channel["channel_type"] in CHAT_CHANNEL_TYPES
        if is_chat and channel_id_value in short_map:
            short_total, short_count = short_map[channel_id_value]
            if short_total > 0:
                raw_short_pct = short_count / short_total * 100
                short_msg_pct = round(raw_short_pct, 1)
                noisy_chat = raw_short_pct > CHAT_NOISE_THRESHOLD
        if noisy_chat:
            flags.append("chat_noise")
        return short_msg_pct

    def _append_suspicious_username_flag(self, flags: list[str], channel) -> None:
        raw_username = channel["username"]
        if raw_username and SUSPICIOUS_USERNAME_RE.match(raw_username):
            flags.append("suspicious_username")

    def _build_channel_result(
        self,
        channel,
        *,
        uniqueness_map: dict,
        subscriber_map: dict,
        short_map: dict,
        cross_dupe_map: dict,
        cyrillic_map: dict,
        min_subs: int,
    ) -> ChannelFilterResult:
        channel_id_value = channel["channel_id"]
        message_count = int(channel["message_count"] or 0)
        flags: list[str] = []

        uniqueness_pct = self._append_uniqueness_flag(flags, channel_id_value, uniqueness_map)
        subscriber_count = subscriber_map.get(channel_id_value)
        subscriber_ratio = self._append_subscriber_flags(flags, channel, message_count, subscriber_count, min_subs)
        cross_dupe_pct = self._append_cross_dupe_flag(flags, channel_id_value, cross_dupe_map)
        cyrillic_pct = self._append_cyrillic_flag(flags, channel_id_value, cyrillic_map)
        short_msg_pct = self._append_chat_noise_flag(flags, channel, channel_id_value, short_map)
        self._append_suspicious_username_flag(flags, channel)

        return ChannelFilterResult(
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

    def _build_channel_results(
        self,
        channels: list,
        *,
        uniqueness_map: dict,
        subscriber_map: dict,
        short_map: dict,
        cross_dupe_map: dict,
        cyrillic_map: dict,
        min_subs: int,
    ) -> list[ChannelFilterResult]:
        return [
            self._build_channel_result(
                channel,
                uniqueness_map=uniqueness_map,
                subscriber_map=subscriber_map,
                short_map=short_map,
                cross_dupe_map=cross_dupe_map,
                cyrillic_map=cyrillic_map,
                min_subs=min_subs,
            )
            for channel in channels
        ]

    def _filter_report_from_results(self, results: list[ChannelFilterResult], started_at: float) -> FilterReport:
        filtered_count = sum(1 for result in results if result.is_filtered)
        logger.info(
            "filter/analyze: report complete — %d channels, %d filtered in %.1fs",
            len(results),
            filtered_count,
            time.monotonic() - started_at,
        )
        return FilterReport(
            results=results,
            total_channels=len(results),
            filtered_count=filtered_count,
        )

    async def _build_report(
        self,
        channel_id: int | None = None,
        *,
        skip_cross_dupe: bool = False,
        sample_size: int | None = None,
    ) -> FilterReport:
        t0 = time.monotonic()
        channels = await self._fetch_channels_for_report(channel_id, t0)
        if not channels:
            return FilterReport()

        uniqueness_map, subscriber_map, short_map, cross_dupe_map, cyrillic_map = await self._fetch_analysis_maps(
            channel_id, skip_cross_dupe=skip_cross_dupe, sample_size=sample_size
        )
        min_subs = await self._load_min_subscribers_filter()
        results = self._build_channel_results(
            channels,
            uniqueness_map=uniqueness_map,
            subscriber_map=subscriber_map,
            short_map=short_map,
            cross_dupe_map=cross_dupe_map,
            cyrillic_map=cyrillic_map,
            min_subs=min_subs,
        )
        return self._filter_report_from_results(results, t0)

    async def analyze_channel(self, channel_id: int) -> ChannelFilterResult:
        report = await self._build_report(channel_id=channel_id)
        if report.results:
            return report.results[0]
        return ChannelFilterResult(channel_id=channel_id)

    async def analyze_all(self, quick: bool = False, *, sample_size: int | None = None) -> FilterReport:
        """Analyze all channels.

        quick=True skips the heavy cross-dupe self-join (#774) AND samples only the
        last N messages per channel for the text metrics instead of scanning the
        whole messages table (#1138) — turning a ~6-min full scan into seconds.
        N defaults to DEFAULT_QUICK_SAMPLE_SIZE (300, calibrated); pass sample_size
        to override. sample_size is ignored unless quick=True (a full analyze always
        scans the whole history).

        A non-positive sample_size (0 or negative) falls back to the calibrated
        default: ``LIMIT 0`` would make every sampled text total zero, so the
        ``if total > 0`` guards suppress low_uniqueness / non_cyrillic / chat_noise
        and EVERY channel would look clean regardless of content (Codex review on
        #1138). The CLI/agent pass an unconstrained int, so the clamp lives here at
        the single chokepoint rather than in each caller.
        """
        if not quick:
            effective_sample = None
        elif sample_size is None or sample_size < 1:
            effective_sample = DEFAULT_QUICK_SAMPLE_SIZE
        else:
            effective_sample = sample_size
        return await self._build_report(skip_cross_dupe=quick, sample_size=effective_sample)

    async def apply_filters(self, report: FilterReport) -> int:
        # Dedupe by channel_id (merge flags via set union) to avoid double updates/count inflation.
        deduped: dict[int, set[str]] = {}
        for result in report.results:
            if result.is_filtered:
                existing = deduped.get(result.channel_id, set())
                existing.update(result.flags)
                deduped[result.channel_id] = existing

        conn = self._database.db
        assert conn is not None

        # Preserve "sticky" flags that are set outside of analyzer — e.g. collector
        # marks channels with `username_changed` when Telegram reports the saved
        # username no longer matches the channel, and operators set `manual` via UI.
        # These must survive reset_all_channel_filters → apply new report cycle.
        sticky_flag_names = ("username_changed", "title_changed", "manual")
        cur = await conn.execute(
            "SELECT channel_id, filter_flags FROM channels "
            "WHERE is_filtered = 1 AND filter_flags IS NOT NULL AND filter_flags != ''"
        )
        sticky_rows = await cur.fetchall()
        for row in sticky_rows:
            existing_flags = {f.strip() for f in (row["filter_flags"] or "").split(",") if f.strip()}
            preserved = existing_flags & set(sticky_flag_names)
            if preserved:
                bucket = deduped.setdefault(row["channel_id"], set())
                bucket.update(preserved)

        updates = [(cid, ",".join(sorted(flags))) for cid, flags in deduped.items()]

        # Run the reset + bulk-apply inside Database.transaction() so they are
        # atomic AND hold _write_lock — preventing any concurrent
        # execute_write() from prematurely committing this transaction
        # (issue #569). The previous manual BEGIN/commit block bypassed the
        # lock and reinstated the exact race the lock was added to close.
        count = 0
        async with self._database.transaction():
            await self._database.reset_all_channel_filters(commit=False)
            if updates:
                count = await self._database.set_channels_filtered_bulk(updates, commit=False)
        return count

    async def precheck_subscriber_ratio(self) -> int:
        """Filter channels by subscriber_count/message_count without Telegram.
        Returns count of newly filtered channels."""
        channels = await self._database.get_channels_with_counts(
            active_only=True,
            include_filtered=False,
        )
        stats_map = await self._database.get_latest_stats_for_all()
        to_filter: list[tuple[int, str]] = []
        for channel in channels:
            stats = stats_map.get(channel.channel_id)
            subscriber_count = stats.subscriber_count if stats else None
            if not subscriber_count or not channel.message_count:
                continue
            is_broadcast = channel.channel_type in BROADCAST_CHANNEL_TYPES
            threshold = (
                LOW_SUBSCRIBER_RATIO_THRESHOLD
                if is_broadcast
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

    async def reset_filters(self) -> int:
        return await self._database.reset_all_channel_filters()

    async def reset_filters_for_pks(self, pks: list[int]) -> int:
        return await self._database.reset_channel_filters_for_pks(pks)
