from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from tg_agent.database import Database
from tg_agent.filters.criteria import (
    AUTHOR_CONTENT_TAG,
    AUTO_FEED_MIN_MESSAGES,
    AUTO_FEED_THRESHOLD,
    CHAT_NOISE_THRESHOLD,
    CROSS_DUPE_THRESHOLD,
    DEFAULT_QUICK_SAMPLE_SIZE,
    FORWARD_SHARE_MIN_MESSAGES,
    FORWARD_SHARE_THRESHOLD,
    HIGH_VOLUME_MIN_24H,
    HIGH_VOLUME_PERCENTILE,
    KEEP_TAG,
    LABEL_ONLY_FLAGS,
    LOW_ENGAGEMENT_MIN_VIEWED_MESSAGES,
    LOW_ENGAGEMENT_THRESHOLD,
    LOW_SUBSCRIBER_RATIO_CHAT_THRESHOLD,
    LOW_SUBSCRIBER_RATIO_THRESHOLD,
    LOW_UNIQUENESS_THRESHOLD,
    NON_CYRILLIC_THRESHOLD,
    SETTING_FILTER_MAX_CROSS_DUPE_PCT,
    SETTING_FILTER_MIN_CYRILLIC_PCT,
    SETTING_FILTER_MIN_SUBSCRIBER_RATIO,
    SETTING_FILTER_MIN_UNIQUENESS,
    SUSPICIOUS_USERNAME_RE,
    WHITELIST_FLAG,
)
from tg_agent.filters.models import ChannelFilterResult, FilterReport
from tg_agent.settings_utils import parse_float_setting, parse_int_setting

logger = logging.getLogger(__name__)

# Chat-type grouping shared across the analyzer's gates so a new Telegram type
# is added in one place (e.g. gigagroup, #971). Everything else — incl. NULL
# channel_type from legacy imports — is judged as broadcast / by the data.
CHAT_CHANNEL_TYPES = ("group", "supergroup", "gigagroup", "forum")


@dataclass(frozen=True)
class FilterThresholds:
    """Операторские пороги из settings (#1596); дефолты — константы criteria.py.

    min_sub_ratio заменяет broadcast-порог LOW_SUBSCRIBER_RATIO_THRESHOLD;
    чат-порог остаётся константой — у метрики чатов своя калибровка, а ключ
    настройки один и означает канальный (broadcast) порог.
    """

    min_uniqueness: float
    min_sub_ratio: float
    max_cross_dupe: float
    min_cyrillic: float


def _percentile(values: list[int], pct: float) -> float | None:
    """Перцентиль `pct` (линейная интерполяция, как numpy.percentile); None на пустой выборке."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = pct / 100 * (len(ordered) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (rank - lo)


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

    async def _load_threshold_setting(
        self,
        key: str,
        default: float,
        *,
        minimum: float,
        maximum: float | None,
    ) -> float:
        raw = await self._database.get_setting(key)
        value = parse_float_setting(raw, setting_name=key, default=default, logger=logger)
        if value < minimum or (maximum is not None and value > maximum):
            logger.warning("%s out of range in settings DB (%r), using default %s", key, raw, default)
            return default
        return value

    async def _load_thresholds(self) -> FilterThresholds:
        """Пороги из settings (#1596): нет ключа / мусор / вне диапазона — дефолт."""
        return FilterThresholds(
            min_uniqueness=await self._load_threshold_setting(
                SETTING_FILTER_MIN_UNIQUENESS, LOW_UNIQUENESS_THRESHOLD, minimum=0.0, maximum=100.0
            ),
            min_sub_ratio=await self._load_threshold_setting(
                SETTING_FILTER_MIN_SUBSCRIBER_RATIO, LOW_SUBSCRIBER_RATIO_THRESHOLD, minimum=0.0, maximum=None
            ),
            max_cross_dupe=await self._load_threshold_setting(
                SETTING_FILTER_MAX_CROSS_DUPE_PCT, CROSS_DUPE_THRESHOLD, minimum=0.0, maximum=100.0
            ),
            min_cyrillic=await self._load_threshold_setting(
                SETTING_FILTER_MIN_CYRILLIC_PCT, NON_CYRILLIC_THRESHOLD, minimum=0.0, maximum=100.0
            ),
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
            (
                uniqueness_map,
                subscriber_map,
                short_map,
                cross_dupe_map,
                cyrillic_map,
                auto_feed_map,
                engagement_map,
                forward_map,
                volume_map,
            ) = await self._repo.fetch_maps_parallel(
                channel_id, include_cross_dupe=not skip_cross_dupe, sample_size=sample_size
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
            auto_feed_map = await _timed_fetch(
                "auto-feed map", self._repo.fetch_auto_feed_map(channel_id, sample_size=sample_size)
            )
            engagement_map = await _timed_fetch(
                "engagement map", self._repo.fetch_engagement_map(channel_id)
            )
            forward_map = await _timed_fetch(
                "forward-share map", self._repo.fetch_forward_share_map(channel_id)
            )
            volume_map = await _timed_fetch("volume map", self._repo.fetch_volume_map(channel_id))
        return (
            uniqueness_map,
            subscriber_map,
            short_map,
            cross_dupe_map,
            cyrillic_map,
            auto_feed_map,
            engagement_map,
            forward_map,
            volume_map,
        )

    def _append_uniqueness_flag(
        self, flags: list[str], channel_id_value: int, uniqueness_map: dict, min_uniqueness: float
    ) -> float | None:
        uniqueness_pct: float | None = None
        low_uniqueness = False
        if channel_id_value in uniqueness_map:
            total, uniq = uniqueness_map[channel_id_value]
            if total > 0:
                raw_uniqueness = uniq / total * 100
                uniqueness_pct = round(raw_uniqueness, 1)
                low_uniqueness = raw_uniqueness < min_uniqueness
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
        sub_ratio_threshold: float,
    ) -> float | None:
        subscriber_ratio: float | None = None
        low_subscriber = False
        if subscriber_count is not None and message_count > 0:
            raw_ratio = subscriber_count / message_count
            subscriber_ratio = round(raw_ratio, 2)
            # Unknown type (NULL, legacy import) judged as broadcast: chats in
            # the DB are typed, and for a chat the metric is meaningless anyway.
            is_broadcast = channel["channel_type"] not in CHAT_CHANNEL_TYPES
            threshold = (
                sub_ratio_threshold
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

    def _append_cross_dupe_flag(
        self, flags: list[str], channel_id_value: int, cross_dupe_map: dict, max_cross_dupe: float
    ) -> float | None:
        cross_dupe_pct: float | None = None
        cross_dupe = False
        if channel_id_value in cross_dupe_map:
            uniq_total, duped = cross_dupe_map[channel_id_value]
            if uniq_total > 0:
                raw_cross_pct = duped / uniq_total * 100
                cross_dupe_pct = round(raw_cross_pct, 1)
                cross_dupe = raw_cross_pct > max_cross_dupe
        if cross_dupe:
            flags.append("cross_channel_spam")
        return cross_dupe_pct

    def _append_cyrillic_flag(
        self, flags: list[str], channel_id_value: int, cyrillic_map: dict, min_cyrillic: float
    ) -> float | None:
        cyrillic_pct: float | None = None
        non_cyrillic = False
        if channel_id_value in cyrillic_map:
            cyr_total, cyr_count = cyrillic_map[channel_id_value]
            if cyr_total > 0:
                raw_cyr_pct = cyr_count / cyr_total * 100
                cyrillic_pct = round(raw_cyr_pct, 1)
                non_cyrillic = raw_cyr_pct < min_cyrillic
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

    def _append_auto_feed_flag(self, flags: list[str], channel_id_value: int, auto_feed_map: dict) -> float | None:
        auto_feed_pct: float | None = None
        auto_feed = False
        if channel_id_value in auto_feed_map:
            total, short_link = auto_feed_map[channel_id_value]
            if total > 0:
                raw_auto_pct = short_link / total * 100
                auto_feed_pct = round(raw_auto_pct, 1)
                auto_feed = (
                    total >= AUTO_FEED_MIN_MESSAGES and raw_auto_pct >= AUTO_FEED_THRESHOLD
                )
        if auto_feed:
            flags.append("auto_feed")
        return auto_feed_pct

    def _append_engagement_flag(
        self,
        flags: list[str],
        channel,
        subscriber_count: int | None,
        engagement_map: dict,
    ) -> float | None:
        views_ratio_pct: float | None = None
        dead_engagement = False
        if (
            subscriber_count
            # Unknown type (NULL, legacy import) judged by the data, not the
            # type; a real chat has no post views, so the guard below skips it.
            and channel["channel_type"] not in CHAT_CHANNEL_TYPES
            and channel["channel_id"] in engagement_map
        ):
            viewed, views_sum = engagement_map[channel["channel_id"]]
            if viewed >= LOW_ENGAGEMENT_MIN_VIEWED_MESSAGES and views_sum > 0:
                raw_pct = (views_sum / viewed) / subscriber_count * 100
                views_ratio_pct = round(raw_pct, 1)
                dead_engagement = raw_pct < LOW_ENGAGEMENT_THRESHOLD
        if dead_engagement:
            flags.append("low_engagement")
        return views_ratio_pct

    def _append_forward_flag(self, flags: list[str], channel_id_value: int, forward_map: dict) -> float | None:
        forward_pct: float | None = None
        forward_heavy = False
        if channel_id_value in forward_map:
            total, fwd = forward_map[channel_id_value]
            if total >= FORWARD_SHARE_MIN_MESSAGES and total > 0:
                raw_pct = fwd / total * 100
                forward_pct = round(raw_pct, 1)
                forward_heavy = raw_pct >= FORWARD_SHARE_THRESHOLD
        if forward_heavy:
            # Label-only (LABEL_ONLY_FLAGS): пересылочный канал — не спам.
            flags.append("forwarder")
        return forward_pct

    def _append_volume_flag(
        self,
        flags: list[str],
        channel_id_value: int,
        volume_map: dict,
        high_volume_p95: float | None,
    ) -> int | None:
        """`high_volume` (#1571): count_24h >= max(P95, HIGH_VOLUME_MIN_24H).

        Порог нестрогий (>=): канал ровно на пороге — уже лента. P95 —
        относительная часть (на плоской выборке опускается полом), None
        (нет данных) флаг не ставит.
        """
        volume_24h: int | None = volume_map.get(channel_id_value)
        if volume_24h is not None and high_volume_p95 is not None:
            if volume_24h >= max(high_volume_p95, HIGH_VOLUME_MIN_24H):
                flags.append("high_volume")
        return volume_24h

    def _build_channel_result(
        self,
        channel,
        *,
        uniqueness_map: dict,
        subscriber_map: dict,
        short_map: dict,
        cross_dupe_map: dict,
        cyrillic_map: dict,
        auto_feed_map: dict,
        engagement_map: dict,
        forward_map: dict,
        volume_map: dict,
        high_volume_p95: float | None,
        min_subs: int,
        thresholds: FilterThresholds,
    ) -> ChannelFilterResult:
        channel_id_value = channel["channel_id"]
        message_count = int(channel["message_count"] or 0)
        flags: list[str] = []

        uniqueness_pct = self._append_uniqueness_flag(
            flags, channel_id_value, uniqueness_map, thresholds.min_uniqueness
        )
        subscriber_count = subscriber_map.get(channel_id_value)
        subscriber_ratio = self._append_subscriber_flags(
            flags, channel, message_count, subscriber_count, min_subs, thresholds.min_sub_ratio
        )
        cross_dupe_pct = self._append_cross_dupe_flag(
            flags, channel_id_value, cross_dupe_map, thresholds.max_cross_dupe
        )
        cyrillic_pct = self._append_cyrillic_flag(
            flags, channel_id_value, cyrillic_map, thresholds.min_cyrillic
        )
        short_msg_pct = self._append_chat_noise_flag(flags, channel, channel_id_value, short_map)
        auto_feed_pct = self._append_auto_feed_flag(flags, channel_id_value, auto_feed_map)
        views_ratio_pct = self._append_engagement_flag(flags, channel, subscriber_count, engagement_map)
        forward_pct = self._append_forward_flag(flags, channel_id_value, forward_map)
        volume_24h = self._append_volume_flag(flags, channel_id_value, volume_map, high_volume_p95)
        self._append_suspicious_username_flag(flags, channel)

        # Флаги-метки (LABEL_ONLY_FLAGS) не блокируют сбор — см. criteria.py.
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
            auto_feed_pct=auto_feed_pct,
            views_ratio_pct=views_ratio_pct,
            forward_pct=forward_pct,
            volume_24h=volume_24h,
            is_filtered=any(f not in LABEL_ONLY_FLAGS for f in flags),
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
        auto_feed_map: dict,
        engagement_map: dict,
        forward_map: dict,
        volume_map: dict,
        min_subs: int,
        thresholds: FilterThresholds,
    ) -> list[ChannelFilterResult]:
        # P95 считается один раз по всей когорте отчёта (включая каналы с нулём
        # за сутки из volume_map) и спускается в каждый результат.
        high_volume_p95 = _percentile(list(volume_map.values()), HIGH_VOLUME_PERCENTILE)
        return [
            self._build_channel_result(
                channel,
                uniqueness_map=uniqueness_map,
                subscriber_map=subscriber_map,
                short_map=short_map,
                cross_dupe_map=cross_dupe_map,
                cyrillic_map=cyrillic_map,
                auto_feed_map=auto_feed_map,
                engagement_map=engagement_map,
                forward_map=forward_map,
                volume_map=volume_map,
                high_volume_p95=high_volume_p95,
                min_subs=min_subs,
                thresholds=thresholds,
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

        (
            uniqueness_map,
            subscriber_map,
            short_map,
            cross_dupe_map,
            cyrillic_map,
            auto_feed_map,
            engagement_map,
            forward_map,
            volume_map,
        ) = await self._fetch_analysis_maps(
            channel_id, skip_cross_dupe=skip_cross_dupe, sample_size=sample_size
        )
        min_subs = await self._load_min_subscribers_filter()
        thresholds = await self._load_thresholds()
        results = self._build_channel_results(
            channels,
            uniqueness_map=uniqueness_map,
            subscriber_map=subscriber_map,
            short_map=short_map,
            cross_dupe_map=cross_dupe_map,
            cyrillic_map=cyrillic_map,
            auto_feed_map=auto_feed_map,
            engagement_map=engagement_map,
            forward_map=forward_map,
            volume_map=volume_map,
            min_subs=min_subs,
            thresholds=thresholds,
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
        # Every flagged result participates (#1517): label-only flags (non_cyrillic) do not set
        # is_filtered but must still survive the reset → apply cycle.
        deduped: dict[int, set[str]] = {}
        for result in report.results:
            if result.flags:
                existing = deduped.get(result.channel_id, set())
                existing.update(result.flags)
                deduped[result.channel_id] = existing

        conn = self._database.db
        assert conn is not None

        # Preserve "sticky" flags that are set outside of analyzer — e.g. collector
        # marks channels with `username_changed` when Telegram reports the saved
        # username no longer matches the channel. These must survive
        # reset_all_channel_filters → apply new report cycle. The operator
        # `manual` flag is NOT sticky anymore (#1312): a human verdict is
        # expressed by filtered_origin='human' and protected by the SQL guards
        # on every writer; a legacy auto-origin `manual` row is re-judged like
        # any other auto channel.
        sticky_flag_names = ("username_changed", "title_changed")
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

        # Тег-эскроу (KEEP_TAG): keep-каналы выпадают из авто-фильтрации — после
        # reset им возвращается is_filtered=0, а флаги (прежние ∪ свежие) остаются
        # метками для кураторства. Без этого apply_filters при каждом прогоне
        # отбрасывал бы ручное решение собирать канал.
        fresh_flags: dict[int, set[str]] = {}
        for result in report.results:
            fresh_flags.setdefault(result.channel_id, set()).update(result.flags)
        keep_rows = await self._database.repos.channels.fetch_channels_with_tag(KEEP_TAG)
        for row in keep_rows:
            deduped.pop(row["channel_id"], None)

        # Партиция (#1517): блокирующие флаги идут через bulk (is_filtered=1);
        # каналы только с метками — отдельным апдейтом с is_filtered=0, иначе
        # reset стирал бы их метки. Human-решения апдейт не сносит.
        updates: list[tuple[int, str]] = []
        label_only_updates: list[tuple[int, str]] = []
        for cid, flags in deduped.items():
            flags_csv = ",".join(sorted(flags))
            if flags - LABEL_ONLY_FLAGS:
                updates.append((cid, flags_csv))
            else:
                label_only_updates.append((cid, flags_csv))

        # Run the reset + bulk-apply inside Database.transaction() so they are
        # atomic AND hold _write_lock — preventing any concurrent
        # execute_write() from prematurely committing this transaction
        # (issue #569). The previous manual BEGIN/commit block bypassed the
        # lock and reinstated the exact race the lock was added to close.
        count = 0
        async with self._database.transaction():
            await self._database.reset_all_channel_filters(commit=False)
            if updates:
                count, _suppressed = await self._database.set_channels_filtered_bulk(
                    updates, commit=False
                )
            for cid, flags_csv in label_only_updates:
                await conn.execute(
                    "UPDATE channels SET is_filtered = 0, filter_flags = ? "
                    "WHERE channel_id = ? AND NOT (is_filtered = 1 AND filtered_origin = 'human')",
                    (flags_csv, cid),
                )
            for row in keep_rows:
                old = {f.strip() for f in (row["filter_flags"] or "").split(",") if f.strip()}
                merged = ",".join(sorted(old | fresh_flags.get(row["channel_id"], set()))) or None
                await conn.execute(
                    "UPDATE channels SET is_filtered = 0, filter_flags = ? WHERE id = ?",
                    (merged, row["pk"]),
                )
        return count

    async def apply_whitelist(self) -> int:
        """Whitelist-режим (#1490, MVP): доверие по тегу «авторский контент».

        Тегированные (AUTHOR_CONTENT_TAG) и keep-эскроу — is_filtered=0: доверие
        сильнее blacklist-флагов, флаги остаются метками (их видит `filter suspects`).
        Остальные без человеческого решения — is_filtered=1 + WHITELIST_FLAG,
        прежние флаги сохраняются. Человеческие вердикты (filtered_origin='human')
        не трогаются в обе стороны. Возвращает число каналов под whitelist-фильтром.
        Метрики сообщений не нужны — вердикт чисто теговый, analyze не запускается.
        """
        trusted_pks = {
            row["pk"]
            for tag in (AUTHOR_CONTENT_TAG, KEEP_TAG)
            for row in await self._database.repos.channels.fetch_channels_with_tag(tag)
        }
        channels = await self._database.get_channels(active_only=False, include_filtered=True)
        conn = self._database.db
        assert conn is not None

        filtered = 0
        async with self._database.transaction():
            for ch in channels:
                old_flags = {f.strip() for f in (ch.filter_flags or "").split(",") if f.strip()}
                if ch.id in trusted_pks:
                    # Доверие: снять авто-фильтр; human-вердикт «фильтровать» не
                    # трогаем. Stale WHITELIST_FLAG снимается: иначе после
                    # «фильтр → тегирование → повторный whitelist» метка
                    # «канал без тега» остаётся навсегда и лжёт куратору (#1573).
                    await conn.execute(
                        "UPDATE channels SET is_filtered = 0, filter_flags = ? "
                        "WHERE id = ? AND is_filtered = 1 AND filtered_origin != 'human'",
                        (",".join(sorted(old_flags - {WHITELIST_FLAG})) or None, ch.id),
                    )
                elif ch.filtered_origin == "human":
                    continue
                else:
                    await conn.execute(
                        "UPDATE channels SET is_filtered = 1, filter_flags = ? WHERE id = ?",
                        (",".join(sorted(old_flags | {WHITELIST_FLAG})), ch.id),
                    )
                    filtered += 1
        return filtered

    async def precheck_subscriber_ratio(self) -> int:
        """Filter channels by subscriber_count/message_count without Telegram.
        Returns count of newly filtered channels."""
        # Keep-эскроу (#1516): тег выводит канал из авто-пречека — тот же
        # контракт, что apply_filters (#1497).
        keep_ids = {
            row["channel_id"]
            for row in await self._database.repos.channels.fetch_channels_with_tag(KEEP_TAG)
        }
        channels = await self._database.get_channels_with_counts(
            active_only=True,
            include_filtered=False,
        )
        stats_map = await self._database.get_latest_stats_for_all()
        thresholds = await self._load_thresholds()
        to_filter: list[tuple[int, str]] = []
        for channel in channels:
            if channel.channel_id in keep_ids:
                continue
            stats = stats_map.get(channel.channel_id)
            subscriber_count = stats.subscriber_count if stats else None
            if not subscriber_count or not channel.message_count:
                continue
            # Unknown type (NULL, legacy import) judged as broadcast — same
            # rule as _append_subscriber_flags.
            is_broadcast = channel.channel_type not in CHAT_CHANNEL_TYPES
            threshold = (
                thresholds.min_sub_ratio
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
                if channel.channel_id in already or channel.channel_id in keep_ids:
                    continue
                stats = stats_map.get(channel.channel_id)
                subs = stats.subscriber_count if stats else None
                if subs is not None and subs < min_subs:
                    to_filter.append((channel.channel_id, "low_subscriber_manual"))

        if not to_filter:
            return 0
        result = await self._database.set_channels_filtered_bulk(to_filter)
        return result.applied

    async def reset_filters(
        self,
        *,
        origin: str = "auto",
        actor: str | None = None,
        reason: str | None = None,
    ) -> int:
        return (
            await self._database.reset_all_channel_filters(
                origin=origin, actor=actor, reason=reason
            )
        )[0]

    async def reset_filters_for_pks(
        self,
        pks: list[int],
        *,
        origin: str = "auto",
        actor: str | None = None,
        reason: str | None = None,
    ) -> int:
        return (
            await self._database.reset_channel_filters_for_pks(
                pks, origin=origin, actor=actor, reason=reason
            )
        )[0]
