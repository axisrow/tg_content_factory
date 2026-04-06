from __future__ import annotations

import logging
from dataclasses import dataclass, field

from src.database import Database

logger = logging.getLogger(__name__)


@dataclass
class ChannelOverview:
    """Summary card for a single channel."""

    channel_id: int
    title: str | None
    username: str | None
    subscriber_count: int | None = None
    subscriber_delta: int | None = None
    subscriber_delta_week: int | None = None
    subscriber_delta_month: int | None = None
    err: float | None = None
    err24: float | None = None
    total_posts: int = 0
    posts_today: int = 0
    posts_week: int = 0
    posts_month: int = 0
    avg_views: float | None = None
    avg_forwards: float | None = None
    avg_reactions: float | None = None


@dataclass
class CitationStats:
    """Forward-based citation statistics for a channel."""

    total_forwards: int = 0
    post_count: int = 0
    avg_forwards: float = 0.0


@dataclass
class ChannelListItem:
    """Minimal channel descriptor for list views."""

    channel_id: int
    title: str | None
    username: str | None


@dataclass
class ChannelRanking:
    """A single channel's position in a ranked comparison."""

    channel_id: int
    title: str | None
    username: str | None
    rank: int
    score: float
    subscriber_count: int | None = None
    posts_period: int = 0
    avg_views_period: float | None = None


@dataclass
class ChannelComparison:
    """Side-by-side metrics for multiple channels."""

    channels: list[ChannelOverview] = field(default_factory=list)
    metrics: list[str] = field(default_factory=lambda: [
        "subscriber_count",
        "err",
        "err24",
        "avg_views",
        "avg_forwards",
        "avg_reactions",
        "posts_today",
        "posts_week",
        "posts_month",
    ])


class ChannelAnalyticsService:
    """Per-channel analytics -- subscribers, ERR, reach, post frequency, citation.

    All public methods accept a ``days`` parameter (default 30) so callers can
    control the time window for time-series and aggregated metrics.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    # ── public API ──────────────────────────────────────────────────

    async def get_active_channels(self) -> list[ChannelListItem]:
        """List active, non-filtered channels as lightweight items."""
        channels = await self._db.get_channels(active_only=True, include_filtered=False)
        return [
            ChannelListItem(
                channel_id=ch.channel_id,
                title=ch.title,
                username=ch.username,
            )
            for ch in channels
        ]

    async def get_channel_overview(
        self,
        channel_id: int,
        days: int = 30,
    ) -> ChannelOverview:
        """Build the summary card for a single channel.

        Args:
            channel_id: Telegram channel_id to analyze.
            days: Time window (in days) for time-series lookups.

        Returns:
            A ChannelOverview dataclass with all computed metrics.
        """
        ch = await self._db.get_channel_by_channel_id(channel_id)
        overview = ChannelOverview(
            channel_id=channel_id,
            title=ch.title if ch else None,
            username=ch.username if ch else None,
        )

        # Subscriber data from channel_stats
        stats_history = await self._db.get_channel_stats(channel_id, limit=60)
        if stats_history:
            latest = stats_history[0]
            overview.subscriber_count = latest.subscriber_count
            overview.avg_views = latest.avg_views
            overview.avg_forwards = latest.avg_forwards
            overview.avg_reactions = latest.avg_reactions

            # Delta vs previous collection
            if len(stats_history) > 1 and latest.subscriber_count is not None:
                prev = stats_history[1].subscriber_count
                if prev is not None:
                    overview.subscriber_delta = latest.subscriber_count - prev

        # Week/month deltas from historical stats
        week_ago_count = await self._db.repos.channel_stats.get_subscriber_count_at(
            channel_id, days_ago=7,
        )
        month_ago_count = await self._db.repos.channel_stats.get_subscriber_count_at(
            channel_id, days_ago=30,
        )
        if overview.subscriber_count is not None:
            if week_ago_count is not None:
                overview.subscriber_delta_week = overview.subscriber_count - week_ago_count
            if month_ago_count is not None:
                overview.subscriber_delta_month = overview.subscriber_count - month_ago_count

        # Post counts -- posts_month uses the days parameter
        overview.total_posts = await self._db.repos.messages.get_channel_message_count(channel_id)
        overview.posts_today = await self._db.repos.messages.get_channel_message_count(
            channel_id, days=1,
        )
        overview.posts_week = await self._db.repos.messages.get_channel_message_count(
            channel_id, days=7,
        )
        overview.posts_month = await self._db.repos.messages.get_channel_message_count(
            channel_id, days=days,
        )

        # ERR calculations
        overview.err = await self._calc_err(channel_id, last_n=20)
        overview.err24 = await self._calc_err24(channel_id)

        return overview

    async def get_subscriber_history(
        self, channel_id: int, days: int = 30,
    ) -> list[dict]:
        """Subscriber count time series for *days* lookback."""
        return await self._db.repos.channel_stats.get_subscriber_history(channel_id, days)

    async def get_views_timeseries(
        self, channel_id: int, days: int = 30,
    ) -> list[dict]:
        """Daily average views and message count for a channel."""
        return await self._db.repos.messages.get_views_timeseries(channel_id, days)

    async def get_post_frequency(
        self, channel_id: int, days: int = 30,
    ) -> list[dict]:
        """Daily post count for a channel."""
        return await self._db.repos.messages.get_post_frequency(channel_id, days)

    async def get_citation_stats(self, channel_id: int) -> CitationStats:
        """Forward-based citation statistics."""
        raw = await self._db.repos.messages.get_citation_stats(channel_id)
        return CitationStats(
            total_forwards=int(raw.get("total_forwards", 0)),
            post_count=int(raw.get("post_count", 0)),
            avg_forwards=round(float(raw.get("avg_forwards", 0)), 1),
        )

    async def get_heatmap(
        self, channel_id: int, days: int = 30,
    ) -> list[dict]:
        """Hour x weekday message-count heatmap data.

        Returns ``[{hour, weekday, count}]`` dicts for a 7x24 grid
        (weekdays on Y, hours on X).  Absent cells = 0.
        Delegates to :meth:`MessagesRepository.get_hour_weekday_heatmap`.
        """
        return await self._db.repos.messages.get_hour_weekday_heatmap(channel_id, days)

    async def get_cross_channel_citations(
        self, channel_id: int, days: int = 30, limit: int = 20,
    ) -> list[dict]:
        """Cross-channel citation index via forward_from_channel_id."""
        return await self._db.repos.messages.get_cross_channel_citations(
            channel_id, days, limit,
        )

    async def get_err(self, channel_id: int) -> float | None:
        """Engagement Rate per Reach for the last 20 posts."""
        return await self._calc_err(channel_id, last_n=20)

    async def get_err24(self, channel_id: int) -> float | None:
        """Engagement Rate per Reach for posts from the last 24 hours."""
        return await self._calc_err24(channel_id)

    async def get_hourly_activity(
        self, channel_id: int, days: int = 30,
    ) -> list[dict]:
        """Message count by hour of day for a channel."""
        rows = await self._db.execute_fetchall(
            """SELECT CAST(strftime('%H', m.date) AS INTEGER) AS hour,
                      COUNT(*) AS count
               FROM messages m
               WHERE m.channel_id = ? AND m.date >= datetime('now', ?)
               GROUP BY hour
               ORDER BY hour""",
            (channel_id, f"-{days} days"),
        )
        return [dict(r) for r in rows]

    async def get_ranked_channels(
        self,
        metric: str = "err",
        days: int = 30,
        limit: int = 20,
    ) -> list[ChannelRanking]:
        """Rank active channels by a given metric.

        Supported metrics: ``err``, ``subscriber_count``, ``avg_views``,
        ``posts_period`` (post count in the last *days* days).

        Returns:
            List of :class:`ChannelRanking` sorted descending by score.
        """
        channels = await self.get_active_channels()
        rankings: list[ChannelRanking] = []

        for item in channels:
            cid = item.channel_id
            overview = await self.get_channel_overview(cid, days=days)

            if metric == "err":
                score = overview.err or 0.0
            elif metric == "subscriber_count":
                score = float(overview.subscriber_count or 0)
            elif metric == "avg_views":
                score = overview.avg_views or 0.0
            elif metric == "posts_period":
                score = float(overview.posts_month)
            else:
                logger.warning("Unknown ranking metric %r, defaulting to err", metric)
                score = overview.err or 0.0

            rankings.append(ChannelRanking(
                channel_id=cid,
                title=item.title,
                username=item.username,
                rank=0,  # assigned below after sort
                score=round(score, 4),
                subscriber_count=overview.subscriber_count,
                posts_period=overview.posts_month,
                avg_views_period=overview.avg_views,
            ))

        rankings.sort(key=lambda r: r.score, reverse=True)
        for i, r in enumerate(rankings, start=1):
            r.rank = i

        return rankings[:limit]

    async def get_channel_comparison(
        self,
        channel_ids: list[int],
        days: int = 30,
    ) -> ChannelComparison:
        """Compare overview metrics for a set of channels side by side."""
        overviews: list[ChannelOverview] = []
        for cid in channel_ids:
            overview = await self.get_channel_overview(cid, days=days)
            overviews.append(overview)
        return ChannelComparison(channels=overviews)

    # -- private helpers -----------------------------------------------------

    async def _get_subscriber_count(self, channel_id: int) -> int | None:
        stats = await self._db.get_channel_stats(channel_id, limit=1)
        if stats and stats[0].subscriber_count is not None:
            return stats[0].subscriber_count
        return None

    async def _calc_err(self, channel_id: int, last_n: int = 20) -> float | None:
        sub_count = await self._get_subscriber_count(channel_id)
        if not sub_count:
            return None
        rows = await self._db.repos.messages.get_err_data(channel_id, last_n)
        if not rows:
            return None
        total_engagement = sum(
            (r.get("views") or 0)
            + (r.get("forwards") or 0)
            + (r.get("reply_count") or 0)
            + r.get("total_reactions", 0)
            for r in rows
        )
        return round(total_engagement / (len(rows) * sub_count) * 100, 2)

    async def _calc_err24(self, channel_id: int) -> float | None:
        sub_count = await self._get_subscriber_count(channel_id)
        if not sub_count:
            return None
        rows = await self._db.repos.messages.get_err24_data(channel_id)
        if not rows:
            return None
        total_engagement = sum(
            (r.get("views") or 0)
            + (r.get("forwards") or 0)
            + (r.get("reply_count") or 0)
            + r.get("total_reactions", 0)
            for r in rows
        )
        return round(total_engagement / (len(rows) * sub_count) * 100, 2)
