"""Internal analytics methods sharing the MessagesRepository connection."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from tg_agent.models import Message
from tg_agent.utils.datetime import (
    parse_date_bound,
    parse_datetime_bound,
)

if TYPE_CHECKING:
    from tg_agent.database.repositories.messages import MessagesRepository

logger = logging.getLogger("tg_agent.database.repositories.messages")



class MessageAnalyticsMixin:
    """Used through MessagesRepository; owns no connection or transaction state."""

    @staticmethod
    def _normalize_date_from(value: str | None) -> str | None:
        """Canonicalize a lower date/datetime bound or None; ValueError on garbage (#1593)."""
        if not value:
            return None
        try:
            return parse_date_bound(value).isoformat()
        except ValueError:
            return parse_datetime_bound(value).isoformat()

    @staticmethod
    def _normalize_date_to(value: str | None) -> tuple[str | None, str]:
        """Canonicalize an upper bound: exclusive next-day for dates, exact instant for datetimes (#1593)."""
        if not value:
            return None, "<="
        try:
            return (parse_date_bound(value) + timedelta(days=1)).isoformat(), "<"
        except ValueError:
            return parse_datetime_bound(value).isoformat(), "<="

    def _append_analytics_date_filter(
        self: "MessagesRepository",
        conditions: list[str],
        params: list,
        date_from: str | None,
        date_to: str | None,
    ) -> None:
        """Append normalized ``m.date`` bounds to analytics conditions/params in place."""
        normalized_date_from = self._normalize_date_from(date_from)
        normalized_date_to, date_to_operator = self._normalize_date_to(date_to)
        if normalized_date_from:
            conditions.append("m.date >= ?")
            params.append(normalized_date_from)
        if normalized_date_to:
            conditions.append(f"m.date {date_to_operator} ?")
            params.append(normalized_date_to)

    async def get_stats(self: "MessagesRepository") -> dict:
        """Сводные счётчики для дашборда: аккаунты, каналы (всего/фильтр/трекинг), сообщения, запросы."""
        cur = await self._db.execute(
            "SELECT"
            " (SELECT COUNT(*) FROM accounts) AS accounts,"
            " (SELECT COUNT(*) FROM channels) AS channels,"
            " (SELECT COUNT(*) FROM channels WHERE is_filtered = 1) AS channels_filtered,"
            " (SELECT COUNT(*) FROM channels WHERE is_filtered = 0 AND is_active = 1) AS channels_tracked,"
            " (SELECT COUNT(*) FROM messages) AS messages,"
            " (SELECT COUNT(*) FROM messages WHERE collected_at >= datetime('now', '-24 hours')) AS messages_today,"
            " (SELECT COUNT(*) FROM search_queries) AS search_queries"
        )
        row = await cur.fetchone()
        if not row:
            return {
                "accounts": 0, "channels": 0, "channels_filtered": 0,
                "channels_tracked": 0, "messages": 0, "messages_today": 0, "search_queries": 0,
            }
        return {
            "accounts": row["accounts"],
            "channels": row["channels"],
            "channels_filtered": row["channels_filtered"],
            "channels_tracked": row["channels_tracked"],
            "messages": row["messages"],
            "messages_today": row["messages_today"],
            "search_queries": row["search_queries"],
        }

    async def get_trending_emojis(self: "MessagesRepository", limit: int = 10, days: int | None = None) -> list[dict]:
        """Return top emojis by total reaction count across all messages.

        Args:
            limit: Maximum number of emojis to return.
            days: If given, restrict to reactions on messages collected within
                  the last *days* days (based on messages.collected_at).

        Returns:
            List of ``{"emoji": str, "count": int}`` dicts ordered by count desc.
        """
        params: list = []
        where_extra = ""
        if days is not None:
            where_extra = " AND m.collected_at >= datetime('now', ?)"
            params.append(f"-{days} days")
        params.append(limit)
        cur = await self._db.execute(
            f"""
            SELECT mr.emoji, SUM(mr.count) AS total
            FROM message_reactions mr
            JOIN messages m ON mr.channel_id = m.channel_id AND mr.message_id = m.message_id
            LEFT JOIN channels c ON m.channel_id = c.channel_id
            WHERE (c.is_filtered IS NULL OR c.is_filtered = 0){where_extra}
            GROUP BY mr.emoji
            ORDER BY total DESC
            LIMIT ?
            """,
            tuple(params),
        )
        rows = await cur.fetchall()
        return [{"emoji": r["emoji"], "count": r["total"]} for r in rows]

    async def get_top_reacted_messages(
        self: "MessagesRepository",
        limit: int = 10,
        channel_id: int | None = None,
        days: int | None = None,
    ) -> list[Message]:
        """Return messages with the highest total reaction count.

        Args:
            limit: Maximum number of messages to return.
            channel_id: If given, restrict to this channel.
            days: If given, restrict to messages collected within the last
                  *days* days.

        Returns:
            List of :class:`Message` objects ordered by total reactions desc.
        """
        conditions: list[str] = ["(c.is_filtered IS NULL OR c.is_filtered = 0)"]
        params: list = []

        if channel_id is not None:
            conditions.append("mr.channel_id = ?")
            params.append(channel_id)
        if days is not None:
            conditions.append("m.collected_at >= datetime('now', ?)")
            params.append(f"-{days} days")

        where = " WHERE " + " AND ".join(conditions)
        cur = await self._db.execute(
            f"""
            SELECT m.*, c.title as channel_title, c.username as channel_username,
                   SUM(mr.count) AS total_reactions
            FROM message_reactions mr
            JOIN messages m ON mr.channel_id = m.channel_id AND mr.message_id = m.message_id
            LEFT JOIN channels c ON m.channel_id = c.channel_id
            {where}
            GROUP BY mr.channel_id, mr.message_id
            ORDER BY total_reactions DESC
            LIMIT ?
            """,
            (*params, limit),
        )
        rows = await cur.fetchall()
        return self._rows_to_messages(rows)

    async def get_top_messages(
        self: "MessagesRepository",
        limit: int = 50,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict]:
        """Return top messages sorted by total reaction count.

        Aggregates the normalized message_reactions table instead of running
        json_each over reactions_json of every messages row — the latter is a
        full-table scan that times out on multi-million-row databases (#826).
        """
        inner_conditions: list[str] = ["(c.is_filtered IS NULL OR c.is_filtered = 0)"]
        params: list = []
        date_conditions: list[str] = []
        self._append_analytics_date_filter(date_conditions, params, date_from, date_to)
        # Date bounds live on messages; join it inside the aggregate only when
        # a date filter is requested — the no-filter path stays index-only.
        messages_join = ""
        if date_conditions:
            messages_join = (
                " JOIN messages m ON m.channel_id = mr.channel_id AND m.message_id = mr.message_id"
            )
            inner_conditions.extend(date_conditions)
        where = " WHERE " + " AND ".join(inner_conditions)
        cur = await self._db.execute(
            f"""SELECT m.id, m.channel_id, m.message_id, m.text, m.media_type,
                       m.date, m.reactions_json,
                       c.title as channel_title, c.username as channel_username,
                       t.total_reactions
                FROM (SELECT mr.channel_id, mr.message_id, SUM(mr.count) AS total_reactions
                      FROM message_reactions mr{messages_join}
                      LEFT JOIN channels c ON mr.channel_id = c.channel_id
                      {where}
                      GROUP BY mr.channel_id, mr.message_id
                      ORDER BY total_reactions DESC
                      LIMIT ?) t
                JOIN messages m ON m.channel_id = t.channel_id AND m.message_id = t.message_id
                LEFT JOIN channels c ON m.channel_id = c.channel_id
                ORDER BY t.total_reactions DESC""",
            (*params, limit),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def _get_engagement_rows(
        self: "MessagesRepository",
        group_expr: str,
        group_alias: str,
        order_by: str,
        date_from: str | None,
        date_to: str | None,
    ) -> list[dict]:
        """Group messages by *group_expr*, averaging reactions over ALL messages
        of the group (zero-reaction messages included in the denominator).

        Runs two cheap aggregates — a plain COUNT over messages and a reaction
        sum over the normalized message_reactions table — instead of a
        json_each scan of every reactions_json (#826).
        """
        conditions: list[str] = ["(c.is_filtered IS NULL OR c.is_filtered = 0)"]
        params: list = []
        self._append_analytics_date_filter(conditions, params, date_from, date_to)
        where = " WHERE " + " AND ".join(conditions)
        count_cur = await self._db.execute(
            f"""SELECT {group_expr} as {group_alias}, COUNT(*) as message_count
                FROM messages m
                LEFT JOIN channels c ON m.channel_id = c.channel_id
                {where}
                GROUP BY {group_alias}
                ORDER BY {order_by}""",
            tuple(params),
        )
        counts = await count_cur.fetchall()
        sum_cur = await self._db.execute(
            f"""SELECT {group_expr} as {group_alias}, SUM(t.total_reactions) as reactions_sum
                FROM {self._REACTIONS_PER_MESSAGE_SQL} t
                JOIN messages m ON m.channel_id = t.channel_id AND m.message_id = t.message_id
                LEFT JOIN channels c ON m.channel_id = c.channel_id
                {where}
                GROUP BY {group_alias}""",
            tuple(params),
        )
        sums = {row[group_alias]: row["reactions_sum"] or 0 for row in await sum_cur.fetchall()}
        return [
            {
                group_alias: row[group_alias],
                "message_count": row["message_count"],
                "avg_reactions": (
                    sums.get(row[group_alias], 0) / row["message_count"]
                    if row["message_count"]
                    else 0.0
                ),
            }
            for row in counts
        ]

    async def get_engagement_by_media_type(
        self: "MessagesRepository",
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict]:
        """Return message count and avg reactions per content type."""
        return await self._get_engagement_rows(
            group_expr="COALESCE(m.media_type, 'text')",
            group_alias="content_type",
            order_by="message_count DESC",
            date_from=date_from,
            date_to=date_to,
        )

    async def get_hourly_activity(
        self: "MessagesRepository",
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict]:
        """Return message count and avg reactions per hour of day (0-23)."""
        return await self._get_engagement_rows(
            group_expr="CAST(strftime('%H', m.date) AS INTEGER)",
            group_alias="hour",
            order_by="hour",
            date_from=date_from,
            date_to=date_to,
        )

    async def get_language_stats(self: "MessagesRepository") -> list[tuple[str, int]]:
        """Return (lang_code, count) pairs for detected languages."""
        cur = await self._db.execute(
            """SELECT detected_lang, COUNT(*) AS cnt
               FROM messages
               WHERE detected_lang IS NOT NULL
               GROUP BY detected_lang
               ORDER BY cnt DESC"""
        )
        rows = await cur.fetchall()
        return [(r["detected_lang"], r["cnt"]) for r in rows]

    async def get_views_timeseries(
        self: "MessagesRepository", channel_id: int, days: int = 30
    ) -> list[dict]:
        """Daily average views and message count for a channel."""
        cur = await self._db.execute(
            """SELECT date(m.date) AS day,
                      COUNT(*) AS message_count,
                      COALESCE(AVG(m.views), 0) AS avg_views
               FROM messages m
               WHERE m.channel_id = ? AND m.date >= strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now', ?)
               GROUP BY day
               ORDER BY day ASC""",
            (channel_id, f"-{days} days"),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_post_frequency(
        self: "MessagesRepository", channel_id: int, days: int = 30
    ) -> list[dict]:
        """Daily post count for a channel."""
        cur = await self._db.execute(
            """SELECT date(m.date) AS day, COUNT(*) AS count
               FROM messages m
               WHERE m.channel_id = ? AND m.date >= strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now', ?)
               GROUP BY day
               ORDER BY day ASC""",
            (channel_id, f"-{days} days"),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_channel_message_count(
        self: "MessagesRepository", channel_id: int, days: int | None = None
    ) -> int:
        """Count messages for a channel, optionally within last N days."""
        if days is not None:
            cur = await self._db.execute(
                "SELECT COUNT(*) AS cnt FROM messages "
                # strftime with the same ISO-'T' format as the stored dates;
                # datetime('now') emits a space separator, so lexicographic
                # comparison wrongly included boundary-date messages (#1665).
                "WHERE channel_id = ? AND date >= strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now', ?)",
                (channel_id, f"-{days} days"),
            )
        else:
            cur = await self._db.execute(
                "SELECT COUNT(*) AS cnt FROM messages WHERE channel_id = ?",
                (channel_id,),
            )
        row = await cur.fetchone()
        return row["cnt"] if row else 0

    async def get_err_data(
        self: "MessagesRepository", channel_id: int, last_n: int = 20
    ) -> list[dict]:
        """Get engagement data for ERR calculation (last N posts)."""
        cur = await self._db.execute(
            """SELECT m.views, m.forwards, m.reply_count,
                      COALESCE((SELECT SUM(mr.count) FROM message_reactions mr
                                WHERE mr.channel_id = m.channel_id
                                AND mr.message_id = m.message_id), 0) AS total_reactions
               FROM messages m
               WHERE m.channel_id = ?
               ORDER BY m.date DESC
               LIMIT ?""",
            (channel_id, last_n),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_err24_data(self: "MessagesRepository", channel_id: int) -> list[dict]:
        """Get engagement data for ERR24 (posts from last 24h)."""
        cur = await self._db.execute(
            """SELECT m.views, m.forwards, m.reply_count,
                      COALESCE((SELECT SUM(mr.count) FROM message_reactions mr
                                WHERE mr.channel_id = m.channel_id
                                AND mr.message_id = m.message_id), 0) AS total_reactions
               FROM messages m
               WHERE m.channel_id = ? AND m.date >= strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now', '-1 day')
               ORDER BY m.date DESC""",
            (channel_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_citation_stats(self: "MessagesRepository", channel_id: int) -> dict:
        """Get forward-based citation stats for a channel."""
        cur = await self._db.execute(
            """SELECT COALESCE(SUM(m.forwards), 0) AS total_forwards,
                      COUNT(*) AS post_count,
                      COALESCE(AVG(COALESCE(m.forwards, 0)), 0) AS avg_forwards
               FROM messages m
               WHERE m.channel_id = ?""",
            (channel_id,),
        )
        row = await cur.fetchone()
        return dict(row) if row else {"total_forwards": 0, "post_count": 0, "avg_forwards": 0}

    async def get_hour_weekday_heatmap(
        self: "MessagesRepository", channel_id: int, days: int = 30,
    ) -> list[dict]:
        """Return message counts grouped by hour (0-23) x weekday (0=Sun..6=Sat).

        Each row has keys: ``hour`` (0-23), ``weekday`` (0=Sunday per SQLite ``%w``),
        ``count`` (messages in that slot).

        Uses ``strftime('%H', date)`` for hour and ``strftime('%w', date)`` for weekday.
        Only includes (hour, weekday) slots with at least one message; absent cells
        should be treated as 0 when rendering the heatmap grid.
        """
        cur = await self._db.execute(
            """SELECT CAST(strftime('%H', m.date) AS INTEGER) AS hour,
                      CAST(strftime('%w', m.date) AS INTEGER) AS weekday,
                      COUNT(*) AS count
               FROM messages m
               WHERE m.channel_id = ? AND m.date >= strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now', ?)
               GROUP BY hour, weekday
               ORDER BY weekday, hour""",
            (channel_id, f"-{days} days"),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_cross_channel_citations(
        self: "MessagesRepository", channel_id: int, days: int = 30, limit: int = 20,
    ) -> list[dict]:
        """Return channels whose messages are forwarded into *channel_id*.

        Each row: ``source_channel_id``, ``source_title``, ``source_username``,
        ``citation_count``, ``latest_date``.
        """
        cur = await self._db.execute(
            """SELECT m.forward_from_channel_id AS source_channel_id,
                      c.title AS source_title,
                      c.username AS source_username,
                      COUNT(*) AS citation_count,
                      MAX(m.date) AS latest_date
               FROM messages m
               LEFT JOIN channels c ON c.channel_id = m.forward_from_channel_id
               WHERE m.channel_id = ?
                 AND m.forward_from_channel_id IS NOT NULL
                 AND m.date >= strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now', ?)
               GROUP BY m.forward_from_channel_id
               ORDER BY citation_count DESC
               LIMIT ?""",
            (channel_id, f"-{days} days", limit),
        )
        return [dict(r) for r in await cur.fetchall()]
