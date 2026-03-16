from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timedelta

import aiosqlite

from src.models import Message, SearchQuery

logger = logging.getLogger(__name__)
_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_reactions_json(reactions_json: str) -> list[dict]:
    """Parse reactions_json string into a list of {emoji, count} dicts."""
    try:
        items = json.loads(reactions_json)
        return [r for r in items if isinstance(r, dict) and "emoji" in r]
    except (json.JSONDecodeError, TypeError):
        return []


def _normalize_date_to(date_to: str) -> tuple[str, str]:
    """Return SQL operator and upper bound for inclusive day filters."""
    try:
        parsed = date.fromisoformat(date_to)
    except ValueError:
        return "<=", date_to
    return "<", (parsed + timedelta(days=1)).isoformat()


class MessagesRepository:
    def __init__(self, db: aiosqlite.Connection, *, fts_available: bool = True):
        self._db = db
        self._fts_available = fts_available

    @staticmethod
    def _normalize_date_from(value: str | None) -> str | None:
        if not value:
            return None
        return value

    @staticmethod
    def _normalize_date_to(value: str | None) -> tuple[str | None, str]:
        if not value:
            return None, "<="
        if _DATE_ONLY_RE.fullmatch(value):
            next_day = date.fromisoformat(value) + timedelta(days=1)
            return next_day.isoformat(), "<"
        return value, "<="

    async def insert_message(self, msg: Message) -> bool:
        try:
            cur = await self._db.execute(
                """INSERT OR IGNORE INTO messages
                   (channel_id, message_id, sender_id, sender_name,
                    text, media_type, topic_id, reactions_json, date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    msg.channel_id,
                    msg.message_id,
                    msg.sender_id,
                    msg.sender_name,
                    msg.text,
                    msg.media_type,
                    msg.topic_id,
                    msg.reactions_json,
                    msg.date.isoformat(),
                ),
            )
            await self._db.commit()
            inserted = cur.rowcount > 0
            if inserted and msg.reactions_json:
                await self._upsert_reactions(msg.channel_id, msg.message_id, msg.reactions_json)
            return inserted
        except Exception:
            logger.exception(
                "Failed to insert message channel_id=%s message_id=%s",
                msg.channel_id,
                msg.message_id,
            )
            return False

    async def _upsert_reactions(
        self, channel_id: int, message_id: int, reactions_json: str
    ) -> None:
        """Parse reactions_json and upsert rows into message_reactions."""
        items = _parse_reactions_json(reactions_json)
        if not items:
            return
        data = [(channel_id, message_id, r["emoji"], r.get("count", 0)) for r in items]
        try:
            await self._db.executemany(
                """INSERT OR REPLACE INTO message_reactions
                   (channel_id, message_id, emoji, count) VALUES (?, ?, ?, ?)""",
                data,
            )
            await self._db.commit()
        except Exception:
            logger.exception(
                "Failed to upsert reactions for channel_id=%s message_id=%s",
                channel_id,
                message_id,
            )

    async def insert_messages_batch(self, messages: list[Message]) -> int:
        if not messages:
            return 0
        data = [
            (
                m.channel_id,
                m.message_id,
                m.sender_id,
                m.sender_name,
                m.text,
                m.media_type,
                m.topic_id,
                m.reactions_json,
                m.date.isoformat(),
            )
            for m in messages
        ]
        try:
            cur = await self._db.executemany(
                """INSERT OR IGNORE INTO messages
                   (channel_id, message_id, sender_id, sender_name,
                    text, media_type, topic_id, reactions_json, date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                data,
            )
            await self._db.commit()
            count = cur.rowcount if cur.rowcount >= 0 else len(messages)
        except Exception as exc:
            logger.error("Failed to insert batch of %d messages: %s", len(messages), exc)
            return 0

        reactions_data = [
            (m.channel_id, m.message_id, r["emoji"], r.get("count", 0))
            for m in messages
            if m.reactions_json
            for r in _parse_reactions_json(m.reactions_json)
        ]
        if reactions_data:
            try:
                await self._db.executemany(
                    """INSERT OR REPLACE INTO message_reactions
                       (channel_id, message_id, emoji, count) VALUES (?, ?, ?, ?)""",
                    reactions_data,
                )
                await self._db.commit()
            except Exception as exc:
                logger.error("Failed to upsert reactions for batch: %s", exc)

        return count

    async def search_messages(
        self,
        query: str = "",
        channel_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
        is_fts: bool = False,
        min_length: int | None = None,
        max_length: int | None = None,
        topic_id: int | None = None,
    ) -> tuple[list[Message], int]:
        # Exclude messages from filtered channels; allow messages whose channel
        # is not yet in the channels table (NULL join) for backward-compat.
        conditions: list[str] = ["(c.is_filtered IS NULL OR c.is_filtered = 0)"]
        params: list = []

        if channel_id:
            conditions.append("m.channel_id = ?")
            params.append(channel_id)
        if topic_id is not None:
            conditions.append("m.topic_id = ?")
            params.append(topic_id)
        normalized_date_from = self._normalize_date_from(date_from)
        normalized_date_to, date_to_operator = self._normalize_date_to(date_to)

        if normalized_date_from:
            conditions.append("m.date >= ?")
            params.append(normalized_date_from)
        if normalized_date_to:
            conditions.append(f"m.date {date_to_operator} ?")
            params.append(normalized_date_to)

        if min_length is not None:
            conditions.append("LENGTH(m.text) > ?")
            params.append(min_length)
        if max_length is not None:
            conditions.append("LENGTH(m.text) < ?")
            params.append(max_length)

        channel_join = " LEFT JOIN channels c ON m.channel_id = c.channel_id"
        where = " WHERE " + " AND ".join(conditions)

        if query:
            if self._fts_available:
                fts_query = self._build_fts_match(query, is_fts)
                fts_join = (
                    " INNER JOIN (SELECT rowid FROM messages_fts"
                    " WHERE messages_fts MATCH ?) AS fts ON m.id = fts.rowid"
                )
                count_cur = await self._db.execute(
                    f"SELECT COUNT(*) as cnt FROM messages m{fts_join}{channel_join}{where}",
                    (fts_query, *params),
                )
                row = await count_cur.fetchone()
                total = row["cnt"] if row else 0

                cur = await self._db.execute(
                    f"""SELECT m.*, c.title as channel_title, c.username as channel_username
                        FROM messages m{fts_join}{channel_join}
                        {where}
                        ORDER BY m.date DESC
                        LIMIT ? OFFSET ?""",
                    (fts_query, *params, limit, offset),
                )
            else:
                logger.debug("FTS5 unavailable, falling back to LIKE search")
                conditions.append("m.text LIKE ?")
                params.append(f"%{query}%")
                where = " WHERE " + " AND ".join(conditions)

                count_cur = await self._db.execute(
                    f"SELECT COUNT(*) as cnt FROM messages m{channel_join}{where}",
                    tuple(params),
                )
                row = await count_cur.fetchone()
                total = row["cnt"] if row else 0

                cur = await self._db.execute(
                    f"""SELECT m.*, c.title as channel_title, c.username as channel_username
                        FROM messages m{channel_join}
                        {where}
                        ORDER BY m.date DESC
                        LIMIT ? OFFSET ?""",
                    (*params, limit, offset),
                )
        else:
            count_cur = await self._db.execute(
                f"SELECT COUNT(*) as cnt FROM messages m{channel_join}{where}", tuple(params)
            )
            row = await count_cur.fetchone()
            total = row["cnt"] if row else 0

            cur = await self._db.execute(
                f"""SELECT m.*, c.title as channel_title, c.username as channel_username
                    FROM messages m{channel_join}
                    {where}
                    ORDER BY m.date DESC
                    LIMIT ? OFFSET ?""",
                (*params, limit, offset),
            )

        rows = await cur.fetchall()
        return self._rows_to_messages(rows), total

    @staticmethod
    def _rows_to_messages(rows) -> list[Message]:
        return [
            Message(
                id=r["id"],
                channel_id=r["channel_id"],
                message_id=r["message_id"],
                sender_id=r["sender_id"],
                sender_name=r["sender_name"],
                text=r["text"],
                media_type=r["media_type"],
                topic_id=r["topic_id"],
                reactions_json=r["reactions_json"],
                date=datetime.fromisoformat(r["date"]),
                collected_at=(
                    datetime.fromisoformat(r["collected_at"]) if r["collected_at"] else None
                ),
                channel_title=r["channel_title"],
                channel_username=r["channel_username"],
            )
            for r in rows
        ]

    @staticmethod
    def _build_fts_match(query: str, is_fts: bool) -> str:
        if is_fts:
            return query
        return '"' + query.replace('"', '""') + '"'

    @staticmethod
    def _build_extra_conditions(sq: SearchQuery) -> tuple[list[str], list]:
        conditions: list[str] = []
        params: list = []
        if sq.max_length is not None:
            conditions.append("LENGTH(m.text) < ?")
            params.append(sq.max_length)
        for pat in sq.exclude_patterns_list:
            stripped = pat.rstrip("*")
            if not stripped:
                continue
            conditions.append("m.text NOT LIKE ?")
            params.append(f"%{stripped}%")
        return conditions, params

    def _build_sq_parts(self, sq: SearchQuery) -> tuple[str, list[str], list]:
        """Build FTS match and WHERE conditions from a SearchQuery.

        Single source of truth for SearchQuery → SQL. All *_for_query methods
        must use this instead of calling _build_fts_match/_build_extra_conditions
        directly.

        Returns (fts_query, extra_conditions, extra_params).
        """
        fts_query = self._build_fts_match(sq.query, sq.is_fts)
        extra_conds, extra_params = self._build_extra_conditions(sq)
        return fts_query, extra_conds, extra_params

    def _require_fts(self) -> None:
        if not self._fts_available:
            raise RuntimeError(
                "FTS5 full-text search is unavailable (index build failed at startup)."
            )

    _FTS_JOIN = (
        " INNER JOIN (SELECT rowid FROM messages_fts"
        " WHERE messages_fts MATCH ?) AS fts ON m.id = fts.rowid"
    )
    _CHANNEL_JOIN = " LEFT JOIN channels c ON m.channel_id = c.channel_id"
    _BASE_FILTER = "(c.is_filtered IS NULL OR c.is_filtered = 0)"

    async def search_messages_for_query(
        self, sq: SearchQuery, limit: int = 1,
    ) -> tuple[list[Message], int]:
        """Search messages using all SearchQuery parameters."""
        self._require_fts()
        fts_query, extra_conds, extra_params = self._build_sq_parts(sq)
        where_parts = [self._BASE_FILTER, *extra_conds]
        where_clause = " AND ".join(where_parts)

        count_cur = await self._db.execute(
            f"SELECT COUNT(*) AS cnt FROM messages m"
            f"{self._FTS_JOIN}{self._CHANNEL_JOIN}"
            f" WHERE {where_clause}",
            (fts_query, *extra_params),
        )
        row = await count_cur.fetchone()
        total = row["cnt"] if row else 0

        cur = await self._db.execute(
            f"SELECT m.*, c.title as channel_title, c.username as channel_username"
            f" FROM messages m{self._FTS_JOIN}{self._CHANNEL_JOIN}"
            f" WHERE {where_clause}"
            f" ORDER BY m.date DESC LIMIT ?",
            (fts_query, *extra_params, limit),
        )
        rows = await cur.fetchall()
        messages = self._rows_to_messages(rows)
        return messages, total

    async def count_fts_matches_for_query(self, sq: SearchQuery) -> int:
        self._require_fts()
        fts_query, extra_conds, extra_params = self._build_sq_parts(sq)
        where_parts = [self._BASE_FILTER, *extra_conds]
        where_clause = " AND ".join(where_parts)
        cur = await self._db.execute(
            f"SELECT COUNT(*) AS cnt FROM messages m"
            f"{self._FTS_JOIN}{self._CHANNEL_JOIN}"
            f" WHERE {where_clause}",
            (fts_query, *extra_params),
        )
        row = await cur.fetchone()
        return row["cnt"] if row else 0

    async def get_fts_daily_stats_for_query(
        self, sq: SearchQuery, days: int = 30
    ) -> list:
        self._require_fts()
        from src.models import SearchQueryDailyStat

        fts_query, extra_conds, extra_params = self._build_sq_parts(sq)
        where_parts = [
            self._BASE_FILTER,
            "m.date >= datetime('now', ?)",
            *extra_conds,
        ]
        where_clause = " AND ".join(where_parts)
        cur = await self._db.execute(
            f"""
            SELECT date(m.date) AS day, COUNT(*) AS count
            FROM messages m{self._FTS_JOIN}{self._CHANNEL_JOIN}
            WHERE {where_clause}
            GROUP BY date(m.date)
            ORDER BY day
            """,
            (fts_query, f"-{days} days", *extra_params),
        )
        rows = await cur.fetchall()
        return [SearchQueryDailyStat(day=r["day"], count=r["count"]) for r in rows]

    async def get_fts_daily_stats_batch(
        self, queries: list[SearchQuery], days: int = 30
    ) -> dict[int, list]:
        self._require_fts()
        from src.models import SearchQueryDailyStat

        result: dict[int, list] = {}
        if not queries:
            return result

        # SQLite SQLITE_MAX_COMPOUND_SELECT defaults to 500; chunk to stay well under it
        _chunk = 100
        valid = [sq for sq in queries if sq.id is not None]
        for chunk_start in range(0, len(valid), _chunk):
            chunk = valid[chunk_start:chunk_start + _chunk]
            union_parts = []
            all_params: list = []
            for sq in chunk:
                fts_query = self._build_fts_match(sq.query, sq.is_fts)
                extra_conds, extra_params = self._build_extra_conditions(sq)
                where_parts = [
                    "(c.is_filtered IS NULL OR c.is_filtered = 0)",
                    "m.date >= datetime('now', ?)",
                ]
                where_parts.extend(extra_conds)
                where_clause = " AND ".join(where_parts)
                union_parts.append(
                    f"SELECT ? AS sq_id, date(m.date) AS day, COUNT(*) AS count"
                    f" FROM messages m"
                    f" INNER JOIN (SELECT rowid FROM messages_fts"
                    f" WHERE messages_fts MATCH ?) AS fts ON m.id = fts.rowid"
                    f" LEFT JOIN channels c ON m.channel_id = c.channel_id"
                    f" WHERE {where_clause}"
                    f" GROUP BY date(m.date)"
                )
                all_params.extend([sq.id, fts_query, f"-{days} days", *extra_params])
                result[sq.id] = []
            sql = " UNION ALL ".join(union_parts) + " ORDER BY sq_id, day"
            cur = await self._db.execute(sql, all_params)
            rows = await cur.fetchall()
            for r in rows:
                result[r["sq_id"]].append(
                    SearchQueryDailyStat(day=r["day"], count=r["count"])
                )
        return result

    async def delete_messages_for_channel(self, channel_id: int) -> int:
        cur = await self._db.execute(
            "DELETE FROM messages WHERE channel_id = ?", (channel_id,)
        )
        await self._db.commit()
        return cur.rowcount or 0

    async def get_stats(self) -> dict:
        stats: dict[str, int] = {}
        queries = {
            "accounts": "SELECT COUNT(*) as cnt FROM accounts",
            "channels": "SELECT COUNT(*) as cnt FROM channels",
            "messages": "SELECT COUNT(*) as cnt FROM messages",
            "search_queries": "SELECT COUNT(*) as cnt FROM search_queries",
        }
        for table, sql in queries.items():
            cur = await self._db.execute(sql)
            row = await cur.fetchone()
            stats[table] = row["cnt"] if row else 0
        return stats

    async def get_trending_emojis(
        self, limit: int = 10, days: int | None = None
    ) -> list[dict]:
        """Return top emojis by total reaction count across all messages.

        Args:
            limit: Maximum number of emojis to return.
            days: If given, restrict to reactions on messages collected within
                  the last *days* days (based on messages.collected_at).

        Returns:
            List of ``{"emoji": str, "count": int}`` dicts ordered by count desc.
        """
        if days is not None:
            cur = await self._db.execute(
                """
                SELECT mr.emoji, SUM(mr.count) AS total
                FROM message_reactions mr
                JOIN messages m ON mr.channel_id = m.channel_id AND mr.message_id = m.message_id
                WHERE m.collected_at >= datetime('now', ?)
                GROUP BY mr.emoji
                ORDER BY total DESC
                LIMIT ?
                """,
                (f"-{days} days", limit),
            )
        else:
            cur = await self._db.execute(
                """
                SELECT emoji, SUM(count) AS total
                FROM message_reactions
                GROUP BY emoji
                ORDER BY total DESC
                LIMIT ?
                """,
                (limit,),
            )
        rows = await cur.fetchall()
        return [{"emoji": r["emoji"], "count": r["total"]} for r in rows]

    async def get_top_reacted_messages(
        self,
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
        conditions: list[str] = []
        params: list = []

        if channel_id is not None:
            conditions.append("mr.channel_id = ?")
            params.append(channel_id)
        if days is not None:
            conditions.append("m.collected_at >= datetime('now', ?)")
            params.append(f"-{days} days")

        where = " WHERE " + " AND ".join(conditions) if conditions else ""
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
