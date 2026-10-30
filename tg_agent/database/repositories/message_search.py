"""Internal search methods sharing the MessagesRepository connection."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

from tg_agent.models import Message, SearchParams, SearchQuery
from tg_agent.utils.datetime import (
    parse_date_bound,
    parse_datetime_bound,
)
from tg_agent.utils.search_query_chat_filter import parse_chat_filter

if TYPE_CHECKING:
    from tg_agent.database.repositories.messages import MessagesRepository

logger = logging.getLogger("tg_agent.database.repositories.messages")



@dataclass(frozen=True)
class MessageSearchPage:
    """One page of search results without an exact total (#766).

    ``total`` is a lower bound — ``offset + len(messages)`` — exact only when
    ``has_more`` is False. The exact ``COUNT(*)`` it replaced took 2-19s (FTS)
    to 72s (LIKE full scan) on a 7M-row DB and starved WAL checkpoints, which
    cascaded into "database is locked" across background loops.

    ``__iter__`` keeps the legacy ``messages, total = ...`` unpacking working.
    """

    messages: list[Message]
    total: int
    has_more: bool = False

    def __iter__(self):
        yield self.messages
        yield self.total

def _normalize_date_to(date_to: str) -> tuple[str, str]:
    """Return SQL operator and upper bound for inclusive date filters.

    Date-only bounds normalize to an exclusive next-day bound; full ISO
    datetimes keep exact precision. Raises ValueError on non-date input — a
    raw string reaching the SQL comparison lexicographically mis-windows the
    query (#1593).
    """
    try:
        parsed = parse_date_bound(date_to)
    except ValueError:
        return "<=", parse_datetime_bound(date_to).isoformat()
    return "<", (parsed + timedelta(days=1)).isoformat()

def _normalize_username(username: str | None) -> str | None:
    if not username:
        return None
    cleaned = str(username).strip().lstrip("@")
    return cleaned or None

class MessageSearchMixin:
    """Used through MessagesRepository; owns no connection or transaction state."""

    def _build_message_filters(
        self: "MessagesRepository",
        *,
        channel_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        min_length: int | None = None,
        max_length: int | None = None,
        topic_id: int | None = None,
        include_filtered: bool = False,
    ) -> tuple[str, list]:
        conditions: list[str] = []
        if not include_filtered:
            conditions.append("(c.is_filtered IS NULL OR c.is_filtered = 0)")
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
            conditions.append("LENGTH(m.text) >= ?")
            params.append(min_length)
        if max_length is not None:
            conditions.append("LENGTH(m.text) <= ?")
            params.append(max_length)
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        return where, params

    async def _load_messages_by_ids(self: "MessagesRepository", message_ids: list[int]) -> list[Message]:
        if not message_ids:
            return []
        placeholders = ", ".join("?" for _ in message_ids)
        ordering = (
            "CASE "
            + " ".join(f"WHEN m.id = ? THEN {index}" for index, _ in enumerate(message_ids))
            + " END"
        )
        cur = await self._db.execute(
            f"""
            SELECT m.*, c.title as channel_title, c.username as channel_username
            FROM messages m
            LEFT JOIN channels c ON m.channel_id = c.channel_id
            WHERE m.id IN ({placeholders})
            ORDER BY {ordering}
            """,
            (*message_ids, *message_ids),
        )
        messages = self._rows_to_messages(await cur.fetchall())
        await self._attach_topic_titles(messages)
        return messages

    async def get_by_ids(self: "MessagesRepository", message_ids: list[int]) -> list[Message]:
        """Fetch ranked candidates in bounded batches, skipping deleted messages."""
        messages: list[Message] = []
        # Each ID uses two parameters in _load_messages_by_ids. Stay below
        # SQLite's portable 999-variable limit even on deep search pages.
        for start in range(0, len(message_ids), 400):
            messages.extend(await self._load_messages_by_ids(message_ids[start : start + 400]))
        return messages

    async def _attach_topic_titles(self: "MessagesRepository", messages: list[Message]) -> None:
        """Дозаполнить ``topic_title`` из кэша ``forum_topics`` (#1552).

        Один батч-SELECT на выдачу для всех (channel_id, topic_id) пар страницы;
        без топиков среди сообщений — ни одного запроса. Неизвестный/удалённый
        топик остаётся ``None``: заголовок — имя, выдача не ходит в сеть,
        кэш заполняет коллектор после сбора.
        """
        topic_ids = sorted({m.topic_id for m in messages if m.topic_id is not None})
        if not topic_ids:
            return
        channel_ids = sorted({m.channel_id for m in messages})
        ch_ph = ", ".join("?" for _ in channel_ids)
        tp_ph = ", ".join("?" for _ in topic_ids)
        cur = await self._db.execute(
            f"SELECT channel_id, topic_id, title FROM forum_topics"
            f" WHERE channel_id IN ({ch_ph}) AND topic_id IN ({tp_ph})",
            (*channel_ids, *topic_ids),
        )
        titles = {(r["channel_id"], r["topic_id"]): r["title"] for r in await cur.fetchall()}
        for m in messages:
            if m.topic_id is not None:
                m.topic_title = titles.get((m.channel_id, m.topic_id))

    async def _search_text_candidate_ids(
        self: "MessagesRepository",
        query: str,
        *,
        channel_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        is_fts: bool = False,
        min_length: int | None = None,
        max_length: int | None = None,
        topic_id: int | None = None,
        limit: int = 100,
        include_filtered: bool = False,
    ) -> list[int]:
        where, params = self._build_message_filters(
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            min_length=min_length,
            max_length=max_length,
            topic_id=topic_id,
            include_filtered=include_filtered,
        )

        channel_join = " LEFT JOIN channels c ON m.channel_id = c.channel_id"
        if self._fts_available:
            fts_query = self._build_fts_match(query, is_fts)
            fts_join = (
                " INNER JOIN (SELECT rowid FROM messages_fts"
                " WHERE messages_fts MATCH ?) AS fts ON m.id = fts.rowid"
            )
            cur = await self._db.execute(
                f"""
                SELECT m.id
                FROM messages m{fts_join}{channel_join}
                {where}
                ORDER BY m.date DESC
                LIMIT ?
                """,
                (fts_query, *params, limit),
            )
        else:
            logger.debug("FTS5 unavailable, falling back to LIKE search")
            params.append(f"%{query}%")
            like_where = (where + " AND m.text LIKE ?") if where else " WHERE m.text LIKE ?"
            cur = await self._db.execute(
                f"""
                SELECT m.id
                FROM messages m{channel_join}
                {like_where}
                ORDER BY m.date DESC
                LIMIT ?
                """,
                (*params, limit),
            )
        rows = await cur.fetchall()
        return [int(row["id"]) for row in rows]

    async def search_messages(self: "MessagesRepository", params: SearchParams) -> MessageSearchPage:
        """Полнотекстовый поиск (FTS5, при недоступности — LIKE) по фильтрам
        [`SearchParams`][tg_agent.models.SearchParams].

        Возвращает [`MessageSearchPage`][tg_agent.database.repositories.messages.MessageSearchPage]
        с нижней оценкой total (limit+1-проба вместо точного COUNT, #766), новые
        первыми. Отфильтрованные каналы исключаются, если не задан ``include_filtered``.
        """
        query = params.query
        limit = params.limit
        offset = params.offset
        where, sql_params = self._build_message_filters(
            channel_id=params.channel_id,
            date_from=params.date_from,
            date_to=params.date_to,
            min_length=params.min_length,
            max_length=params.max_length,
            topic_id=params.topic_id,
            include_filtered=params.include_filtered,
        )
        channel_join = " LEFT JOIN channels c ON m.channel_id = c.channel_id"

        # LIMIT limit+1 probes for a next page instead of an exact COUNT(*) —
        # see MessageSearchPage for why the COUNT had to go (#766).
        probe_limit = limit + 1

        if query:
            if self._fts_available:
                fts_query = self._build_fts_match(query, params.is_fts)
                fts_join = (
                    " INNER JOIN (SELECT rowid FROM messages_fts"
                    " WHERE messages_fts MATCH ?) AS fts ON m.id = fts.rowid"
                )
                from_where = f"FROM messages m{fts_join}{channel_join} {where}"
                row_params: tuple = (fts_query, *sql_params)
            else:
                logger.debug("FTS5 unavailable, falling back to LIKE search")
                sql_params.append(f"%{query}%")
                like_where = (where + " AND m.text LIKE ?") if where else " WHERE m.text LIKE ?"
                from_where = f"FROM messages m{channel_join} {like_where}"
                row_params = (*sql_params,)
        else:
            from_where = f"FROM messages m{channel_join} {where}"
            row_params = (*sql_params,)

        cur = await self._db.execute(
            f"""SELECT m.*, c.title as channel_title, c.username as channel_username
                {from_where}
                ORDER BY m.date DESC
                LIMIT ? OFFSET ?""",
            (*row_params, probe_limit, offset),
        )

        rows = await cur.fetchall()
        has_more = len(rows) > limit
        messages = self._rows_to_messages(rows[:limit])
        await self._attach_topic_titles(messages)
        total = offset + len(messages)
        # `offset + len(messages)` is exact for any non-empty page and for has_more
        # pages, but undershoots/overshoots when offset is past the end (empty page):
        # the contract promises an exact total when has_more is False, so fall back
        # to a real COUNT(*) for that rare overflow case only (#971).
        if not has_more and not messages and offset > 0:
            count_cur = await self._db.execute(f"SELECT COUNT(*) AS cnt {from_where}", row_params)
            count_row = await count_cur.fetchone()
            total = count_row["cnt"] if count_row else 0
        return MessageSearchPage(
            messages=messages,
            total=total,
            has_more=has_more,
        )

    async def get_channel_messages_for_export(
        self: "MessagesRepository",
        channel_id: int,
        *,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 5000,
        offset: int = 0,
    ) -> MessageSearchPage:
        """Oldest-first page of a channel's messages for Telegram-Desktop export (#834).

        Telegram Desktop exports from the start of the channel, so this orders by
        ``message_id ASC`` (monotonic per channel) — unlike ``search_messages``
        which is newest-first. ``has_more`` (limit+1 probe) lets the caller flag a
        truncated export instead of silently dropping the newest messages.
        """
        where, sql_params = self._build_message_filters(
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            include_filtered=True,
        )
        channel_join = " LEFT JOIN channels c ON m.channel_id = c.channel_id"
        probe_limit = limit + 1
        cur = await self._db.execute(
            f"""SELECT m.*, c.title as channel_title, c.username as channel_username
                FROM messages m{channel_join}
                {where}
                ORDER BY m.message_id ASC
                LIMIT ? OFFSET ?""",
            (*sql_params, probe_limit, offset),
        )
        rows = await cur.fetchall()
        has_more = len(rows) > limit
        messages = self._rows_to_messages(rows[:limit])
        return MessageSearchPage(
            messages=messages,
            total=offset + len(messages),
            has_more=has_more,
        )

    async def _search_semantic_candidates(
        self: "MessagesRepository",
        query_embedding: list[float],
        *,
        channel_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        min_length: int | None = None,
        max_length: int | None = None,
        topic_id: int | None = None,
        limit: int = 100,
        max_candidates: int = 50_000,
        include_filtered: bool = False,
    ) -> list[tuple[int, float]]:
        dimensions = await self.get_embedding_dimensions()
        if dimensions is None:
            return []
        if len(query_embedding) != dimensions:
            raise RuntimeError(
                "Query embedding dimensions "
                f"{len(query_embedding)} do not match index {dimensions}."
            )
        where, params = self._build_message_filters(
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            min_length=min_length,
            max_length=max_length,
            topic_id=topic_id,
            include_filtered=include_filtered,
        )
        where_clause = where.replace(" WHERE ", "WHERE ", 1) if where else ""
        cur = await self._db.execute(
            f"""
            SELECT e.message_id, e.embedding
            FROM message_embeddings e
            JOIN messages m ON m.id = e.message_id
            LEFT JOIN channels c ON m.channel_id = c.channel_id
            {where_clause}
            LIMIT ?
            """,
            (*params, max_candidates),
        )
        rows = await cur.fetchall()
        if not rows:
            return []

        ids = [int(row["message_id"]) for row in rows]
        try:
            import numpy as np
            from sklearn.neighbors import NearestNeighbors
        except ImportError as exc:
            raise RuntimeError(
                "numpy and scikit-learn are required for semantic search fallback."
            ) from exc

        matrix = np.array([np.frombuffer(row["embedding"], dtype=np.float32) for row in rows])
        query_vec = np.array([query_embedding], dtype=np.float32)

        k = min(limit, len(ids))
        if k <= 0:
            return []
        index = NearestNeighbors(metric="cosine", n_neighbors=k)
        index.fit(matrix)
        distances, indices = index.kneighbors(query_vec)
        return [(ids[int(idx)], float(distance)) for distance, idx in zip(distances[0], indices[0], strict=False)]

    async def search_semantic_messages(
        self: "MessagesRepository",
        query_embedding: list[float],
        *,
        channel_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
        min_length: int | None = None,
        max_length: int | None = None,
        topic_id: int | None = None,
        candidate_limit: int | None = None,
        include_filtered: bool = False,
    ) -> tuple[list[Message], int]:
        """Семантический поиск по эмбеддингу запроса (косинусная близость); вернуть ``(страница, всего кандидатов)``.

        ``candidate_limit`` ограничивает размер пула кандидатов до ранжирования;
        страница вырезается ``offset``/``limit`` уже из отсортированного списка.
        """
        fetch_limit = candidate_limit or max(offset + limit, 50)
        candidates = await self._search_semantic_candidates(
            query_embedding,
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            min_length=min_length,
            max_length=max_length,
            topic_id=topic_id,
            limit=fetch_limit,
            include_filtered=include_filtered,
        )
        page_ids = [message_id for message_id, _distance in candidates[offset : offset + limit]]
        messages = await self._load_messages_by_ids(page_ids)
        return messages, len(candidates)

    async def search_hybrid_messages(
        self: "MessagesRepository",
        query: str,
        query_embedding: list[float],
        *,
        channel_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
        is_fts: bool = False,
        min_length: int | None = None,
        max_length: int | None = None,
        topic_id: int | None = None,
        candidate_limit: int | None = None,
        rrf_k: int = 60,
        include_filtered: bool = False,
    ) -> tuple[list[Message], int]:
        """Гибридный поиск: объединяет текстовых и семантических кандидатов через Reciprocal Rank Fusion.

        ``rrf_k`` — сглаживающая константа RRF (вклад позиции = 1/(rrf_k+rank)).
        Возвращает ``(страница сообщений, число уникальных кандидатов)``.
        """
        fetch_limit = candidate_limit or max(offset + limit, 50)
        text_ids = await self._search_text_candidate_ids(
            query,
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            is_fts=is_fts,
            min_length=min_length,
            max_length=max_length,
            topic_id=topic_id,
            limit=fetch_limit,
            include_filtered=include_filtered,
        )
        semantic_candidates = await self._search_semantic_candidates(
            query_embedding,
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            min_length=min_length,
            max_length=max_length,
            topic_id=topic_id,
            limit=fetch_limit,
            include_filtered=include_filtered,
        )
        if not text_ids and not semantic_candidates:
            return [], 0
        fused_scores: dict[int, float] = {}
        for rank, message_id in enumerate(text_ids, start=1):
            fused_scores[message_id] = fused_scores.get(message_id, 0.0) + (1.0 / (rrf_k + rank))
        for rank, (message_id, _distance) in enumerate(semantic_candidates, start=1):
            fused_scores[message_id] = fused_scores.get(message_id, 0.0) + (1.0 / (rrf_k + rank))
        ranked_ids = [
            message_id
            for message_id, _score in sorted(
                fused_scores.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ]
        page_ids = ranked_ids[offset : offset + limit]
        messages = await self._load_messages_by_ids(page_ids)
        return messages, len(ranked_ids)

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
        chat_filter = parse_chat_filter(sq.chat_filter)
        if chat_filter.has_filter:
            chat_parts = []
            if chat_filter.numeric_values:
                placeholders = ", ".join("?" for _ in chat_filter.numeric_values)
                chat_parts.append(f"m.channel_id IN ({placeholders})")
                params.extend(chat_filter.numeric_values)
                chat_parts.append(f"c.id IN ({placeholders})")
                params.extend(chat_filter.numeric_values)
            if chat_filter.usernames:
                placeholders = ", ".join("?" for _ in chat_filter.usernames)
                chat_parts.append(f"LOWER(c.username) IN ({placeholders})")
                params.extend(chat_filter.usernames)
            if chat_parts:
                conditions.append("(" + " OR ".join(chat_parts) + ")")
            else:
                conditions.append("0 = 1")
        return conditions, params

    def _build_sq_parts(self: "MessagesRepository", sq: SearchQuery) -> tuple[str, list[str], list]:
        """Build FTS match and WHERE conditions from a SearchQuery.

        Single source of truth for SearchQuery → SQL. All *_for_query methods
        must use this instead of calling _build_fts_match/_build_extra_conditions
        directly.

        Returns (fts_query, extra_conditions, extra_params).
        """
        fts_query = self._build_fts_match(sq.query, sq.is_fts)
        extra_conds, extra_params = self._build_extra_conditions(sq)
        return fts_query, extra_conds, extra_params

    def _require_fts(self: "MessagesRepository") -> None:
        if not self._fts_available:
            raise RuntimeError(
                "FTS5 full-text search is unavailable (index build failed at startup)."
            )

    async def search_messages_for_query(
        self: "MessagesRepository",
        sq: SearchQuery,
        limit: int = 1,
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

    async def search_messages_for_query_since(
        self: "MessagesRepository",
        sq: SearchQuery,
        since: str,
        limit: int = 3,
    ) -> tuple[list[Message], int]:
        """Count + preview matches for sq among messages collected since `since`."""
        self._require_fts()
        fts_query, extra_conds, extra_params = self._build_sq_parts(sq)
        where_parts = [self._BASE_FILTER, "m.collected_at >= ?", *extra_conds]
        where_clause = " AND ".join(where_parts)
        params = (fts_query, since, *extra_params)

        count_cur = await self._db.execute(
            f"SELECT COUNT(*) AS cnt FROM messages m"
            f"{self._FTS_JOIN}{self._CHANNEL_JOIN}"
            f" WHERE {where_clause}",
            params,
        )
        row = await count_cur.fetchone()
        total = row["cnt"] if row else 0

        cur = await self._db.execute(
            f"SELECT m.*, c.title as channel_title, c.username as channel_username"
            f" FROM messages m{self._FTS_JOIN}{self._CHANNEL_JOIN}"
            f" WHERE {where_clause}"
            f" ORDER BY m.date DESC LIMIT ?",
            (*params, limit),
        )
        rows = await cur.fetchall()
        messages = self._rows_to_messages(rows)
        return messages, total

    async def count_fts_matches_for_query(self: "MessagesRepository", sq: SearchQuery) -> int:
        """Точное число FTS-совпадений сохранённого запроса ``sq`` (учитывает его фильтры/исключения)."""
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

    async def get_fts_daily_stats_for_query(self: "MessagesRepository", sq: SearchQuery, days: int = 30) -> list:
        """Ряд «день → число совпадений» сохранённого запроса за последние ``days`` дней (по дате сообщений)."""
        self._require_fts()
        from tg_agent.models import SearchQueryDailyStat

        fts_query, extra_conds, extra_params = self._build_sq_parts(sq)
        where_parts = [
            self._BASE_FILTER,
            # ISO-'T' boundary to match the stored dates (#1665); datetime('now')
            # emits a space separator and wrongly includes boundary-date rows.
            "m.date >= strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now', ?)",
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
        self: "MessagesRepository", queries: list[SearchQuery], days: int = 30
    ) -> dict[int, list]:
        """Дневная статистика сразу для многих запросов: карта ``sq_id → [SearchQueryDailyStat]`` за ``days`` дней.

        Считает UNION ALL чанками (≤100 запросов на чанк, лимит SQLite), чтобы не
        делать по запросу на каждый ``sq``.
        """
        self._require_fts()
        from tg_agent.models import SearchQueryDailyStat

        result: dict[int, list] = {}
        if not queries:
            return result

        # SQLite SQLITE_MAX_COMPOUND_SELECT defaults to 500; chunk to stay well under it
        _chunk = 100
        valid = [sq for sq in queries if sq.id is not None]
        for chunk_start in range(0, len(valid), _chunk):
            chunk = valid[chunk_start : chunk_start + _chunk]
            union_parts = []
            all_params: list = []
            for sq in chunk:
                assert sq.id is not None  # ``valid`` filtered out None ids above
                fts_query = self._build_fts_match(sq.query, sq.is_fts)
                extra_conds, extra_params = self._build_extra_conditions(sq)
                where_parts = [
                    "(c.is_filtered IS NULL OR c.is_filtered = 0)",
                    # ISO-'T' boundary to match the stored dates (#1665).
                    "m.date >= strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now', ?)",
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
                result[r["sq_id"]].append(SearchQueryDailyStat(day=r["day"], count=r["count"]))
        return result
