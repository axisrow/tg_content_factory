"""Репозиторий сообщений: вставка, поиск (FTS/семантика/гибрид) и аналитика.

Доступ через `db.repos.messages`. Самый горячий репозиторий проекта: пакетная
вставка с дедупликацией по ``UNIQUE(channel_id, message_id)``, обновление
изменяемых полей (реакции/просмотры) уже известных сообщений, три режима поиска
(полнотекстовый FTS5 с LIKE-фолбэком, семантический по эмбеддингам, гибридный
RRF) и агрегаты для аналитики по нормализованной таблице ``message_reactions``.

Замечания по производительности: поиск отдаёт страницу с нижней оценкой total
вместо точного ``COUNT(*)`` (см.
[`MessageSearchPage`][src.database.repositories.messages.MessageSearchPage], #766);
фильтр отсекает отфильтрованные каналы JOIN-ом на ``channels`` по ``channel_id``
(конвенция ключей, CLAUDE.md).
"""

from __future__ import annotations

import json
import logging
import re
import struct
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING, AsyncIterator

import aiosqlite

from src.models import Message, SearchParams, SearchQuery
from src.telegram.reactions import parse_reactions_json
from src.utils.datetime import parse_datetime, parse_required_datetime
from src.utils.search_query_chat_filter import parse_chat_filter

if TYPE_CHECKING:
    from src.database.facade import Database

logger = logging.getLogger(__name__)
_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_EMBEDDING_DIMENSIONS_SETTING = "semantic_embedding_dimensions"


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

def _parse_reactions_json(reactions_json: str) -> list[dict]:
    """Parse reactions_json string into a list of {emoji, count} dicts."""
    return parse_reactions_json(reactions_json)


def _normalize_date_to(date_to: str) -> tuple[str, str]:
    """Return SQL operator and upper bound for inclusive day filters."""
    try:
        parsed = date.fromisoformat(date_to)
    except ValueError:
        return "<=", date_to
    return "<", (parsed + timedelta(days=1)).isoformat()


def _normalize_username(username: str | None) -> str | None:
    if not username:
        return None
    cleaned = str(username).strip().lstrip("@")
    return cleaned or None


class MessagesRepository:
    """Вставка, поиск и аналитика сообщений (`messages` + сайдкары реакций/эмбеддингов)."""

    def __init__(
        self,
        db: aiosqlite.Connection,
        *,
        fts_available: bool = True,
        database: "Database | None" = None,
    ):
        self._db = db
        self._fts_available = fts_available
        self._database = database

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

    def _append_analytics_date_filter(
        self,
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

    async def _get_setting(self, key: str) -> str | None:
        cur = await self._db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cur.fetchone()
        return row["value"] if row else None

    async def _set_setting(self, key: str, value: str) -> None:
        assert self._database is not None, (
            "MessagesRepository._set_setting requires a Database reference"
        )
        await self._database.execute_write(
            """
            INSERT INTO settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )

    async def get_embedding_dimensions(self) -> int | None:
        """Размерность векторов индекса эмбеддингов (из настроек), либо ``None`` если индекс ещё не создан."""
        raw_value = await self._get_setting(_EMBEDDING_DIMENSIONS_SETTING)
        if raw_value in (None, ""):
            return None
        try:
            return int(raw_value)
        except (TypeError, ValueError):
            logger.warning("Invalid %s setting value %r", _EMBEDDING_DIMENSIONS_SETTING, raw_value)
            return None

    async def count_embeddings(self) -> int:
        """Число сохранённых эмбеддингов (0, если таблицы ещё нет)."""
        try:
            cur = await self._db.execute("SELECT COUNT(*) AS cnt FROM message_embeddings")
        except Exception:
            return 0
        row = await cur.fetchone()
        return int(row["cnt"]) if row else 0

    async def reset_embeddings_index(self) -> None:
        """Полностью сбросить семантический индекс: оба стора эмбеддингов и их настройки (одной транзакцией с DDL)."""
        assert self._database is not None, (
            "MessagesRepository.reset_embeddings_index requires a Database reference"
        )
        # DROP TABLE is DDL — under SQLite it causes an implicit COMMIT of
        # any open transaction on the connection. Wrap all three statements
        # in a single transaction() so the DDL holds _write_lock and cannot
        # commit a concurrent BEGIN IMMEDIATE block prematurely.
        async with self._database.transaction() as conn:
            await conn.execute("DROP TABLE IF EXISTS vec_messages")
            await conn.execute("DELETE FROM message_embeddings_json")
            await conn.execute(
                "DELETE FROM settings WHERE key IN (?, ?)",
                (_EMBEDDING_DIMENSIONS_SETTING, "semantic_last_embedded_id"),
            )

    async def insert_message(self, msg: Message) -> bool:
        """Вставить одно сообщение (дубликат по ключу обновляет изменяемые поля); вернуть ``True``, если строка новая.

        Реакции апсёртятся в нормализованную таблицу. Транзиентную блокировку БД
        пробрасывает наружу (не маскирует под «не сохранилось»), чтобы вызывающий
        пересобрал заново.
        """
        assert self._database is not None, (
            "MessagesRepository.insert_message requires a Database reference"
        )
        # Local import — see insert_messages_batch for the partial-init rationale.
        from src.database import DatabaseBusyError

        try:
            cur = await self._database.execute_write(
                """INSERT OR IGNORE INTO messages
                   (channel_id, message_id, sender_id, sender_name,
                    sender_first_name, sender_last_name, sender_username,
                    text, message_kind, media_type, service_action_raw,
                    service_action_semantic, service_action_payload_json, sender_kind,
                    topic_id, reactions_json,
                    views, forwards, reply_count, date, detected_lang,
                    forward_from_channel_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    msg.channel_id,
                    msg.message_id,
                    msg.sender_id,
                    msg.sender_name,
                    msg.sender_first_name,
                    msg.sender_last_name,
                    _normalize_username(msg.sender_username),
                    msg.text,
                    msg.message_kind,
                    msg.media_type,
                    msg.service_action_raw,
                    msg.service_action_semantic,
                    msg.service_action_payload_json,
                    msg.sender_kind,
                    msg.topic_id,
                    msg.reactions_json,
                    msg.views,
                    msg.forwards,
                    msg.reply_count,
                    msg.date.isoformat(),
                    msg.detected_lang,
                    getattr(msg, "forward_from_channel_id", None),
                ),
            )
            inserted = cur.rowcount > 0
            if not inserted:
                await self._refresh_existing_messages([msg], clear_premium_search_query=True)
            if msg.reactions_json:
                await self._upsert_reactions(
                    msg.channel_id, msg.message_id, msg.reactions_json, msg.date.isoformat()
                )
            return inserted
        except DatabaseBusyError:
            # Transient lock — propagate instead of masking it as a persistence
            # failure, mirroring insert_messages_batch. The caller re-collects.
            raise
        except Exception:
            logger.exception(
                "Failed to insert message channel_id=%s message_id=%s",
                msg.channel_id,
                msg.message_id,
            )
            return False

    async def _upsert_reactions(
        self, channel_id: int, message_id: int, reactions_json: str, message_date: str
    ) -> None:
        """Replace message_reactions rows for the message with reactions_json contents.

        Delete-then-insert (not bare INSERT OR REPLACE) so emoji that vanished
        from the refreshed JSON don't linger and inflate the analytics
        aggregates built on this table (#826/#827 review).

        ``message_date`` is the parent message's ISO date, stored on each reaction
        row so analytics can filter by recency without joining messages (#760).
        """
        assert self._database is not None, (
            "MessagesRepository._upsert_reactions requires a Database reference"
        )
        items = _parse_reactions_json(reactions_json)
        data = [(channel_id, message_id, r["emoji"], r.get("count", 0), message_date) for r in items]
        try:
            async with self._database.transaction() as conn:
                await conn.execute(
                    "DELETE FROM message_reactions WHERE channel_id = ? AND message_id = ?",
                    (channel_id, message_id),
                )
                if data:
                    await conn.executemany(
                        """INSERT OR REPLACE INTO message_reactions
                           (channel_id, message_id, emoji, count, date) VALUES (?, ?, ?, ?, ?)""",
                        data,
                    )
        except Exception:
            logger.exception(
                "Failed to upsert reactions for channel_id=%s message_id=%s",
                channel_id,
                message_id,
            )

    async def insert_messages_batch(
        self, messages: list[Message], premium_search_query: str | None = None
    ) -> int:
        """Пакетно вставить сообщения (INSERT OR IGNORE), обновив уже известные и их реакции; вернуть число новых строк.

        Дубликаты по ``UNIQUE(channel_id, message_id)`` пропускаются вставкой и
        затем обновляются (просмотры/реакции/язык). ``premium_search_query`` тегирует
        строки, добытые Premium-поиском (для последующей очистки). Транзиентная
        блокировка пробрасывается наружу, как и в :meth:`insert_message`.
        """
        assert self._database is not None, (
            "MessagesRepository.insert_messages_batch requires a Database reference"
        )
        if not messages:
            return 0
        data = [
            (
                m.channel_id,
                m.message_id,
                m.sender_id,
                m.sender_name,
                m.sender_first_name,
                m.sender_last_name,
                _normalize_username(m.sender_username),
                m.text,
                m.message_kind,
                m.media_type,
                m.service_action_raw,
                m.service_action_semantic,
                m.service_action_payload_json,
                m.sender_kind,
                m.topic_id,
                m.reactions_json,
                m.views,
                m.forwards,
                m.reply_count,
                m.date.isoformat(),
                m.detected_lang,
                getattr(m, "forward_from_channel_id", None),
                premium_search_query,
            )
            for m in messages
        ]
        try:
            existing_keys = await self._existing_message_keys(messages)
        except Exception as exc:
            logger.warning(
                "Could not pre-fetch existing message keys for refresh, proceeding without: %s",
                exc,
            )
            existing_keys = set()

        # Local import: facade.py imports this module at top level, so a
        # module-level `from src.database import DatabaseBusyError` would fail
        # at startup (partially initialized package). By call time the package
        # is fully initialized, so the import here is safe and cheap (cached).
        from src.database import DatabaseBusyError

        try:
            cur = await self._database.executemany_write(
                """INSERT OR IGNORE INTO messages
                   (channel_id, message_id, sender_id, sender_name,
                    sender_first_name, sender_last_name, sender_username,
                    text, message_kind, media_type, service_action_raw,
                    service_action_semantic, service_action_payload_json, sender_kind,
                    topic_id, reactions_json,
                    views, forwards, reply_count, date, detected_lang,
                    forward_from_channel_id, premium_search_query)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                data,
            )
            count = cur.rowcount if cur.rowcount >= 0 else len(messages)
        except DatabaseBusyError:
            # Transient lock — let the caller decide (the collector reverts
            # last_collected_id and re-collects next cycle). Swallowing this
            # into return 0 masks it as a false "Failed to persist" error.
            raise
        except Exception as exc:
            logger.error("Failed to insert batch of %d messages: %s", len(messages), exc)
            return 0

        if existing_keys:
            duplicates = [m for m in messages if (m.channel_id, m.message_id) in existing_keys]
            if duplicates:
                await self._refresh_existing_messages(
                    duplicates,
                    clear_premium_search_query=premium_search_query is None,
                )

        # Replace, not append: drop the messages' old rows first so emoji that
        # vanished from a refreshed reactions_json don't linger and inflate the
        # analytics aggregates built on this table (#826/#827 review).
        reaction_keys = list({(m.channel_id, m.message_id) for m in messages if m.reactions_json})
        # Carry each message's date onto its reaction rows so reaction analytics
        # filter by recency without joining messages (#760). isoformat() is computed
        # once per message, not once per reaction.
        reactions_data = []
        for m in messages:
            if not m.reactions_json:
                continue
            message_date = m.date.isoformat()
            for r in _parse_reactions_json(m.reactions_json):
                reactions_data.append(
                    (m.channel_id, m.message_id, r["emoji"], r.get("count", 0), message_date)
                )
        if reaction_keys:
            try:
                async with self._database.transaction() as conn:
                    await conn.executemany(
                        "DELETE FROM message_reactions WHERE channel_id = ? AND message_id = ?",
                        reaction_keys,
                    )
                    if reactions_data:
                        await conn.executemany(
                            """INSERT OR REPLACE INTO message_reactions
                               (channel_id, message_id, emoji, count, date) VALUES (?, ?, ?, ?, ?)""",
                            reactions_data,
                        )
            except Exception as exc:
                logger.error("Failed to upsert reactions for batch: %s", exc)

        return count

    async def _existing_message_keys(self, messages: list[Message]) -> set[tuple[int, int]]:
        """Return the subset of (channel_id, message_id) pairs already present in the DB."""
        if not messages:
            return set()
        unique_keys = list({(m.channel_id, m.message_id) for m in messages})
        existing: set[tuple[int, int]] = set()
        chunk_size = 400
        for start in range(0, len(unique_keys), chunk_size):
            chunk = unique_keys[start : start + chunk_size]
            predicates = " OR ".join("(channel_id = ? AND message_id = ?)" for _ in chunk)
            params = [value for key in chunk for value in key]
            cur = await self._db.execute(
                f"SELECT channel_id, message_id FROM messages WHERE {predicates}",
                params,
            )
            rows = await cur.fetchall()
            existing.update((row["channel_id"], row["message_id"]) for row in rows)
        return existing

    async def _refresh_existing_messages(
        self,
        messages: list[Message],
        *,
        clear_premium_search_query: bool = False,
    ) -> None:
        """Refresh mutable fields for already-known Telegram messages."""
        assert self._database is not None, (
            "MessagesRepository._refresh_existing_messages requires a Database reference"
        )
        if not messages:
            return
        data = [
            (
                m.views,
                m.views,
                m.forwards,
                m.forwards,
                m.reply_count,
                m.reply_count,
                m.reactions_json,
                m.reactions_json,
                m.detected_lang,
                m.detected_lang,
                int(clear_premium_search_query),
                m.channel_id,
                m.message_id,
            )
            for m in messages
        ]
        try:
            await self._database.executemany_write(
                """UPDATE messages
                   SET views = CASE WHEN ? IS NOT NULL THEN ? ELSE views END,
                       forwards = CASE WHEN ? IS NOT NULL THEN ? ELSE forwards END,
                       reply_count = CASE WHEN ? IS NOT NULL THEN ? ELSE reply_count END,
                       reactions_json = CASE WHEN ? IS NOT NULL THEN ? ELSE reactions_json END,
                       detected_lang = CASE WHEN ? IS NOT NULL THEN ? ELSE detected_lang END,
                       premium_search_query = CASE WHEN ? = 1 THEN NULL ELSE premium_search_query END
                   WHERE channel_id = ? AND message_id = ?""",
                data,
            )
        except Exception as exc:
            logger.error("Failed to refresh %d existing messages: %s", len(messages), exc)

    async def ensure_embeddings_table(self, dimensions: int) -> None:
        """Зафиксировать размерность индекса эмбеддингов при первом вызове; несовпадение с существующей — ошибка."""
        if dimensions < 1:
            raise ValueError("Embedding dimensions must be positive.")
        existing_dimensions = await self.get_embedding_dimensions()
        if existing_dimensions is not None and existing_dimensions != dimensions:
            raise RuntimeError(
                "Existing embeddings index uses "
                f"{existing_dimensions} dimensions; got {dimensions}."
            )
        if existing_dimensions is None:
            await self._set_setting(_EMBEDDING_DIMENSIONS_SETTING, str(dimensions))

    async def get_messages_for_embedding(
        self,
        *,
        after_id: int = 0,
        limit: int = 100,
    ) -> list[tuple[int, str]]:
        """Страница ``(id, text)`` непустых сообщений после ``after_id`` для построения эмбеддингов (курсор по id)."""
        cur = await self._db.execute(
            """
            SELECT id, text
            FROM messages
            WHERE id > ?
              AND COALESCE(TRIM(text), '') <> ''
            ORDER BY id ASC
            LIMIT ?
            """,
            (after_id, limit),
        )
        rows = await cur.fetchall()
        return [(int(row["id"]), str(row["text"])) for row in rows]

    async def upsert_message_embeddings(self, embeddings: list[tuple[int, list[float]]]) -> int:
        """Сохранить эмбеддинги ``(message_id, vector)`` в бинарный BLOB-индекс; вернуть число записей.

        Все векторы должны быть одной размерности (она фиксируется индексом).
        """
        assert self._database is not None, (
            "MessagesRepository.upsert_message_embeddings requires a Database reference"
        )
        if not embeddings:
            return 0
        dimensions = len(embeddings[0][1])
        if dimensions < 1:
            raise ValueError("Embeddings payload is empty.")
        if any(len(vector) != dimensions for _, vector in embeddings):
            raise ValueError("All embedding vectors must use the same dimensions.")
        await self.ensure_embeddings_table(dimensions)
        payload = [
            (message_id, struct.pack(f"{dimensions}f", *vector))
            for message_id, vector in embeddings
        ]
        cur = await self._database.executemany_write(
            "INSERT OR REPLACE INTO message_embeddings(message_id, embedding) VALUES (?, ?)",
            payload,
        )
        return cur.rowcount if cur.rowcount >= 0 else len(payload)

    async def upsert_message_embedding_json(
        self, embeddings: list[tuple[int, list[float]]]
    ) -> int:
        """Store embeddings in the portable JSON table (issue #173)."""
        assert self._database is not None, (
            "MessagesRepository.upsert_message_embedding_json requires a Database reference"
        )
        if not embeddings:
            return 0
        dimensions = len(embeddings[0][1])
        await self.ensure_embeddings_table(dimensions)
        payload = [
            (message_id, json.dumps(vector, separators=(",", ":")), dimensions)
            for message_id, vector in embeddings
        ]
        cur = await self._database.executemany_write(
            "INSERT OR REPLACE INTO message_embeddings_json (message_id, embedding, dims) "
            "VALUES (?, ?, ?)",
            payload,
        )
        return cur.rowcount if cur.rowcount >= 0 else len(payload)

    async def load_all_embeddings_json(self) -> list[tuple[int, list[float]]]:
        """Load embeddings from the portable JSON table, excluding filtered channels."""
        cur = await self._db.execute(
            "SELECT e.message_id, e.embedding FROM message_embeddings_json e "
            "JOIN messages m ON m.id = e.message_id "
            "LEFT JOIN channels c ON m.channel_id = c.channel_id "
            "WHERE (c.is_filtered IS NULL OR c.is_filtered = 0) "
            "ORDER BY e.message_id"
        )
        rows = await cur.fetchall()
        result: list[tuple[int, list[float]]] = []
        for row in rows:
            try:
                vec = json.loads(row["embedding"])
                result.append((int(row["message_id"]), vec))
            except Exception:
                continue
        return result

    def _build_message_filters(
        self,
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
            conditions.append("LENGTH(m.text) > ?")
            params.append(min_length)
        if max_length is not None:
            conditions.append("LENGTH(m.text) < ?")
            params.append(max_length)
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        return where, params

    async def _load_messages_by_ids(self, message_ids: list[int]) -> list[Message]:
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
        return self._rows_to_messages(await cur.fetchall())

    async def _search_text_candidate_ids(
        self,
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

    async def search_messages(self, params: SearchParams) -> MessageSearchPage:
        """Полнотекстовый поиск (FTS5, при недоступности — LIKE) по фильтрам [`SearchParams`][src.models.SearchParams].

        Возвращает [`MessageSearchPage`][src.database.repositories.messages.MessageSearchPage]
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
        self,
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
        self,
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
        self,
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
        self,
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

    async def get_by_id(self, message_db_id: int) -> Message | None:
        """Fetch a single message by its DB primary key (id column)."""
        cur = await self._db.execute(
            "SELECT m.*, c.title as channel_title, c.username as channel_username "
            "FROM messages m LEFT JOIN channels c ON m.channel_id = c.channel_id "
            "WHERE m.id = ?",
            (message_db_id,),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        msgs = self._rows_to_messages([row])
        return msgs[0] if msgs else None

    @staticmethod
    def _rows_to_messages(rows) -> list[Message]:
        return [
            Message(
                id=r["id"],
                channel_id=r["channel_id"],
                message_id=r["message_id"],
                sender_id=r["sender_id"],
                sender_name=r["sender_name"],
                sender_first_name=r["sender_first_name"] if "sender_first_name" in r.keys() else None,
                sender_last_name=r["sender_last_name"] if "sender_last_name" in r.keys() else None,
                sender_username=r["sender_username"] if "sender_username" in r.keys() else None,
                text=r["text"],
                message_kind=r["message_kind"] if "message_kind" in r.keys() else None,
                media_type=r["media_type"],
                service_action_raw=r["service_action_raw"] if "service_action_raw" in r.keys() else None,
                service_action_semantic=(
                    r["service_action_semantic"] if "service_action_semantic" in r.keys() else None
                ),
                service_action_payload_json=(
                    r["service_action_payload_json"] if "service_action_payload_json" in r.keys() else None
                ),
                sender_kind=r["sender_kind"] if "sender_kind" in r.keys() else None,
                topic_id=r["topic_id"],
                reactions_json=r["reactions_json"],
                views=r["views"],
                forwards=r["forwards"],
                reply_count=r["reply_count"],
                date=parse_required_datetime(r["date"]),
                collected_at=(
                    parse_datetime(r["collected_at"]) if r["collected_at"] else None
                ),
                detected_lang=r["detected_lang"] if "detected_lang" in r.keys() else None,
                translation_en=r["translation_en"] if "translation_en" in r.keys() else None,
                translation_custom=r["translation_custom"] if "translation_custom" in r.keys() else None,
                forward_from_channel_id=r["forward_from_channel_id"] if "forward_from_channel_id" in r.keys() else None,
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
        self,
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
        self,
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

    async def count_fts_matches_for_query(self, sq: SearchQuery) -> int:
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

    async def get_fts_daily_stats_for_query(self, sq: SearchQuery, days: int = 30) -> list:
        """Ряд «день → число совпадений» сохранённого запроса за последние ``days`` дней (по дате сообщений)."""
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
        """Дневная статистика сразу для многих запросов: карта ``sq_id → [SearchQueryDailyStat]`` за ``days`` дней.

        Считает UNION ALL чанками (≤100 запросов на чанк, лимит SQLite), чтобы не
        делать по запросу на каждый ``sq``.
        """
        self._require_fts()
        from src.models import SearchQueryDailyStat

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
                result[r["sq_id"]].append(SearchQueryDailyStat(day=r["day"], count=r["count"]))
        return result

    async def delete_messages_for_channel(self, channel_id: int) -> int:
        """Мягко удалить (purge) сообщения канала и их эмбеддинги; вернуть число удалённых строк.

        Это soft-delete: канал остаётся отслеживаемым, а леджеры дедупликации
        (`notified_messages`, `pipeline_action_log`) сознательно НЕ трогаются —
        иначе после повторного сбора задвоились бы уведомления и внешние
        Telegram-действия (#1039). Реакции уходят каскадом по FK.
        """
        assert self._database is not None, (
            "MessagesRepository.delete_messages_for_channel requires a Database reference"
        )
        async with self._database.transaction() as conn:
            # Both embedding stores key on messages.id (no FK) and use
            # INSERT OR REPLACE on that id alone. messages.id is INTEGER PRIMARY
            # KEY without AUTOINCREMENT, so SQLite may reissue a deleted rowid to
            # a future message, which would then join a stale vector. Clear both
            # the JSON store (#173) and the older BLOB index together (#1039,
            # Codex cycle-2 review) while the rows still resolve the subquery.
            await conn.execute(
                "DELETE FROM message_embeddings_json WHERE message_id IN "
                "(SELECT id FROM messages WHERE channel_id = ?)",
                (channel_id,),
            )
            await conn.execute(
                "DELETE FROM message_embeddings WHERE message_id IN "
                "(SELECT id FROM messages WHERE channel_id = ?)",
                (channel_id,),
            )
            # NOTE (#1039): `notified_messages` and `pipeline_action_log` are
            # deliberately NOT deleted here. purge is a *soft* delete — the
            # channel stays tracked and the same Telegram (channel_id, message_id)
            # can be collected again. Those two tables are dedup ledgers
            # (sent-notification ledger; "already reacted/forwarded/deleted"
            # ledger, #471), not message-owned sidecars: clearing them would
            # replay duplicate notifications and repeat external Telegram actions
            # after recollection. They are cleared only by hard-delete, where the
            # channel is gone for good (see channels.delete_channel).
            # `message_reactions` IS removed, but implicitly: its composite FK on
            # messages(channel_id, message_id) is ON DELETE CASCADE, so SQLite
            # (foreign_keys=ON) drops it together with the message.
            cur = await conn.execute(
                "DELETE FROM messages WHERE channel_id = ?", (channel_id,)
            )
            rowcount = cur.rowcount or 0
        return rowcount

    async def delete_premium_search_results(self, query: str) -> int:
        """Delete messages cached solely by a Premium global search for *query*.

        Only rows tagged with ``premium_search_query`` are removed — messages that
        already existed (collected by the worker or a prior search) are skipped by
        ``INSERT OR IGNORE`` and never receive the tag, and later normal collection
        refreshes clear stale tags, so user data is never touched.
        Used by the live Premium-search test to clean up after itself. Returns the
        number of deleted rows.
        """
        assert self._database is not None, (
            "MessagesRepository.delete_premium_search_results requires a Database reference"
        )
        async with self._database.transaction() as conn:
            # Clear both embedding stores (JSON + BLOB) before the messages they
            # key on are gone, same rowid-reuse reasoning as the channel deletes
            # (#1039, cycle-2 review).
            await conn.execute(
                "DELETE FROM message_embeddings_json WHERE message_id IN "
                "(SELECT id FROM messages WHERE premium_search_query = ?)",
                (query,),
            )
            await conn.execute(
                "DELETE FROM message_embeddings WHERE message_id IN "
                "(SELECT id FROM messages WHERE premium_search_query = ?)",
                (query,),
            )
            cur = await conn.execute(
                "DELETE FROM messages WHERE premium_search_query = ?", (query,)
            )
            rowcount = cur.rowcount or 0
        return rowcount

    async def get_stats(self) -> dict:
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

    async def get_trending_emojis(self, limit: int = 10, days: int | None = None) -> list[dict]:
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
                LEFT JOIN channels c ON m.channel_id = c.channel_id
                WHERE (c.is_filtered IS NULL OR c.is_filtered = 0)
                  AND m.collected_at >= datetime('now', ?)
                GROUP BY mr.emoji
                ORDER BY total DESC
                LIMIT ?
                """,
                (f"-{days} days", limit),
            )
        else:
            cur = await self._db.execute(
                """
                SELECT mr.emoji, SUM(mr.count) AS total
                FROM message_reactions mr
                JOIN messages m ON mr.channel_id = m.channel_id AND mr.message_id = m.message_id
                LEFT JOIN channels c ON m.channel_id = c.channel_id
                WHERE (c.is_filtered IS NULL OR c.is_filtered = 0)
                GROUP BY mr.emoji
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
        self,
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

    _REACTIONS_PER_MESSAGE_SQL = """(SELECT channel_id, message_id, SUM(count) AS total_reactions
                       FROM message_reactions GROUP BY channel_id, message_id)"""

    async def _get_engagement_rows(
        self,
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
        self,
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
        self,
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

    async def get_message_by_id(self, message_db_id: int) -> Message | None:
        """Get a single message by its DB primary key (id)."""
        cur = await self._db.execute(
            """SELECT m.*, c.title AS channel_title, c.username AS channel_username
               FROM messages m
               LEFT JOIN channels c ON m.channel_id = c.channel_id
               WHERE m.id = ?""",
            (message_db_id,),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        msgs = self._rows_to_messages([row])
        return msgs[0] if msgs else None

    async def get_messages_by_channel_message_ids(self, keys: list[tuple[int, int]]) -> list[Message]:
        """Load messages by Telegram identity, preserving the requested order."""
        if not keys:
            return []
        unique_keys = list(dict.fromkeys(keys))
        predicates = " OR ".join("(m.channel_id = ? AND m.message_id = ?)" for _ in unique_keys)
        params = [value for key in unique_keys for value in key]
        cur = await self._db.execute(
            f"""SELECT m.*, c.title AS channel_title, c.username AS channel_username
                FROM messages m
                LEFT JOIN channels c ON m.channel_id = c.channel_id
                WHERE {predicates}""",
            params,
        )
        rows = await cur.fetchall()
        by_key = {(msg.channel_id, msg.message_id): msg for msg in self._rows_to_messages(rows)}
        return [by_key[key] for key in keys if key in by_key]

    async def update_detected_lang(self, message_db_id: int, lang: str) -> None:
        """Update detected_lang for a message."""
        assert self._database is not None, (
            "MessagesRepository.update_detected_lang requires a Database reference"
        )
        await self._database.execute_write("UPDATE messages SET detected_lang = ? WHERE id = ?", (lang, message_db_id))

    # ── translation helpers ──────────────────────────────────────────

    async def get_untranslated_messages(
        self,
        target: str,
        source_langs: list[str] | None = None,
        limit: int = 20,
        after_id: int = 0,
    ) -> list[Message]:
        """Get messages needing translation for a given target language.

        ``target`` is the destination language code (e.g. 'en'); it also selects the
        storage column ('en' → translation_en, otherwise translation_custom).
        """
        col = "translation_en" if target == "en" else "translation_custom"
        # Exclude the 'und' sentinel (undetectable source language) — translating
        # those wastes LLM calls on emoji/too-short messages (audit #836/1).
        # Also exclude rows already in the target language: translate_batch skips
        # source==target by design, so selecting them only churns no-work batches
        # through the cursor chain (#866 review cleanup).
        conditions = [
            f"m.{col} IS NULL",
            "m.text IS NOT NULL",
            "m.text != ''",
            "m.detected_lang IS NOT NULL",
            "m.detected_lang != 'und'",
            "m.detected_lang != ?",
        ]
        params: list = [target]
        if after_id:
            conditions.append("m.id > ?")
            params.append(after_id)
        if source_langs:
            placeholders = ", ".join("?" for _ in source_langs)
            conditions.append(f"m.detected_lang IN ({placeholders})")
            params.extend(source_langs)
        where = " AND ".join(conditions)
        cur = await self._db.execute(
            f"""SELECT m.*, c.title AS channel_title, c.username AS channel_username
                FROM messages m
                LEFT JOIN channels c ON m.channel_id = c.channel_id
                WHERE {where}
                ORDER BY m.id ASC
                LIMIT ?""",
            (*params, limit),
        )
        rows = await cur.fetchall()
        return self._rows_to_messages(rows)

    async def update_translation(self, message_db_id: int, target: str, translated_text: str) -> None:
        """Update translation_en or translation_custom for a message."""
        assert self._database is not None, (
            "MessagesRepository.update_translation requires a Database reference"
        )
        col = "translation_en" if target == "en" else "translation_custom"
        await self._database.execute_write(
            f"UPDATE messages SET {col} = ? WHERE id = ?",
            (translated_text, message_db_id),
        )

    async def get_language_stats(self) -> list[tuple[str, int]]:
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

    async def backfill_language_detection(self, batch_size: int = 1000) -> int:
        """Detect language for messages with detected_lang IS NULL.

        Returns the number of rows *considered* (not just successfully detected) so
        a caller's ``while considered == batch_size`` loop terminates correctly.
        Undetectable rows (too short / emoji-only) are stamped with the sentinel
        ``'und'`` so they are not re-selected forever, and the scan is ordered by
        id for stable progress (audit #836/1).
        """
        import asyncio

        from src.services.translation_service import TranslationService

        cur = await self._db.execute(
            "SELECT id, text FROM messages WHERE detected_lang IS NULL "
            "AND text IS NOT NULL AND text != '' ORDER BY id LIMIT ?",
            (batch_size,),
        )
        rows = await cur.fetchall()
        if not rows:
            return 0
        # Run CPU-bound detection in thread to avoid blocking the event loop
        detect_results = await asyncio.to_thread(
            lambda: [(row["id"], TranslationService.detect_language(row["text"])) for row in rows]
        )
        assert self._database is not None, (
            "MessagesRepository.backfill_language_detection requires a Database reference"
        )
        async with self._database.transaction() as conn:
            for row_id, lang in detect_results:
                await conn.execute(
                    "UPDATE messages SET detected_lang = ? WHERE id = ?",
                    (lang or "und", row_id),
                )
        return len(rows)

    async def get_views_timeseries(
        self, channel_id: int, days: int = 30
    ) -> list[dict]:
        """Daily average views and message count for a channel."""
        cur = await self._db.execute(
            """SELECT date(m.date) AS day,
                      COUNT(*) AS message_count,
                      COALESCE(AVG(m.views), 0) AS avg_views
               FROM messages m
               WHERE m.channel_id = ? AND m.date >= datetime('now', ?)
               GROUP BY day
               ORDER BY day ASC""",
            (channel_id, f"-{days} days"),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_post_frequency(
        self, channel_id: int, days: int = 30
    ) -> list[dict]:
        """Daily post count for a channel."""
        cur = await self._db.execute(
            """SELECT date(m.date) AS day, COUNT(*) AS count
               FROM messages m
               WHERE m.channel_id = ? AND m.date >= datetime('now', ?)
               GROUP BY day
               ORDER BY day ASC""",
            (channel_id, f"-{days} days"),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_channel_message_count(
        self, channel_id: int, days: int | None = None
    ) -> int:
        """Count messages for a channel, optionally within last N days."""
        if days is not None:
            cur = await self._db.execute(
                "SELECT COUNT(*) AS cnt FROM messages "
                "WHERE channel_id = ? AND date >= datetime('now', ?)",
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
        self, channel_id: int, last_n: int = 20
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

    async def get_err24_data(self, channel_id: int) -> list[dict]:
        """Get engagement data for ERR24 (posts from last 24h)."""
        cur = await self._db.execute(
            """SELECT m.views, m.forwards, m.reply_count,
                      COALESCE((SELECT SUM(mr.count) FROM message_reactions mr
                                WHERE mr.channel_id = m.channel_id
                                AND mr.message_id = m.message_id), 0) AS total_reactions
               FROM messages m
               WHERE m.channel_id = ? AND m.date >= datetime('now', '-1 day')
               ORDER BY m.date DESC""",
            (channel_id,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_citation_stats(self, channel_id: int) -> dict:
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
        self, channel_id: int, days: int = 30,
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
               WHERE m.channel_id = ? AND m.date >= datetime('now', ?)
               GROUP BY hour, weekday
               ORDER BY weekday, hour""",
            (channel_id, f"-{days} days"),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_cross_channel_citations(
        self, channel_id: int, days: int = 30, limit: int = 20,
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
                 AND m.date >= datetime('now', ?)
               GROUP BY m.forward_from_channel_id
               ORDER BY citation_count DESC
               LIMIT ?""",
            (channel_id, f"-{days} days", limit),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_messages_collected_since(self, since: str, limit: int = 5000) -> list[Message]:
        """Return non-empty messages collected at/after *since* (across all channels).

        Used by the notification dry-run preview, which then matches them with the
        SAME predicate as production (NotificationMatcher) instead of FTS, so the
        preview no longer diverges for regex/partial-substring queries (#838/3).

        Note: capped at *limit*. For an exact dry-run total that the uncapped production
        path would agree with, use iter_messages_collected_since, which pages through the
        whole window so the count is never silently truncated.
        """
        cur = await self._db.execute(
            f"""SELECT m.*, c.title AS channel_title, c.username AS channel_username
                FROM messages m{self._CHANNEL_JOIN}
                WHERE {self._BASE_FILTER}
                  AND m.collected_at >= ?
                  AND m.text IS NOT NULL AND m.text != ''
                ORDER BY m.date DESC
                LIMIT ?""",
            (since, limit),
        )
        rows = await cur.fetchall()
        return self._rows_to_messages(rows)

    async def iter_messages_collected_since(
        self, since: str, page_size: int = 5000
    ) -> AsyncIterator[list[Message]]:
        """Yield ALL non-empty messages collected at/after *since*, in pages.

        Keyset-paginated on the stable (date, message_id) ordering so the dry-run preview
        can evaluate the production predicate over the entire window. The live notification
        path is uncapped, so a single LIMIT made the preview undercount (even report 0) when
        a window held more than one page of messages (#838/3 review).
        """
        last_date: str | None = None
        last_message_id: int | None = None
        while True:
            if last_date is None:
                cur = await self._db.execute(
                    f"""SELECT m.*, c.title AS channel_title, c.username AS channel_username
                        FROM messages m{self._CHANNEL_JOIN}
                        WHERE {self._BASE_FILTER}
                          AND m.collected_at >= ?
                          AND m.text IS NOT NULL AND m.text != ''
                        ORDER BY m.date DESC, m.message_id DESC
                        LIMIT ?""",
                    (since, page_size),
                )
            else:
                cur = await self._db.execute(
                    f"""SELECT m.*, c.title AS channel_title, c.username AS channel_username
                        FROM messages m{self._CHANNEL_JOIN}
                        WHERE {self._BASE_FILTER}
                          AND m.collected_at >= ?
                          AND m.text IS NOT NULL AND m.text != ''
                          AND (m.date < ? OR (m.date = ? AND m.message_id < ?))
                        ORDER BY m.date DESC, m.message_id DESC
                        LIMIT ?""",
                    (since, last_date, last_date, last_message_id, page_size),
                )
            rows = await cur.fetchall()
            if not rows:
                return
            yield self._rows_to_messages(rows)
            if len(rows) < page_size:
                return
            last_row = rows[-1]
            last_date = last_row["date"]
            last_message_id = last_row["message_id"]

    async def get_recent_for_channels(
        self, channel_ids: list[int], since_hours: float
    ) -> list[Message]:
        """Return messages from the given channels newer than *since_hours* ago."""
        if not channel_ids:
            return []
        from datetime import datetime, timedelta, timezone

        cutoff = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).replace(tzinfo=None)
        cutoff_str = cutoff.isoformat()
        placeholders = ",".join("?" * len(channel_ids))
        cur = await self._db.execute(
            f"""SELECT m.*, c.title AS channel_title, c.username AS channel_username
                FROM messages m
                JOIN channels c ON m.channel_id = c.channel_id
                WHERE m.channel_id IN ({placeholders})
                  AND m.date >= ?
                ORDER BY m.date DESC""",
            (*channel_ids, cutoff_str),
        )
        rows = await cur.fetchall()
        return self._rows_to_messages(rows)
