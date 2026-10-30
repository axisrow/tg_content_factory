"""Репозиторий сообщений: вставка, поиск (FTS/семантика/гибрид) и аналитика.

Доступ через `db.repos.messages`. Самый горячий репозиторий проекта: пакетная
вставка с дедупликацией по ``UNIQUE(channel_id, message_id)``, обновление
изменяемых полей (реакции/просмотры) уже известных сообщений, три режима поиска
(полнотекстовый FTS5 с LIKE-фолбэком, семантический по эмбеддингам, гибридный
RRF) и агрегаты для аналитики по нормализованной таблице ``message_reactions``.

Замечания по производительности: поиск отдаёт страницу с нижней оценкой total
вместо точного ``COUNT(*)`` (см.
[`MessageSearchPage`][tg_agent.database.repositories.messages.MessageSearchPage], #766);
фильтр отсекает отфильтрованные каналы JOIN-ом на ``channels`` по ``channel_id``
(конвенция ключей, CLAUDE.md).
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import TYPE_CHECKING, AsyncIterator, cast

from tg_agent.database.pool import ReadConnection
from tg_agent.database.repositories.message_analytics import MessageAnalyticsMixin
from tg_agent.database.repositories.message_embeddings import MessageEmbeddingsMixin
from tg_agent.database.repositories.message_search import MessageSearchMixin
from tg_agent.database.repositories.message_search import MessageSearchPage as MessageSearchPage
from tg_agent.database.repositories.message_search import _normalize_date_to as _normalize_date_to
from tg_agent.database.repositories.message_search import _normalize_username as _normalize_username
from tg_agent.database.udf import has_cyrillic_udf
from tg_agent.models import Message
from tg_agent.telegram.reactions import parse_reactions_json
from tg_agent.utils.datetime import (
    parse_datetime,
    parse_required_datetime,
)

if TYPE_CHECKING:
    from tg_agent.database.facade import Database

logger = logging.getLogger(__name__)


def _parse_reactions_json(reactions_json: str) -> list[dict]:
    """Parse reactions_json string into a list of {emoji, count} dicts."""
    return parse_reactions_json(reactions_json)


class _TransactionWrites:
    """Reuse repository write helpers inside an already owned transaction."""

    def __init__(self, connection):
        self.connection = connection

    async def executemany_write(self, sql, rows):
        return await self.connection.executemany(sql, rows)

    async def execute_write(self, sql, params=()):
        return await self.connection.execute(sql, params)

    @asynccontextmanager
    async def transaction(self):
        yield self.connection


class MessagesRepository(MessageEmbeddingsMixin, MessageSearchMixin, MessageAnalyticsMixin):
    """Вставка, поиск и аналитика сообщений (`messages` + сайдкары реакций/эмбеддингов)."""

    def __init__(
        self,
        db: ReadConnection,
        *,
        fts_available: bool = True,
        database: "Database | None" = None,
    ):
        self._db = db
        self._fts_available = fts_available
        self._database = database
        self._strict_writes = False

    async def save_collected_page(self, channel_id: int, messages: list[Message], last_id: int) -> int:
        """Commit messages, derived statistics and the channel cursor together."""
        if any(message.channel_id != channel_id or message.message_id > last_id for message in messages):
            raise ValueError("Collection page does not match its channel/cursor")
        assert self._database is not None
        async with self._database.transaction() as conn:
            repository = MessagesRepository(conn, fts_available=self._fts_available,
                                            database=cast("Database", _TransactionWrites(conn)))
            repository._strict_writes = True
            count = await repository.insert_messages_batch(messages)
            cursor = await conn.execute("UPDATE channels SET last_collected_id=MAX(last_collected_id,?) "
                                        "WHERE channel_id=?", (last_id, channel_id))
            if cursor.rowcount != 1:
                raise ValueError("Collected channel was removed")
            return count


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
        from tg_agent.database import DatabaseBusyError

        try:
            cur = await self._database.execute_write(
                """INSERT OR IGNORE INTO messages
                   (channel_id, message_id, sender_id, sender_name,
                    sender_first_name, sender_last_name, sender_username,
                    text, message_kind, media_type, service_action_raw,
                    service_action_semantic, service_action_payload_json, sender_kind,
                    topic_id, reactions_json, entities_json,
                    views, forwards, reply_count, date, detected_lang,
                    forward_from_channel_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                    msg.entities_json,
                    msg.views,
                    msg.forwards,
                    msg.reply_count,
                    msg.date.isoformat(),
                    msg.detected_lang,
                    getattr(msg, "forward_from_channel_id", None),
                ),
            )
            inserted = cur.rowcount > 0
            if inserted:
                await self._apply_stats_deltas([msg])
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

    async def _apply_stats_deltas(self, messages: list[Message]) -> None:
        """Инкрементальная поддержка справочника фильтратора (#1679).

        text/date сообщения неизменяемы после вставки, поэтому per-channel
        агрегаты и префиксную таблицу можно поддерживать дельтами при вставке —
        `filter analyze` читает справочник вместо пяти полных сканов истории.
        Условия счётчиков повторяют полные SQL-шаблоны filters.py в точности:
        total_text/short_link/cyr — по непустому тексту (форварды включены),
        short — по любому не-NULL тексту (пустая строка считается, len 0),
        префиксы — только не-форвардный непустой текст. Сбой справочника не
        роняет вставку сообщения — расхождение лечит `filter analyze --rebuild`.
        """
        if not messages:
            return
        assert self._database is not None, (
            "MessagesRepository._apply_stats_deltas requires a Database reference"
        )
        counters: dict[int, list[int]] = {}
        prefixes: list[tuple[int, str, str]] = []
        for m in messages:
            text = m.text
            row = counters.setdefault(m.channel_id, [0, 0, 0, 0, 0, 0])
            row[0] += 1  # total_all
            if text is None:
                continue
            nonempty = text != ""
            if nonempty:
                row[1] += 1  # total_text
            if len(text) <= 10:
                row[3] += 1  # short: не-NULL текст любой длины ≤10
            if nonempty and has_cyrillic_udf(text):
                row[4] += 1  # cyr
            if nonempty and len(text) < 200:
                # LIKE в SQLite регистронезависим для ASCII — повторяем через lower();
                # entities_json проверяем только если текст сам не совпал (он бывает
                # объёмным — зря не копируем).
                low = text.lower()
                if "http" in low or "t.me" in low:
                    row[5] += 1  # short_link
                else:
                    entities = (getattr(m, "entities_json", None) or "").lower()
                    if "http" in entities or "t.me" in entities:
                        row[5] += 1  # short_link
            if nonempty and getattr(m, "forward_from_channel_id", None) is None:
                row[2] += 1  # total_text_no_fwd
                prefixes.append((m.channel_id, text[:100], m.date.isoformat()))
        try:
            # Одна транзакция вместо двух executemany_write: один write-lock
            # и один commit, обе таблицы справочника обновляются атомарно
            # (паттерн _upsert_reactions).
            async with self._database.transaction() as conn:
                await conn.executemany(
                    """INSERT INTO channel_text_stats
                         (channel_id, total_all, total_text, total_text_no_fwd,
                          short, cyr, short_link)
                       VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(channel_id) DO UPDATE SET
                         total_all = total_all + excluded.total_all,
                         total_text = total_text + excluded.total_text,
                         total_text_no_fwd = total_text_no_fwd + excluded.total_text_no_fwd,
                         short = short + excluded.short,
                         cyr = cyr + excluded.cyr,
                         short_link = short_link + excluded.short_link""",
                    [(cid, *d) for cid, d in counters.items()],
                )
                if prefixes:
                    await conn.executemany(
                        """INSERT INTO channel_prefix_stats (channel_id, prefix, first_date, cnt)
                           VALUES (?, ?, ?, 1)
                           ON CONFLICT(channel_id, prefix) DO UPDATE SET
                             first_date = MIN(first_date, excluded.first_date),
                             cnt = cnt + 1""",
                        prefixes,
                    )
        except Exception:
            logger.exception(
                "Failed to update filter text stats for %d message(s)", len(messages)
            )
            if self._strict_writes:
                raise

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
                m.entities_json,
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
            if self._strict_writes:
                raise
            existing_keys = set()

        # Local import: facade.py imports this module at top level, so a
        # module-level `from tg_agent.database import DatabaseBusyError` would fail
        # at startup (partially initialized package). By call time the package
        # is fully initialized, so the import here is safe and cheap (cached).
        from tg_agent.database import DatabaseBusyError

        try:
            cur = await self._database.executemany_write(
                """INSERT OR IGNORE INTO messages
                   (channel_id, message_id, sender_id, sender_name,
                    sender_first_name, sender_last_name, sender_username,
                    text, message_kind, media_type, service_action_raw,
                    service_action_semantic, service_action_payload_json, sender_kind,
                    topic_id, reactions_json, entities_json,
                    views, forwards, reply_count, date, detected_lang,
                    forward_from_channel_id, premium_search_query)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                data,
            )
            count = cur.rowcount if cur.rowcount >= 0 else len(messages)
            # Дельты справочника — только по реально новым строкам: дубликат,
            # пропущенный INSERT OR IGNORE, не должен надувать счётчики.
            # При сбое пре-фетча existing_keys пуст → считаем всё новыми
            # (возможен завысивший счётчик; лечится `filter analyze --rebuild`).
            new_messages = [
                m for m in messages if (m.channel_id, m.message_id) not in existing_keys
            ]
            await self._apply_stats_deltas(new_messages)
        except DatabaseBusyError:
            # Transient lock — let the caller decide (the collector reverts
            # last_collected_id and re-collects next cycle). Swallowing this
            # into return 0 masks it as a false "Failed to persist" error.
            raise
        except Exception as exc:
            logger.error("Failed to insert batch of %d messages: %s", len(messages), exc)
            if self._strict_writes:
                raise
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
                if self._strict_writes:
                    raise

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
            if self._strict_writes:
                raise


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
                entities_json=r["entities_json"] if "entities_json" in r.keys() else None,
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


    _FTS_JOIN = (
        " INNER JOIN (SELECT rowid FROM messages_fts"
        " WHERE messages_fts MATCH ?) AS fts ON m.id = fts.rowid"
    )
    _CHANNEL_JOIN = " LEFT JOIN channels c ON m.channel_id = c.channel_id"
    _BASE_FILTER = "(c.is_filtered IS NULL OR c.is_filtered = 0)"


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
            # Справочник фильтратора (#1679) — sidecar без FK: после purge строк
            # сообщений нет, живой SQL видит нули; сбрасываем и справочник, чтобы
            # счётчики не завышали. Новые вставки снова наполнят его дельтами.
            await conn.execute(
                "DELETE FROM channel_text_stats WHERE channel_id = ?", (channel_id,),
            )
            await conn.execute(
                "DELETE FROM channel_prefix_stats WHERE channel_id = ?", (channel_id,),
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
            # Каналы, чей кэш уходит: справочник фильтратора (#1679) после
            # частичного удаления надо пересчитать по ним точечно.
            cur = await conn.execute(
                "SELECT DISTINCT channel_id FROM messages WHERE premium_search_query = ?",
                (query,),
            )
            affected_channels = [row["channel_id"] for row in await cur.fetchall()]
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
        if affected_channels:
            await self._database.repos.filters.rebuild_channel_text_stats(affected_channels)
        return rowcount


    _REACTIONS_PER_MESSAGE_SQL = """(SELECT channel_id, message_id, SUM(count) AS total_reactions
                       FROM message_reactions GROUP BY channel_id, message_id)"""


    async def get_message_by_id(self, message_db_id: int) -> Message | None:
        """Compatibility alias for get_by_id."""
        return await self.get_by_id(message_db_id)

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
        # Exclude the 'und' sentinel (undetectable source language) — translating
        # those wastes LLM calls on emoji/too-short messages (audit #836/1).
        # Also exclude rows already in the target language: translate_batch skips
        # source==target by design, so selecting them only churns no-work batches
        # through the cursor chain (#866 review cleanup).
        #
        # A cached non-English translation satisfies only requests for the SAME
        # destination language: translation_custom is shared by every non-en
        # language, so the language it was produced for is recorded alongside
        # (translation_custom_lang, issue #1542) and a request for another
        # language keeps the message pending. Legacy rows with a text but no
        # recorded language are treated as translated — re-translating all of
        # them would burn the LLM budget on a maybe-wrong cache.
        if target == "en":
            untranslated_condition = "m.translation_en IS NULL"
        else:
            untranslated_condition = (
                "(m.translation_custom IS NULL "
                "OR (m.translation_custom_lang IS NOT NULL AND m.translation_custom_lang != ?))"
            )
        conditions = [
            untranslated_condition,
            "m.text IS NOT NULL",
            "m.text != ''",
            "m.detected_lang IS NOT NULL",
            "m.detected_lang != 'und'",
            "m.detected_lang != ?",
        ]
        params: list = []
        if target != "en":
            params.append(target)
        params.append(target)
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
        if target == "en":
            await self._database.execute_write(
                "UPDATE messages SET translation_en = ? WHERE id = ?",
                (translated_text, message_db_id),
            )
        else:
            # Record which language the shared custom column now holds so a
            # request for a different destination language stays pending (#1542).
            await self._database.execute_write(
                "UPDATE messages SET translation_custom = ?, translation_custom_lang = ? WHERE id = ?",
                (translated_text, target, message_db_id),
            )


    async def backfill_language_detection(self, batch_size: int = 1000) -> int:
        """Detect language for messages with detected_lang IS NULL.

        Returns the number of rows *considered* (not just successfully detected) so
        a caller's ``while considered == batch_size`` loop terminates correctly.
        Undetectable rows (too short / emoji-only) are stamped with the sentinel
        ``'und'`` so they are not re-selected forever, and the scan is ordered by
        id for stable progress (audit #836/1).
        """
        import asyncio

        from tg_agent.services.translation_service import TranslationService

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
        from datetime import datetime, timezone

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
