"""Репозиторий отслеживаемых каналов и их тегов (таблицы ``channels``/``tags``).

Доступ через `db.repos.channels`. Хранит метаданные канала, флаги отслеживания
(`is_active`), фильтрации (`is_filtered`/`filter_flags`) и карантина на ревью
(`needs_review`), а также курсор инкрементального сбора (`last_collected_id`).

Конвенция ключей (CLAUDE.md): сторонние таблицы соединяются по Telegram
``channel_id``, а DB-первичный ключ ``id`` (он же ``pk``) используется только в
адресных операциях над самой строкой канала (`*_by_pk`, set/delete). Параметр с
именем ``pk`` — это ``channels.id``, ``channel_id`` — Telegram-идентификатор.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import aiosqlite

from src.database.pool import ReadConnection
from src.models import Channel
from src.utils.datetime import parse_datetime

if TYPE_CHECKING:
    from src.database.facade import Database


class ChannelsRepository:
    """CRUD каналов: метаданные, флаги отслеживания/фильтрации/ревью и теги."""

    def __init__(
        self,
        db: ReadConnection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

    async def add_channel(self, channel: Channel) -> int:
        """Добавить канал или обновить существующий по Telegram ``channel_id`` (UPSERT); вернуть pk.

        При конфликте обновляет метаданные, но сохраняет уже накопленные
        ``about``/``linked_chat_id``/``created_at`` (COALESCE — не затирает их
        значениями NULL из частичного апдейта).

        Возвращает `cur.lastrowid`: надёжный pk только на ветке вставки; при
        конфликте-обновлении lastrowid остаётся от последней вставки в
        соединении — для гарантированного pk существующего канала читайте его по
        ``channel_id`` (например `get_channel_by_channel_id`).
        """
        assert self._database is not None, (
            "ChannelsRepository.add_channel requires a Database reference"
        )
        cur = await self._database.execute_write(
            """INSERT INTO channels (channel_id, title, username, channel_type, is_active,
                                     about, linked_chat_id, has_comments, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(channel_id) DO UPDATE
               SET title=excluded.title, username=excluded.username,
                   channel_type=excluded.channel_type,
                   is_active=excluded.is_active,
                   about=COALESCE(excluded.about, channels.about),
                   linked_chat_id=COALESCE(excluded.linked_chat_id, channels.linked_chat_id),
                   has_comments=CASE WHEN COALESCE(excluded.linked_chat_id, channels.linked_chat_id)
                                          IS NOT NULL THEN 1 ELSE 0 END,
                   created_at=COALESCE(excluded.created_at, channels.created_at)""",
            (
                channel.channel_id,
                channel.title,
                channel.username,
                channel.channel_type,
                int(channel.is_active),
                channel.about,
                channel.linked_chat_id,
                int(channel.has_comments),
                channel.created_at.isoformat() if channel.created_at else None,
            ),
        )
        return cur.lastrowid or 0

    @staticmethod
    def _map_channel(row: aiosqlite.Row) -> Channel:
        keys = row.keys()
        return Channel(
            id=row["id"],
            channel_id=row["channel_id"],
            title=row["title"],
            username=row["username"],
            channel_type=row["channel_type"],
            is_active=bool(row["is_active"]),
            is_filtered=bool(row["is_filtered"]) if "is_filtered" in keys else False,
            filter_flags=(
                row["filter_flags"] if "filter_flags" in keys and row["filter_flags"] else ""
            ),
            about=row["about"] if "about" in keys else None,
            linked_chat_id=row["linked_chat_id"] if "linked_chat_id" in keys else None,
            has_comments=bool(row["has_comments"]) if "has_comments" in keys and row["has_comments"] else False,
            last_collected_id=row["last_collected_id"],
            added_at=parse_datetime(row["added_at"]),
            created_at=(
                parse_datetime(row["created_at"])
                if "created_at" in keys and row["created_at"]
                else None
            ),
            message_count=(
                row["message_count"]
                if "message_count" in keys and row["message_count"] is not None
                else 0
            ),
            preferred_phone=(
                row["preferred_phone"] if "preferred_phone" in keys else None
            ),
            needs_review=bool(row["needs_review"]) if "needs_review" in keys and row["needs_review"] else False,
            review_reason=(
                row["review_reason"] if "review_reason" in keys and row["review_reason"] else None
            ),
        )

    async def get_channels(
        self, active_only: bool = False, include_filtered: bool = True
    ) -> list[Channel]:
        """Каналы по возрастанию pk; ``active_only`` — только активные,
        ``include_filtered=False`` скрывает отфильтрованные."""
        conditions = []
        if active_only:
            conditions.append("is_active = 1")
        if not include_filtered:
            conditions.append("is_filtered = 0")
        sql = "SELECT * FROM channels"
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY id ASC"
        cur = await self._db.execute(sql)
        rows = await cur.fetchall()
        return [self._map_channel(r) for r in rows]

    async def get_channel_by_pk(self, pk: int) -> Channel | None:
        """Канал по DB-первичному ключу (``channels.id``), либо ``None``."""
        cur = await self._db.execute("SELECT * FROM channels WHERE id = ?", (pk,))
        row = await cur.fetchone()
        if not row:
            return None
        return self._map_channel(row)

    async def get_channel_by_channel_id(self, channel_id: int) -> Channel | None:
        """Канал по Telegram ``channel_id``, либо ``None``."""
        cur = await self._db.execute(
            "SELECT * FROM channels WHERE channel_id = ?",
            (channel_id,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        return self._map_channel(row)

    async def get_channels_with_counts(
        self, active_only: bool = False, include_filtered: bool = True
    ) -> list[Channel]:
        """Каналы вместе с числом сообщений (`message_count`); фильтры как у :meth:`get_channels`.

        JOIN агрегата по ``channel_id`` (не по pk) — см. конвенцию ключей в docstring модуля.
        """
        sql = """
            SELECT c.*, COALESCE(cnt.total, 0) AS message_count
            FROM channels c
            LEFT JOIN (
                SELECT channel_id, COUNT(*) AS total FROM messages GROUP BY channel_id
            ) cnt ON c.channel_id = cnt.channel_id
        """
        conditions = []
        if active_only:
            conditions.append("c.is_active = 1")
        if not include_filtered:
            conditions.append("c.is_filtered = 0")
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY c.id ASC"
        cur = await self._db.execute(sql)
        rows = await cur.fetchall()
        return [self._map_channel(r) for r in rows]

    async def count_channels(
        self, active_only: bool = False, include_filtered: bool = True
    ) -> int:
        """Число каналов под теми же фильтрами, что и :meth:`get_channels`."""
        conditions = []
        if active_only:
            conditions.append("is_active = 1")
        if not include_filtered:
            conditions.append("is_filtered = 0")
        sql = "SELECT COUNT(*) FROM channels"
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        cur = await self._db.execute(sql)
        row = await cur.fetchone()
        return row[0] if row else 0

    async def update_channel_last_id(self, channel_id: int, last_id: int) -> None:
        """Продвинуть курсор инкрементального сбора (`last_collected_id`) — только вперёд (монотонно)."""
        assert self._database is not None, (
            "ChannelsRepository.update_channel_last_id requires a Database reference"
        )
        await self._database.execute_write(
            """
            UPDATE channels
            SET last_collected_id = CASE
                WHEN COALESCE(last_collected_id, 0) < ? THEN ?
                ELSE last_collected_id
            END
            WHERE channel_id = ?
            """,
            (last_id, last_id, channel_id),
        )

    async def set_channel_active(self, pk: int, active: bool) -> None:
        """Включить/выключить отслеживание канала (неактивный не собирается)."""
        assert self._database is not None, (
            "ChannelsRepository.set_channel_active requires a Database reference"
        )
        await self._database.execute_write("UPDATE channels SET is_active = ? WHERE id = ?", (int(active), pk))

    async def set_channel_review(self, pk: int, reason: str) -> None:
        """Flag a channel for human review (quarantine) — stays active until resolved."""
        assert self._database is not None, (
            "ChannelsRepository.set_channel_review requires a Database reference"
        )
        await self._database.execute_write(
            "UPDATE channels SET needs_review = 1, review_reason = ? WHERE id = ?",
            (reason, pk),
        )

    async def clear_channel_review(self, pk: int) -> None:
        """Clear the review flag (operator decided, or the channel resolved live again)."""
        assert self._database is not None, (
            "ChannelsRepository.clear_channel_review requires a Database reference"
        )
        await self._database.execute_write(
            "UPDATE channels SET needs_review = 0, review_reason = '' WHERE id = ?",
            (pk,),
        )

    async def list_channels_for_review(self) -> list[Channel]:
        """Channels currently quarantined for human review (needs_review = 1)."""
        cur = await self._db.execute(
            "SELECT * FROM channels WHERE needs_review = 1 ORDER BY id ASC"
        )
        rows = await cur.fetchall()
        return [self._map_channel(row) for row in rows]

    async def set_channel_filtered(self, pk: int, filtered: bool) -> None:
        """Ручная (раз)фильтрация канала: ставит/снимает ``is_filtered`` с флагом ``filter_flags='manual'``."""
        assert self._database is not None, (
            "ChannelsRepository.set_channel_filtered requires a Database reference"
        )
        if filtered:
            await self._database.execute_write(
                "UPDATE channels SET is_filtered = 1, filter_flags = 'manual' WHERE id = ?",
                (pk,),
            )
        else:
            await self._database.execute_write(
                "UPDATE channels SET is_filtered = 0, filter_flags = '' WHERE id = ?",
                (pk,),
            )

    async def set_filtered_bulk(
        self, updates: list[tuple[int, str]], *, commit: bool = True
    ) -> int:
        """Массово пометить каналы отфильтрованными по ``(channel_id, flags_csv)``; вернуть число изменённых строк.

        С ``commit=True`` владеет транзакцией сам (#569); с ``commit=False`` пишет
        на write-соединение внутри уже открытой транзакции вызывающего — см.
        комментарий в теле о write- против read-пула (#760).
        """
        assert self._database is not None, (
            "ChannelsRepository.set_filtered_bulk requires a Database reference"
        )
        if not updates:
            return 0
        # When commit=False, the caller already holds a Database.transaction()
        # block on the WRITE connection and owns the commit — we MUST write on
        # that same write connection (self._database.db) so our rows land in the
        # open transaction. self._db is the read pool (#760) and would route the
        # write to a read connection, outside the transaction (→ database is locked).
        # When commit=True (standalone, e.g. ensure_channel_filtered) we take
        # Database._write_lock ourselves via execute_write/transaction (#569).
        if commit:
            assert self._database is not None, (
                "ChannelsRepository.set_filtered_bulk requires a Database reference "
                "when commit=True"
            )
            updated_rows = 0
            async with self._database.transaction() as conn:
                for channel_id, flags_csv in updates:
                    cur = await conn.execute(
                        "UPDATE channels SET is_filtered = 1, filter_flags = ? WHERE channel_id = ?",
                        (flags_csv, channel_id),
                    )
                    rowcount = cur.rowcount if cur.rowcount is not None else 0
                    if rowcount > 0:
                        updated_rows += rowcount
            return updated_rows
        db = self._database
        write_conn = db.db
        assert write_conn is not None, (
            "ChannelsRepository.set_filtered_bulk requires an active Database connection"
        )
        # Enforce the #569 invariant: the only safe caller is one that already
        # owns a Database.transaction() block (holding _write_lock transitively).
        # Without this guard a future commit=False caller outside any transaction
        # would write through the shared connection with no lock and resurrect the
        # race that execute_write/transaction exist to close (#1182).
        assert write_conn.in_transaction, (
            "ChannelsRepository.set_filtered_bulk(commit=False) must run inside a "
            "caller-owned Database.transaction() block (write-lock invariant #569)"
        )
        updated_rows = 0
        for channel_id, flags_csv in updates:
            cur = await write_conn.execute(
                "UPDATE channels SET is_filtered = 1, filter_flags = ? WHERE channel_id = ?",
                (flags_csv, channel_id),
            )
            rowcount = cur.rowcount if cur.rowcount is not None else 0
            if rowcount > 0:
                updated_rows += rowcount
        return updated_rows

    async def reset_all_filters(self, *, commit: bool = True) -> int:
        """Снять фильтрацию со всех каналов; вернуть число снятых. ``commit`` — как в :meth:`set_filtered_bulk`."""
        if commit:
            assert self._database is not None, (
                "ChannelsRepository.reset_all_filters requires a Database reference "
                "when commit=True"
            )
            cur = await self._database.execute_write(
                "UPDATE channels SET is_filtered = 0, filter_flags = ''"
            )
            rowcount = cur.rowcount if cur.rowcount is not None else 0
            return rowcount if rowcount > 0 else 0
        # commit=False: write on the caller's open write transaction (see set_filtered_bulk).
        assert self._database is not None, (
            "ChannelsRepository.reset_all_filters requires a Database reference"
        )
        db = self._database
        write_conn = db.db
        assert write_conn is not None, (
            "ChannelsRepository.reset_all_filters requires an active Database connection"
        )
        # Enforce the #569 invariant — see set_filtered_bulk(commit=False) (#1182).
        assert write_conn.in_transaction, (
            "ChannelsRepository.reset_all_filters(commit=False) must run inside a "
            "caller-owned Database.transaction() block (write-lock invariant #569)"
        )
        cur = await write_conn.execute("UPDATE channels SET is_filtered = 0, filter_flags = ''")
        rowcount = cur.rowcount if cur.rowcount is not None else 0
        return rowcount if rowcount > 0 else 0

    async def reset_filters_for_pks(self, pks: list[int], *, commit: bool = True) -> int:
        """Снять фильтрацию только с указанных pk; вернуть число снятых. ``commit`` как в :meth:`set_filtered_bulk`."""
        assert self._database is not None, (
            "ChannelsRepository.reset_filters_for_pks requires a Database reference"
        )
        if not pks:
            return 0
        placeholders = ",".join("?" * len(pks))
        sql = (
            f"UPDATE channels SET is_filtered = 0, filter_flags = '' "
            f"WHERE is_filtered = 1 AND id IN ({placeholders})"
        )
        if commit:
            assert self._database is not None, (
                "ChannelsRepository.reset_filters_for_pks requires a Database reference "
                "when commit=True"
            )
            cur = await self._database.execute_write(sql, tuple(pks))
            rowcount = cur.rowcount if cur.rowcount is not None else 0
            return rowcount if rowcount > 0 else 0
        # commit=False: write on the caller's open write transaction (see set_filtered_bulk).
        db = self._database
        write_conn = db.db
        assert write_conn is not None, (
            "ChannelsRepository.reset_filters_for_pks requires an active Database connection"
        )
        # Enforce the #569 invariant — see set_filtered_bulk(commit=False) (#1182).
        assert write_conn.in_transaction, (
            "ChannelsRepository.reset_filters_for_pks(commit=False) must run inside a "
            "caller-owned Database.transaction() block (write-lock invariant #569)"
        )
        cur = await write_conn.execute(sql, tuple(pks))
        rowcount = cur.rowcount if cur.rowcount is not None else 0
        return rowcount if rowcount > 0 else 0

    async def set_channel_type(self, channel_id: int, channel_type: str) -> None:
        """Обновить тип канала (channel/supergroup/group/…) по Telegram ``channel_id``."""
        assert self._database is not None, (
            "ChannelsRepository.set_channel_type requires a Database reference"
        )
        await self._database.execute_write(
            "UPDATE channels SET channel_type=? WHERE channel_id=?",
            (channel_type, channel_id),
        )

    async def update_channel_meta(
        self, channel_id: int, *, username: str | None, title: str | None
    ) -> None:
        """Обновить username и title канала (после переименования/смены @username)."""
        assert self._database is not None, (
            "ChannelsRepository.update_channel_meta requires a Database reference"
        )
        await self._database.execute_write(
            "UPDATE channels SET username = ?, title = ? WHERE channel_id = ?",
            (username, title, channel_id),
        )

    async def update_channel_full_meta(
        self, channel_id: int, *, about: str | None, linked_chat_id: int | None, has_comments: bool
    ) -> None:
        """Обновить расширенные метаданные канала: описание, привязанный чат и наличие комментариев."""
        assert self._database is not None, (
            "ChannelsRepository.update_channel_full_meta requires a Database reference"
        )
        await self._database.execute_write(
            "UPDATE channels SET about = ?, linked_chat_id = ?, has_comments = ? WHERE channel_id = ?",
            (about, linked_chat_id, int(has_comments), channel_id),
        )

    async def update_channel_preferred_phone(
        self, channel_id: int, phone: str | None
    ) -> None:
        """Set or clear the preferred Telegram account phone for collecting this channel."""
        assert self._database is not None, (
            "ChannelsRepository.update_channel_preferred_phone requires a Database reference"
        )
        await self._database.execute_write(
            "UPDATE channels SET preferred_phone = ? WHERE channel_id = ?",
            (phone, channel_id),
        )

    async def get_preferred_phone(self, channel_id: int) -> str | None:
        """Return the preferred phone for a channel, or None if not set."""
        cur = await self._db.execute(
            "SELECT preferred_phone FROM channels WHERE channel_id = ?",
            (channel_id,),
        )
        row = await cur.fetchone()
        return row["preferred_phone"] if row else None

    async def clear_preferred_phone_if_matches(
        self, channel_id: int, expected_phone: str
    ) -> None:
        """Atomically clear preferred_phone only if it still equals ``expected_phone``.

        A single conditional ``UPDATE ... WHERE preferred_phone = ?`` — no SELECT.
        The read-then-write form (SELECT current, compare in Python, unconditional
        UPDATE) has a TOCTOU window: a concurrent task that persisted a NEW valid
        owner between the SELECT and the UPDATE would be clobbered to NULL. The
        conditional WHERE makes the compare-and-clear atomic at the DB, so a
        stale error-recovery task only NULLs the row while it still points at the
        account that just failed (#1245 dual-review).
        """
        assert self._database is not None, (
            "ChannelsRepository.clear_preferred_phone_if_matches requires a Database reference"
        )
        await self._database.execute_write(
            "UPDATE channels SET preferred_phone = NULL "
            "WHERE channel_id = ? AND preferred_phone = ?",
            (channel_id, expected_phone),
        )

    async def update_channel_created_at(self, channel_id: int, created_at) -> None:
        """Set created_at only if currently NULL (backfill from entity.date)."""
        assert self._database is not None, (
            "ChannelsRepository.update_channel_created_at requires a Database reference"
        )
        iso = created_at.isoformat() if hasattr(created_at, "isoformat") else created_at
        await self._database.execute_write(
            "UPDATE channels SET created_at = ? WHERE channel_id = ? AND created_at IS NULL",
            (iso, channel_id),
        )

    async def get_forum_topics(self, channel_id: int) -> list[dict]:
        """Темы форум-супергруппы как ``[{"id", "title"}]``, по возрастанию topic_id."""
        cur = await self._db.execute(
            "SELECT topic_id, title FROM forum_topics WHERE channel_id = ? ORDER BY topic_id",
            (channel_id,),
        )
        rows = await cur.fetchall()
        return [{"id": row["topic_id"], "title": row["title"]} for row in rows]

    async def upsert_forum_topics(self, channel_id: int, topics: list[dict]) -> None:
        """Полностью заменить список тем форума канала на ``topics`` (delete-then-insert в одной транзакции)."""
        assert self._database is not None, (
            "ChannelsRepository.upsert_forum_topics requires a Database reference"
        )
        async with self._database.transaction() as conn:
            await conn.execute("DELETE FROM forum_topics WHERE channel_id = ?", (channel_id,))
            if topics:
                await conn.executemany(
                    "INSERT INTO forum_topics (channel_id, topic_id, title, updated_at)"
                    " VALUES (?, ?, ?, datetime('now'))",
                    [(channel_id, t["id"], t["title"]) for t in topics],
                )

    async def delete_channel(self, pk: int) -> None:
        """Жёстко удалить канал и все его сайдкар-данные одной атомарной транзакцией (#569/#1039).

        Чистит сообщения, оба стора эмбеддингов, статистику, темы и леджеры
        (reactions каскадом, rename/rating/notified/action_log явно), чтобы не
        осталось сирот, указывающих на исчезнувший канал. Поднимает
        ``IntegrityError``, если на канал ссылается ``pipeline_sources``
        (RESTRICT FK) — тогда удаление откатывается целиком.
        """
        # Atomic delete via the connection-wide write lock + BEGIN
        # IMMEDIATE (issue #569). The only RESTRICT FK on `channels` is
        # `pipeline_sources.channel_id` (src/database/schema.py:326);
        # the preflight check and the child/parent deletes run inside
        # Database.transaction(), which holds Database._write_lock for
        # the whole block — no other coroutine on this aiosqlite
        # connection can interleave a DML statement and commit our open
        # transaction prematurely. BEGIN IMMEDIATE itself blocks
        # writers on *other* connections behind SQLite's RESERVED lock.
        #
        # If a new RESTRICT FK on `channels` is added later, the
        # preflight check below must grow with it.
        assert self._database is not None, (
            "ChannelsRepository.delete_channel requires a Database reference"
        )
        async with self._database.transaction() as conn:
            cur = await conn.execute(
                "SELECT channel_id FROM channels WHERE id = ?", (pk,),
            )
            row = await cur.fetchone()
            if not row:
                return
            channel_id = row["channel_id"]
            cur = await conn.execute(
                "SELECT 1 FROM pipeline_sources WHERE channel_id = ? LIMIT 1",
                (channel_id,),
            )
            if await cur.fetchone() is not None:
                raise aiosqlite.IntegrityError(
                    "FOREIGN KEY constraint failed: pipeline_sources references "
                    f"channel_id={channel_id}"
                )
            # Both embedding stores key on messages.id (the rowid) with no FK, so
            # they must be cleared *before* the messages they point at are gone —
            # the subquery resolves messages.id while the rows still exist (#1039).
            # Leaving them orphaned is not just dead rows: messages.id is INTEGER
            # PRIMARY KEY without AUTOINCREMENT, so SQLite can reissue a deleted
            # rowid to a future message, and both stores use INSERT OR REPLACE on
            # message_id alone — a new message could silently inherit a stale
            # vector. Clear the JSON store (#173) and the older BLOB index
            # (Codex cycle-2 review) together; purge does the same.
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
            await conn.execute(
                "DELETE FROM messages WHERE channel_id = ?", (channel_id,),
            )
            await conn.execute(
                "DELETE FROM channel_stats WHERE channel_id = ?", (channel_id,),
            )
            await conn.execute(
                "DELETE FROM forum_topics WHERE channel_id = ?", (channel_id,),
            )
            # Sidecar tables keyed on `channel_id`/`message_id` with no FK back to
            # `channels` (so no automatic cascade) would otherwise survive as
            # orphans pointing at a channel that no longer exists (#1039). These
            # run after the FK RESTRICT preflight above, so a blocked delete still
            # rolls back fully — atomicity is preserved. `message_reactions` is
            # cascaded by the messages DELETE above; `channel_tags` cascades on the
            # `channels` row delete below.
            await conn.execute(
                "DELETE FROM channel_ratings WHERE channel_id = ?", (channel_id,),
            )
            await conn.execute(
                "DELETE FROM channel_rename_events WHERE channel_id = ?", (channel_id,),
            )
            await conn.execute(
                "DELETE FROM notified_messages WHERE channel_id = ?", (channel_id,),
            )
            await conn.execute(
                "DELETE FROM pipeline_action_log WHERE channel_id = ?", (channel_id,),
            )
            await conn.execute("DELETE FROM channels WHERE id = ?", (pk,))

    # ── Tag helpers ──────────────────────────────────────────────────────────

    async def list_all_tags(self) -> list[str]:
        """Все существующие имена тегов, отсортированные по алфавиту."""
        cur = await self._db.execute("SELECT name FROM tags ORDER BY name")
        return [row["name"] for row in await cur.fetchall()]

    async def create_tag(self, name: str) -> None:
        """Создать тег по имени (пустое игнорируется, дубликат — no-op через INSERT OR IGNORE)."""
        assert self._database is not None, (
            "ChannelsRepository.create_tag requires a Database reference"
        )
        name = name.strip()
        if not name:
            return
        await self._database.execute_write("INSERT OR IGNORE INTO tags (name) VALUES (?)", (name,))

    async def delete_tag(self, name: str) -> None:
        """Удалить тег по имени (связи каналов с ним уходят каскадом по FK)."""
        assert self._database is not None, (
            "ChannelsRepository.delete_tag requires a Database reference"
        )
        await self._database.execute_write("DELETE FROM tags WHERE name = ?", (name,))

    async def get_channel_tags(self, channel_pk: int) -> list[str]:
        """Имена тегов, присвоенных каналу (по его pk), по алфавиту."""
        cur = await self._db.execute(
            """SELECT t.name FROM tags t
               JOIN channel_tags ct ON ct.tag_id = t.id
               WHERE ct.channel_pk = ?
               ORDER BY t.name""",
            (channel_pk,),
        )
        return [row["name"] for row in await cur.fetchall()]

    async def set_channel_tags(self, channel_pk: int, tag_names: list[str]) -> None:
        """Полностью заменить набор тегов канала на ``tag_names`` (недостающие теги создаются), одной транзакцией."""
        assert self._database is not None, (
            "ChannelsRepository.set_channel_tags requires a Database reference"
        )
        tag_names = [n.strip() for n in tag_names if n.strip()]
        async with self._database.transaction() as conn:
            await conn.execute("DELETE FROM channel_tags WHERE channel_pk = ?", (channel_pk,))
            for name in tag_names:
                await conn.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (name,))
                await conn.execute(
                    """INSERT OR IGNORE INTO channel_tags (channel_pk, tag_id)
                       SELECT ?, id FROM tags WHERE name = ?""",
                    (channel_pk, name),
                )
