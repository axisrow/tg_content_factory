"""Internal embeddings methods sharing the MessagesRepository connection."""

from __future__ import annotations

import json
import logging
import struct
from typing import TYPE_CHECKING, AsyncIterator

if TYPE_CHECKING:
    from tg_agent.database.repositories.messages import MessagesRepository

logger = logging.getLogger("tg_agent.database.repositories.messages")
_EMBEDDING_DIMENSIONS_SETTING = "semantic_embedding_dimensions"



class MessageEmbeddingsMixin:
    """Used through MessagesRepository; owns no connection or transaction state."""

    async def _get_setting(self: "MessagesRepository", key: str) -> str | None:
        cur = await self._db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cur.fetchone()
        return row["value"] if row else None

    async def _set_setting(self: "MessagesRepository", key: str, value: str) -> None:
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

    async def get_embedding_dimensions(self: "MessagesRepository") -> int | None:
        """Размерность векторов индекса эмбеддингов (из настроек), либо ``None`` если индекс ещё не создан."""
        raw_value = await self._get_setting(_EMBEDDING_DIMENSIONS_SETTING)
        if raw_value is None or raw_value == "":
            return None
        try:
            return int(raw_value)
        except (TypeError, ValueError):
            logger.warning("Invalid %s setting value %r", _EMBEDDING_DIMENSIONS_SETTING, raw_value)
            return None

    async def count_embeddings(self: "MessagesRepository") -> int:
        """Число сохранённых эмбеддингов (0, если таблицы ещё нет)."""
        try:
            cur = await self._db.execute("SELECT COUNT(*) AS cnt FROM message_embeddings")
        except Exception:
            return 0
        row = await cur.fetchone()
        return int(row["cnt"]) if row else 0

    async def reset_embeddings_index(self: "MessagesRepository") -> None:
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

    async def ensure_embeddings_table(self: "MessagesRepository", dimensions: int) -> None:
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
        self: "MessagesRepository",
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

    async def upsert_message_embeddings(self: "MessagesRepository", embeddings: list[tuple[int, list[float]]]) -> int:
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
        self: "MessagesRepository", embeddings: list[tuple[int, list[float]]]
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

    async def iter_embedding_json_batches(
        self: "MessagesRepository", include_filtered: bool = False, *, batch_size: int = 512
    ) -> AsyncIterator[list[tuple[int, str]]]:
        """Read bounded keyset pages; never buffer the full JSON corpus in the pool."""
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        last_id = -1
        while True:
            sql = (
                "SELECT e.message_id, e.embedding FROM message_embeddings_json e "
                "JOIN messages m ON m.id = e.message_id "
                "LEFT JOIN channels c ON m.channel_id = c.channel_id "
                "WHERE e.message_id > ? "
            )
            if not include_filtered:
                sql += "AND (c.is_filtered IS NULL OR c.is_filtered = 0) "
            sql += "ORDER BY e.message_id LIMIT ?"
            cur = await self._db.execute(sql, (last_id, batch_size))
            rows = await cur.fetchall()
            if not rows:
                return
            last_id = int(rows[-1]["message_id"])
            yield [(int(row["message_id"]), row["embedding"]) for row in rows]

    async def load_all_embeddings_json(
        self: "MessagesRepository", include_filtered: bool = False
    ) -> list[tuple[int, list[float]]]:
        """Load embeddings from the portable JSON table.

        Filtered channels are excluded unless ``include_filtered`` — the
        caller decides; excluding them unconditionally made
        ``search_semantic(include_filtered=True)`` return nothing because the
        messages were never in the index at all (issue #1541).
        """
        sql = (
            "SELECT e.message_id, e.embedding FROM message_embeddings_json e "
            "JOIN messages m ON m.id = e.message_id "
            "LEFT JOIN channels c ON m.channel_id = c.channel_id "
        )
        if not include_filtered:
            sql += "WHERE (c.is_filtered IS NULL OR c.is_filtered = 0) "
        sql += "ORDER BY e.message_id"
        cur = await self._db.execute(sql)
        rows = await cur.fetchall()
        result: list[tuple[int, list[float]]] = []
        for row in rows:
            try:
                vec = json.loads(row["embedding"])
                result.append((int(row["message_id"]), vec))
            except Exception:
                continue
        return result
