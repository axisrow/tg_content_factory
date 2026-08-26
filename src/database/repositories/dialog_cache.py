"""Репозиторий кэша диалогов Telegram по аккаунту (снимок для UI без обращения к TG)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from src.database.pool import ReadConnection
from src.utils.datetime import parse_utc_datetime

if TYPE_CHECKING:
    from src.database.facade import Database


class DialogCacheRepository:
    """Кэш диалогов Telegram по аккаунту (`phone`).

    Хранит снимок списка диалогов (id, название, username, тип, флаги
    deactivate/is_own), чтобы web/CLI показывали диалоги без обращения к
    Telegram. Полный снимок записывается через `replace_dialogs`, а прогресс
    частичного обхода — через `upsert_dialogs`; читается остальными слоями.
    """

    def __init__(
        self,
        db: ReadConnection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

    async def get_dialog(self, phone: str, dialog_id: int) -> dict | None:
        """Один кэшированный диалог аккаунта по его id, либо None."""
        cur = await self._db.execute(
            """
            SELECT dialog_id, title, username, channel_type, deactivate, is_own
            FROM dialog_cache
            WHERE phone = ? AND dialog_id = ?
            LIMIT 1
            """,
            (phone, dialog_id),
        )
        row = await cur.fetchone()
        if not row:
            return None
        return {
            "channel_id": row["dialog_id"],
            "title": row["title"],
            "username": row["username"],
            "channel_type": row["channel_type"],
            "deactivate": bool(row["deactivate"]),
            "is_own": bool(row["is_own"]),
        }

    async def list_dialogs(self, phone: str) -> list[dict]:
        """Все кэшированные диалоги аккаунта в порядке их сохранения."""
        cur = await self._db.execute(
            """
            SELECT dialog_id, title, username, channel_type, deactivate, is_own
            FROM dialog_cache
            WHERE phone = ?
            ORDER BY id ASC
            """,
            (phone,),
        )
        rows = await cur.fetchall()
        return [
            {
                "channel_id": row["dialog_id"],
                "title": row["title"],
                "username": row["username"],
                "channel_type": row["channel_type"],
                "deactivate": bool(row["deactivate"]),
                "is_own": bool(row["is_own"]),
            }
            for row in rows
        ]

    async def replace_dialogs(self, phone: str, dialogs: list[dict]) -> None:
        """Атомарно заменить весь кэш диалогов аккаунта новым снимком.

        Под транзакцией удаляет прежние строки телефона и вставляет переданные,
        проставляя `cached_at`. Так кэш всегда консистентен — без частично
        обновлённого состояния.
        """
        assert self._database is not None, (
            "DialogCacheRepository.replace_dialogs requires a Database reference"
        )
        async with self._database.transaction() as conn:
            await conn.execute("DELETE FROM dialog_cache WHERE phone = ?", (phone,))
            if dialogs:
                cached_at = datetime.now(timezone.utc).isoformat()
                await conn.executemany(
                    """
                    INSERT INTO dialog_cache (
                        phone, dialog_id, title, username, channel_type,
                        deactivate, is_own, cached_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            phone,
                            int(dialog["channel_id"]),
                            dialog.get("title"),
                            dialog.get("username"),
                            dialog.get("channel_type"),
                            1 if dialog.get("deactivate") else 0,
                            1 if dialog.get("is_own") else 0,
                            cached_at,
                        )
                        for dialog in dialogs
                    ],
                )

    async def upsert_dialogs(self, phone: str, dialogs: list[dict]) -> None:
        """Add or update the dialogs reached by a partial Telegram walk.

        A partial walk must not replace the existing snapshot: Telethon can be
        stopped by a timeout or FloodWait after yielding only the first page.
        Keeping those rows makes newly-created dialogs visible while retaining
        the remainder of the last successful snapshot for the next pass.
        """
        assert self._database is not None, (
            "DialogCacheRepository.upsert_dialogs requires a Database reference"
        )
        if not dialogs:
            return
        async with self._database.transaction() as conn:
            # Keep a partial snapshot stale when it was already stale.  The
            # cache freshness check uses MAX(cached_at) for the whole phone;
            # stamping only the rows reached in this pass would incorrectly
            # make the untouched tail look fresh.
            cur = await conn.execute(
                "SELECT MAX(cached_at) AS cached_at FROM dialog_cache WHERE phone = ?",
                (phone,),
            )
            row = await cur.fetchone()
            # MAX() without GROUP BY always yields one row, even for an empty
            # phone snapshot; keep that invariant explicit for type checkers.
            assert row is not None
            # A first partial snapshot is not complete/fresh.  Use an old
            # marker so the next ordinary read retries Telegram instead of
            # treating incomplete rows as authoritative.
            cached_at = row["cached_at"] or "1970-01-01T00:00:00+00:00"
            await conn.executemany(
                """
                INSERT INTO dialog_cache (
                    phone, dialog_id, title, username, channel_type,
                    deactivate, is_own, cached_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(phone, dialog_id) DO UPDATE SET
                    title = excluded.title,
                    username = excluded.username,
                    channel_type = excluded.channel_type,
                    deactivate = excluded.deactivate,
                    is_own = excluded.is_own,
                    cached_at = excluded.cached_at
                """,
                [
                    (
                        phone,
                        int(dialog["channel_id"]),
                        dialog.get("title"),
                        dialog.get("username"),
                        dialog.get("channel_type"),
                        1 if dialog.get("deactivate") else 0,
                        1 if dialog.get("is_own") else 0,
                        cached_at,
                    )
                    for dialog in dialogs
                ],
            )

    async def clear_dialogs(self, phone: str) -> None:
        """Удалить кэш диалогов одного аккаунта."""
        assert self._database is not None, (
            "DialogCacheRepository.clear_dialogs requires a Database reference"
        )
        await self._database.execute_write("DELETE FROM dialog_cache WHERE phone = ?", (phone,))

    async def remove_dialogs(self, phone: str, dialog_ids: list[int] | set[int]) -> None:
        """Remove only the specified dialogs from an account's cache."""
        assert self._database is not None, (
            "DialogCacheRepository.remove_dialogs requires a Database reference"
        )
        ids = [int(dialog_id) for dialog_id in dialog_ids]
        if not ids:
            return
        placeholders = ", ".join("?" for _ in ids)
        await self._database.execute_write(
            f"DELETE FROM dialog_cache WHERE phone = ? AND dialog_id IN ({placeholders})",
            (phone, *ids),
        )

    async def get_cached_at(self, phone: str) -> datetime | None:
        """Время самого свежего кэшированного диалога аккаунта (для оценки устаревания).

        Всегда UTC-aware: строки, вставленные без явного `cached_at`, получают его
        из схемного DEFAULT `datetime('now')` — naive-значение без офсета, вычитание
        которого из aware-времени падало бы с TypeError.
        """
        cur = await self._db.execute(
            "SELECT MAX(cached_at) AS cached_at FROM dialog_cache WHERE phone = ?",
            (phone,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        return parse_utc_datetime(row["cached_at"])

    async def get_all_phones(self) -> list[str]:
        """Return all distinct phone numbers that have entries in dialog_cache."""
        cur = await self._db.execute("SELECT DISTINCT phone FROM dialog_cache ORDER BY phone ASC")
        rows = await cur.fetchall()
        return [row["phone"] for row in rows]

    async def count_dialogs(self, phone: str) -> int:
        """Return the number of cached dialog entries for the given phone."""
        cur = await self._db.execute(
            "SELECT COUNT(*) AS cnt FROM dialog_cache WHERE phone = ?",
            (phone,),
        )
        row = await cur.fetchone()
        return int(row["cnt"]) if row else 0

    async def clear_all_dialogs(self) -> None:
        """Delete all entries from dialog_cache regardless of phone."""
        assert self._database is not None, (
            "DialogCacheRepository.clear_all_dialogs requires a Database reference"
        )
        await self._database.execute_write("DELETE FROM dialog_cache")
