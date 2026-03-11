from __future__ import annotations

from datetime import datetime

import aiosqlite


def _dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


class DialogCacheRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    async def get_dialog(self, phone: str, dialog_id: int) -> dict | None:
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
        await self._db.execute("BEGIN IMMEDIATE")
        try:
            await self._db.execute("DELETE FROM dialog_cache WHERE phone = ?", (phone,))
            if dialogs:
                cached_at = datetime.now().isoformat()
                await self._db.executemany(
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
            await self._db.commit()
        except Exception:
            await self._db.rollback()
            raise

    async def clear_dialogs(self, phone: str) -> None:
        await self._db.execute("DELETE FROM dialog_cache WHERE phone = ?", (phone,))
        await self._db.commit()

    async def has_dialogs(self, phone: str) -> bool:
        cur = await self._db.execute(
            "SELECT 1 FROM dialog_cache WHERE phone = ? LIMIT 1",
            (phone,),
        )
        return bool(await cur.fetchone())

    async def get_cached_at(self, phone: str) -> datetime | None:
        cur = await self._db.execute(
            "SELECT MAX(cached_at) AS cached_at FROM dialog_cache WHERE phone = ?",
            (phone,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        return _dt(row["cached_at"])
