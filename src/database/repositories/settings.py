from __future__ import annotations

import aiosqlite


class SettingsRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    async def get_setting(self, key: str) -> str | None:
        cur = await self._db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cur.fetchone()
        return row["value"] if row else None

    async def list_all(self) -> list[tuple[str, str]]:
        cur = await self._db.execute("SELECT key, value FROM settings ORDER BY key")
        rows = await cur.fetchall()
        return [(r["key"], r["value"]) for r in rows]

    async def set_setting(self, key: str, value: str) -> None:
        await self._db.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        await self._db.commit()
