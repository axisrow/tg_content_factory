from __future__ import annotations

from pathlib import Path

import aiosqlite


class DBConnection:
    def __init__(self, db_path: str):
        self._db_path = db_path
        self.db: aiosqlite.Connection | None = None

    async def connect(self) -> aiosqlite.Connection:
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db = await aiosqlite.connect(self._db_path, timeout=10.0, isolation_level=None)
        self.db.row_factory = aiosqlite.Row
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("PRAGMA synchronous=NORMAL")
        await self.db.execute("PRAGMA foreign_keys=ON")
        return self.db

    async def close(self) -> None:
        if self.db:
            db = self.db
            self.db = None
            await db.close()

    async def execute(self, sql: str, params: tuple = ()) -> aiosqlite.Cursor:
        assert self.db is not None
        return await self.db.execute(sql, params)

    async def execute_fetchall(self, sql: str, params: tuple = ()) -> list:
        assert self.db is not None
        return await self.db.execute_fetchall(sql, params)
