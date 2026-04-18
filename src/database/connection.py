from __future__ import annotations

import os
import time
from pathlib import Path

import aiosqlite

_PROFILING_ENABLED = os.environ.get("ENV", "PROD").upper() == "DEV"


class ProfilingConnection:
    """Thin proxy around aiosqlite.Connection that records query timing via ContextVar."""

    __slots__ = ("_conn",)

    def __init__(self, conn: aiosqlite.Connection):
        self._conn = conn

    def __getattr__(self, name):
        return getattr(self._conn, name)

    async def execute(self, sql, parameters=()):
        from src.web.timing import get_current_profiler

        profiler = get_current_profiler()
        if profiler is None:
            return await self._conn.execute(sql, parameters)
        t0 = time.perf_counter_ns()
        try:
            return await self._conn.execute(sql, parameters)
        finally:
            profiler.record_db(time.perf_counter_ns() - t0)

    async def execute_fetchall(self, sql, parameters=()):
        from src.web.timing import get_current_profiler

        profiler = get_current_profiler()
        if profiler is None:
            return await self._conn.execute_fetchall(sql, parameters)
        t0 = time.perf_counter_ns()
        try:
            return await self._conn.execute_fetchall(sql, parameters)
        finally:
            profiler.record_db(time.perf_counter_ns() - t0)

    async def executemany(self, sql, parameters):
        from src.web.timing import get_current_profiler

        profiler = get_current_profiler()
        if profiler is None:
            return await self._conn.executemany(sql, parameters)
        t0 = time.perf_counter_ns()
        try:
            return await self._conn.executemany(sql, parameters)
        finally:
            profiler.record_db(time.perf_counter_ns() - t0)

    async def executescript(self, sql):
        from src.web.timing import get_current_profiler

        profiler = get_current_profiler()
        if profiler is None:
            return await self._conn.executescript(sql)
        t0 = time.perf_counter_ns()
        try:
            return await self._conn.executescript(sql)
        finally:
            profiler.record_db(time.perf_counter_ns() - t0)


class DBConnection:
    def __init__(self, db_path: str):
        self._db_path = db_path
        self.db: aiosqlite.Connection | None = None

    async def connect(self) -> aiosqlite.Connection:
        if self._db_path != ":memory:":
            Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db = await aiosqlite.connect(self._db_path, timeout=10.0, isolation_level=None)
        self.db.row_factory = aiosqlite.Row
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("PRAGMA synchronous=NORMAL")
        await self.db.execute("PRAGMA foreign_keys=ON")
        await self.db.execute("PRAGMA cache_size=-64000")  # 64MB
        await self.db.execute("PRAGMA temp_store=MEMORY")
        await self.db.execute("PRAGMA mmap_size=30000000")  # 30MB
        if _PROFILING_ENABLED:
            self.db = ProfilingConnection(self.db)  # type: ignore[assignment]
        return self.db

    async def close(self) -> None:
        if self.db:
            db = self.db
            await db.close()
            self.db = None

    async def execute(self, sql: str, params: tuple = ()) -> aiosqlite.Cursor:
        assert self.db is not None
        if sql.strip().upper().startswith("BEGIN") and self.db.in_transaction:
            await self.db.rollback()
        return await self.db.execute(sql, params)

    async def execute_fetchall(self, sql: str, params: tuple = ()) -> list:
        assert self.db is not None
        return await self.db.execute_fetchall(sql, params)
