from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import aiosqlite

logger = logging.getLogger(__name__)

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


async def apply_pragmas(conn: aiosqlite.Connection, *, role: str = "write") -> None:
    """Apply the connection-tuning PRAGMAs shared by every connection (#760).

    ``role="write"`` runs the full set including WAL setup/hygiene; ``role="read"``
    skips the write-only checkpoint PRAGMAs (a read connection never checkpoints)
    but keeps the per-connection cache/mmap/temp tuning so pool readers are as fast
    as the writer.
    """
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.execute("PRAGMA foreign_keys=ON")
    await conn.execute("PRAGMA cache_size=-64000")  # 64MB
    await conn.execute("PRAGMA temp_store=MEMORY")
    await conn.execute("PRAGMA mmap_size=30000000")  # 30MB
    if role == "write":
        await conn.execute("PRAGMA journal_mode=WAL")
        # WAL hygiene (#766): make the autocheckpoint threshold explicit and
        # trim a WAL grown by a previous run. PASSIVE never blocks other
        # connections — it simply does nothing while readers hold snapshots.
        await conn.execute("PRAGMA wal_autocheckpoint=1000")
        await conn.execute("PRAGMA wal_checkpoint(PASSIVE)")


def maybe_wrap_profiling(conn: aiosqlite.Connection) -> aiosqlite.Connection:
    """Wrap a connection in ProfilingConnection when profiling is enabled (ENV=DEV)."""
    if _PROFILING_ENABLED:
        return ProfilingConnection(conn)  # type: ignore[return-value]
    return conn


async def open_connection(db_path: str, *, role: str = "write") -> aiosqlite.Connection:
    """Open a single aiosqlite connection with the shared tuning applied.

    Used both for the lone write connection and for each reader in the pool (#760).
    """
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(db_path, timeout=10.0, isolation_level=None)
    conn.row_factory = aiosqlite.Row
    await apply_pragmas(conn, role=role)
    return maybe_wrap_profiling(conn)


class DBConnection:
    def __init__(self, db_path: str):
        self._db_path = db_path
        self.db: aiosqlite.Connection | None = None

    async def connect(self) -> aiosqlite.Connection:
        self.db = await open_connection(self._db_path, role="write")
        return self.db

    async def close(self) -> None:
        if self.db:
            db = self.db
            await db.close()
            self.db = None

    async def execute(self, sql: str, params: tuple = ()) -> aiosqlite.Cursor:
        assert self.db is not None
        if sql.strip().upper().startswith("BEGIN") and self.db.in_transaction:
            logger.warning("DBConnection.execute: rolling back active transaction before BEGIN")
            await self.db.rollback()
        return await self.db.execute(sql, params)

    async def execute_fetchall(self, sql: str, params: tuple = ()) -> list:
        assert self.db is not None
        return await self.db.execute_fetchall(sql, params)
