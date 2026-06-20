from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

import aiosqlite

logger = logging.getLogger(__name__)

_PROFILING_ENABLED = os.environ.get("ENV", "PROD").upper() == "DEV"


@dataclass(frozen=True)
class ConnectionTuning:
    """Per-connection SQLite tuning knobs (#760).

    ``cache_size_kb`` is page cache per connection — total committed RAM is
    roughly ``cache_size_kb * (read_pool_size + 1)``, so it stays operator-tunable
    rather than blindly raised. ``mmap_size_mb`` is memory-mapped I/O size; it is
    virtual address space backed by the shared OS page cache (not per-connection
    committed RAM), so a larger default helps big databases at little cost.
    """

    cache_size_kb: int = 64000  # 64 MB page cache per connection
    mmap_size_mb: int = 256  # 256 MB mmap window (was 30 MB) — issue #760


DEFAULT_TUNING = ConnectionTuning()


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


async def apply_pragmas(
    conn: aiosqlite.Connection, *, role: str = "write", tuning: ConnectionTuning = DEFAULT_TUNING
) -> None:
    """Apply the connection-tuning PRAGMAs shared by every connection (#760).

    ``role="write"`` runs the full set including WAL setup/hygiene; ``role="read"``
    skips the write-only checkpoint PRAGMAs (a read connection never checkpoints)
    but keeps the per-connection cache/mmap/temp tuning so pool readers are as fast
    as the writer.
    """
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.execute("PRAGMA foreign_keys=ON")
    await conn.execute(f"PRAGMA cache_size=-{tuning.cache_size_kb}")
    await conn.execute("PRAGMA temp_store=MEMORY")
    await conn.execute(f"PRAGMA mmap_size={tuning.mmap_size_mb * 1024 * 1024}")
    # Bound ANALYZE work so `PRAGMA optimize` (run on close) stays fast on
    # million-row tables yet still refreshes stale planner stats (#760, SQLite
    # recommended value).
    await conn.execute("PRAGMA analysis_limit=400")
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


async def open_connection(
    db_path: str, *, role: str = "write", tuning: ConnectionTuning = DEFAULT_TUNING
) -> aiosqlite.Connection:
    """Open a single aiosqlite connection with the shared tuning applied.

    Used both for the lone write connection and for each reader in the pool (#760).
    """
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(db_path, timeout=10.0, isolation_level=None)
    conn.row_factory = aiosqlite.Row
    await apply_pragmas(conn, role=role, tuning=tuning)
    return maybe_wrap_profiling(conn)


class DBConnection:
    def __init__(self, db_path: str, *, tuning: ConnectionTuning = DEFAULT_TUNING):
        self._db_path = db_path
        self._tuning = tuning
        self.db: aiosqlite.Connection | None = None

    async def connect(self) -> aiosqlite.Connection:
        self.db = await open_connection(self._db_path, role="write", tuning=self._tuning)
        return self.db

    async def close(self) -> None:
        if self.db:
            db = self.db
            # Refresh planner statistics for tables that changed enough since the
            # last run, so the next process start plans queries well on big DBs
            # (#760). Bounded by PRAGMA analysis_limit; best-effort.
            try:
                await db.execute("PRAGMA optimize")
            except Exception:
                logger.debug("PRAGMA optimize on close failed", exc_info=True)
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
