"""Async read-connection pool for SQLite (#760).

The web process historically shared a single aiosqlite connection, which serialises
every query through one background thread: a single slow SELECT (e.g. a >90s trend
aggregation on a large DB) blocks *all* other queries, so the whole site hangs.

This module adds a pool of N read connections. WAL mode allows many concurrent
readers alongside the lone writer, so a long-running SELECT holds only its own
read connection while the remaining readers stay free for navigation. All writes
continue to go through the single write connection under the facade write-lock
(issue #569), preserving the "one writer" contract.

Read repositories call ``self._db.execute(...)`` then ``cur.fetchall()`` in a
*separate* await. A pooled connection would be returned to the pool before that
second await, so a live cursor is unsafe. ``ReadPoolProxy.execute`` therefore reads
all rows while still holding the connection and returns a ``BufferedCursor`` — the
fetch* calls then read from the buffer, not the (already-released) connection.
"""
from __future__ import annotations

import asyncio
from collections.abc import Sequence
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Protocol

import aiosqlite

from src.database.connection import DEFAULT_TUNING, ConnectionTuning, open_connection


class ReadConnection(Protocol):
    """Structural read-only connection interface used by repositories."""

    async def execute(self, sql: str, params: Sequence[Any] = ()) -> Any:
        """Return a cursor-like object with async fetchone/fetchall."""
        ...

    async def create_function(self, name: str, narg: int, func: Any, **kwargs: Any) -> None: ...


class BufferedCursor:
    """A cursor whose rows are already materialised, safe after the connection is released.

    Read repositories only call ``fetchone``/``fetchall`` on read cursors (verified: no
    ``async for`` over a live cursor, and ``lastrowid``/``rowcount`` are read only on write
    cursors), so buffering the rows up front is transparent. ``.description`` is
    intentionally unsupported — no read caller uses it.
    """

    __slots__ = ("_rows", "_pos")

    def __init__(self, rows: list[Any]):
        self._rows = rows
        self._pos = 0

    @property
    def rowcount(self) -> int:
        return len(self._rows)

    async def fetchone(self) -> Any | None:
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    async def fetchall(self) -> list[Any]:
        rows = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rows

    async def close(self) -> None:
        # No-op: the underlying connection was already released back to the pool.
        return


class ReadConnectionPool:
    """A fixed-size pool of read connections handed out via an asyncio.Queue.

    The Queue gives FIFO hand-out and natural backpressure: when all readers are
    busy, ``acquire_read`` waits for one to be released rather than opening more.

    For ``:memory:`` the pool degenerates to a single connection that *is* the
    write connection (each ``:memory:`` connect would otherwise be a separate empty
    DB). ``owns_conns`` is then False so ``close`` does not double-close the shared
    write connection.
    """

    def __init__(self, db_path: str, *, size: int, tuning: ConnectionTuning = DEFAULT_TUNING):
        self._db_path = db_path
        self._size = max(1, size)
        self._tuning = tuning
        self._queue: asyncio.Queue[aiosqlite.Connection] = asyncio.Queue()
        self._all_conns: list[aiosqlite.Connection] = []
        self.owns_conns = True

    async def open(self, shared_conn: aiosqlite.Connection | None = None) -> None:
        """Open the pool. For ``:memory:`` pass ``shared_conn`` (the write connection)."""
        if shared_conn is not None:
            # :memory: — read and write share one connection so they see one DB.
            self.owns_conns = False
            self._all_conns = [shared_conn]
            self._queue.put_nowait(shared_conn)
            return
        for _ in range(self._size):
            conn = await open_connection(self._db_path, role="read", tuning=self._tuning)
            self._all_conns.append(conn)
            self._queue.put_nowait(conn)

    async def register_function(self, name: str, narg: int, func: Any, **kwargs: Any) -> None:
        """Register a SQLite UDF on every connection in the pool (idempotent)."""
        for conn in self._all_conns:
            await conn.create_function(name, narg, func, **kwargs)

    @asynccontextmanager
    async def acquire_read(self) -> AsyncIterator[aiosqlite.Connection]:
        conn = await self._queue.get()
        try:
            yield conn
        finally:
            self._queue.put_nowait(conn)

    async def close(self) -> None:
        if not self.owns_conns:
            self._all_conns = []
            return
        for conn in self._all_conns:
            try:
                await conn.close()
            except Exception:  # noqa: BLE001 — best-effort close on shutdown
                pass
        self._all_conns = []


_WRITE_KEYWORDS = ("insert", "update", "delete", "replace", "create", "drop", "alter")


def _reject_writes(sql: str) -> None:
    """Guard: the read pool is for SELECTs only.

    A write routed here (e.g. a repo accidentally using ``self._db`` instead of the
    write connection) would land on a read connection outside any open transaction —
    silently failing with "database is locked". Fail loudly and early instead so the
    mistake is obvious in tests rather than as a flaky lock error (#760).
    """
    first = sql.lstrip().split(None, 1)
    if first and first[0].lower() in _WRITE_KEYWORDS:
        raise ValueError(
            f"Write statement routed through the read pool: {first[0].upper()} ... "
            "— use Database.execute_write()/transaction() or the write connection."
        )


class ReadPoolProxy:
    """Drop-in replacement for the raw connection that repositories read through.

    Exposes exactly the read API repositories call on ``self._db`` (``execute`` →
    fetch*, ``execute_fetchall``, ``create_function``). Each ``execute`` borrows a
    pool connection, runs the query, buffers the rows, and releases the connection
    before returning — so the result survives the repository's later ``fetchall()``.
    """

    def __init__(self, pool: ReadConnectionPool):
        self._pool = pool

    async def execute(self, sql: str, params: Sequence[Any] = ()) -> BufferedCursor:
        _reject_writes(sql)
        async with self._pool.acquire_read() as conn:
            cur = await conn.execute(sql, params)
            # fetchall() returns a fresh owned list, so BufferedCursor can take it directly.
            return BufferedCursor(await cur.fetchall())

    async def execute_fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[Any]:
        _reject_writes(sql)
        async with self._pool.acquire_read() as conn:
            return await conn.execute_fetchall(sql, params)

    async def create_function(self, name: str, narg: int, func: Any, **kwargs: Any) -> None:
        """Register a UDF on every read connection (filter queries call this lazily)."""
        await self._pool.register_function(name, narg, func, **kwargs)
