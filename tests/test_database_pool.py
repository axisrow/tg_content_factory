"""Tests for the async read-connection pool (#760).

These exercise the pool in isolation (file-backed tmp DB) before it is wired into
the Database facade. They import aiosqlite directly, so conftest auto-marks the
module aiosqlite_serial.
"""
from __future__ import annotations

import asyncio
import time

import pytest

from src.database.connection import open_connection
from src.database.pool import BufferedCursor, ReadConnectionPool, ReadPoolProxy

# A heavy pure-SQL query: a recursive CTE that SQLite evaluates in C, so it occupies
# its connection's worker thread WITHOUT holding the Python GIL (a time.sleep UDF would
# hold the GIL and stall the event loop, defeating the concurrency we want to prove).
_HEAVY_SQL = (
    "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 4000000) "
    "SELECT COUNT(*) AS n FROM c"
)


async def _seed(db_path: str) -> None:
    conn = await open_connection(db_path, role="write")
    try:
        await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        await conn.executemany("INSERT INTO t (v) VALUES (?)", [("a",), ("b",), ("c",)])
        await conn.commit()
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_long_select_does_not_block_other_reads(tmp_path):
    """A heavy SELECT on one pooled connection must not stall a quick SELECT (#760)."""
    db_path = str(tmp_path / "pool.db")
    await _seed(db_path)
    pool = ReadConnectionPool(db_path, size=2)
    await pool.open()
    proxy = ReadPoolProxy(pool)
    try:
        async def heavy():
            await proxy.execute(_HEAVY_SQL)

        heavy_task = asyncio.create_task(heavy())
        await asyncio.sleep(0.05)  # let the heavy query grab its connection first

        # The quick SELECT runs on the *other* pooled connection while heavy is busy.
        t0 = time.perf_counter()
        cur = await proxy.execute("SELECT COUNT(*) AS n FROM t")
        row = await cur.fetchone()
        elapsed = time.perf_counter() - t0

        assert row["n"] == 3
        # If the pool serialised reads, the quick query would wait for the whole heavy
        # one. On a free second connection it returns promptly.
        assert not heavy_task.done(), "heavy query already finished — test is not exercising concurrency"
        assert elapsed < 0.5, f"quick SELECT blocked for {elapsed:.3f}s behind the heavy one"
        await heavy_task
    finally:
        await pool.close()


@pytest.mark.anyio
async def test_pool_exhaustion_waits_not_deadlocks(tmp_path):
    """With size=2, four heavy SELECTs all complete (extra ones wait for release)."""
    db_path = str(tmp_path / "pool.db")
    await _seed(db_path)
    pool = ReadConnectionPool(db_path, size=2)
    await pool.open()
    proxy = ReadPoolProxy(pool)
    try:
        async def q():
            await proxy.execute(_HEAVY_SQL)

        await asyncio.wait_for(asyncio.gather(*[q() for _ in range(4)]), timeout=30)
    finally:
        await pool.close()


@pytest.mark.anyio
async def test_read_proxy_buffers_cursor(tmp_path):
    """fetchall() works after the connection is back in the pool (buffered rows)."""
    db_path = str(tmp_path / "pool.db")
    await _seed(db_path)
    pool = ReadConnectionPool(db_path, size=1)
    await pool.open()
    proxy = ReadPoolProxy(pool)
    try:
        cur = await proxy.execute("SELECT v FROM t ORDER BY id")
        # Connection already released; a fresh execute could reuse it before we fetch.
        await proxy.execute("SELECT 1")
        rows = await cur.fetchall()
        assert [r["v"] for r in rows] == ["a", "b", "c"]
    finally:
        await pool.close()


@pytest.mark.anyio
async def test_close_closes_pool(tmp_path):
    """close() shuts every reader and is idempotent."""
    db_path = str(tmp_path / "pool.db")
    await _seed(db_path)
    pool = ReadConnectionPool(db_path, size=3)
    await pool.open()
    assert len(pool._all_conns) == 3
    await pool.close()
    assert pool._all_conns == []
    await pool.close()  # idempotent


@pytest.mark.anyio
async def test_memory_shared_conn_not_double_closed():
    """:memory: degenerate pool shares the write connection and never closes it."""
    write_conn = await open_connection(":memory:", role="write")
    try:
        pool = ReadConnectionPool(":memory:", size=1)
        await pool.open(shared_conn=write_conn)
        assert pool.owns_conns is False
        proxy = ReadPoolProxy(pool)
        await write_conn.execute("CREATE TABLE m (id INTEGER)")
        await write_conn.execute("INSERT INTO m VALUES (1)")
        await write_conn.commit()
        # Read sees the write because they share one connection.
        cur = await proxy.execute("SELECT COUNT(*) AS n FROM m")
        row = await cur.fetchone()
        assert row["n"] == 1
        await pool.close()  # must NOT close the shared write connection
        cur2 = await write_conn.execute("SELECT 1 AS v")
        assert (await cur2.fetchone())["v"] == 1
    finally:
        await write_conn.close()


def test_buffered_cursor_semantics():
    """BufferedCursor matches the fetch* contract repositories rely on."""

    async def _run():
        cur = BufferedCursor([{"v": 1}, {"v": 2}])
        assert cur.rowcount == 2
        assert await cur.fetchone() == {"v": 1}
        assert await cur.fetchall() == [{"v": 2}]
        assert await cur.fetchall() == []
        assert await cur.fetchone() is None

    asyncio.run(_run())
