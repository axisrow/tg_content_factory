from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database.connection import DBConnection, ProfilingConnection


@pytest.mark.anyio
async def test_db_connection_connect_and_close(tmp_path):
    db_path = str(tmp_path / "test_conn.db")
    conn = DBConnection(db_path)
    db = await conn.connect()
    assert db is not None
    cur = await db.execute("SELECT 1")
    row = await cur.fetchone()
    assert row[0] == 1
    await conn.close()
    assert conn.db is None


@pytest.mark.anyio
async def test_db_connection_memory():
    conn = DBConnection(":memory:")
    db = await conn.connect()
    assert db is not None
    await conn.close()


@pytest.mark.anyio
async def test_db_connection_execute():
    conn = DBConnection(":memory:")
    await conn.connect()
    cur = await conn.execute("CREATE TABLE t(x)")
    await cur.close()
    cur = await conn.execute("INSERT INTO t(x) VALUES (?)", (42,))
    await cur.close()
    cur = await conn.execute("SELECT x FROM t")
    row = await cur.fetchone()
    assert row[0] == 42
    await conn.close()


@pytest.mark.anyio
async def test_db_connection_execute_fetchall():
    conn = DBConnection(":memory:")
    await conn.connect()
    await conn.execute("CREATE TABLE t(x)")
    await conn.execute("INSERT INTO t(x) VALUES (1)")
    await conn.execute("INSERT INTO t(x) VALUES (2)")
    rows = await conn.execute_fetchall("SELECT x FROM t ORDER BY x")
    assert [r[0] for r in rows] == [1, 2]
    await conn.close()


@pytest.mark.anyio
async def test_profiling_connection_execute():
    inner = AsyncMock()
    mock_cursor = MagicMock()
    inner.execute = AsyncMock(return_value=mock_cursor)
    inner.row_factory = None

    pc = ProfilingConnection(inner)
    with patch("src.web.timing.get_current_profiler", return_value=None):
        result = await pc.execute("SELECT 1")
        assert result == mock_cursor


@pytest.mark.anyio
async def test_profiling_connection_execute_with_profiler():
    from src.web.timing import RequestProfiler

    inner = AsyncMock()
    mock_cursor = MagicMock()
    inner.execute = AsyncMock(return_value=mock_cursor)

    profiler = RequestProfiler()
    profiler.activate()
    try:
        pc = ProfilingConnection(inner)
        await pc.execute("SELECT 1")
        assert profiler.db_queries == 1
        assert profiler.db_ns > 0
    finally:
        profiler.deactivate()


@pytest.mark.anyio
async def test_profiling_connection_execute_fetchall():
    from src.web.timing import RequestProfiler

    inner = AsyncMock()
    inner.execute_fetchall = AsyncMock(return_value=[[1, 2]])

    profiler = RequestProfiler()
    profiler.activate()
    try:
        pc = ProfilingConnection(inner)
        rows = await pc.execute_fetchall("SELECT 1, 2")
        assert rows == [[1, 2]]
        assert profiler.db_queries == 1
    finally:
        profiler.deactivate()


@pytest.mark.anyio
async def test_profiling_connection_executemany():
    from src.web.timing import RequestProfiler

    inner = AsyncMock()
    inner.executemany = AsyncMock(return_value=None)

    profiler = RequestProfiler()
    profiler.activate()
    try:
        pc = ProfilingConnection(inner)
        await pc.executemany("INSERT INTO t VALUES(?)", [(1,), (2,)])
        assert profiler.db_queries == 1
    finally:
        profiler.deactivate()


@pytest.mark.anyio
async def test_profiling_connection_executescript():
    from src.web.timing import RequestProfiler

    inner = AsyncMock()
    inner.executescript = AsyncMock(return_value=None)

    profiler = RequestProfiler()
    profiler.activate()
    try:
        pc = ProfilingConnection(inner)
        await pc.executescript("CREATE TABLE t(x)")
        assert profiler.db_queries == 1
    finally:
        profiler.deactivate()


def test_profiling_connection_getattr():
    inner = MagicMock()
    inner.foo = "bar"
    pc = ProfilingConnection(inner)
    assert pc.foo == "bar"
