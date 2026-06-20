"""SQLite connection tuning PRAGMAs for large databases (issue #760)."""

from __future__ import annotations

import aiosqlite

from src.config import DatabaseConfig
from src.database.connection import ConnectionTuning, DBConnection, apply_pragmas
from src.database.facade import Database


async def _pragma(conn, name: str):
    cur = await conn.execute(f"PRAGMA {name}")
    row = await cur.fetchone()
    return row[0]


async def test_apply_pragmas_uses_configured_cache_and_mmap(tmp_path):
    conn = await aiosqlite.connect(str(tmp_path / "t.db"), isolation_level=None)
    try:
        await apply_pragmas(conn, role="write", tuning=ConnectionTuning(cache_size_kb=128000, mmap_size_mb=512))
        assert await _pragma(conn, "cache_size") == -128000
        assert await _pragma(conn, "mmap_size") == 512 * 1024 * 1024
        assert await _pragma(conn, "analysis_limit") == 400
    finally:
        await conn.close()


async def test_apply_pragmas_defaults(tmp_path):
    conn = await aiosqlite.connect(str(tmp_path / "t.db"), isolation_level=None)
    try:
        await apply_pragmas(conn, role="write")
        assert await _pragma(conn, "cache_size") == -64000
        assert await _pragma(conn, "mmap_size") == 256 * 1024 * 1024  # bumped from 30MB (#760)
    finally:
        await conn.close()


async def test_read_role_still_tunes_cache_and_mmap(tmp_path):
    conn = await aiosqlite.connect(str(tmp_path / "t.db"), isolation_level=None)
    try:
        await apply_pragmas(conn, role="read", tuning=ConnectionTuning(cache_size_kb=70000, mmap_size_mb=128))
        assert await _pragma(conn, "cache_size") == -70000
        assert await _pragma(conn, "mmap_size") == 128 * 1024 * 1024
        assert await _pragma(conn, "analysis_limit") == 400
    finally:
        await conn.close()


async def test_db_connection_close_runs_optimize(tmp_path):
    dbc = DBConnection(str(tmp_path / "opt.db"))
    conn = await dbc.connect()
    await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    await conn.execute("INSERT INTO t (v) VALUES ('x')")
    await dbc.close()  # runs PRAGMA optimize then closes — must not raise
    assert dbc.db is None
    # DB remains usable after the optimize-on-close.
    again = await aiosqlite.connect(str(tmp_path / "opt.db"), isolation_level=None)
    try:
        cur = await again.execute("SELECT COUNT(*) FROM t")
        assert (await cur.fetchone())[0] == 1
    finally:
        await again.close()


async def test_database_threads_tuning_to_write_and_read_connections(tmp_path):
    db = Database(str(tmp_path / "facade.db"), cache_size_kb=96000, mmap_size_mb=384)
    await db.initialize()
    try:
        assert await _pragma(db._db, "cache_size") == -96000
        assert await _pragma(db._db, "mmap_size") == 384 * 1024 * 1024
        async with db._read_pool.acquire_read() as rconn:
            assert await _pragma(rconn, "cache_size") == -96000
            assert await _pragma(rconn, "mmap_size") == 384 * 1024 * 1024
    finally:
        await db.close()


def test_database_config_tuning_defaults():
    cfg = DatabaseConfig()
    assert cfg.cache_size_kb == 64000
    assert cfg.mmap_size_mb == 256
