from __future__ import annotations

import json
import struct

import aiosqlite
import pytest

from src.database.migrations import _migrate_vec_to_portable


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_migrate_vec_to_portable_converts_json_to_blob(tmp_path):
    db_path = str(tmp_path / "test.db")
    conn = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row
    try:
        await conn.executescript("""
            CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE message_embeddings (message_id INTEGER PRIMARY KEY, embedding BLOB NOT NULL);
            CREATE TABLE vec_messages (message_id INTEGER PRIMARY KEY, embedding TEXT);
        """)
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES ('semantic_embedding_dimensions', '3')"
        )
        vec = [0.1, 0.2, 0.3]
        await conn.execute(
            "INSERT INTO vec_messages (message_id, embedding) VALUES (?, ?)",
            (1, json.dumps(vec)),
        )
        await conn.commit()

        await _migrate_vec_to_portable(conn)

        cur = await conn.execute("SELECT message_id, embedding FROM message_embeddings")
        row = await cur.fetchone()
        assert row is not None
        assert row["message_id"] == 1
        restored = list(struct.unpack("3f", row["embedding"]))
        assert restored == pytest.approx(vec)
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_migrate_vec_skips_when_no_vec_table(tmp_path):
    db_path = str(tmp_path / "test.db")
    conn = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row
    try:
        await conn.executescript("""
            CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE message_embeddings (message_id INTEGER PRIMARY KEY, embedding BLOB NOT NULL);
        """)
        await conn.commit()

        await _migrate_vec_to_portable(conn)

        cur = await conn.execute("SELECT COUNT(*) AS cnt FROM message_embeddings")
        row = await cur.fetchone()
        assert row["cnt"] == 0
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_migrate_vec_is_idempotent_with_existing_data(tmp_path):
    """Migration adds missing rows from vec_messages even if message_embeddings already has data."""
    db_path = str(tmp_path / "test.db")
    conn = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row
    try:
        await conn.executescript("""
            CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE message_embeddings (message_id INTEGER PRIMARY KEY, embedding BLOB NOT NULL);
            CREATE TABLE vec_messages (message_id INTEGER PRIMARY KEY, embedding TEXT);
        """)
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES ('semantic_embedding_dimensions', '2')"
        )
        existing_blob = struct.pack("2f", 1.0, 0.0)
        await conn.execute(
            "INSERT INTO message_embeddings (message_id, embedding) VALUES (?, ?)",
            (99, existing_blob),
        )
        await conn.execute(
            "INSERT INTO vec_messages (message_id, embedding) VALUES (?, ?)",
            (1, json.dumps([0.5, 0.5])),
        )
        await conn.commit()

        await _migrate_vec_to_portable(conn)

        cur = await conn.execute("SELECT COUNT(*) AS cnt FROM message_embeddings")
        row = await cur.fetchone()
        assert row["cnt"] == 2  # pre-existing + migrated
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_migrate_vec_skips_empty_vec_table(tmp_path):
    db_path = str(tmp_path / "test.db")
    conn = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row
    try:
        await conn.executescript("""
            CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE message_embeddings (message_id INTEGER PRIMARY KEY, embedding BLOB NOT NULL);
            CREATE TABLE vec_messages (message_id INTEGER PRIMARY KEY, embedding TEXT);
        """)
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES ('semantic_embedding_dimensions', '2')"
        )
        await conn.commit()

        await _migrate_vec_to_portable(conn)

        cur = await conn.execute("SELECT COUNT(*) AS cnt FROM message_embeddings")
        row = await cur.fetchone()
        assert row["cnt"] == 0
    finally:
        await conn.close()
