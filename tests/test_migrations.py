from __future__ import annotations

import json
import struct

import aiosqlite
import pytest

from src.database.migrations import _migrate_vec_to_portable, _migrate_zai_legacy_base_url


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


_ZAI_PROVIDERS_KEY = "agent_deepagents_providers_v1"


def _zai_payload(base_url: str, *, last_validation_error: str = "") -> str:
    return json.dumps(
        [
            {
                "provider": "zai",
                "enabled": True,
                "priority": 0,
                "selected_model": "glm-5-turbo",
                "plain_fields": {"base_url": base_url},
                "secret_fields_enc": {},
                "last_validation_error": last_validation_error,
            }
        ]
    )


async def _open_zai_db(tmp_path):
    conn = await aiosqlite.connect(str(tmp_path / "zai.db"))
    conn.row_factory = aiosqlite.Row
    await conn.execute("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    await conn.commit()
    return conn


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
@pytest.mark.parametrize(
    "legacy_url",
    ["https://api.z.ai/api/anthropic", "https://api.z.ai/api/anthropic/v1"],
)
async def test_migrate_zai_legacy_base_url_rewrites_legacy(tmp_path, legacy_url):
    conn = await _open_zai_db(tmp_path)
    try:
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?)",
            (
                _ZAI_PROVIDERS_KEY,
                _zai_payload(
                    legacy_url,
                    last_validation_error="Anthropic-compatible proxy ...",
                ),
            ),
        )
        await conn.commit()

        await _migrate_zai_legacy_base_url(conn)

        cur = await conn.execute(
            "SELECT value FROM settings WHERE key = ?", (_ZAI_PROVIDERS_KEY,)
        )
        row = await cur.fetchone()
        data = json.loads(row["value"])
        assert data[0]["plain_fields"]["base_url"] == "https://api.z.ai/api/paas/v4"
        assert data[0]["last_validation_error"] == ""
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_migrate_zai_legacy_base_url_leaves_valid_unchanged(tmp_path):
    conn = await _open_zai_db(tmp_path)
    try:
        valid_payload = _zai_payload("https://api.z.ai/api/paas/v4")
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?)",
            (_ZAI_PROVIDERS_KEY, valid_payload),
        )
        await conn.commit()

        await _migrate_zai_legacy_base_url(conn)

        cur = await conn.execute(
            "SELECT value FROM settings WHERE key = ?", (_ZAI_PROVIDERS_KEY,)
        )
        row = await cur.fetchone()
        assert json.loads(row["value"]) == json.loads(valid_payload)
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_migrate_zai_legacy_base_url_idempotent(tmp_path):
    conn = await _open_zai_db(tmp_path)
    try:
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?)",
            (_ZAI_PROVIDERS_KEY, _zai_payload("https://api.z.ai/api/anthropic")),
        )
        await conn.commit()

        await _migrate_zai_legacy_base_url(conn)
        await _migrate_zai_legacy_base_url(conn)

        cur = await conn.execute(
            "SELECT value FROM settings WHERE key = ?", (_ZAI_PROVIDERS_KEY,)
        )
        row = await cur.fetchone()
        data = json.loads(row["value"])
        assert data[0]["plain_fields"]["base_url"] == "https://api.z.ai/api/paas/v4"
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_migrate_zai_legacy_base_url_handles_missing_setting(tmp_path):
    conn = await _open_zai_db(tmp_path)
    try:
        await _migrate_zai_legacy_base_url(conn)

        cur = await conn.execute(
            "SELECT value FROM settings WHERE key = ?", (_ZAI_PROVIDERS_KEY,)
        )
        assert await cur.fetchone() is None
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_migrate_zai_legacy_base_url_handles_malformed_json(tmp_path):
    conn = await _open_zai_db(tmp_path)
    try:
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?)",
            (_ZAI_PROVIDERS_KEY, "not-json{"),
        )
        await conn.commit()

        await _migrate_zai_legacy_base_url(conn)

        cur = await conn.execute(
            "SELECT value FROM settings WHERE key = ?", (_ZAI_PROVIDERS_KEY,)
        )
        row = await cur.fetchone()
        assert row["value"] == "not-json{"
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_migrate_zai_legacy_base_url_skips_other_providers(tmp_path):
    conn = await _open_zai_db(tmp_path)
    try:
        payload = json.dumps(
            [
                {
                    "provider": "openai",
                    "enabled": True,
                    "priority": 0,
                    "selected_model": "gpt-4o-mini",
                    "plain_fields": {"base_url": "https://api.z.ai/api/anthropic"},
                    "secret_fields_enc": {},
                    "last_validation_error": "",
                }
            ]
        )
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?)",
            (_ZAI_PROVIDERS_KEY, payload),
        )
        await conn.commit()

        await _migrate_zai_legacy_base_url(conn)

        cur = await conn.execute(
            "SELECT value FROM settings WHERE key = ?", (_ZAI_PROVIDERS_KEY,)
        )
        row = await cur.fetchone()
        assert json.loads(row["value"]) == json.loads(payload)
    finally:
        await conn.close()
