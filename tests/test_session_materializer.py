from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest
from telethon.crypto import AuthKey
from telethon.sessions import MemorySession, SQLiteSession, StringSession

from src.telegram.session_materializer import SessionMaterializer

pytestmark = pytest.mark.real_materializer


def _make_session_string(*, dc_id: int, ip: str, port: int, fill: int) -> str:
    session = MemorySession()
    session.set_dc(dc_id, ip, port)
    session.auth_key = AuthKey(data=bytes([fill]) * 256)
    return StringSession.save(session)


def test_materialize_string_session_round_trip(tmp_path):
    session_string = _make_session_string(
        dc_id=2,
        ip="149.154.167.51",
        port=443,
        fill=7,
    )
    materializer = SessionMaterializer(tmp_path / "sessions")

    session_path = materializer.materialize("+70000000001", session_string)
    session = SQLiteSession(session_path)
    try:
        assert session.dc_id == 2
        assert session.server_address == "149.154.167.51"
        assert session.port == 443
        assert session.auth_key is not None
        assert session.auth_key.key == bytes([7]) * 256
    finally:
        session.close()


def test_materialize_regenerates_cached_session_when_string_changes(tmp_path):
    materializer = SessionMaterializer(tmp_path / "sessions")
    first = _make_session_string(dc_id=2, ip="149.154.167.51", port=443, fill=3)
    second = _make_session_string(dc_id=2, ip="149.154.167.51", port=443, fill=9)

    session_path = materializer.materialize("+70000000001", first)
    session_file = Path(f"{session_path}.session")
    hash_file = (tmp_path / "sessions" / "70000000001").with_suffix(".sha256")
    first_hash = hash_file.read_text(encoding="ascii").strip()

    materializer.materialize("+70000000001", second)
    second_hash = hash_file.read_text(encoding="ascii").strip()

    assert session_file.exists()
    assert first_hash == hashlib.sha256(first.encode("utf-8")).hexdigest()
    assert second_hash == hashlib.sha256(second.encode("utf-8")).hexdigest()
    assert second_hash != first_hash

    session = SQLiteSession(session_path)
    try:
        assert session.auth_key is not None
        assert session.auth_key.key == bytes([9]) * 256
    finally:
        session.close()


def _journal_mode(session_path: str) -> str:
    """Read the persisted journal mode of a materialized session file."""
    conn = sqlite3.connect(f"{session_path}.session")
    try:
        return str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
    finally:
        conn.close()


def test_materialized_session_uses_wal_journal(tmp_path):
    """The session file must be WAL so a second process can read it concurrently.

    Regression: `serve` (web + embedded worker) and a separate CLI process open
    the very same ``data/telegram_sessions/<phone>.session`` file — the path is
    derived from the phone alone. Under the SQLite default rollback journal the
    second process dies with ``sqlite3.OperationalError: database is locked``
    inside telethon's ``process_entities``. WAL is persisted in the file header,
    so setting it once at materialization time covers every later open.
    """
    materializer = SessionMaterializer(tmp_path / "sessions")
    session_string = _make_session_string(dc_id=2, ip="149.154.167.51", port=443, fill=5)

    session_path = materializer.materialize("+70000000001", session_string)

    assert _journal_mode(session_path) == "wal"


def test_materialized_session_survives_concurrent_reader(tmp_path):
    """A live reader must not lock out a writer — the exact production symptom.

    Mirrors `serve` holding an open read transaction on the entity cache while a
    second process writes to it. Under the rollback journal this raises
    ``database is locked``; under WAL readers and the writer coexist.
    """
    materializer = SessionMaterializer(tmp_path / "sessions")
    session_string = _make_session_string(dc_id=2, ip="149.154.167.51", port=443, fill=5)
    session_file = f"{materializer.materialize('+70000000001', session_string)}.session"

    reader = sqlite3.connect(session_file)
    writer = sqlite3.connect(session_file, timeout=1)
    try:
        reader.execute("BEGIN")
        reader.execute("SELECT * FROM entities").fetchall()

        writer.executemany(
            "INSERT OR REPLACE INTO entities VALUES (?,?,?,?,?,?)",
            [(1234, 5678, None, None, None, 0)],
        )
        writer.commit()
    finally:
        reader.close()
        writer.close()


def test_materialize_upgrades_preexisting_session_to_wal(tmp_path):
    """Sessions materialized before this fix must be upgraded on the next call.

    The hash-match fast path returns early without recreating the file, so the
    four already-materialized production sessions would otherwise keep the
    rollback journal forever. Simulates that state by forcing the file back to
    DELETE journal and re-materializing with an unchanged session string.
    """
    materializer = SessionMaterializer(tmp_path / "sessions")
    session_string = _make_session_string(dc_id=2, ip="149.154.167.51", port=443, fill=5)
    session_path = materializer.materialize("+70000000001", session_string)

    conn = sqlite3.connect(f"{session_path}.session")
    try:
        conn.execute("PRAGMA journal_mode=DELETE")
    finally:
        conn.close()
    assert _journal_mode(session_path) == "delete"

    # Same string → hash matches → early-return branch.
    assert materializer.materialize("+70000000001", session_string) == session_path
    assert _journal_mode(session_path) == "wal"
