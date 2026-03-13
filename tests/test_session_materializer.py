from __future__ import annotations

import hashlib
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
