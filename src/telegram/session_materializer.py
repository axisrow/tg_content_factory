from __future__ import annotations

import hashlib
import re
import sqlite3
from pathlib import Path

from telethon.sessions import SQLiteSession, StringSession


class SessionMaterializer:
    """Materialize DB-backed StringSession values into app-managed SQLite sessions."""

    def __init__(self, cache_dir: str | Path):
        self._cache_dir = Path(cache_dir)

    @property
    def cache_dir(self) -> Path:
        return self._cache_dir

    def materialize(self, phone: str, session_string: str) -> str:
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(session_string.encode("utf-8")).hexdigest()
        base_path = self._base_path(phone)
        hash_path = self._hash_path(phone)
        session_file = self._session_file(base_path)

        if (
            session_file.exists()
            and hash_path.exists()
            and hash_path.read_text(encoding="ascii").strip() == digest
        ):
            self._enable_wal(session_file)
            return str(base_path)

        source = StringSession(session_string)
        if not source.auth_key or not source.server_address or not source.port or not source.dc_id:
            raise ValueError(f"Invalid Telegram session for {phone}")

        if session_file.exists():
            session_file.unlink()
        if hash_path.exists():
            hash_path.unlink()

        target = SQLiteSession(str(base_path))
        try:
            target.set_dc(source.dc_id, source.server_address, source.port)
            target.auth_key = source.auth_key
            target.save()
        finally:
            target.close()

        self._enable_wal(session_file)
        hash_path.write_text(digest, encoding="ascii")
        return str(base_path)

    def ensure_empty_env_file(self) -> str:
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        env_path = self._cache_dir / ".telethon-cli.env"
        if not env_path.exists():
            env_path.write_text("", encoding="ascii")
        return str(env_path)

    @staticmethod
    def _enable_wal(session_file: Path) -> None:
        """Switch a session file to WAL so several processes can share it.

        Telethon opens session files with a bare ``sqlite3.connect`` and sets no
        pragmas, so they default to the rollback journal — under which a reader
        holding an open transaction locks out every writer. `serve` (web plus the
        embedded worker) and any separate CLI process resolve the *same*
        ``<cache_dir>/<phone>.session`` path, so the second one died with
        ``database is locked`` inside telethon's ``process_entities``.

        WAL lives in the file header, so setting it once here applies to every
        later open by any process. Failures are non-fatal: a locked or unreadable
        file must not break session materialization, which is the caller's
        actual job.
        """
        try:
            conn = sqlite3.connect(session_file, timeout=1)
        except sqlite3.Error:
            return
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.Error:
            pass
        finally:
            conn.close()

    def _base_path(self, phone: str) -> Path:
        safe_phone = re.sub(r"[^A-Za-z0-9_.-]+", "_", phone).strip("._-") or "account"
        return self._cache_dir / safe_phone

    def _hash_path(self, phone: str) -> Path:
        return self._base_path(phone).with_suffix(".sha256")

    @staticmethod
    def _session_file(base_path: Path) -> Path:
        return Path(f"{base_path}.session")
