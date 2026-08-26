from __future__ import annotations

import hashlib
import logging
import os
import re
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

if os.name == "nt":
    import msvcrt
else:
    import fcntl

from telethon.sessions import SQLiteSession, StringSession

logger = logging.getLogger(__name__)


class SessionMaterializer:
    """Materialize DB-backed StringSession values into app-managed SQLite sessions."""

    def __init__(self, cache_dir: str | Path):
        self._cache_dir = Path(cache_dir)

    @property
    def cache_dir(self) -> Path:
        return self._cache_dir

    def materialize(self, phone: str, session_string: str) -> str:
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        with self._phone_lock(phone):
            return self._materialize_locked(phone, session_string)

    @contextmanager
    def _phone_lock(self, phone: str):
        lock_path = self._cache_dir / f"{self._base_path(phone).name}.lock"
        with lock_path.open("a+b") as lock_file:
            if os.name == "nt":
                # msvcrt.locking requires a byte to exist at the current
                # position and locks from that position.
                lock_file.seek(0)
                lock_file.write(b"\0")
                lock_file.flush()
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
            else:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if os.name == "nt":
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def _materialize_locked(self, phone: str, session_string: str) -> str:
        digest = hashlib.sha256(session_string.encode("utf-8")).hexdigest()
        base_path = self._base_path(phone)
        hash_path = self._hash_path(phone)
        session_file = self._session_file(base_path)

        if (
            session_file.exists()
            and hash_path.exists()
            and hash_path.read_text(encoding="ascii").strip() == digest
        ):
            self._enable_wal(session_file, phone)
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

        self._enable_wal(session_file, phone)
        hash_path.write_text(digest, encoding="ascii")
        return str(base_path)

    def ensure_empty_env_file(self) -> str:
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        env_path = self._cache_dir / ".telethon-cli.env"
        if not env_path.exists():
            env_path.write_text("", encoding="ascii")
        return str(env_path)

    _WAL_UPGRADE_ATTEMPTS = 3
    _WAL_UPGRADE_RETRY_DELAY_SECONDS = 0.1

    @classmethod
    def _enable_wal(cls, session_file: Path, phone: str) -> None:
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
        actual job. But a failure must not be silent either — ``PRAGMA
        journal_mode=WAL`` itself needs an exclusive lock, so it can lose the
        race against a live process (e.g. `serve`) already holding an open
        transaction on this same file. That blocking transaction is normally
        short-lived (a single `serve` request), so a few bounded retries with a
        short backoff let the upgrade land before the caller opens the file —
        closing the gap between "we logged the failure" and "we prevented it".
        If every attempt is still blocked we log a warning and give up rather
        than delay materialization indefinitely.
        """
        for attempt in range(1, cls._WAL_UPGRADE_ATTEMPTS + 1):
            last_attempt = attempt == cls._WAL_UPGRADE_ATTEMPTS
            try:
                conn = sqlite3.connect(session_file, timeout=1)
            except sqlite3.Error:
                if last_attempt:
                    logger.warning("Could not open session file for %s to enable WAL: %s", phone, session_file)
                    return
                time.sleep(cls._WAL_UPGRADE_RETRY_DELAY_SECONDS)
                continue
            try:
                mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()
                if mode and str(mode[0]).lower() == "wal":
                    return
                if last_attempt:
                    logger.warning(
                        "Session file for %s did not switch to WAL (still %s) after %d attempt(s) — "
                        "likely a live process holds an open transaction on %s; it will keep the old "
                        "locking mode until a future materialization succeeds.",
                        phone,
                        mode[0] if mode else "unknown",
                        attempt,
                        session_file,
                    )
            except sqlite3.Error:
                if last_attempt:
                    logger.warning("Failed to enable WAL for %s on %s", phone, session_file)
            finally:
                conn.close()
            if not last_attempt:
                time.sleep(cls._WAL_UPGRADE_RETRY_DELAY_SECONDS)

    def _base_path(self, phone: str) -> Path:
        safe_phone = re.sub(r"[^A-Za-z0-9_.-]+", "_", phone).strip("._-") or "account"
        return self._cache_dir / safe_phone

    def _hash_path(self, phone: str) -> Path:
        return self._base_path(phone).with_suffix(".sha256")

    @staticmethod
    def _session_file(base_path: Path) -> Path:
        return Path(f"{base_path}.session")
