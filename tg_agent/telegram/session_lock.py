# SPDX-License-Identifier: Apache-2.0
# Ported from axisrow/telegram-mcp@e224762 (telegram_mcp/singleton.py), 2026-10-16.
# Upstream is predominantly third-party authorship; the portable code is
# treated as Apache-2.0 (see THIRD_PARTY_NOTICES.md and LICENSES/apache-2.0.txt).
# Deviations from upstream (kept minimal, see git history for the wiring commits):
#   - module docstring / error wording recontextualised for tg-agent
#     (worker / web / CLI processes instead of MCP clients);
#   - DEFAULT_LOCK_DIR points at <tempdir>/tg-agent-session-locks;
#   - narrow `_fh is None` guards in _record_holder/_clear_holder (Optional
#     typing, type-checker hygiene; upstream relied on call ordering);
#   - upstream's shared-lock mode (``acquire(shared=True)``) removed: nothing
#     used it, and since #1868 non-owner processes reach Telegram through the
#     owner's relay instead of sharing an auth key.
"""Per-session file lock guarding against concurrent Telegram connections.

Several independent processes run against the same accounts in tg-agent: the
worker daemon (the default) and one-shot CLI commands. While the daemon runs,
the CLI goes through its relay and never touches the auth key (#1868); the lock
is the backstop for every other way two processes could connect the *same*
auth key — Telegram's abuse protection (``AuthKeyDuplicatedError``) can knock
out *both* connections rather than just the newcomer, killing the session.

This module provides an OS-level advisory file lock (``flock`` on POSIX,
``msvcrt.locking`` on Windows) keyed by the session's actual identity (its
string value or file path), not just the account label, since two different
projects/labels could otherwise collide or one label could map to different
sessions across deployments. The lock is held for the lifetime of the
process and is released automatically by the OS if the process exits or is
killed, so there is no stale-lock file to clean up.

A second instance racing for the same session waits briefly (covers the
"restart replaces the old process" case) and, if the lock is still held once
the grace period elapses, raises :class:`SessionLockError` instead of ever
calling ``connect()`` -- so the live session is never disturbed.

The lock can only ever see processes on this host. The holder leaves its PID
in the lock file so a refused contender can name it.
"""

from __future__ import annotations

import asyncio
import atexit
import hashlib
import os
import tempfile
import time
from pathlib import Path
from typing import IO, Optional

if os.name == "nt":
    import msvcrt

    # Windows locks are mandatory, so the holder writes its PID past the locked
    # byte, where a refused contender can still read it.
    _PID_OFFSET = 1

    def _try_lock(fh: IO) -> bool:
        try:
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True

    def _unlock(fh: IO) -> None:
        try:
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass

else:
    import fcntl

    _PID_OFFSET = 0

    def _try_lock(fh: IO) -> bool:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False

    def _unlock(fh: IO) -> None:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass


DEFAULT_LOCK_DIR = Path(tempfile.gettempdir()) / "tg-agent-session-locks"
DEFAULT_GRACE_SECONDS = 20.0
DEFAULT_POLL_INTERVAL = 0.5


class SessionLockError(RuntimeError):
    """Raised when a session lock isn't acquired before its grace period elapses."""


class SessionLock:
    """Exclusive, OS-released lock for one Telegram session, held for process lifetime."""

    def __init__(self, label: str, session_identity: str, *, lock_dir: Path = DEFAULT_LOCK_DIR):
        digest = hashlib.sha256(session_identity.encode("utf-8")).hexdigest()[:16]
        lock_dir.mkdir(parents=True, exist_ok=True)
        # Keyed by the session alone: with the label in the name, renaming an
        # account (e.g. "default" -> "work") let an old and a new process both
        # connect the same session, each holding its own lock file.
        self.path = lock_dir / f"session-{digest}.lock"
        self._fh: Optional[IO] = None

    def acquire(
        self,
        *,
        grace_seconds: float = DEFAULT_GRACE_SECONDS,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
    ) -> None:
        """Block (up to ``grace_seconds``) until the lock is free, then take it.

        Raises :class:`SessionLockError` if the lock is still held once the
        grace period elapses.
        """
        fh = open(self.path, "a+")
        deadline = time.monotonic() + grace_seconds
        while True:
            if _try_lock(fh):
                self._fh = fh
                self._record_holder()
                return
            if time.monotonic() >= deadline:
                fh.close()
                holder = self.holder_pid()
                where = (
                    f"lock {self.path} held by PID {holder}"
                    if holder
                    else f"lock held: {self.path}"
                )
                raise SessionLockError(
                    "Another tg-agent process is already connected with this "
                    f"session ({where}). Refusing to connect a second time to "
                    "avoid Telegram's AuthKeyDuplicatedError. If that other "
                    "process already exited, this lock will clear on its own -- "
                    "retry."
                )
            time.sleep(poll_interval)

    def release(self) -> None:
        if self._fh is not None:
            self._clear_holder()
            _unlock(self._fh)
            self._fh.close()
            self._fh = None

    def holder_pid(self) -> Optional[int]:
        """PID left in the lock file by the current holder, if any (best effort)."""
        try:
            with open(self.path, "rb") as fh:
                fh.seek(_PID_OFFSET)
                text = fh.read().decode("ascii", "replace").strip()
        except OSError:
            return None
        parts = text.split()
        if parts and parts[0].isdigit() and int(parts[0]) > 0:
            return int(parts[0])
        return None

    def _record_holder(self) -> None:
        # The holder leaves its PID for a refused contender to report.
        self._clear_holder()
        fh = self._fh
        if fh is None:  # pragma: no cover - acquire() always sets _fh first
            return
        try:
            fh.seek(0)
            fh.write(" " * _PID_OFFSET + str(os.getpid()))
            fh.flush()
        except OSError:
            pass

    def _clear_holder(self) -> None:
        fh = self._fh
        if fh is None:  # pragma: no cover - release-time call, lock not held
            return
        try:
            fh.seek(0)
            fh.truncate(0)
            fh.flush()
        except OSError:
            pass


def session_identity(client: object) -> str:
    """Derive a stable identity string for a ``TelegramClient``'s session.

    File-based sessions key on the absolute file path; string sessions key on
    their serialized value (same auth key -> same identity, regardless of
    which process or working directory loaded it). Falls back to the
    client's object id for sessionless test doubles.
    """
    session = getattr(client, "session", None)
    if session is not None:
        filename = getattr(session, "filename", None)
        if filename:
            return f"file:{os.path.abspath(filename)}"
        save = getattr(session, "save", None)
        if callable(save):
            try:
                saved = save()
            except Exception:
                saved = None
            if saved:
                return f"string:{saved}"
    return f"anon:{id(client)}"


# --- tg-agent wiring (original tg-agent code; see THIRD_PARTY_NOTICES.md) ---


def _lock_dir() -> Path:
    """Lock directory: ``TGCF_SESSION_LOCK_DIR`` override, else the default.

    The default lives under the shared tempdir, so the worker daemon, the
    legacy web panel, and one-shot CLI processes resolve the same directory
    without sharing any project config.
    """
    return Path(os.environ.get("TGCF_SESSION_LOCK_DIR") or DEFAULT_LOCK_DIR)


def _grace_seconds() -> float:
    raw = os.environ.get("TGCF_SESSION_LOCK_GRACE_SECONDS")
    if not raw:
        return DEFAULT_GRACE_SECONDS
    try:
        return float(raw)
    except ValueError:
        return DEFAULT_GRACE_SECONDS


# Locks held by this process, keyed by session identity. The registry makes
# acquisition re-entrant within the process: an ephemeral force-native client
# opened over a live pooled session must not deadlock against its own lock.
_held_locks: dict[str, SessionLock] = {}

# Per-identity mutex serialising the check/acquire/register sequence. Without
# it, two concurrent acquisitions of the same session in this process each
# build their own SessionLock; the loser waits out the whole grace on flock and
# fails with SessionLockError naming the winner — which reports THIS process's
# PID (review #1556). Serialised per identity, the second caller joins the
# first via the registry instead. Distinct identities use distinct mutexes, so
# one session's grace wait never blocks another's acquisition.
_identity_locks: dict[str, asyncio.Lock] = {}


def release_all_session_locks() -> None:
    """Release every session lock this process holds (also wired via atexit)."""
    for lock in _held_locks.values():
        lock.release()
    _held_locks.clear()


atexit.register(release_all_session_locks)


async def acquire_session_lock(session_string: str, *, phone: str) -> SessionLock | None:
    """Take this process's cross-process lock for a stored session string.

    Returns ``None`` for an empty session string — interactive auth flows use
    fresh sessions, which carry no auth key to duplicate. Re-entrant within
    the process (see ``_held_locks``); concurrent acquisitions of the same
    session are serialised on a per-identity mutex (see ``_identity_locks``).
    The blocking grace-period poll runs in a worker thread so the event loop
    stays responsive while another holder winds down.

    The lock is held until process exit: the pool keeps its clients connected
    for the process lifetime, so per-lease release would churn the lock (and
    momentarily allow a second process in) while the connection is still open.
    OS release on process death is the backstop for crashes.
    """
    if not session_string:
        return None
    identity = f"string:{session_string}"
    async with _identity_locks.setdefault(identity, asyncio.Lock()):
        held = _held_locks.get(identity)
        if held is not None:
            return held
        lock = SessionLock(phone, identity, lock_dir=_lock_dir())
        await asyncio.to_thread(lock.acquire, grace_seconds=_grace_seconds())
        _held_locks[identity] = lock
        return lock
