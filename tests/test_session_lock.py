"""Tests for the cross-process session lock and auth-key retry (#1548).

Fake-first: the multiprocess cases exercise real ``flock()`` contention between
OS processes but never touch Telegram; the retry cases run on mock clients.
"""

from __future__ import annotations

import multiprocessing
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon.errors import AuthKeyDuplicatedError

from tg_agent.models import Account
from tg_agent.telegram import backends as backends_module
from tg_agent.telegram.backends import (
    BackendClientLease,
    BackendRouter,
    TelegramTransportSession,
    TelethonCliBackend,
)
from tg_agent.telegram.session_lock import (
    SessionLock,
    SessionLockError,
    acquire_session_lock,
    release_all_session_locks,
)
from tg_shared.auth import TelegramAuth, connect_with_auth_key_retry

PHONE = "+15550000001"
SESSION = "test-session-string-1548"


def _make_lock(tmp_path: Path, identity: str) -> SessionLock:
    return SessionLock("tester", identity, lock_dir=tmp_path / "locks")


def _account(session_string: str = SESSION) -> Account:
    return Account(phone=PHONE, session_string=session_string, is_active=True)


class _FlakyConnectClient:
    """Mock transport client failing the first N connects with ``error``."""

    def __init__(self, failures: int, error: Exception | None = None):
        self._failures = failures
        self._error = error if error is not None else AuthKeyDuplicatedError(None)
        self.connect_calls = 0
        self.disconnect_calls = 0

    async def connect(self) -> None:
        self.connect_calls += 1
        if self.connect_calls <= self._failures:
            raise self._error

    async def disconnect(self) -> None:
        self.disconnect_calls += 1


class _FakeBackend:
    name = "fake_primary"

    def __init__(self) -> None:
        self.calls: list[Account] = []
        self.lease = BackendClientLease(
            phone=PHONE,
            session=TelegramTransportSession(SimpleNamespace()),
            backend_name=self.name,
        )

    async def acquire_client(self, account: Account, *, ephemeral: bool = False):
        self.calls.append(account)
        return self.lease


class _FakeNativeBackend(_FakeBackend):
    name = "native"


class TestSessionLockUnit:
    """Same-process semantics of the vendored lock primitive."""

    def test_conflict_reports_holder_pid_and_release_unblocks(self, tmp_path):
        first = _make_lock(tmp_path, "ident-1")
        first.acquire(grace_seconds=1.0, poll_interval=0.01)
        assert first.holder_pid() == os.getpid()

        second = _make_lock(tmp_path, "ident-1")
        with pytest.raises(SessionLockError) as exc_info:
            second.acquire(grace_seconds=0.1, poll_interval=0.01)
        assert str(os.getpid()) in str(exc_info.value)

        first.release()
        second.acquire(grace_seconds=1.0, poll_interval=0.01)  # stale PID reclaimed
        assert second.holder_pid() == os.getpid()
        second.release()

    def test_distinct_identities_do_not_conflict(self, tmp_path):
        a = _make_lock(tmp_path, "ident-a")
        b = _make_lock(tmp_path, "ident-b")
        a.acquire(grace_seconds=1.0, poll_interval=0.01)
        b.acquire(grace_seconds=1.0, poll_interval=0.01)
        a.release()
        b.release()


class TestAcquireSessionLockWiring:
    async def test_empty_session_string_is_noop(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TGCF_SESSION_LOCK_DIR", str(tmp_path / "locks"))
        assert await acquire_session_lock("", phone=PHONE) is None

    async def test_registry_reuses_held_lock_within_process(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("TGCF_SESSION_LOCK_DIR", str(tmp_path / "locks"))
        monkeypatch.setenv("TGCF_SESSION_LOCK_GRACE_SECONDS", "1")
        first = await acquire_session_lock(SESSION, phone=PHONE)
        second = await acquire_session_lock(SESSION, phone=PHONE)
        # Re-entrant: an ephemeral force-native client over a live pooled
        # session must not deadlock against its own lock.
        assert first is second
        release_all_session_locks()
        third = await acquire_session_lock(SESSION, phone=PHONE)
        assert third is not first
        release_all_session_locks()

    async def test_concurrent_acquisitions_share_held_lock(
        self, tmp_path, monkeypatch
    ):
        """Regression (review #1556): two acquisitions STARTED TOGETHER must
        join, not race. Without the per-identity mutex the second call built
        its own SessionLock, waited out the whole grace on flock, and failed
        with SessionLockError naming this process's own PID as the holder."""
        import asyncio

        monkeypatch.setenv("TGCF_SESSION_LOCK_DIR", str(tmp_path / "locks"))
        monkeypatch.setenv("TGCF_SESSION_LOCK_GRACE_SECONDS", "5")
        first, second = await asyncio.gather(
            acquire_session_lock(SESSION, phone=PHONE),
            acquire_session_lock(SESSION, phone=PHONE),
        )
        assert first is not None
        assert first is second
        release_all_session_locks()


def _holder_child(
    session_string: str, lock_dir: str, ready_path: str, hold_seconds: float
) -> None:
    """Spawned holder process target (module level: picklable under spawn)."""
    os.environ["TGCF_SESSION_LOCK_DIR"] = lock_dir
    lock = SessionLock("child", f"string:{session_string}", lock_dir=Path(lock_dir))
    lock.acquire(grace_seconds=10.0, poll_interval=0.05)
    Path(ready_path).write_text(str(os.getpid()), encoding="ascii")
    time.sleep(hold_seconds)
    lock.release()


@pytest.mark.integration
@pytest.mark.skipif(
    sys.platform == "win32",
    reason="multiprocess lock coverage is POSIX-only; the msvcrt path stays ported but untested",
)
class TestCrossProcess:
    async def test_second_process_refused_with_holder_pid_then_stale_pid_reclaimed(
        self, tmp_path, monkeypatch
    ):
        lock_dir = tmp_path / "locks"
        ready = tmp_path / "holder-ready"
        monkeypatch.setenv("TGCF_SESSION_LOCK_DIR", str(lock_dir))
        monkeypatch.setenv("TGCF_SESSION_LOCK_GRACE_SECONDS", "0.3")

        child = multiprocessing.Process(
            target=_holder_child,
            args=(SESSION, str(lock_dir), str(ready), 1.0),
        )
        child.start()
        try:
            deadline = time.monotonic() + 10.0
            while not ready.exists() and time.monotonic() < deadline:
                time.sleep(0.02)
            assert ready.exists(), "holder child never signalled readiness"

            with pytest.raises(SessionLockError) as exc_info:
                await acquire_session_lock(SESSION, phone=PHONE)
            assert child.pid is not None
            assert str(child.pid) in str(exc_info.value)
        finally:
            child.join(timeout=10.0)
        assert child.exitcode == 0

        # Holder exited: the OS released the lock, no manual intervention —
        # a new acquisition succeeds and reclaims the stale PID entry.
        monkeypatch.setenv("TGCF_SESSION_LOCK_GRACE_SECONDS", "5")
        lock = await acquire_session_lock(SESSION, phone=PHONE)
        assert lock is not None
        release_all_session_locks()


class TestConnectWithAuthKeyRetry:
    async def test_transient_failures_recover(self):
        client = _FlakyConnectClient(failures=2)
        await connect_with_auth_key_retry(
            client, phone=PHONE, max_attempts=4, initial_delay=0.01, max_delay=0.02
        )
        assert client.connect_calls == 3
        # Half-open transport torn down between attempts.
        assert client.disconnect_calls == 2

    async def test_attempts_bounded_then_reraises(self):
        client = _FlakyConnectClient(failures=99)
        with pytest.raises(AuthKeyDuplicatedError):
            await connect_with_auth_key_retry(
                client, phone=PHONE, max_attempts=4, initial_delay=0.01, max_delay=0.02
            )
        assert client.connect_calls == 4
        assert client.disconnect_calls == 4

    async def test_other_errors_not_retried(self):
        boom = ConnectionError("no route")
        client = _FlakyConnectClient(failures=99, error=boom)
        with pytest.raises(ConnectionError):
            await connect_with_auth_key_retry(
                client, phone=PHONE, max_attempts=4, initial_delay=0.01, max_delay=0.02
            )
        assert client.connect_calls == 1


class TestRouterSessionLock:
    async def test_lock_taken_before_backend_dispatch(self, monkeypatch):
        primary, native = _FakeBackend(), _FakeNativeBackend()
        order: list[str] = []

        async def _spy(session_string: str, *, phone: str):
            order.append("lock")
            return None

        monkeypatch.setattr(backends_module, "acquire_session_lock", _spy)

        original_acquire = primary.acquire_client

        async def _primary_spy(account, *, ephemeral: bool = False):
            order.append("backend")
            return await original_acquire(account)

        primary.acquire_client = _primary_spy  # type: ignore[method-assign]
        router = BackendRouter(mode="auto", primary=primary, native=native)

        lease = await router.acquire_client(_account())

        assert lease is primary.lease
        assert order == ["lock", "backend"]

    async def test_lock_failure_blocks_backend(self, monkeypatch):
        async def _deny(session_string: str, *, phone: str):
            raise SessionLockError("lock held by PID 12345")

        monkeypatch.setattr(backends_module, "acquire_session_lock", _deny)
        primary, native = _FakeBackend(), _FakeNativeBackend()
        router = BackendRouter(mode="auto", primary=primary, native=native)

        with pytest.raises(SessionLockError):
            await router.acquire_client(_account())
        assert primary.calls == []
        assert native.calls == []

    async def test_empty_session_string_reaches_helper_verbatim(self, monkeypatch):
        # The no-op for fresh/empty strings lives inside acquire_session_lock
        # (TestAcquireSessionLockWiring); the router must forward the value
        # unchanged and proceed to the backend either way.
        seen: list[str] = []

        async def _spy(session_string: str, *, phone: str):
            seen.append(session_string)
            return None

        monkeypatch.setattr(backends_module, "acquire_session_lock", _spy)
        primary, native = _FakeBackend(), _FakeNativeBackend()
        router = BackendRouter(mode="auto", primary=primary, native=native)

        await router.acquire_client(_account(session_string=""))
        assert seen == [""]
        assert len(primary.calls) == 1

    async def test_auth_key_duplicated_does_not_fall_back_to_native(self):
        class _DupBackend:
            name = "dup"

            async def acquire_client(self, account: Account, *, ephemeral: bool = False):
                raise AuthKeyDuplicatedError(None)

        native = _FakeNativeBackend()
        router = BackendRouter(mode="auto", primary=_DupBackend(), native=native)

        with pytest.raises(AuthKeyDuplicatedError):
            await router.acquire_client(_account())
        # The native fallback materialises the SAME session string; retrying
        # there would only repeat the bounded retry storm.
        assert native.calls == []


class TestCliBackendRetryWiring:
    async def test_cli_backend_connect_retries_auth_key_duplicated(
        self, monkeypatch, tmp_path
    ):
        class _Client:
            def __init__(self) -> None:
                self.connect_calls = 0

            async def connect(self) -> None:
                self.connect_calls += 1
                if self.connect_calls == 1:
                    raise AuthKeyDuplicatedError(None)

            async def disconnect(self) -> None:
                return None

            async def is_user_authorized(self) -> bool:
                return True

        client = _Client()
        monkeypatch.setattr(
            backends_module.telethon_cli_runtime, "create_client", lambda ns: client
        )
        backend = TelethonCliBackend(
            TelegramAuth(1, "hash"), _FakeMaterializer(tmp_path / "cache")
        )

        lease = await backend.acquire_client(_account())

        assert client.connect_calls == 2
        assert lease.phone == PHONE


class _FakeMaterializer:
    """Minimal SessionMaterializer double: passthrough path, no FS work."""

    def __init__(self, cache_dir: Path) -> None:
        self._cache_dir = cache_dir

    def materialize(self, phone: str, session_string: str) -> str:
        return str(self._cache_dir / phone)

    def ensure_empty_env_file(self) -> str:
        return str(self._cache_dir / ".env")
