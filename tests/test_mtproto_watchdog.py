"""Tests for the MTProto security-warning watchdog (#556).

Telethon's ``MTProtoSender._recv_loop`` logs «Security error while unpacking a
received message: Too many messages had to be ignored consecutively» and keeps
the connection formally alive — but every incoming update is dropped (a silent
brick). The watchdog listens on per-phone Telethon loggers and force-reconnects
the affected client.
"""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock

import pytest
from telethon import TelegramClient
from telethon.sessions import StringSession

from src.telegram.mtproto_watchdog import (
    SECURITY_WARNING_SUBSTR,
    TGCF_TELETHON_LOGGER_ROOT,
    MTProtoSecurityWatchdog,
    bind_telethon_base_logger,
)

PHONE_A = "+70001112233"
PHONE_B = "+70002223344"


def _make_watchdog(**kwargs) -> tuple[MTProtoSecurityWatchdog, AsyncMock]:
    cb = AsyncMock()
    wd = MTProtoSecurityWatchdog(cb, **kwargs)
    return wd, cb


def _security_record(logger: logging.Logger, detail: str = "Too many messages had to be ignored consecutively"):
    """Emit the exact warning shape Telethon's MTProtoSender produces."""
    sender_logger_name = f"{logger.name}.network.mtprotosender"
    record = logging.LogRecord(
        name=sender_logger_name,
        level=logging.WARNING,
        pathname="mtprotosender.py",
        lineno=1,
        msg="Security error while unpacking a received message: %s",
        args=(detail,),
        exc_info=None,
    )
    return record


class TestEmitFiltering:
    def test_threshold_triggers_reconnect_once(self):
        async def _run():
            wd, cb = _make_watchdog(threshold=3, window_sec=60.0, cooldown_sec=300.0)
            loop = asyncio.get_running_loop()
            wd.install(loop)
            try:
                base = wd.register_phone(PHONE_A)
                for _ in range(6):
                    wd.emit(_security_record(base))
                # Let call_soon_threadsafe callbacks and the spawned task run.
                await asyncio.sleep(0)
                await asyncio.sleep(0)
                cb.assert_awaited_once_with(PHONE_A)
            finally:
                wd.uninstall()

        asyncio.run(_run())

    def test_below_threshold_does_not_trigger(self):
        async def _run():
            wd, cb = _make_watchdog(threshold=3)
            wd.install(asyncio.get_running_loop())
            try:
                base = wd.register_phone(PHONE_A)
                wd.emit(_security_record(base))
                wd.emit(_security_record(base))
                await asyncio.sleep(0)
                cb.assert_not_awaited()
            finally:
                wd.uninstall()

        asyncio.run(_run())

    def test_per_phone_isolation(self):
        async def _run():
            wd, cb = _make_watchdog(threshold=3)
            wd.install(asyncio.get_running_loop())
            try:
                base_a = wd.register_phone(PHONE_A)
                base_b = wd.register_phone(PHONE_B)
                wd.emit(_security_record(base_a))
                wd.emit(_security_record(base_a))
                wd.emit(_security_record(base_b))
                await asyncio.sleep(0)
                # Neither phone reached 3 on its own.
                cb.assert_not_awaited()
            finally:
                wd.uninstall()

        asyncio.run(_run())

    def test_irrelevant_message_and_logger_ignored(self):
        async def _run():
            wd, cb = _make_watchdog(threshold=1)
            wd.install(asyncio.get_running_loop())
            try:
                base = wd.register_phone(PHONE_A)
                # Right logger, harmless message.
                rec = _security_record(base)
                rec.msg = "Connection to %s complete!"
                rec.args = ("dc",)
                wd.emit(rec)
                # Right message, unrelated logger (no registered slug).
                rec2 = logging.LogRecord(
                    name="telethon.network.mtprotosender",
                    level=logging.WARNING,
                    pathname="x",
                    lineno=1,
                    msg=f"{SECURITY_WARNING_SUBSTR}: boom",
                    args=(),
                    exc_info=None,
                )
                wd.emit(rec2)
                await asyncio.sleep(0)
                cb.assert_not_awaited()
            finally:
                wd.uninstall()

        asyncio.run(_run())

    def test_unregister_stops_tracking(self):
        async def _run():
            wd, cb = _make_watchdog(threshold=1)
            wd.install(asyncio.get_running_loop())
            try:
                base = wd.register_phone(PHONE_A)
                wd.unregister_phone(PHONE_A)
                wd.emit(_security_record(base))
                await asyncio.sleep(0)
                cb.assert_not_awaited()
            finally:
                wd.uninstall()

        asyncio.run(_run())

    def test_cooldown_blocks_repeat_reconnects(self):
        async def _run():
            wd, cb = _make_watchdog(threshold=1, cooldown_sec=300.0)
            wd.install(asyncio.get_running_loop())
            try:
                base = wd.register_phone(PHONE_A)
                wd.emit(_security_record(base))
                wd.emit(_security_record(base))
                await asyncio.sleep(0)
                await asyncio.sleep(0)
                cb.assert_awaited_once()
            finally:
                wd.uninstall()

        asyncio.run(_run())

    def test_emit_without_loop_is_safe(self):
        wd, cb = _make_watchdog(threshold=1)
        base = wd.register_phone(PHONE_A)
        # Not installed — must not raise.
        wd.emit(_security_record(base))
        cb.assert_not_awaited()


class TestPropagationThroughLoggingTree:
    def test_handler_on_root_receives_child_warning(self):
        """The handler is installed once on the telethon.tgcf root; warnings on
        per-phone child loggers must reach it via propagation."""

        async def _run():
            wd, cb = _make_watchdog(threshold=1)
            wd.install(asyncio.get_running_loop())
            try:
                base = wd.register_phone(PHONE_A)
                sender_logger = logging.getLogger(f"{base.name}.network.mtprotosender")
                sender_logger.warning(
                    "Security error while unpacking a received message: %s",
                    "Too many messages had to be ignored consecutively",
                )
                await asyncio.sleep(0)
                await asyncio.sleep(0)
                cb.assert_awaited_once_with(PHONE_A)
            finally:
                wd.uninstall()

        asyncio.run(_run())


@pytest.mark.telegram_unit
class TestTelethonIntegration:
    """Against the installed Telethon — catches breaking upgrades of the
    private ``_log`` / ``_sender._log`` attributes."""

    def test_native_base_logger_param_routes_sender_logger(self):
        wd, _cb = _make_watchdog()
        base = wd.register_phone(PHONE_A)
        client = TelegramClient(StringSession(), 1, "x", base_logger=base)
        assert client._sender._log.name.startswith(f"{TGCF_TELETHON_LOGGER_ROOT}.")
        assert client._sender._log.name.endswith(".network.mtprotosender")

    def test_bind_telethon_base_logger_rebinds_existing_client(self):
        """CLI-backend clients are built without base_logger; bind must swap
        both the client loggers dict and the already-captured sender logger."""
        wd, _cb = _make_watchdog()
        base = wd.register_phone(PHONE_A)
        client = TelegramClient(StringSession(), 1, "x")
        assert not client._sender._log.name.startswith(TGCF_TELETHON_LOGGER_ROOT)

        ok = bind_telethon_base_logger(client, base)

        assert ok is True
        assert client._sender._log.name == f"{base.name}.network.mtprotosender"
        # The loggers mapping serves arbitrary telethon.* keys from our base.
        assert client._log["telethon.client.updates"].name == f"{base.name}.client.updates"

    def test_bind_degrades_gracefully(self):
        wd, _cb = _make_watchdog()
        base = wd.register_phone(PHONE_A)
        assert bind_telethon_base_logger(object(), base) is False


class TestForceReconnectPhone:
    @staticmethod
    def _pool_with_client(raw) -> object:
        from types import SimpleNamespace

        from src.telegram.client_pool import ClientPool

        pool = ClientPool.__new__(ClientPool)
        pool.clients = {PHONE_A: SimpleNamespace(raw_client=raw)}
        return pool

    async def test_reconnects_even_when_formally_connected(self):
        from unittest.mock import MagicMock

        raw = AsyncMock()
        raw.is_connected = MagicMock(return_value=True)
        raw.is_user_authorized = AsyncMock(return_value=True)
        pool = self._pool_with_client(raw)

        ok = await pool.force_reconnect_phone(PHONE_A)

        assert ok is True
        raw.disconnect.assert_awaited_once()
        raw.connect.assert_awaited_once()

    async def test_unknown_phone_returns_false(self):
        from src.telegram.client_pool import ClientPool

        pool = ClientPool.__new__(ClientPool)
        pool.clients = {}
        assert await pool.force_reconnect_phone(PHONE_A) is False

    async def test_unauthorized_session_returns_false(self):
        from unittest.mock import MagicMock

        raw = AsyncMock()
        raw.is_connected = MagicMock(return_value=True)
        raw.is_user_authorized = AsyncMock(return_value=False)
        pool = self._pool_with_client(raw)

        assert await pool.force_reconnect_phone(PHONE_A) is False


class TestBackendLoggerProvider:
    async def test_native_backend_passes_per_phone_logger(self):
        from unittest.mock import MagicMock

        from src.models import Account
        from src.telegram.backends import NativeTelethonBackend

        auth = MagicMock()
        client = MagicMock()
        auth.create_client_from_session = AsyncMock(return_value=client)
        base = logging.getLogger("telethon.tgcf.testslug")
        provider = MagicMock(return_value=base)
        backend = NativeTelethonBackend(auth, client_logger_provider=provider)
        account = Account(phone=PHONE_A, session_string="sess", is_active=True)

        lease = await backend.acquire_client(account)

        provider.assert_called_once_with(PHONE_A)
        auth.create_client_from_session.assert_awaited_once_with(
            "sess", base_logger=base
        )
        assert lease.phone == PHONE_A

    async def test_ephemeral_native_client_gets_no_watchdog_logger(self):
        """Review P2 (#817): force_native sessions are short-lived and never
        replace ``clients[phone]`` — giving them the per-phone watchdog logger
        would misattribute their security warnings to the phone and trigger a
        reconnect of the healthy *pooled* client."""
        from unittest.mock import MagicMock

        from src.models import Account
        from src.telegram.backends import NativeTelethonBackend

        auth = MagicMock()
        auth.create_client_from_session = AsyncMock(return_value=MagicMock())
        provider = MagicMock()
        backend = NativeTelethonBackend(auth, client_logger_provider=provider)
        account = Account(phone=PHONE_A, session_string="sess", is_active=True)

        await backend.acquire_client(account, ephemeral=True)

        provider.assert_not_called()
        auth.create_client_from_session.assert_awaited_once_with(
            "sess", base_logger=None
        )

    async def test_router_marks_force_native_as_ephemeral(self):
        from unittest.mock import MagicMock

        from src.models import Account
        from src.telegram.backends import BackendRouter

        native = MagicMock()
        native.acquire_client = AsyncMock(return_value=MagicMock())
        router = BackendRouter(mode="auto", primary=MagicMock(), native=native)
        account = Account(phone=PHONE_A, session_string="sess", is_active=True)

        await router.acquire_client(account, force_native=True)

        native.acquire_client.assert_awaited_once_with(account, ephemeral=True)

    async def test_router_native_mode_pooled_client_is_supervised(self):
        from unittest.mock import MagicMock

        from src.models import Account
        from src.telegram.backends import BackendRouter

        native = MagicMock()
        native.acquire_client = AsyncMock(return_value=MagicMock())
        router = BackendRouter(mode="native", primary=MagicMock(), native=native)
        account = Account(phone=PHONE_A, session_string="sess", is_active=True)

        await router.acquire_client(account)

        native.acquire_client.assert_awaited_once_with(account, ephemeral=False)


class TestDisconnectAllUninstall:
    async def test_disconnect_all_uninstalls_watchdog_handler(self):
        """Review F1 (#817): pool teardown must detach the watchdog handler
        from the global ``telethon.tgcf`` logger — a stale handler would keep
        the dead pool referenced and pile up across pool re-creations."""
        from unittest.mock import MagicMock

        from src.telegram.client_pool import ClientPool

        pool = ClientPool.__new__(ClientPool)
        pool.clients = {}
        pool._in_use = set()
        pool._active_leases = {}
        pool._dialogs_fetched = set()
        watchdog = MagicMock()
        pool._mtproto_watchdog = watchdog

        await pool.disconnect_all()

        watchdog.uninstall.assert_called_once_with()
