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
import os
import struct
import time
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telethon import TelegramClient
from telethon.crypto import AES, AuthKey
from telethon.network.mtprotostate import MTProtoState
from telethon.sessions import StringSession
from telethon.tl.types import Pong

from tests.helpers import drain_loop, wait_until
from tg_agent.telegram.mtproto_watchdog import (
    SECURITY_WARNING_SUBSTR,
    TGCF_TELETHON_LOGGER_ROOT,
    MTProtoSecurityWatchdog,
    bind_telethon_base_logger,
)
from tg_agent.telegram.pool_lifecycle import _reset_stale_time_offset
from tg_agent.utils.safe_logging import mask_phone

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


def _clock_record(logger: logging.Logger, offset: int = -41) -> logging.LogRecord:
    """Emit the exact INFO shape of Telethon's clock re-sync (#1873).

    mtprotosender._handle_bad_notification logs «System clock is wrong, set
    time offset to %ds» at INFO — below what gateway.log keeps.
    """
    sender_logger_name = f"{logger.name}.network.mtprotosender"
    record = logging.LogRecord(
        name=sender_logger_name,
        level=logging.INFO,
        pathname="mtprotosender.py",
        lineno=1,
        msg="System clock is wrong, set time offset to %ds",
        args=(offset,),
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
                # Wait for the call_soon_threadsafe callback to spawn the
                # reconnect task and that task to invoke the callback.
                await wait_until(lambda: cb.await_count >= 1)
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
                await drain_loop()
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
                await drain_loop()
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
                await drain_loop()
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
                await drain_loop()
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
                await wait_until(lambda: cb.await_count >= 1)
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
                await wait_until(lambda: cb.await_count >= 1)
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

        from tg_agent.telegram.client_pool import ClientPool

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
        from tg_agent.telegram.client_pool import ClientPool

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

        from tg_agent.models import Account
        from tg_agent.telegram.backends import NativeTelethonBackend

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

        from tg_agent.models import Account
        from tg_agent.telegram.backends import NativeTelethonBackend

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

        from tg_agent.models import Account
        from tg_agent.telegram.backends import BackendRouter

        native = MagicMock()
        native.acquire_client = AsyncMock(return_value=MagicMock())
        router = BackendRouter(mode="auto", primary=MagicMock(), native=native)
        account = Account(phone=PHONE_A, session_string="sess", is_active=True)

        await router.acquire_client(account, force_native=True)

        native.acquire_client.assert_awaited_once_with(account, ephemeral=True)

    async def test_router_native_mode_pooled_client_is_supervised(self):
        from unittest.mock import MagicMock

        from tg_agent.models import Account
        from tg_agent.telegram.backends import BackendRouter

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

        from tg_agent.telegram.client_pool import ClientPool

        pool = ClientPool.__new__(ClientPool)
        pool.clients = {}
        pool._in_use = set()
        pool._active_leases = {}
        pool._dialogs_fetched = set()
        watchdog = MagicMock()
        pool._mtproto_watchdog = watchdog

        await pool.disconnect_all()

        watchdog.uninstall.assert_called_once_with()


class _RawClientWithState:
    """Raw-client double carrying a REAL ``MTProtoState`` — the #1873 target.

    The helper reaches the state through the real Telethon attribute chain
    ``client._sender._state`` — the fake must reproduce it exactly.
    """

    def __init__(self, state: MTProtoState) -> None:
        self._sender = SimpleNamespace(_state=state)
        self.disconnect = AsyncMock()
        self.connect = AsyncMock()
        self.catch_up = AsyncMock()
        self.is_user_authorized = AsyncMock(return_value=True)

    def is_connected(self) -> bool:
        return True


class _TelethonLoggers(dict):
    """Mirrors TelegramBaseClient.__init__._Loggers (real MTProtoState needs it)."""

    def __missing__(self, key: str) -> logging.Logger:
        if key.startswith("telethon."):
            key = key.split(".", maxsplit=1)[1]
        return logging.getLogger("tests.mtproto_state").getChild(key)


def _real_state(time_offset: int) -> MTProtoState:
    state = MTProtoState(AuthKey(os.urandom(256)), _TelethonLoggers())
    state.time_offset = time_offset
    return state


def _encrypt_like_server(state: MTProtoState, msg_id: int) -> bytes:
    """Build the body ``MTProtoState.decrypt_message_data`` accepts.

    Mirrors decrypt's own layout and key derivation (x=False): salt, session
    id, msg_id, seq, msg_len, a Pong payload, padding; msg_key over
    sha256(auth_key[96:128] + plaintext). The contract under test is the
    acceptance logic of decrypt_message_data, not the wire format.
    """
    payload = struct.pack("<Iqq", Pong.CONSTRUCTOR_ID, msg_id, 42)
    body = struct.pack("<qq", state.salt, state.id)
    body += struct.pack("<qii", msg_id, 1, len(payload)) + payload
    padding = os.urandom(-(len(body) + 12) % 16 + 12)
    plaintext = body + padding
    msg_key = sha256(state.auth_key.key[96:96 + 32] + plaintext).digest()[8:24]
    aes_key, aes_iv = MTProtoState._calc_key(state.auth_key.key, msg_key, False)
    return (
        struct.pack("<Q", state.auth_key.key_id) + msg_key
        + AES.encrypt_ige(plaintext, aes_key, aes_iv)
    )


def _fresh_server_msg_id() -> int:
    # Server msg_ids are odd; time lives in the high 32 bits.
    return (int(time.time()) << 32) | 1


class TestTimeOffsetRecovery:
    """#1873: a corrupted ``time_offset`` (≤ -30 s) makes every incoming
    message be dropped as «very new»; it must not survive a reconnect."""

    @pytest.mark.telegram_unit
    async def test_force_reconnect_resets_corrupted_offset(self):
        state = _real_state(time_offset=-60)
        pool = TestForceReconnectPhone._pool_with_client(_RawClientWithState(state))

        ok = await pool.force_reconnect_phone(PHONE_A)

        assert ok is True
        assert state.time_offset == 0
        assert state._last_msg_id == 0

    @pytest.mark.telegram_unit
    async def test_reconnect_phone_resets_corrupted_offset(self):
        from unittest.mock import MagicMock

        state = _real_state(time_offset=-60)
        pool = TestForceReconnectPhone._pool_with_client(_RawClientWithState(state))
        # is_connected False→True: the reconnect branch must fire.
        raw = pool.clients[PHONE_A].raw_client
        raw.is_connected = MagicMock(side_effect=[False, True])

        ok = await pool.reconnect_phone(PHONE_A)

        assert ok is True
        assert state.time_offset == 0
        assert state._last_msg_id == 0

    @pytest.mark.telegram_unit
    def test_reset_helper_skips_zero_offset_and_missing_state(self, caplog):
        state = _real_state(time_offset=0)
        with caplog.at_level(logging.WARNING):
            _reset_stale_time_offset(PHONE_A, SimpleNamespace(_sender=SimpleNamespace(_state=state)))
            _reset_stale_time_offset(PHONE_A, SimpleNamespace())  # no _sender
            _reset_stale_time_offset(PHONE_A, SimpleNamespace(_sender=SimpleNamespace()))  # no _state
        assert state.time_offset == 0
        assert not [r for r in caplog.records if "time_offset" in r.getMessage()]

    @pytest.mark.telegram_unit
    async def test_brick_recovery_restores_message_acceptance(self):
        """The issue's regression: a client whose state has time_offset = −60
        drops a fresh msg_id; after the watchdog's recovery the next fresh
        message is accepted (through the real ``decrypt_message_data``)."""
        state = _real_state(time_offset=-60)
        pool = TestForceReconnectPhone._pool_with_client(_RawClientWithState(state))

        dropped = state.decrypt_message_data(
            _encrypt_like_server(state, _fresh_server_msg_id())
        )
        assert dropped is None  # «Server sent a very new message», the #556 brick

        assert await pool.force_reconnect_phone(PHONE_A) is True
        assert state.time_offset == 0

        accepted = state.decrypt_message_data(
            _encrypt_like_server(state, _fresh_server_msg_id())
        )
        assert accepted is not None


class TestClockOffsetSurfacing:
    """#1873: the INFO clock correction is the brick trigger — the watchdog
    must surface it as a WARNING with the phone instead of letting gateway.log
    drop it, and its sender-logger routing must not flood or double-count."""

    def test_clock_info_surfaces_as_phone_attributed_warning(self, caplog):
        async def _run():
            wd, cb = _make_watchdog()
            wd.install(asyncio.get_running_loop())
            try:
                base = wd.register_phone(PHONE_A)
                with caplog.at_level(logging.WARNING):
                    wd.emit(_clock_record(base))
                await drain_loop()
                assert cb.await_count == 0  # INFO never counts toward the threshold
                hits = [
                    r for r in caplog.records
                    if r.levelno == logging.WARNING and "adjusted time offset" in r.getMessage()
                ]
                assert hits, "the clock correction must be re-emitted at WARNING"
                assert mask_phone(PHONE_A) in hits[0].getMessage()
                assert "-41s" in hits[0].getMessage()
            finally:
                wd.uninstall()

        asyncio.run(_run())

    def test_other_sender_info_is_dropped(self, caplog):
        async def _run():
            wd, cb = _make_watchdog()
            wd.install(asyncio.get_running_loop())
            try:
                base = wd.register_phone(PHONE_A)
                rec = _clock_record(base)
                rec.msg = "Connecting to %s..."
                rec.args = ("dc",)
                with caplog.at_level(logging.INFO):
                    wd.emit(rec)
                await drain_loop()
                assert cb.await_count == 0
                assert not [
                    r for r in caplog.records if r.name == "tg_agent.telegram.mtproto_watchdog"
                ]
            finally:
                wd.uninstall()

        asyncio.run(_run())

    def test_sender_warning_reemitted_with_phone(self, caplog):
        async def _run():
            # Default threshold: one record must not schedule a reconnect task.
            wd, cb = _make_watchdog()
            wd.install(asyncio.get_running_loop())
            try:
                base = wd.register_phone(PHONE_A)
                with caplog.at_level(logging.WARNING):
                    wd.emit(_security_record(base))
                await drain_loop()
                assert cb.await_count == 0
                reemitted = [
                    r for r in caplog.records
                    if "[mtproto" in r.getMessage() and SECURITY_WARNING_SUBSTR in r.getMessage()
                ]
                assert reemitted, "sender warnings must stay in the log after the routing change"
                assert mask_phone(PHONE_A) in reemitted[0].getMessage()
                assert reemitted[0].levelno == logging.WARNING
            finally:
                wd.uninstall()

        asyncio.run(_run())

    def test_register_phone_routes_and_unregister_restores_sender_logger(self):
        wd, _cb = _make_watchdog()
        base = wd.register_phone(PHONE_A)
        sender = logging.getLogger(f"{base.name}.network.mtprotosender")
        assert sender.getEffectiveLevel() == logging.INFO
        assert sender.propagate is False
        assert wd in sender.handlers
        wd.unregister_phone(PHONE_A)
        assert sender.level == logging.NOTSET
        assert sender.propagate is True
        assert wd not in sender.handlers

    def test_uninstall_detaches_sender_loggers(self):
        wd, _cb = _make_watchdog()
        base = wd.register_phone(PHONE_A)
        sender = logging.getLogger(f"{base.name}.network.mtprotosender")
        wd.uninstall()
        assert wd not in sender.handlers
        assert not wd._sender_loggers


class TestForceReconnectFailureLogging:
    """#1873: a failed force reconnect must never be silent."""

    async def test_unknown_phone_failure_is_logged(self, caplog):
        from tg_agent.telegram.client_pool import ClientPool

        pool = ClientPool.__new__(ClientPool)
        pool.clients = {}
        with caplog.at_level(logging.WARNING):
            assert await pool.force_reconnect_phone(PHONE_A) is False
        assert any(
            "no client in the pool" in r.getMessage() and r.levelno == logging.WARNING
            for r in caplog.records
        )

    async def test_transport_down_after_connect_failure_is_logged(self, caplog):
        from unittest.mock import MagicMock

        raw = AsyncMock()
        raw.is_connected = MagicMock(return_value=False)
        raw.is_user_authorized = AsyncMock(return_value=True)
        pool = TestForceReconnectPhone._pool_with_client(raw)

        with caplog.at_level(logging.WARNING):
            assert await pool.force_reconnect_phone(PHONE_A) is False
        assert any(
            "did not come back" in r.getMessage() and r.levelno == logging.WARNING
            for r in caplog.records
        )

    async def test_reconnect_phone_unknown_phone_is_logged(self, caplog):
        from tg_agent.telegram.client_pool import ClientPool

        pool = ClientPool.__new__(ClientPool)
        pool.clients = {}
        with caplog.at_level(logging.WARNING):
            assert await pool.reconnect_phone(PHONE_A) is False
        assert any(
            "no client in the pool" in r.getMessage() for r in caplog.records
        )
