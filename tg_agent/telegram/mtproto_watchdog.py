"""Watchdog for Telethon MTProto security warnings (#556).

Telethon's ``MTProtoSender._recv_loop`` handles a ``SecurityError`` («Too many
messages had to be ignored consecutively») by logging a warning and continuing:
the connection stays formally «connected», but every incoming message is
dropped — a silent brick. ``ClientPool.reconnect_phone`` only reconnects when
``not client.is_connected()``, so nothing ever recovers such a client.

This module attributes those warnings to a phone and force-reconnects the
affected client:

- every pool client gets a per-phone Telethon ``base_logger``
  (``telethon.tgcf.<sha-slug>``; the slug comes from
  :func:`tg_agent.utils.safe_logging.text_hash` so phone numbers never leak into
  logger names);
- one :class:`MTProtoSecurityWatchdog` handler sits on the
  ``telethon.tgcf`` root and receives the per-phone children via propagation;
- the per-phone ``network.mtprotosender`` logger is additionally lifted to
  INFO and routed here with propagation off (#1873): Telethon logs the
  clock-offset correction — the brick trigger — at INFO, and gateway.log keeps
  WARNING+; this handler surfaces it as a WARNING and re-emits the sender's
  warnings with the phone they lack;
- ``threshold`` warnings within ``window_sec`` trigger the async
  ``reconnect_cb(phone)`` (scheduled via ``call_soon_threadsafe`` — Telethon
  may log from a non-loop thread), with a per-phone ``cooldown_sec`` guard
  against reconnect storms.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable

from tg_agent.utils.safe_logging import mask_phone, text_hash

logger = logging.getLogger(__name__)

SECURITY_WARNING_SUBSTR = "Security error while unpacking a received message"
BRICK_DETAIL_SUBSTR = "Too many messages had to be ignored consecutively"
TGCF_TELETHON_LOGGER_ROOT = "telethon.tgcf"
_SENDER_LOGGER_SUFFIX = ".network.mtprotosender"
# Telethon logs its clock re-sync («System clock is wrong, set time offset to
# %ds», mtprotosender._handle_bad_notification) at INFO — the level gateway.log
# drops. A corrupted offset is exactly the #556 brick trigger (#1873), so the
# per-phone sender loggers are lifted to INFO and this substring surfaces as a
# WARNING through our logger.
TIME_OFFSET_INFO_SUBSTR = "System clock is wrong, set time offset to"

DEFAULT_THRESHOLD = 3
DEFAULT_WINDOW_SEC = 60.0
DEFAULT_COOLDOWN_SEC = 300.0


class MTProtoSecurityWatchdog(logging.Handler):
    """Counts MTProto security warnings per phone and fires a reconnect."""

    def __init__(
        self,
        reconnect_cb: Callable[[str], Awaitable[None]],
        *,
        threshold: int = DEFAULT_THRESHOLD,
        window_sec: float = DEFAULT_WINDOW_SEC,
        cooldown_sec: float = DEFAULT_COOLDOWN_SEC,
    ) -> None:
        # INFO, not WARNING: the per-phone sender loggers are lifted to INFO so
        # the clock-offset correction (#1873) reaches this handler; everything
        # below WARNING that is not that line is dropped inside emit().
        super().__init__(level=logging.INFO)
        self._reconnect_cb = reconnect_cb
        self._threshold = max(1, int(threshold))
        self._window_sec = float(window_sec)
        self._cooldown_sec = float(cooldown_sec)
        self._slug_to_phone: dict[str, str] = {}
        self._sender_loggers: dict[str, logging.Logger] = {}
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._last_reconnect_at: dict[str, float] = {}
        self._reconnect_count: dict[str, int] = defaultdict(int)
        self._loop: asyncio.AbstractEventLoop | None = None

    # -- lifecycle ---------------------------------------------------------

    def install(self, loop: asyncio.AbstractEventLoop) -> None:
        """Attach to the telethon.tgcf root logger and capture the loop."""
        self._loop = loop
        root = logging.getLogger(TGCF_TELETHON_LOGGER_ROOT)
        if self not in root.handlers:
            root.addHandler(self)

    def uninstall(self) -> None:
        logging.getLogger(TGCF_TELETHON_LOGGER_ROOT).removeHandler(self)
        for sender in self._sender_loggers.values():
            sender.removeHandler(self)
        self._sender_loggers.clear()
        self._loop = None

    def register_phone(self, phone: str) -> logging.Logger:
        """Return the per-phone Telethon base logger and start tracking it."""
        phone = str(phone)
        slug = text_hash(phone)
        self._slug_to_phone[slug] = phone
        # Lift the per-phone sender logger to INFO and route it here (#1873):
        # the clock-offset correction is logged at INFO, below every handler
        # gateway.log keeps. propagate=False — the same record must not reach
        # this handler a second time via the telethon.tgcf attachment (that
        # would double-count toward the brick threshold), and the raw lines
        # must not flood the root loggers with Telethon's INFO chatter.
        sender = logging.getLogger(f"{TGCF_TELETHON_LOGGER_ROOT}.{slug}{_SENDER_LOGGER_SUFFIX}")
        sender.setLevel(logging.INFO)
        sender.propagate = False
        if self not in sender.handlers:
            sender.addHandler(self)
        self._sender_loggers[slug] = sender
        return logging.getLogger(f"{TGCF_TELETHON_LOGGER_ROOT}.{slug}")

    def unregister_phone(self, phone: str) -> None:
        phone = str(phone)
        slug = text_hash(phone)
        self._slug_to_phone.pop(slug, None)
        sender = self._sender_loggers.pop(slug, None)
        if sender is not None:
            sender.removeHandler(self)
            sender.setLevel(logging.NOTSET)
            sender.propagate = True
        self._events.pop(phone, None)
        self._last_reconnect_at.pop(phone, None)

    def get_stats(self) -> dict[str, int]:
        """Reconnects triggered per phone (for diagnostics)."""
        return dict(self._reconnect_count)

    # -- logging.Handler ----------------------------------------------------

    def emit(self, record: logging.LogRecord) -> None:  # pragma: no branch
        try:
            phone = self._phone_for_record(record)
            if phone is None:
                return
            message = record.getMessage()
            if record.levelno < logging.WARNING:
                # The per-phone sender loggers are INFO-enabled (#1873) so the
                # clock-offset correction is visible; among their INFO only
                # that line matters — the rest stays out of the log.
                if TIME_OFFSET_INFO_SUBSTR in message:
                    logger.warning(
                        "Telethon adjusted time offset for %s — %s (a corrupted "
                        "offset is the #556 brick trigger)",
                        mask_phone(phone),
                        message,
                    )
                return
            # Sender records stop propagating (#1873): re-emit their warnings
            # so gateway.log keeps them, now with the phone they always lacked.
            self._reemit(phone, record, message)
            if SECURITY_WARNING_SUBSTR not in message:
                return
            now = time.monotonic()
            events = self._events[phone]
            events.append(now)
            while events and now - events[0] > self._window_sec:
                events.popleft()
            if len(events) < self._threshold:
                return
            last = self._last_reconnect_at.get(phone)
            if last is not None and now - last < self._cooldown_sec:
                return
            self._last_reconnect_at[phone] = now
            events.clear()
            loop = self._loop
            if loop is None or loop.is_closed():
                return
            # emit() may run on a non-loop thread (Telethon recv loop /
            # logging from anywhere) — never touch asyncio state directly.
            loop.call_soon_threadsafe(self._spawn_reconnect, phone)
        except Exception:
            self.handleError(record)

    def _reemit(self, phone: str, record: logging.LogRecord, message: str) -> None:
        """Re-emit a WARNING+ per-phone sender record through our logger.

        Only sender-logger records need it (they no longer propagate); records
        from other telethon children keep their normal path to the root
        handlers and must not be duplicated. The re-emitted line carries the
        masked phone the raw Telethon line never had.
        """
        if not record.name.endswith(_SENDER_LOGGER_SUFFIX):
            return
        logger.log(
            record.levelno,
            "[mtproto %s] %s",
            mask_phone(phone),
            message,
            exc_info=record.exc_info,
        )

    def _phone_for_record(self, record: logging.LogRecord) -> str | None:
        name = record.name
        if not name.endswith(_SENDER_LOGGER_SUFFIX):
            return None
        prefix = f"{TGCF_TELETHON_LOGGER_ROOT}."
        if not name.startswith(prefix):
            return None
        slug = name[len(prefix) : -len(_SENDER_LOGGER_SUFFIX)]
        return self._slug_to_phone.get(slug)

    def _spawn_reconnect(self, phone: str) -> None:
        self._reconnect_count[phone] += 1
        logger.warning(
            "MTProto security warnings exceeded threshold on %s — scheduling "
            "force reconnect (#%d)",
            mask_phone(phone),
            self._reconnect_count[phone],
        )
        task = asyncio.ensure_future(self._reconnect_cb(phone))
        task.add_done_callback(_log_reconnect_outcome)


def _log_reconnect_outcome(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("MTProto watchdog reconnect failed: %s", exc)


def bind_telethon_base_logger(client: object, base_logger: logging.Logger) -> bool:
    """Rebind an already-constructed TelegramClient onto our base logger.

    The CLI backend builds clients via ``telethon_cli_runtime.create_client``
    which does not expose Telethon's ``base_logger=`` parameter, and
    ``MTProtoSender`` captures its logger once in ``__init__`` — so we replace
    both the client's ``_log`` mapping and the sender's captured logger.
    Defensive by design: on a breaking Telethon upgrade the client simply
    stays without per-phone attribution (returns False), nothing crashes.
    """
    try:

        class _Loggers(dict):
            # Mirrors TelegramBaseClient.__init__._Loggers.
            def __missing__(self, key: str) -> logging.Logger:
                if key.startswith("telethon."):
                    key = key.split(".", maxsplit=1)[1]
                return base_logger.getChild(key)

        loggers = _Loggers()
        sender = client._sender  # type: ignore[attr-defined]
        client._log = loggers  # type: ignore[attr-defined]
        if sender is not None:
            sender._log = loggers["telethon.network.mtprotosender"]
        return True
    except AttributeError:
        logger.debug(
            "Could not bind per-phone telethon base logger (telethon internals "
            "changed?); MTProto watchdog will not cover this client",
            exc_info=True,
        )
        return False
