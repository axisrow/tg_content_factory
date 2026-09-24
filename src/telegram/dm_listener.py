"""Real-time incoming-DM listener (#1426, stage 2.1 of epic #1416).

Subscribes to Telethon ``NewMessage`` events on the pool's *already connected*
clients — no second MTProto connection, no account lease. The pool connections
are persistent and ``receive_updates`` is enabled, so updates already arrive and
are silently dropped today; this module starts consuming them.

Three invariants shape the design:

- **No lease.** An exclusive lease would pull the account out of the collection
  rotation (``account_lease_pool``). The listener reads ``pool.clients`` directly
  and never touches the lease machinery.
- **Reconcile.** The pool silently replaces ``clients[phone]`` with a fresh
  client object (re-auth, add_client, failed acquire) — handlers attached to the
  old object fall off without any signal. A periodic reconcile re-attaches onto
  the current raw client and detaches accounts the owner disabled.
- **Liveness.** The mtproto watchdog (#556) heals a bricked recv-loop with
  minutes of delay and no listener-side signal. ``status()`` exposes per-account
  ``last_update_at`` / ``seconds_since_update`` so "updates stopped arriving" is
  observable; the history-read catch-up itself is stage #1428.

Worker-only by construction: the container builds the listener only when
``runtime_mode == "worker"``, so ``serve`` (embedded worker) plus a standalone
``worker`` cannot double-listen on the same accounts.

Receiving is deliberately NOT gated by ``LiveRuntimePauseGate`` — pausing
receive would accumulate an unbounded lag; only sending is pause-gated.

The event bus of the tg_messenger reference implementation drops old items at
100 per subscriber; here the handler only does ``put_nowait`` into an unbounded
queue (DMs are low-volume) and the consumer task does all further work, so
nothing is dropped at the queue boundary — end-to-end persistence arrives with
the storage stage (#1427).
"""

from __future__ import annotations

import asyncio
import functools
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from telethon import events

from src.database import Database

logger = logging.getLogger(__name__)

RECONCILE_INTERVAL_SEC = 30.0


def _is_private_dm(event: Any) -> bool:
    """Telethon dispatch filter: only direct user chats (DMs), no groups/channels."""
    return bool(getattr(event, "is_private", False))


@dataclass(frozen=True)
class IncomingDmEvent:
    """One incoming DM, normalized off the raw Telethon event."""

    phone: str
    chat_id: int | None
    message_id: int | None
    text: str | None
    message_date: datetime | None
    received_at: datetime


@dataclass(frozen=True)
class _Attachment:
    """Handler currently attached to one account's raw client."""

    client: Any
    callback: Callable[[Any], None]


class DmListener:
    """Long-lived worker task: listen for incoming DMs on pool clients.

    Follows the ``start()``/``stop()``/``_stop_event`` pattern of
    :class:`~src.services.telegram_command_dispatcher.TelegramCommandDispatcher`.
    ``stop()`` must run before ``pool.disconnect_all()`` so handlers are removed
    from live clients (``stop_container`` guarantees this order).
    """

    def __init__(
        self,
        pool: Any,
        db: Database,
        *,
        reconcile_interval: float = RECONCILE_INTERVAL_SEC,
        event_callback: Callable[[IncomingDmEvent], Awaitable[None]] | None = None,
    ):
        self._pool = pool
        self._db = db
        self._reconcile_interval = reconcile_interval
        self._event_callback = event_callback
        self._event_cls = events.NewMessage(incoming=True, func=_is_private_dm)
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        # ponytail: unbounded queue — the handler only put_nowaits and DM volume
        # is tiny; swap to bounded+backpressure if a flood of DMs ever matters.
        self._queue: asyncio.Queue[IncomingDmEvent] = asyncio.Queue()
        self._attached: dict[str, _Attachment] = {}
        self._updates_received: dict[str, int] = {}
        self._last_update_monotonic: dict[str, float] = {}
        self._last_update_wall: dict[str, datetime] = {}

    # --- lifecycle ---

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop(), name="dm_listener")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._detach_all()

    async def _run_loop(self) -> None:
        consumer = asyncio.create_task(
            self._process_loop(), name="dm_listener_consumer"
        )
        try:
            while not self._stop_event.is_set():
                try:
                    await self._reconcile()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("dm_listener reconcile failed; retrying next interval")
                await asyncio.sleep(self._reconcile_interval)
        finally:
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)

    # --- subscription / reconcile ---

    async def _enabled_phones(self) -> set[str]:
        summaries = await self._db.repos.accounts.get_account_summaries(active_only=True)
        return {str(summary.phone) for summary in summaries}

    def _connected_raw_clients(self) -> dict[str, Any]:
        raw: dict[str, Any] = {}
        for phone, session in list(self._pool.clients.items()):
            client = getattr(session, "raw_client", None)
            if client is not None:
                raw[str(phone)] = client
        return raw

    async def _reconcile(self) -> None:
        """Sync handler attachments with (enabled accounts) x (connected clients)."""
        enabled = await self._enabled_phones()
        current = self._connected_raw_clients()

        for phone, attachment in list(self._attached.items()):
            if phone not in enabled or current.get(phone) is not attachment.client:
                self._detach(phone)

        for phone, client in current.items():
            if phone in enabled and phone not in self._attached:
                self._attach(phone, client)

    def _attach(self, phone: str, client: Any) -> None:
        callback = functools.partial(self._on_event, phone)
        try:
            client.add_event_handler(callback, self._event_cls)
        except Exception:
            logger.exception("dm_listener: failed to attach handler for %s", phone)
            return
        self._attached[phone] = _Attachment(client=client, callback=callback)
        logger.info("dm_listener: listening for DMs on %s", phone)

    def _detach(self, phone: str) -> None:
        attachment = self._attached.pop(phone, None)
        if attachment is None:
            return
        try:
            attachment.client.remove_event_handler(attachment.callback, self._event_cls)
        except Exception:
            # The replaced client object may already be disconnected/dead —
            # dropping our handler reference is enough.
            logger.debug("dm_listener: detach for %s raised", phone, exc_info=True)
        logger.info("dm_listener: stopped listening on %s", phone)

    def _detach_all(self) -> None:
        for phone in list(self._attached):
            self._detach(phone)

    # --- event path ---

    def _on_event(self, phone: str, event: Any) -> None:
        """Telethon handler — sync and cheap: filter, enqueue, done."""
        try:
            if not _is_private_dm(event):
                return
            message = getattr(event, "message", None)
            dm = IncomingDmEvent(
                phone=phone,
                chat_id=getattr(event, "chat_id", None),
                message_id=getattr(message, "id", None),
                text=getattr(message, "text", None),
                message_date=getattr(message, "date", None),
                received_at=datetime.now(timezone.utc),
            )
            self._queue.put_nowait(dm)
            self._updates_received[phone] = self._updates_received.get(phone, 0) + 1
            self._last_update_monotonic[phone] = time.monotonic()
            self._last_update_wall[phone] = dm.received_at
        except Exception:
            # Never let an exception escape into the client's recv loop.
            logger.exception("dm_listener: failed to enqueue DM for %s", phone)

    async def _process_loop(self) -> None:
        while True:
            dm = await self._queue.get()
            if self._event_callback is not None:
                try:
                    await self._event_callback(dm)
                except Exception:
                    logger.exception(
                        "dm_listener: event callback failed for %s chat %s",
                        dm.phone,
                        dm.chat_id,
                    )

    # --- observability ---

    def status(self) -> dict[str, Any]:
        """Liveness snapshot: per attached account, when updates last arrived."""
        now_monotonic = time.monotonic()
        accounts = {
            phone: {
                "attached": True,
                "updates_received": self._updates_received.get(phone, 0),
                "last_update_at": (
                    self._last_update_wall[phone].isoformat()
                    if phone in self._last_update_wall
                    else None
                ),
                "seconds_since_update": (
                    round(now_monotonic - self._last_update_monotonic[phone], 1)
                    if phone in self._last_update_monotonic
                    else None
                ),
            }
            for phone in sorted(self._attached)
        }
        return {
            "running": bool(self._task and not self._task.done()),
            "accounts": accounts,
        }
