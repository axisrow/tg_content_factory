"""``messages wait --chat``: the settled incoming-message long-poll (#1871).

One blocking call replaces the blind ``messages read --live`` polling loop: the
process subscribes to live ``NewMessage`` updates, collects a *settled burst*
(every new message in the chat resets the ``--settle-ms`` timer, so "file +
text" or three rapid lines arrive as one record) and exits.

Two producers feed the same collector:

- **Session-owner relay** (worker daemon or gateway live): the CLI never sees
  updates — only the owner's connection receives them. A ``wait.messages``
  relay op attaches a temporary Telethon handler on the owner's client and
  pushes matches over the existing relay push channel.
- **No daemon**: this process holds the only connection, so the handler is
  attached to its own client directly (the ``dm_listener`` pattern: async
  callback, exceptions never escape into the recv loop).

The ``--timeout`` deadline applies to the *first* matching message; the settle
tail after it may run past the deadline by design — that is the debounce.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_SETTLE_MS = 6000
DEFAULT_WAIT_TIMEOUT_SEC = 60

PUSH_KIND = "wait.message"


class WaitTimeoutError(Exception):
    """No matching incoming message arrived within ``--timeout``."""


@dataclass(frozen=True)
class NormalizedIncoming:
    """One matching incoming message, serialized identically on both paths."""

    phone: str
    chat_id: int | None
    chat_title: str | None
    message_id: int | None
    sender_name: str | None
    text: str | None
    date: str | None

    def as_item(self) -> dict:
        return {
            "phone": self.phone,
            "chat_id": self.chat_id,
            "chat_title": self.chat_title,
            "message_id": self.message_id,
            "sender_name": self.sender_name,
            "text": self.text,
            "date": self.date,
        }


def sender_name(message: Any) -> str | None:
    sender = getattr(message, "sender", None)
    if sender is None:
        return None
    username = getattr(sender, "username", None)
    if username:
        return f"@{username}"
    name = " ".join(
        part
        for part in (getattr(sender, "first_name", None), getattr(sender, "last_name", None))
        if part
    ).strip()
    return name or getattr(sender, "title", None) or None


def matches_chat(event: Any, chat_id: int | None) -> bool:
    """``chat_id=None`` is the explicit "any chat" mode (``--chat any``)."""
    if chat_id is None:
        return True
    return getattr(event, "chat_id", None) == chat_id


def normalize_incoming(phone: str, event: Any) -> NormalizedIncoming | None:
    """Keep incoming (``out=False``) user messages; drop own and service ones."""
    message = getattr(event, "message", None)
    if message is None or getattr(message, "out", False):
        return None
    if getattr(message, "action", None) is not None:
        return None
    date = getattr(message, "date", None)
    chat = getattr(event, "chat", None)
    return NormalizedIncoming(
        phone=phone,
        chat_id=getattr(event, "chat_id", None),
        chat_title=getattr(chat, "title", None),
        message_id=getattr(message, "id", None),
        sender_name=sender_name(message),
        text=getattr(message, "text", None),
        date=date.isoformat() if date is not None else None,
    )


@dataclass(frozen=True)
class IncomingBurst:
    """One settled burst, the JSON record shape of the issue (#1871)."""

    chat_id: int | None
    name: str | None
    phone: str
    message_count: int
    first_message_id: int | None
    last_message_id: int | None
    message_ids: list[int | None]
    texts: list[str | None]
    senders: list[str | None]
    dates: list[str | None]

    def to_json(self) -> dict:
        return {
            "event": True,
            "chat_id": self.chat_id,
            "name": self.name,
            "phone": self.phone,
            "message_count": self.message_count,
            "first_message_id": self.first_message_id,
            "last_message_id": self.last_message_id,
            "message_ids": self.message_ids,
            "texts": self.texts,
            "senders": self.senders,
            "dates": self.dates,
        }


def _as_incoming(item: Any) -> NormalizedIncoming:
    if isinstance(item, BaseException):
        raise item
    if isinstance(item, NormalizedIncoming):
        return item
    return NormalizedIncoming(**item)


def build_burst(items: list[Any]) -> IncomingBurst:
    incoming = [_as_incoming(item) for item in items]
    first = incoming[0]
    return IncomingBurst(
        chat_id=first.chat_id,
        name=first.chat_title,
        phone=first.phone,
        message_count=len(incoming),
        first_message_id=incoming[0].message_id,
        last_message_id=incoming[-1].message_id,
        message_ids=[item.message_id for item in incoming],
        texts=[item.text for item in incoming],
        senders=[item.sender_name for item in incoming],
        dates=[item.date for item in incoming],
    )


def _chat_of(item: Any) -> int | None:
    if isinstance(item, NormalizedIncoming):
        return item.chat_id
    return item.get("chat_id")


async def wait_burst(
    queue: asyncio.Queue,
    *,
    timeout: float,
    settle_ms: int,
) -> IncomingBurst:
    """First match before the deadline, then collect until the chat is quiet.

    Raises :class:`WaitTimeoutError` when nothing matches in time; re-raises any
    exception a producer dropped into the queue (relay failure, unknown op).
    The burst is the FIRST message's chat only: in ``--chat any`` mode items
    from other conversations are consumed and dropped, and only matching
    arrivals reset the settle timer — a busy inbox cannot stretch one burst.
    """
    deadline = time.monotonic() + max(timeout, 0.0)
    try:
        first = await asyncio.wait_for(queue.get(), timeout=max(deadline - time.monotonic(), 0.0))
    except TimeoutError:
        raise WaitTimeoutError from None
    if isinstance(first, BaseException):
        raise first
    items = [first]
    settle_sec = max(settle_ms, 0) / 1000.0
    settle_deadline = time.monotonic() + settle_sec
    while True:
        remaining = settle_deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            item = await asyncio.wait_for(queue.get(), timeout=remaining)
        except TimeoutError:
            break
        if isinstance(item, BaseException):
            raise item
        if _chat_of(item) != _chat_of(first):
            continue
        items.append(item)
        settle_deadline = time.monotonic() + settle_sec
    return build_burst(items)


def attach_local_listener(
    client: Any,
    phone: str,
    chat_id: int | None,
    queue: asyncio.Queue,
) -> Callable[[], None]:
    """Attach the Telethon handler for the no-daemon path; returns the detacher.

    Imports ``telethon.events`` lazily: the module itself stays telethon-free so
    CLI layers can import it without tripping the import-linter contracts.
    """
    from telethon import events

    new_message = events.NewMessage()

    async def _handler(event: Any) -> None:
        # Async callback: telethon 1.45 awaits every handler unconditionally
        # (#1477). Body wrapped so nothing escapes into the recv loop.
        try:
            if not matches_chat(event, chat_id):
                return
            item = normalize_incoming(phone, event)
            if item is not None:
                queue.put_nowait(item)
        except Exception:
            logger.exception("messages wait: failed to process an incoming event")

    client.add_event_handler(_handler, new_message)

    def _detach() -> None:
        try:
            client.remove_event_handler(_handler, new_message)
        except Exception:
            logger.debug("messages wait: detach raised", exc_info=True)

    return _detach
