"""Live dialog history reads via the tg_messenger library — no second connection.

Reuses tg_messenger's `StandaloneTelegramClient.history()` for its model mapping
and flood-wait retry, but never lets it open its own MTProto connection: opening
a second Telethon client on the same session in parallel with our live
`ClientPool` connection risks desyncing the account's MTProto state (see
`src/telegram/mtproto_watchdog.py` for the "silent brick" failure this project
has already hit once). Instead we pass a ``client_factory`` that hands back the
already-connected client leased from our pool — zero new connections.

Two hard invariants for callers of this module:
- Never call ``connect()``/``disconnect()`` on the wrapped client — disconnect
  would tear down the pool's live connection.
- Never use ``listen*()`` — it attaches event handlers to a client we don't own.

Only `history()` is used. tg_messenger's `Message` model never leaves this
module — everything is mapped into our own `DialogMessage` (`src/models.py`) so
an upstream tg_messenger release can't change our web/CLI output shape.

On caching: the client is built per call, so tg_messenger's per-instance history
TTL cache never gets a hit and every call reaches Telegram. That is deliberate —
repeat reads are absorbed one level up by `runtime_snapshots` (the web handler
serves a cached snapshot and only enqueues a worker command on a miss), so a
second cache here would just add a staleness window we don't control.
"""
from __future__ import annotations

from tg_messenger.core.client import StandaloneTelegramClient
from tg_messenger.core.models import Message as _TgMessengerMessage

from src.models import DialogMessage


def _client_factory(client: object):
    """Return a factory tg_messenger calls once, ignoring its own session args."""

    def _factory(*_args, **_kwargs):
        return client

    return _factory


def _to_dialog_message(msg: _TgMessengerMessage) -> DialogMessage:
    media_type = msg.media.kind if msg.media is not None else None
    sender_name = None
    if msg.sender is not None:
        sender_name = " ".join(
            part for part in (msg.sender.first_name, msg.sender.last_name) if part
        ).strip() or msg.sender.username or None
    return DialogMessage(
        id=msg.id,
        dialog_id=msg.dialog_id,
        sender_id=msg.sender_id,
        sender_name=sender_name,
        out=msg.out,
        date=msg.date,
        text=msg.text,
        media_type=media_type,
        reply_to_id=msg.reply_to_id,
        is_forward=msg.is_forward,
    )


async def read_dialog_history(
    client: object,
    *,
    api_id: int,
    api_hash: str,
    peer: int,
    limit: int = 50,
    offset_id: int = 0,
) -> list[DialogMessage]:
    """Read a dialog's message history using an already-connected pool client.

    ``client`` must be a live, connected Telethon client leased from our
    `ClientPool` (e.g. via `TelegramActionService._client`). This function does
    not connect, disconnect, or attach event handlers to it.
    """
    standalone = StandaloneTelegramClient(
        api_id,
        api_hash,
        external_session="",
        client_factory=_client_factory(client),
    )
    messages = await standalone.history(peer, limit=limit, offset_id=offset_id)
    return [_to_dialog_message(msg) for msg in messages]
