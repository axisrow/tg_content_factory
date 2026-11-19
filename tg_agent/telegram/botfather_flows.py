"""BotFather flows wherever the account's updates arrive (#1868).

Bot setup and teardown are BotFather conversations: they wait for replies, which
Telegram pushes as updates to the connection that owns the session. A relayed
client (next to a running daemon, or in a gateway project) has no update stream,
so the flow runs on the session owner and only its outcome crosses the relay.
"""

from __future__ import annotations

from typing import Any

from tg_agent.telegram.relay import RelayTelegramClient
from tg_shared import botfather


def _relayed(client: Any) -> RelayTelegramClient | None:
    raw = getattr(client, "raw_client", client)
    return raw if isinstance(raw, RelayTelegramClient) else None


async def create_bot(client: Any, name: str, username: str) -> str:
    """Create a bot via BotFather; returns its token."""
    relayed = _relayed(client)
    if relayed is None:
        return await botfather.create_bot(client, name, username)
    return await relayed.owner_create_bot(name, username)


async def delete_bot(client: Any, bot_username: str) -> None:
    """Delete a bot via BotFather; raises ``BotNotFoundError`` when BotFather does not list it."""
    relayed = _relayed(client)
    if relayed is None:
        await botfather.delete_bot(client, bot_username)
    elif not await relayed.owner_delete_bot(bot_username):
        raise botfather.BotNotFoundError(f"Bot @{bot_username.lstrip('@')} not found in /mybots")
