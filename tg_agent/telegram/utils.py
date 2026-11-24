from __future__ import annotations

from typing import Any

from tg_agent.utils.datetime import normalize_utc

__all__ = ["chat_id_and_type", "normalize_utc"]


def chat_id_and_type(entity: Any) -> tuple[int | None, str]:
    """Classify a resolved peer for the sent/observed stores (#1893).

    Returns ``(chat_id, chat_type)`` in the ``observed_deletions`` shape: the
    marked ``-100…`` id for channels/supergroups, NULL for DMs and small
    groups — Telegram names no chat there in deletion updates and ids are
    account-global, so those rows carry NULL to join on exactly.
    """
    from telethon import utils

    marked = int(utils.get_peer_id(entity))
    if marked >= 0:
        return None, "dm"
    # The marked channel form is -(1e12 + bare id); same marker magnitude as
    # parsers._MTPROTO_CHANNEL_MARKER, but that constant is bare-id semantics.
    if marked <= -1_000_000_000_000:
        return marked, "channel"
    return None, "group"
