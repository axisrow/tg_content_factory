"""Sent-messages registry recording shared by every send path (#1893).

One wrapper for the accounting rule — classify the chat, write the rows,
never let a registry failure fail the send. TelegramActionService,
PublishService and PhotoPublishService all route through it so the policy
lives in exactly one place. The same policy journals the deletions a
process makes itself (:func:`record_deletion`, #1950).
"""

from __future__ import annotations

import logging
from typing import Any

from tg_agent.telegram.utils import chat_id_and_type

logger = logging.getLogger(__name__)


async def record_send(
    db: Any,
    *,
    phone: str,
    entity: Any,
    recipient: Any,
    message_ids: list[int],
    text: str | None,
    source: str,
    ref: str | None = None,
    chat_type: str | None = None,
) -> None:
    """Registry rows for delivered messages; never fails the caller.

    No db handle (bare construction) or an empty id list (a send without a
    confirmed id — relay/timeout uncertainty) records nothing: an id-less
    row could neither dedupe nor join the deletion store. ``chat_type``
    overrides the class read off the entity — a comment lands in a
    discussion group but is recorded as what it is (#1946).
    """
    if db is None or not message_ids:
        return
    try:
        chat_id, peer_type = chat_id_and_type(entity)
        await db.repos.sent_messages.record_many(
            phone,
            chat_id,
            [int(message_id) for message_id in message_ids],
            chat_type=chat_type or peer_type,
            source=source,
            recipient=str(recipient) if recipient is not None else None,
            text=text,
            ref=ref,
        )
    except Exception:
        logger.warning(
            "sent_messages registry write failed for %d id(s)", len(message_ids), exc_info=True
        )


async def record_deletion(
    db: Any,
    *,
    phone: str,
    entity: Any,
    message_ids: list[int],
) -> None:
    """Journal the deletions a process just made; never fails the caller.

    In Saved Messages and private chats Telegram reports a deletion only
    inside the answer to ``messages.deleteMessages`` — the gateway's update
    loop never sees it — so the deleting process writes the row the gateway
    would have (#1950). A channel deletion arrives as an update as well: both
    name one row, and the store's unique index keeps it single.
    """
    if db is None or not message_ids:
        return
    try:
        chat_id, _peer_type = chat_id_and_type(entity)
        await db.repos.observed_deletions.record_many(
            phone, chat_id, [int(message_id) for message_id in message_ids]
        )
    except Exception:
        logger.warning(
            "observed_deletions write failed for %d id(s)", len(message_ids), exc_info=True
        )
