"""Pure Telethon ``msg`` → :class:`~src.models.Message` conversion.

Extracted from ``Collector._stream_channel_messages`` and the ``Collector._get_*``
helper methods (#1045). Everything here is **stateless** — it depends only on the
Telethon message object, not on collector/pool/db state — so it lives as plain
module functions (composition over inheritance, per project convention). The
``Collector`` keeps thin delegate methods so the rest of the codebase and the
test-suite can still reach this logic via ``Collector._get_*`` / ``_build_message``.

Behavior is preserved exactly, including the forum-topic id fallback chain
(``reply_to_top_id`` → ``reply_to_msg_id`` → ``1``), the forward-source channel
extraction for cross-channel citations, and the naive→UTC date normalization.
"""

from __future__ import annotations

import json
from datetime import date as date_cls
from datetime import datetime, timezone

from src.models import Message
from src.services.translation_service import TranslationService
from src.telegram.identity import extract_message_sender_identity
from src.telegram.media import get_media_type
from src.telegram.reactions import extract_message_reactions_json

# Telethon service-action class name → coarse semantic label. Lifted verbatim
# from ``Collector._SERVICE_ACTION_SEMANTICS`` so the mapping has a single home.
SERVICE_ACTION_SEMANTICS: dict[str, str] = {
    "MessageActionChatAddUser": "join",
    "MessageActionChatJoinedByLink": "join",
    "MessageActionChatJoinedByRequest": "join",
    "MessageActionChatDeleteUser": "leave",
    "MessageActionPinMessage": "pin",
    "MessageActionChatEditTitle": "title_changed",
    "MessageActionChatEditPhoto": "photo_changed",
    "MessageActionChatDeletePhoto": "photo_changed",
    "MessageActionChatMigrateTo": "migrate",
    "MessageActionChannelMigrateFrom": "migrate",
    "MessageActionChatCreate": "created",
    "MessageActionChannelCreate": "created",
}


def get_media_type_for(msg) -> str | None:
    """Determine media type from a Telethon message."""
    return get_media_type(msg)


def extract_reactions(msg) -> str | None:
    """Extract reactions from a Telethon message as a JSON string."""
    return extract_message_reactions_json(msg)


def get_sender_name(msg) -> str | None:
    sender = getattr(msg, "sender", None)
    if sender:
        if hasattr(sender, "first_name"):
            parts = [sender.first_name or "", sender.last_name or ""]
            return " ".join(p for p in parts if p) or None
        if hasattr(sender, "title"):
            return sender.title
    return None


def get_message_kind(msg) -> str:
    return "service" if getattr(msg, "action", None) is not None else "regular"


def get_service_action_raw(msg) -> str | None:
    action = getattr(msg, "action", None)
    return type(action).__name__ if action is not None else None


def get_service_action_semantic(msg) -> str | None:
    action_raw = get_service_action_raw(msg)
    if action_raw is None:
        return None
    return SERVICE_ACTION_SEMANTICS.get(action_raw, "other")


def get_service_action_payload(msg) -> str | None:
    action = getattr(msg, "action", None)
    if action is None:
        return None
    payload = {key: value for key, value in action.to_dict().items() if key != "_"}

    def _default(obj):
        if isinstance(obj, (datetime, date_cls)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.hex()
        return repr(obj)

    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=_default)
        if payload
        else None
    )


def get_sender_kind(msg) -> str:
    if getattr(msg, "post", False):
        return "channel"
    if getattr(msg, "sender_id", None) is None:
        return "anonymous_admin"
    return "user"


def extract_topic_id(msg) -> int | None:
    """Resolve the forum-topic id, preserving the original fallback chain.

    Only forum messages carry a topic; a plain reply (``forum_topic`` falsy)
    has no topic. For a forum reply we prefer the topic's top id, then the
    replied-to message id, then default to ``1`` (the General topic).
    """
    reply_to = getattr(msg, "reply_to", None)
    if reply_to and getattr(reply_to, "forum_topic", False):
        return (
            getattr(reply_to, "reply_to_top_id", None)
            or getattr(reply_to, "reply_to_msg_id", None)
            or 1
        )
    return None


def extract_forward_from_channel_id(msg) -> int | None:
    """Forward source channel id for cross-channel citation tracking."""
    fwd_from = getattr(msg, "fwd_from", None)
    if fwd_from and getattr(fwd_from, "from_id", None):
        from_id = fwd_from.from_id
        if hasattr(from_id, "channel_id"):
            return from_id.channel_id
    return None


def _normalize_date(msg):
    """Coerce a naive Telethon ``msg.date`` to UTC; pass through aware/None."""
    msg_date = getattr(msg, "date", None)
    if msg_date and msg_date.tzinfo is None:
        return msg_date.replace(tzinfo=timezone.utc)
    return msg_date


def build_message_from_telethon(msg, channel_id: int) -> Message:
    """Build a :class:`Message` from a Telethon message.

    This is the per-message conversion that used to be inlined in the streaming
    hot loop; pulling it out keeps the loop's control flow (cancellation /
    flushing) readable and shrinks its cyclomatic complexity. The field mapping
    is identical to the previous inline construction.
    """
    sender_identity = extract_message_sender_identity(msg)
    return Message(
        channel_id=channel_id,
        message_id=msg.id,
        sender_id=sender_identity.sender_id,
        sender_name=sender_identity.sender_name,
        sender_first_name=sender_identity.sender_first_name,
        sender_last_name=sender_identity.sender_last_name,
        sender_username=sender_identity.sender_username,
        text=msg.text,
        message_kind=get_message_kind(msg),
        detected_lang=TranslationService.detect_language(msg.text),
        media_type=get_media_type_for(msg),
        service_action_raw=get_service_action_raw(msg),
        service_action_semantic=get_service_action_semantic(msg),
        service_action_payload_json=get_service_action_payload(msg),
        sender_kind=get_sender_kind(msg),
        topic_id=extract_topic_id(msg),
        reactions_json=extract_reactions(msg),
        views=getattr(msg, "views", None),
        forwards=getattr(msg, "forwards", None),
        reply_count=getattr(getattr(msg, "replies", None), "replies", None),
        date=_normalize_date(msg),
        forward_from_channel_id=extract_forward_from_channel_id(msg),
    )
