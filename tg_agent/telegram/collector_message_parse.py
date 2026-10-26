"""Pure Telethon ``msg`` → :class:`~tg_agent.models.Message` conversion.

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
import re
from datetime import date as date_cls
from datetime import datetime, timezone

from tg_agent.models import Message
from tg_agent.services.translation_service import TranslationService
from tg_agent.telegram.reactions import extract_message_reactions_json
from tg_agent.telegram.rich_text import webpage_page_text
from tg_shared.identity import extract_message_sender_identity
from tg_shared.media import get_media_type

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


_CAMEL_BOUNDARY_RE = re.compile(r"(?<!^)(?=[A-Z])")


def _entity_type(entity) -> str:
    """``MessageEntityCustomEmoji`` → ``custom_emoji`` (snake_case TL subtype)."""
    name = type(entity).__name__.removeprefix("MessageEntity")
    return _CAMEL_BOUNDARY_RE.sub("_", name).lower()


# MessageEntityTextUrl.url is serialized (#1492) — masked-hyperlink targets must
# survive collection for feed heuristics and link analytics.
_ENTITY_DROPPED_FIELDS = frozenset({"_"})


def extract_message_entities_json(msg) -> str | None:
    """Serialize a message's ``MessageEntity*`` formatting spans as JSON.

    Each entity keeps its TL fields (``offset``/``length`` plus the subtype's
    own, e.g. ``document_id`` for custom emoji — same raw id the reactions
    layer encodes as ``custom:<document_id>``) and gains a ``type`` label
    (``bold``, ``custom_emoji``, ``spoiler``, ...). ``msg.text`` stays the
    guaranteed plain-text rendering; the entities are the span metadata on top
    of it. No entities → ``None`` (not ``"[]"``), so legacy rows and fresh
    ones are indistinguishable.
    """
    entities = getattr(msg, "entities", None)
    if not entities:
        return None
    serialized = [
        {
            **{
                key: value
                for key, value in entity.to_dict().items()
                if key not in _ENTITY_DROPPED_FIELDS and value is not None
            },
            "type": _entity_type(entity),
        }
        for entity in entities
    ]
    return json.dumps(serialized, ensure_ascii=False, sort_keys=True) if serialized else None


def plain_text_and_entities_json(msg) -> tuple[str | None, str | None]:
    """``(plain text, entities JSON)`` shared by both ingestion pipelines (#1547).

    Plain text stays the FTS source; an iv-page fills it in only when the raw
    message carries no words of its own (link-only posts). Entities are the
    span metadata on top of the plain text (``None`` when there are none).
    Raw ``message`` wins over ``text``: the Telethon ``.text`` property formats
    through the client parse mode, ``.message`` is always the raw string.
    Only ``str`` counts as text — a non-string stand-in (MagicMock-based test
    fake) is not a word source and must not crash the funnel.
    """
    raw = getattr(msg, "message", None)
    if not isinstance(raw, str):
        raw = getattr(msg, "text", None)
    text = (raw if isinstance(raw, str) else None) or extract_webpage_plain_text(msg)
    return text, extract_message_entities_json(msg)


def extract_webpage_plain_text(msg) -> str | None:
    """Plain text of the link preview's instant-view page, ``None`` when absent.

    A link-only post leaves ``msg.text`` empty while the words sit in the
    preview's cached iv blocks; returning them as plain text keeps the FTS
    index (built on ``text``) from silently missing such posts. ``None`` means
    "no page to add" — the caller keeps the original text untouched.
    """
    text = webpage_page_text(msg)
    return text or None


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
    text, entities_json = plain_text_and_entities_json(msg)
    return Message(
        channel_id=channel_id,
        message_id=msg.id,
        sender_id=sender_identity.sender_id,
        sender_name=sender_identity.sender_name,
        sender_first_name=sender_identity.sender_first_name,
        sender_last_name=sender_identity.sender_last_name,
        sender_username=sender_identity.sender_username,
        text=text,
        message_kind=get_message_kind(msg),
        detected_lang=TranslationService.detect_language(text),
        media_type=get_media_type_for(msg),
        service_action_raw=get_service_action_raw(msg),
        service_action_semantic=get_service_action_semantic(msg),
        service_action_payload_json=get_service_action_payload(msg),
        sender_kind=get_sender_kind(msg),
        topic_id=extract_topic_id(msg),
        reactions_json=extract_reactions(msg),
        entities_json=entities_json,
        views=getattr(msg, "views", None),
        forwards=getattr(msg, "forwards", None),
        reply_count=getattr(getattr(msg, "replies", None), "replies", None),
        date=_normalize_date(msg),
        forward_from_channel_id=extract_forward_from_channel_id(msg),
    )
