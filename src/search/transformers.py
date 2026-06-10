from __future__ import annotations

from datetime import timezone

from src.models import Message
from src.services.translation_service import TranslationService
from src.telegram.identity import SenderIdentity, extract_message_sender_identity, extract_sender_identity
from src.telegram.reactions import extract_message_reactions_json


class TelegramMessageTransformer:
    @staticmethod
    def _optional_int(value) -> int | None:
        if isinstance(value, bool) or value is None:
            return None
        return value if isinstance(value, int) else None

    @staticmethod
    def engagement_fields_from_message(msg) -> dict:
        """Extract mutable post stats from a Telethon-like message."""
        return {
            "reactions_json": extract_message_reactions_json(msg),
            "views": TelegramMessageTransformer._optional_int(getattr(msg, "views", None)),
            "forwards": TelegramMessageTransformer._optional_int(getattr(msg, "forwards", None)),
            "reply_count": TelegramMessageTransformer._optional_int(
                getattr(getattr(msg, "replies", None), "replies", None)
            ),
            "detected_lang": TranslationService.detect_language(
                getattr(msg, "message", None) or getattr(msg, "text", None)
            ),
        }

    @staticmethod
    def media_type_from_message(msg) -> str | None:
        from src.telegram.media import get_media_type

        return get_media_type(msg)

    @staticmethod
    def convert_telethon_message(msg) -> Message | None:
        chat = getattr(msg, "chat", None)
        if chat is None:
            return None

        chat_id = getattr(chat, "id", 0)
        chat_title = getattr(chat, "title", None)
        chat_username = getattr(chat, "username", None)

        sender_identity = extract_message_sender_identity(msg)

        date = msg.date
        if date and date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)

        return Message(
            channel_id=chat_id,
            message_id=msg.id,
            sender_id=sender_identity.sender_id,
            sender_name=sender_identity.sender_name,
            sender_first_name=sender_identity.sender_first_name,
            sender_last_name=sender_identity.sender_last_name,
            sender_username=sender_identity.sender_username,
            text=getattr(msg, "message", None) or getattr(msg, "text", None),
            media_type=TelegramMessageTransformer.media_type_from_message(msg),
            date=date,
            channel_title=chat_title,
            channel_username=chat_username,
            **TelegramMessageTransformer.engagement_fields_from_message(msg),
        )

    @staticmethod
    def resolve_sender_identity(msg, chats_map, users_map) -> SenderIdentity:
        from telethon.tl.types import PeerChannel, PeerUser

        from_id = getattr(msg, "from_id", None)

        if isinstance(from_id, PeerUser):
            sender_id = from_id.user_id
            user = users_map.get(sender_id)
            return extract_sender_identity(user, fallback_sender_id=sender_id)
        elif isinstance(from_id, PeerChannel):
            sender_id = from_id.channel_id
            ch = chats_map.get(sender_id)
            return extract_sender_identity(ch, fallback_sender_id=sender_id)

        return extract_message_sender_identity(msg)

    @staticmethod
    def resolve_sender(msg, chats_map, users_map) -> tuple[int | None, str | None]:
        identity = TelegramMessageTransformer.resolve_sender_identity(msg, chats_map, users_map)
        return identity.sender_id, identity.sender_name
