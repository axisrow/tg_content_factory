from __future__ import annotations

from datetime import timezone

from src.models import Message
from src.telegram.collector import Collector


class TelegramMessageTransformer:
    @staticmethod
    def convert_telethon_message(msg) -> Message | None:
        chat = getattr(msg, "chat", None)
        if chat is None:
            return None

        chat_id = getattr(chat, "id", 0)
        chat_title = getattr(chat, "title", None)
        chat_username = getattr(chat, "username", None)

        sender = getattr(msg, "sender", None)
        sender_id = getattr(sender, "id", None) if sender else None
        sender_name = None
        if sender:
            first = getattr(sender, "first_name", "") or ""
            last = getattr(sender, "last_name", "") or ""
            title = getattr(sender, "title", "") or ""
            sender_name = " ".join(p for p in (first, last) if p) or title or None

        date = msg.date
        if date and date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)

        return Message(
            channel_id=chat_id,
            message_id=msg.id,
            sender_id=sender_id,
            sender_name=sender_name,
            text=getattr(msg, "message", None) or getattr(msg, "text", None),
            media_type=Collector._get_media_type(msg),
            date=date,
            channel_title=chat_title,
            channel_username=chat_username,
        )

    @staticmethod
    def resolve_sender(msg, chats_map, users_map) -> tuple[int | None, str | None]:
        from telethon.tl.types import PeerChannel, PeerUser

        sender_id = None
        sender_name = None
        from_id = getattr(msg, "from_id", None)

        if isinstance(from_id, PeerUser):
            sender_id = from_id.user_id
            user = users_map.get(sender_id)
            if user:
                parts = [
                    getattr(user, "first_name", "") or "",
                    getattr(user, "last_name", "") or "",
                ]
                sender_name = " ".join(p for p in parts if p) or None
        elif isinstance(from_id, PeerChannel):
            sender_id = from_id.channel_id
            ch = chats_map.get(sender_id)
            sender_name = getattr(ch, "title", None) if ch else None

        return sender_id, sender_name
