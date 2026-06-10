"""Shared media-type classification for Telethon messages.

Single source of truth used by both the search transformer
(:class:`~src.search.transformers.TelegramMessageTransformer`) and the
collector (:class:`~src.telegram.collector.Collector`) so that adding a new
media type updates classification everywhere at once.
"""

from __future__ import annotations


def get_media_type(msg) -> str | None:
    """Determine the media type from a Telethon message."""
    from telethon.tl.types import (
        DocumentAttributeAnimated,
        DocumentAttributeAudio,
        DocumentAttributeSticker,
        DocumentAttributeVideo,
        MessageMediaContact,
        MessageMediaDice,
        MessageMediaDocument,
        MessageMediaGame,
        MessageMediaGeo,
        MessageMediaGeoLive,
        MessageMediaPhoto,
        MessageMediaPoll,
        MessageMediaWebPage,
    )

    media = msg.media
    if media is None:
        return None
    if isinstance(media, MessageMediaPhoto):
        return "photo"
    if isinstance(media, MessageMediaDocument):
        doc = media.document
        if doc and hasattr(doc, "attributes"):
            for attr in doc.attributes:
                if isinstance(attr, DocumentAttributeSticker):
                    return "sticker"
                if isinstance(attr, DocumentAttributeVideo):
                    return "video_note" if getattr(attr, "round_message", False) else "video"
                if isinstance(attr, DocumentAttributeAudio):
                    return "voice" if getattr(attr, "voice", False) else "audio"
                if isinstance(attr, DocumentAttributeAnimated):
                    return "gif"
        return "document"
    if isinstance(media, MessageMediaWebPage):
        return "web_page"
    if isinstance(media, MessageMediaGeo):
        return "location"
    if isinstance(media, MessageMediaGeoLive):
        return "geo_live"
    if isinstance(media, MessageMediaContact):
        return "contact"
    if isinstance(media, MessageMediaPoll):
        return "poll"
    if isinstance(media, MessageMediaDice):
        return "dice"
    if isinstance(media, MessageMediaGame):
        return "game"
    return "unknown"
