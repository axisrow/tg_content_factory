from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from telethon.tl.types import (
    Document,
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
    PeerChannel,
    PeerUser,
)

from src.search.transformers import TelegramMessageTransformer


def test_media_type_none():
    msg = MagicMock(media=None)
    assert TelegramMessageTransformer.media_type_from_message(msg) is None


def test_media_type_photo():
    msg = MagicMock(media=MagicMock(spec=MessageMediaPhoto))
    assert TelegramMessageTransformer.media_type_from_message(msg) == "photo"


def test_media_type_sticker():
    doc = MagicMock(spec=Document)
    doc.attributes = [DocumentAttributeSticker(alt="😀", stickerset=MagicMock())]
    msg = MagicMock(media=MessageMediaDocument(document=doc, ttl_seconds=None))
    assert TelegramMessageTransformer.media_type_from_message(msg) == "sticker"


def test_media_type_video():
    doc = MagicMock(spec=Document)
    doc.attributes = [DocumentAttributeVideo(duration=1, w=1, h=1)]
    msg = MagicMock(media=MessageMediaDocument(document=doc, ttl_seconds=None))
    assert TelegramMessageTransformer.media_type_from_message(msg) == "video"


def test_media_type_video_note():
    doc = MagicMock(spec=Document)
    doc.attributes = [DocumentAttributeVideo(duration=1, w=1, h=1, round_message=True)]
    msg = MagicMock(media=MessageMediaDocument(document=doc, ttl_seconds=None))
    assert TelegramMessageTransformer.media_type_from_message(msg) == "video_note"


def test_media_type_voice():
    doc = MagicMock(spec=Document)
    doc.attributes = [DocumentAttributeAudio(duration=1, voice=True)]
    msg = MagicMock(media=MessageMediaDocument(document=doc, ttl_seconds=None))
    assert TelegramMessageTransformer.media_type_from_message(msg) == "voice"


def test_media_type_audio():
    doc = MagicMock(spec=Document)
    doc.attributes = [DocumentAttributeAudio(duration=1)]
    msg = MagicMock(media=MessageMediaDocument(document=doc, ttl_seconds=None))
    assert TelegramMessageTransformer.media_type_from_message(msg) == "audio"


def test_media_type_gif():
    doc = MagicMock(spec=Document)
    doc.attributes = [DocumentAttributeAnimated()]
    msg = MagicMock(media=MessageMediaDocument(document=doc, ttl_seconds=None))
    assert TelegramMessageTransformer.media_type_from_message(msg) == "gif"


def test_media_type_simple_types():
    mt = TelegramMessageTransformer.media_type_from_message
    assert mt(MagicMock(media=MagicMock(spec=MessageMediaWebPage))) == "web_page"
    assert mt(MagicMock(media=MagicMock(spec=MessageMediaGeo))) == "location"
    assert mt(MagicMock(media=MagicMock(spec=MessageMediaGeoLive))) == "geo_live"
    assert mt(MagicMock(media=MagicMock(spec=MessageMediaContact))) == "contact"
    assert mt(MagicMock(media=MagicMock(spec=MessageMediaPoll))) == "poll"
    assert mt(MagicMock(media=MagicMock(spec=MessageMediaDice))) == "dice"
    assert mt(MagicMock(media=MagicMock(spec=MessageMediaGame))) == "game"


def test_convert_telethon_message_basic():
    msg = MagicMock()
    msg.id = 123
    msg.chat.id = 456
    msg.chat.title = "Chat"
    msg.chat.username = "user"
    msg.sender.id = 789
    msg.sender.first_name = "John"
    msg.sender.last_name = "Doe"
    msg.sender.username = "jdoe"
    msg.date = datetime(2025, 1, 1, tzinfo=timezone.utc).replace(tzinfo=None)
    msg.message = "hello"
    msg.media = None

    res = TelegramMessageTransformer.convert_telethon_message(msg)
    assert res.channel_id == 456
    assert res.sender_id == 789
    assert res.sender_name == "John Doe"
    assert res.sender_first_name == "John"
    assert res.sender_last_name == "Doe"
    assert res.sender_username == "jdoe"
    assert res.date.tzinfo == timezone.utc


def test_convert_telethon_message_includes_engagement_fields():
    msg = SimpleNamespace(
        id=124,
        chat=SimpleNamespace(id=456, title="Chat", username="user"),
        sender=SimpleNamespace(id=789, first_name="John", last_name="Doe", username="jdoe"),
        date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        message="Это достаточно длинный русский текст для определения языка",
        text="Это достаточно длинный русский текст для определения языка",
        media=None,
        views=123,
        forwards=4,
        replies=SimpleNamespace(replies=5),
        reactions=SimpleNamespace(
            results=[
                SimpleNamespace(reaction=SimpleNamespace(emoticon="👍"), count=7),
            ]
        ),
    )

    res = TelegramMessageTransformer.convert_telethon_message(msg)

    assert res.views == 123
    assert res.forwards == 4
    assert res.reply_count == 5
    assert res.reactions_json == '[{"emoji": "👍", "count": 7}]'
    assert res.detected_lang == "ru"


def test_convert_telethon_message_no_chat():
    msg = MagicMock(chat=None)
    assert TelegramMessageTransformer.convert_telethon_message(msg) is None


def test_resolve_sender_user():
    msg = MagicMock(from_id=PeerUser(user_id=1))
    user = MagicMock(first_name="First", last_name="Last", username="firstlast")
    res_id, res_name = TelegramMessageTransformer.resolve_sender(msg, {}, {1: user})
    assert res_id == 1
    assert res_name == "First Last"
    identity = TelegramMessageTransformer.resolve_sender_identity(msg, {}, {1: user})
    assert identity.sender_first_name == "First"
    assert identity.sender_last_name == "Last"
    assert identity.sender_username == "firstlast"


def test_resolve_sender_channel():
    msg = MagicMock(from_id=PeerChannel(channel_id=2))
    chat = MagicMock(title="Chan Title", username="chan")
    res_id, res_name = TelegramMessageTransformer.resolve_sender(msg, {2: chat}, {})
    assert res_id == 2
    assert res_name == "Chan Title"
    identity = TelegramMessageTransformer.resolve_sender_identity(msg, {2: chat}, {})
    assert identity.sender_name == "Chan Title"
    assert identity.sender_first_name is None
    assert identity.sender_last_name is None
    assert identity.sender_username == "chan"
