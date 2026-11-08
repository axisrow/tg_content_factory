"""Characterization tests for Telethon ``msg`` → :class:`Message` conversion.

The conversion lived inline inside ``Collector._stream_channel_messages`` (the
E-rank hot loop). Before extracting it into ``tg_agent/telegram/collector_message_parse``
(#1045) these tests pin the exact mapping — forum-topic id resolution, forward
source channel extraction, tz normalization, and every helper field — so the
refactor is provably behavior-preserving.

They exercise the conversion through ``Collector._build_message`` (the thin
delegate the collector keeps) and the module-level pure functions directly, so
both the public surface tests already hit (``Collector._get_*``) and the new
extraction are covered.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tg_agent.telegram.collector import Collector
from tg_agent.telegram.collector_message_parse import (
    build_message_from_telethon,
    extract_forward_from_channel_id,
    extract_message_entities_json,
    extract_topic_id,
    get_message_kind,
    get_sender_kind,
    get_service_action_raw,
    get_service_action_semantic,
    plain_text_and_entities_json,
)
from tg_agent.telegram.rich_text import _page_lines, rich_text_to_str, webpage_page_text


def _msg(**kwargs) -> SimpleNamespace:
    """A Telethon-message stand-in with the attributes the converter reads."""
    base = dict(
        id=42,
        text="hello",
        media=None,
        sender_id=None,
        sender=None,
        post=False,
        action=None,
        reply_to=None,
        fwd_from=None,
        views=None,
        forwards=None,
        replies=None,
        date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        reactions=None,
    )
    base.update(kwargs)
    return SimpleNamespace(**base)


# --- topic_id resolution ------------------------------------------------------


def test_topic_id_none_without_reply():
    assert extract_topic_id(_msg(reply_to=None)) is None


def test_topic_id_none_for_plain_reply_not_forum():
    reply = SimpleNamespace(forum_topic=False, reply_to_top_id=7, reply_to_msg_id=9)
    assert extract_topic_id(_msg(reply_to=reply)) is None


def test_topic_id_prefers_top_id():
    reply = SimpleNamespace(forum_topic=True, reply_to_top_id=7, reply_to_msg_id=9)
    assert extract_topic_id(_msg(reply_to=reply)) == 7


def test_topic_id_falls_back_to_msg_id():
    reply = SimpleNamespace(forum_topic=True, reply_to_top_id=None, reply_to_msg_id=9)
    assert extract_topic_id(_msg(reply_to=reply)) == 9


def test_topic_id_defaults_to_one_when_no_ids():
    reply = SimpleNamespace(forum_topic=True, reply_to_top_id=None, reply_to_msg_id=None)
    assert extract_topic_id(_msg(reply_to=reply)) == 1


# --- forward source channel ---------------------------------------------------


def test_forward_channel_none_without_fwd():
    assert extract_forward_from_channel_id(_msg(fwd_from=None)) is None


def test_forward_channel_none_when_from_id_has_no_channel():
    fwd = SimpleNamespace(from_id=SimpleNamespace(user_id=5))
    assert extract_forward_from_channel_id(_msg(fwd_from=fwd)) is None


def test_forward_channel_extracted_from_peer_channel():
    fwd = SimpleNamespace(from_id=SimpleNamespace(channel_id=12345))
    assert extract_forward_from_channel_id(_msg(fwd_from=fwd)) == 12345


# --- service / sender helpers (pure functions mirror Collector classmethods) --


def test_message_kind_regular_vs_service():
    assert get_message_kind(_msg(action=None)) == "regular"
    assert get_message_kind(_msg(action=SimpleNamespace())) == "service"


def test_service_action_semantic_known_and_other():
    from telethon.tl.types import MessageActionChatJoinedByLink

    join = _msg(action=MessageActionChatJoinedByLink(inviter_id=1))
    assert get_service_action_raw(join) == "MessageActionChatJoinedByLink"
    assert get_service_action_semantic(join) == "join"

    class _Unknown:
        pass

    unknown = _msg(action=_Unknown())
    assert get_service_action_semantic(unknown) == "other"
    assert get_service_action_semantic(_msg(action=None)) is None


def test_sender_kind_channel_admin_user():
    assert get_sender_kind(_msg(post=True, sender_id=None)) == "channel"
    assert get_sender_kind(_msg(post=False, sender_id=None)) == "anonymous_admin"
    assert get_sender_kind(_msg(post=False, sender_id=99)) == "user"


def test_sender_kind_linked_channel_post_copy_is_channel():
    """A linked channel's post copied into its discussion group comes with
    ``post=False`` and the channel as sender: it is the channel, not a member (#1793)."""
    chat = SimpleNamespace(channel_id=1997)
    copy = _msg(sender_id=-1002110, from_id=SimpleNamespace(channel_id=2110), peer_id=chat)
    assert get_sender_kind(copy) == "channel"
    # The group itself as sender is not a channel post.
    as_group = _msg(sender_id=-1001997, from_id=SimpleNamespace(channel_id=1997), peer_id=chat)
    assert get_sender_kind(as_group) != "channel"
    member = _msg(sender_id=99, from_id=SimpleNamespace(user_id=99), peer_id=chat)
    assert get_sender_kind(member) == "user"
    # Suite-wide MagicMock messages carry mock attributes, not peer ids.
    assert get_sender_kind(MagicMock(post=False, sender_id=5)) == "user"


# --- full Message build (the inline block from _stream_channel_messages) ------


def test_build_message_maps_core_fields():
    msg = _msg(
        id=101,
        text="body",
        sender_id=500,
        views=12,
        forwards=3,
        replies=SimpleNamespace(replies=4),
    )
    message = build_message_from_telethon(msg, channel_id=-100777)

    assert message.channel_id == -100777
    assert message.message_id == 101
    assert message.text == "body"
    assert message.views == 12
    assert message.forwards == 3
    assert message.reply_count == 4
    assert message.message_kind == "regular"
    assert message.sender_kind == "user"


def test_build_message_naive_date_coerced_to_utc():
    naive = datetime(2025, 6, 1, 10, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    message = build_message_from_telethon(_msg(date=naive), channel_id=-100123)
    assert message.date is not None
    assert message.date.tzinfo == timezone.utc


def test_build_message_aware_date_preserved():
    aware = datetime(2025, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    message = build_message_from_telethon(_msg(date=aware), channel_id=-100123)
    assert message.date == aware


def test_build_message_forum_topic_and_forward_source():
    reply = SimpleNamespace(forum_topic=True, reply_to_top_id=55, reply_to_msg_id=88)
    fwd = SimpleNamespace(from_id=SimpleNamespace(channel_id=999))
    msg = _msg(reply_to=reply, fwd_from=fwd)
    message = build_message_from_telethon(msg, channel_id=-100123)

    assert message.topic_id == 55
    assert message.forward_from_channel_id == 999


def test_build_message_reactions_serialized():
    from tests.helpers import make_mock_reactions

    msg = _msg(reactions=make_mock_reactions([("👍", 5)]))
    message = build_message_from_telethon(msg, channel_id=-100123)
    assert message.reactions_json is not None
    assert json.loads(message.reactions_json) == [{"emoji": "👍", "count": 5}]


def test_collector_delegate_matches_module_function():
    """``Collector._build_message`` must stay a thin delegate to the module."""
    msg = _msg(id=7, text="x", sender_id=3)
    via_class = Collector._build_message(msg, channel_id=-100555)
    via_module = build_message_from_telethon(msg, channel_id=-100555)
    assert via_class.model_dump() == via_module.model_dump()


@pytest.mark.parametrize("post,sender_id,expected", [(True, None, "channel"), (False, 1, "user")])
def test_collector_helpers_still_exposed(post, sender_id, expected):
    """Tests across the suite call ``Collector._get_*`` — keep them working."""
    msg = _msg(post=post, sender_id=sender_id)
    assert Collector._get_sender_kind(msg) == expected
    assert Collector._get_message_kind(_msg(action=None)) == "regular"


# --- MessageEntity* serialization (#1547) -------------------------------------


def test_extract_entities_none_without_entities():
    assert extract_message_entities_json(_msg()) is None
    assert extract_message_entities_json(_msg(entities=[])) is None


def test_extract_entities_serializes_formatting_spans():
    from telethon.tl.types import MessageEntityBold, MessageEntitySpoiler

    msg = _msg(
        text="bold secret",
        entities=[
            MessageEntityBold(offset=0, length=4),
            MessageEntitySpoiler(offset=5, length=6),
        ],
    )
    assert json.loads(extract_message_entities_json(msg)) == [
        {"length": 4, "offset": 0, "type": "bold"},
        {"length": 6, "offset": 5, "type": "spoiler"},
    ]


def test_extract_entities_custom_emoji_keeps_document_id():
    """Custom emoji keep their raw document id — same id the reactions layer
    labels as ``custom:<document_id>``."""
    from telethon.tl.types import MessageEntityCustomEmoji

    msg = _msg(
        text="a 🎯 b",
        entities=[MessageEntityCustomEmoji(offset=2, length=2, document_id=555999)],
    )
    assert json.loads(extract_message_entities_json(msg)) == [
        {"document_id": 555999, "length": 2, "offset": 2, "type": "custom_emoji"}
    ]


def test_extract_entities_keeps_masked_link_url():
    """MessageEntityTextUrl.url is serialized (#1492) — masked-hyperlink targets
    must survive collection instead of leaving only the visible label."""
    from telethon.tl.types import MessageEntityTextUrl

    msg = _msg(
        text="masked",
        entities=[MessageEntityTextUrl(offset=0, length=6, url="https://example.internal")],
    )
    assert json.loads(extract_message_entities_json(msg)) == [
        {"length": 6, "offset": 0, "type": "text_url", "url": "https://example.internal"}
    ]


def test_build_message_carries_entities_json():
    from telethon.tl.types import MessageEntityBold

    msg = _msg(text="hello", entities=[MessageEntityBold(offset=0, length=5)])
    message = build_message_from_telethon(msg, channel_id=-100123)
    assert json.loads(message.entities_json) == [{"length": 5, "offset": 0, "type": "bold"}]


def test_plain_text_and_entities_ignores_nonstring_message_fakes():
    """MagicMock-based stream fakes auto-generate a truthy non-string `message`
    attribute; only str counts as text, so the funnel takes `text` instead of
    crashing Message validation (CI shard regression, #1547)."""
    fake = MagicMock()
    fake.id = 1
    fake.date = datetime(2025, 1, 1, tzinfo=timezone.utc)
    fake.message = MagicMock()  # truthy non-string
    fake.text = "msg 0"
    text, entities_json = plain_text_and_entities_json(fake)
    assert text == "msg 0"
    assert entities_json is None


# --- RichText / iv-page flattening (port of telegram-mcp rich_text) -----------


def test_rich_text_to_str_flattens_nested_nodes():
    from telethon.tl.types import TextBold, TextPlain

    node = [TextPlain(text="see "), TextBold(text="this"), TextPlain(text=" now")]
    assert rich_text_to_str(node) == "see this now"
    assert rich_text_to_str("plain") == "plain"


def test_rich_text_to_str_custom_emoji_keeps_alt():
    from telethon.tl.types import TextCustomEmoji

    assert rich_text_to_str(TextCustomEmoji(document_id=1, alt="🎯")) == "🎯"


def test_rich_text_to_str_empty_for_textless_nodes():
    from telethon.tl.types import TextEmpty

    assert rich_text_to_str(TextEmpty()) == ""
    assert rich_text_to_str(None) == ""


def test_page_lines_blocks_and_table_row():
    from telethon.tl.types import PageBlockParagraph, PageBlockTitle, PageTableCell, PageTableRow, TextPlain

    blocks = [
        PageBlockTitle(text=TextPlain(text="Heading")),
        PageBlockParagraph(text=TextPlain(text="Para line")),
    ]
    assert _page_lines(blocks) == ["Heading", "Para line"]

    row = PageTableRow(
        cells=[PageTableCell(text=TextPlain(text="a")), PageTableCell(text=TextPlain(text="b"))]
    )
    assert _page_lines(row) == ["a | b"]


def test_webpage_page_text_walks_cached_page_blocks():
    from telethon.tl.types import PageBlockParagraph, PageBlockTitle, TextPlain

    blocks = [
        PageBlockTitle(text=TextPlain(text="Title")),
        PageBlockParagraph(text=TextPlain(text="Body")),
    ]
    page = SimpleNamespace(blocks=blocks)
    assert webpage_page_text(SimpleNamespace(media=SimpleNamespace(webpage=SimpleNamespace(cached_page=page)))) == (
        "Title\n\nBody"
    )
    assert webpage_page_text(SimpleNamespace(media=None)) == ""
    no_page = SimpleNamespace(media=SimpleNamespace(webpage=SimpleNamespace(cached_page=None)))
    assert webpage_page_text(no_page) == ""


def test_build_message_falls_back_to_webpage_text():
    """A link-only post (empty raw text) takes its words from the preview's iv
    page instead of reporting an empty message (#1350/#1352 lesson)."""
    from telethon.tl.types import PageBlockParagraph, TextPlain

    blocks = [PageBlockParagraph(text=TextPlain(text="IV body"))]
    msg = _msg(
        text="",
        media=SimpleNamespace(webpage=SimpleNamespace(cached_page=SimpleNamespace(blocks=blocks))),
    )
    message = build_message_from_telethon(msg, channel_id=-100123)
    assert message.text == "IV body"


def test_build_message_keeps_plain_text_over_webpage():
    """Non-empty raw text is never replaced by the preview page."""
    page = SimpleNamespace(blocks=[])
    msg = _msg(text="raw", media=SimpleNamespace(webpage=SimpleNamespace(cached_page=page)))
    message = build_message_from_telethon(msg, channel_id=-100123)
    assert message.text == "raw"
