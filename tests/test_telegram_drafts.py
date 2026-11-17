"""Drafts keep formatting as real Telegram entities, without live connections (#1854)."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from telethon import types
from telethon.extensions import html as _html

from tg_agent.services.telegram_actions import TelegramActionService
from tg_agent.telegram.outgoing_text import parse_outgoing_text, unparse_outgoing_html


def _pool_with_client(client):
    pool = MagicMock()
    pool.get_native_client_by_phone = AsyncMock(return_value=(client, "+1"))
    pool.get_client_by_phone = AsyncMock(return_value=(client, "+1"))
    pool.release_client = AsyncMock()
    return pool


# --- formatting module ------------------------------------------------------


def test_parse_plain_keeps_text_verbatim():
    assert parse_outgoing_text("**not bold**", None) == ("**not bold**", None)
    assert parse_outgoing_text("**not bold**", "plain") == ("**not bold**", None)


def test_parse_html_carries_bold_underline_and_link():
    text, entities = parse_outgoing_text(
        "<b>01 CASE</b>\n<u>Ждём</u> <a href='https://x.y?b=1'>гайд</a>", "html"
    )
    assert text == "01 CASE\nЖдём гайд"
    assert {type(e) for e in entities} == {
        types.MessageEntityBold,
        types.MessageEntityUnderline,
        types.MessageEntityTextUrl,
    }


def test_parse_markdown_carries_bold():
    text, entities = parse_outgoing_text("**01 CASE** rest", "md")
    assert text == "01 CASE rest"
    assert [type(e) for e in entities] == [types.MessageEntityBold]


def test_parse_unknown_mode_raises_without_touching_telethon():
    with pytest.raises(ValueError, match="parse_mode"):
        parse_outgoing_text("x", "rich")


def test_unparse_round_trips_html_back_to_same_text_and_entities():
    text, entities = parse_outgoing_text("<b>bold</b> <u>подчёрк</u>", "html")
    html_out = unparse_outgoing_html(text, entities)
    assert parse_outgoing_text(html_out, "html") == (text, entities)


# --- service: save_draft ----------------------------------------------------


def _draft_client():
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")
    client.get_input_entity = AsyncMock(return_value="entity")
    client.save_draft = AsyncMock(return_value=True)
    return client


@pytest.mark.anyio
async def test_save_draft_plain_saves_verbatim_without_entities():
    client = _draft_client()
    await TelegramActionService(_pool_with_client(client)).save_draft(
        phone="+1", chat_id="@ch", text="**not bold**"
    )
    client.save_draft.assert_awaited_once_with(
        "entity", "**not bold**", entities=None, reply_to=None, no_webpage=False
    )


@pytest.mark.anyio
async def test_save_draft_html_pre_parses_entities():
    client = _draft_client()
    await TelegramActionService(_pool_with_client(client)).save_draft(
        phone="+1", chat_id="@ch", text="<b>01 CASE</b>", parse_mode="html"
    )
    args, kwargs = client.save_draft.await_args
    assert args == ("entity", "01 CASE")
    assert [type(e) for e in kwargs["entities"]] == [types.MessageEntityBold]


@pytest.mark.anyio
async def test_save_draft_markdown_pre_parses_entities():
    client = _draft_client()
    await TelegramActionService(_pool_with_client(client)).save_draft(
        phone="+1", chat_id="@ch", text="**01 CASE** rest", parse_mode="md"
    )
    args, kwargs = client.save_draft.await_args
    assert args == ("entity", "01 CASE rest")
    assert [type(e) for e in kwargs["entities"]] == [types.MessageEntityBold]


@pytest.mark.anyio
async def test_save_draft_unknown_parse_mode_raises_before_any_request():
    client = _draft_client()
    with pytest.raises(ValueError, match="parse_mode"):
        await TelegramActionService(_pool_with_client(client)).save_draft(
            phone="+1", chat_id="@ch", text="x", parse_mode="rich"
        )
    client.get_entity.assert_not_awaited()
    client.save_draft.assert_not_awaited()


@pytest.mark.anyio
async def test_save_draft_passes_reply_to_and_no_webpage():
    client = _draft_client()
    await TelegramActionService(_pool_with_client(client)).save_draft(
        phone="+1", chat_id="@ch", text="draft", reply_to=9, no_webpage=True
    )
    client.save_draft.assert_awaited_once_with(
        "entity", "draft", entities=None, reply_to=9, no_webpage=True
    )


# --- service: list_drafts ---------------------------------------------------


def _drafts_update(draft, user_id=42):
    return types.Updates(
        updates=[types.UpdateDraftMessage(peer=types.PeerUser(user_id), draft=draft)],
        users=[types.User(id=user_id, first_name="Alice", username="alice42")],
        chats=[],
        date=None,
        seq=0,
    )


@pytest.mark.anyio
async def test_list_drafts_returns_message_html_that_round_trips():
    source = "<b>01 CASE</b>\n<u>Ждём</u> — <a href=\"https://example.com/a?b=1&amp;c=2\">гайд</a> 👍"
    text, entities = _html.parse(source)
    client = _draft_client()
    client.get_all_drafts = AsyncMock(
        return_value=_drafts_update(
            types.DraftMessage(message=text, date=datetime(2026, 11, 17, 10, 0, tzinfo=timezone.utc), entities=entities)
        )
    )
    result = await TelegramActionService(_pool_with_client(client)).list_drafts(phone="+1")
    draft = result.drafts[0]
    assert draft["message"] == "01 CASE\nЖдём — гайд 👍"
    assert parse_outgoing_text(draft["message_html"], "html") == (text, entities)
    assert draft["peer_id"] == 42
    assert draft["first_name"] == "Alice"
    assert draft["username"] == "alice42"
    assert draft["date"] == "2026-11-17T10:00:00+00:00"


@pytest.mark.anyio
async def test_list_drafts_plain_draft_has_no_message_html():
    client = _draft_client()
    client.get_all_drafts = AsyncMock(
        return_value=_drafts_update(types.DraftMessage(message="**not bold**", date=None, entities=None))
    )
    result = await TelegramActionService(_pool_with_client(client)).list_drafts(phone="+1")
    draft = result.drafts[0]
    assert draft["message"] == "**not bold**"
    assert "message_html" not in draft


@pytest.mark.anyio
async def test_list_drafts_skips_empty_draft_rows():
    client = _draft_client()
    client.get_all_drafts = AsyncMock(
        return_value=_drafts_update(types.DraftMessage(message="", date=None, entities=None))
    )
    result = await TelegramActionService(_pool_with_client(client)).list_drafts(phone="+1")
    assert result.drafts == []
