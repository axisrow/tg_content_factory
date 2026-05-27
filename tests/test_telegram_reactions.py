from __future__ import annotations

from types import SimpleNamespace

import pytest
from telethon.tl.types import ReactionPaid

from src.telegram.reactions import (
    extract_message_reactions,
    extract_message_reactions_json,
    extract_reaction_users,
    fetch_message_reaction_users,
    format_message_reactions,
    format_reaction_counts,
    format_reaction_users,
    format_reaction_users_result,
    format_reactions_json,
    normalize_reaction_users_limit,
    parse_reactions_json,
)
from tests.helpers import make_mock_reactions


def test_extract_message_reactions_none():
    msg = SimpleNamespace(reactions=None)

    assert extract_message_reactions(msg) == []
    assert extract_message_reactions_json(msg) is None
    assert format_message_reactions(msg) == ""


def test_extract_message_reactions_multiple_emoji():
    msg = SimpleNamespace(reactions=make_mock_reactions([("👍", 5), ("❤️", 2)]))

    assert extract_message_reactions(msg) == [
        {"emoji": "👍", "count": 5},
        {"emoji": "❤️", "count": 2},
    ]
    assert format_message_reactions(msg) == "👍 5 ❤️ 2"


def test_extract_message_reactions_custom_emoji():
    msg = SimpleNamespace(reactions=make_mock_reactions([(12345678, 2)]))

    assert extract_message_reactions(msg) == [{"emoji": "custom:12345678", "count": 2}]
    assert format_message_reactions(msg) == "custom:12345678 2"


def test_extract_message_reactions_paid_and_unknown():
    msg = SimpleNamespace(
        reactions=SimpleNamespace(
            results=[
                SimpleNamespace(reaction=ReactionPaid(), count=4),
                SimpleNamespace(reaction=SimpleNamespace(), count=9),
            ]
        )
    )

    assert extract_message_reactions(msg) == [{"emoji": "paid", "count": 4}]
    assert format_message_reactions(msg) == "paid 4"


def test_parse_reactions_json_valid_and_format():
    items = parse_reactions_json('[{"emoji": "🔥", "count": 7}, {"emoji": "custom:42", "count": "3"}]')

    assert items == [{"emoji": "🔥", "count": 7}, {"emoji": "custom:42", "count": 3}]
    assert format_reaction_counts(items) == "🔥 7 custom:42 3"
    assert format_reactions_json('[{"emoji": "🔥", "count": 7}]') == "🔥 7"


def test_parse_reactions_json_invalid():
    assert parse_reactions_json("not json") == []
    assert parse_reactions_json(None) == []
    assert parse_reactions_json('{"emoji": "👍", "count": 1}') == []


def test_extract_reaction_users_groups_users_chats_custom_and_paid():
    result = SimpleNamespace(
        reactions=[
            SimpleNamespace(
                peer_id=SimpleNamespace(user_id=10),
                reaction=SimpleNamespace(emoticon="👍"),
            ),
            SimpleNamespace(
                peer_id=SimpleNamespace(user_id=11),
                reaction=SimpleNamespace(document_id=42),
            ),
            SimpleNamespace(
                peer_id=SimpleNamespace(channel_id=12),
                reaction=ReactionPaid(),
            ),
            SimpleNamespace(
                peer_id=SimpleNamespace(user_id=13),
                reaction=SimpleNamespace(),
            ),
        ],
        users=[
            SimpleNamespace(id=10, username="ivan", first_name="Ivan", last_name="Petrov"),
            SimpleNamespace(id=11, username=None, first_name="Maria", last_name=""),
        ],
        chats=[SimpleNamespace(id=12, title="Channel Reactor")],
    )

    items = extract_reaction_users(result)

    assert items == [
        {"emoji": "👍", "display": "@ivan", "peer_id": 10},
        {"emoji": "custom:42", "display": "Maria", "peer_id": 11},
        {"emoji": "paid", "display": "Channel Reactor", "peer_id": 12},
    ]
    assert format_reaction_users(items) == "👍 @ivan; custom:42 Maria; paid Channel Reactor"


def test_extract_reaction_users_falls_back_to_peer_id():
    result = SimpleNamespace(
        reactions=[
            SimpleNamespace(peer_id=SimpleNamespace(user_id=55), reaction=SimpleNamespace(emoticon="❤️")),
        ],
        users=[],
        chats=[],
    )

    assert extract_reaction_users(result) == [{"emoji": "❤️", "display": "id=55", "peer_id": 55}]


def test_format_reaction_users_unavailable_and_limited():
    assert format_reaction_users([], unavailable=True) == "пользователи реакций недоступны"
    assert format_reaction_users([{"emoji": "👍", "display": "@ivan"}], limited=True) == "👍 @ivan ..."


def test_normalize_reaction_users_limit():
    assert normalize_reaction_users_limit(None) == 20
    assert normalize_reaction_users_limit("3") == 3
    assert normalize_reaction_users_limit(0) == 1
    assert normalize_reaction_users_limit(999) == 100


@pytest.mark.anyio
async def test_fetch_message_reaction_users_invokes_telethon_request():
    class FakeClient:
        request = None

        async def __call__(self, request):
            self.request = request
            return SimpleNamespace(
                count=2,
                next_offset="next",
                reactions=[
                    SimpleNamespace(
                        peer_id=SimpleNamespace(user_id=10),
                        reaction=SimpleNamespace(emoticon="👍"),
                    ),
                ],
                users=[SimpleNamespace(id=10, username="ivan")],
                chats=[],
            )

    client = FakeClient()
    result = await fetch_message_reaction_users(client, "peer", 123, limit=500)

    assert client.request.peer == "peer"
    assert client.request.id == 123
    assert client.request.limit == 100
    assert result.items == [{"emoji": "👍", "display": "@ivan", "peer_id": 10}]
    assert result.limited is True
    assert format_reaction_users_result(result) == "👍 @ivan ..."


@pytest.mark.anyio
async def test_fetch_message_reaction_users_returns_unavailable_on_error():
    class FakeClient:
        async def __call__(self, request):
            raise RuntimeError("forbidden")

    result = await fetch_message_reaction_users(FakeClient(), "peer", 123)

    assert result.items == []
    assert result.unavailable == "forbidden"
    assert format_reaction_users_result(result) == "пользователи реакций недоступны"
