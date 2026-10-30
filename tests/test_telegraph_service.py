"""Tests for the Telegraph service (#1703): markdown → Node converter and the
``createAccount``/``createPage`` aiohttp client. Fake-first — no network."""

from __future__ import annotations

import json

import pytest

from tg_agent.services import telegraph_service
from tg_agent.services.telegraph_service import (
    create_account,
    create_page,
    markdown_to_telegraph_nodes,
)

# --------------------------------------------------------------------------- #
# markdown → Node converter
# --------------------------------------------------------------------------- #


def test_empty_body_yields_no_nodes():
    assert markdown_to_telegraph_nodes("") == []
    assert markdown_to_telegraph_nodes("\n \n") == []


def test_headings_map_to_h3_h4():
    nodes = markdown_to_telegraph_nodes("# One\n## Two\n### Three\n#### Four")
    assert [n["tag"] for n in nodes] == ["h3", "h3", "h4", "h4"]
    assert nodes[0]["children"] == ["One"]
    assert nodes[2]["children"] == ["Three"]


def test_paragraph_lines_join_and_inline_parse():
    nodes = markdown_to_telegraph_nodes("Первый кусок\nвторой кусок.\n\nНовый абзац.")
    assert len(nodes) == 2
    assert nodes[0] == {"tag": "p", "children": ["Первый кусок второй кусок."]}
    assert nodes[1] == {"tag": "p", "children": ["Новый абзац."]}


def test_bold_italic_code_and_links():
    nodes = markdown_to_telegraph_nodes("**жирный** и *курсив* и `код`")
    children = nodes[0]["children"]
    assert {"tag": "strong", "children": ["жирный"]} in children
    assert {"tag": "em", "children": ["курсив"]} in children
    assert {"tag": "code", "children": ["код"]} in children

    linked = markdown_to_telegraph_nodes("[текст](https://example.com/x)")
    assert linked[0]["children"] == [
        {"tag": "a", "attrs": {"href": "https://example.com/x"}, "children": ["текст"]}
    ]


def test_inline_nesting_bold_contains_link():
    nodes = markdown_to_telegraph_nodes("**см. [источник](https://example.com)**")
    assert nodes[0]["children"] == [
        {
            "tag": "strong",
            "children": [
                "см. ",
                {"tag": "a", "attrs": {"href": "https://example.com"}, "children": ["источник"]},
            ],
        }
    ]


def test_bare_url_becomes_clickable_link():
    nodes = markdown_to_telegraph_nodes("Источник: https://example.com/a?b=1 — конец.")
    children = nodes[0]["children"]
    assert "Источник: " in children
    assert {
        "tag": "a",
        "attrs": {"href": "https://example.com/a?b=1"},
        "children": ["https://example.com/a?b=1"],
    } in children
    assert " — конец." in children


def test_multiplication_stars_stay_literal():
    nodes = markdown_to_telegraph_nodes("умножение 2 * 3 * 4 = 24")
    assert nodes[0]["children"] == ["умножение 2 * 3 * 4 = 24"]


def test_unordered_and_ordered_lists():
    nodes = markdown_to_telegraph_nodes("- один\n- два\n\n1. первый\n2. второй")
    assert nodes[0] == {
        "tag": "ul",
        "children": [
            {"tag": "li", "children": ["один"]},
            {"tag": "li", "children": ["два"]},
        ],
    }
    assert nodes[1]["tag"] == "ol"
    assert [c["children"] for c in nodes[1]["children"]] == [["первый"], ["второй"]]


def test_blockquote_groups_consecutive_lines():
    nodes = markdown_to_telegraph_nodes("> цитата раз\n> цитата два\n\nпосле")
    assert nodes[0] == {"tag": "blockquote", "children": ["цитата раз цитата два"]}
    assert nodes[1]["tag"] == "p"


def test_fenced_code_block():
    nodes = markdown_to_telegraph_nodes("до\n```python\nx = 1\ny = 2\n```\nпосле")
    assert nodes[0] == {"tag": "p", "children": ["до"]}
    assert nodes[1] == {
        "tag": "pre",
        "children": [{"tag": "code", "children": ["x = 1\ny = 2"]}],
    }
    assert nodes[2]["children"] == ["после"]


def test_unclosed_fence_does_not_hang():
    nodes = markdown_to_telegraph_nodes("```txt\nнезакрытый блок")
    assert nodes[0]["tag"] == "pre"
    assert nodes[0]["children"][0]["children"] == ["незакрытый блок"]


def test_hr_and_closing_hashes():
    nodes = markdown_to_telegraph_nodes("## Заголовок ##\n---\nтекст")
    assert [n["tag"] for n in nodes] == ["h3", "hr", "p"]
    assert nodes[1] == {"tag": "hr", "children": []}


def test_table_lines_pass_through_as_text():
    nodes = markdown_to_telegraph_nodes("| a | b |\n|---|---|\n| 1 | 2 |")
    assert all(n["tag"] == "p" for n in nodes)  # no crash, no table node
    assert "| a | b |" in nodes[0]["children"][0]  # rows survive as literal text
    assert "| 1 | 2 |" in nodes[0]["children"][0]


# --------------------------------------------------------------------------- #
# create_account / create_page — fake aiohttp transport
# --------------------------------------------------------------------------- #


class _FakeResponse:
    def __init__(self, *, status: int = 200, payload: dict | None = None):
        self.status = status
        self._payload = payload if payload is not None else {}

    async def text(self) -> str:
        return json.dumps(self._payload)

    async def json(self, content_type: str | None = None) -> dict:
        return self._payload

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        return False


class _FakeSession:
    """Returns the queued responses in order; records every POST form."""

    def __init__(self, responses: list[_FakeResponse]):
        self._responses = list(responses)
        self.posts: list[dict] = []

    def post(self, url: str, *, data: dict | None = None, **kwargs: object) -> _FakeResponse:
        self.posts.append({"url": url, "data": data})
        return self._responses.pop(0)

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        return False


def _patch_session(monkeypatch: pytest.MonkeyPatch, session: _FakeSession) -> None:
    monkeypatch.setattr(
        telegraph_service.aiohttp, "ClientSession", lambda timeout=None: session
    )


async def test_create_account_posts_short_name(monkeypatch):
    payload = {"ok": True, "result": {"access_token": "tok-1", "short_name": "Клуб"}}
    session = _FakeSession([_FakeResponse(payload=payload)])
    _patch_session(monkeypatch, session)

    result = await create_account("Клуб", author_url="https://t.me/x")

    assert result == payload
    post = session.posts[0]
    assert post["url"] == telegraph_service.CREATE_ACCOUNT_URL
    assert post["data"] == {"short_name": "Клуб", "author_url": "https://t.me/x"}


async def test_create_page_posts_token_and_form(monkeypatch):
    payload = {
        "ok": True,
        "result": {"path": "Slug-10-30", "url": "https://telegra.ph/Slug-10-30"},
    }
    session = _FakeSession([_FakeResponse(payload=payload)])
    _patch_session(monkeypatch, session)

    content = [{"tag": "p", "children": ["привет"]}]
    result = await create_page(
        "tok-1", "Титул", content, author_name="Клуб", author_url="https://t.me/x"
    )

    assert result == payload
    post = session.posts[0]
    assert post["url"] == telegraph_service.CREATE_PAGE_URL
    form = post["data"]
    assert form["access_token"] == "tok-1"
    assert form["title"] == "Титул"
    assert json.loads(form["content"]) == content
    assert form["return_content"] == "false"
    assert form["author_name"] == "Клуб"
    assert form["author_url"] == "https://t.me/x"


async def test_create_page_omits_author_fields_when_unset(monkeypatch):
    session = _FakeSession([_FakeResponse(payload={"ok": True, "result": {}})])
    _patch_session(monkeypatch, session)

    await create_page("tok-1", "Т", [])
    form = session.posts[0]["data"]
    assert "author_name" not in form
    assert "author_url" not in form


async def test_create_page_raises_on_http_error(monkeypatch):
    session = _FakeSession([_FakeResponse(status=502, payload={"error": "bad gateway"})])
    _patch_session(monkeypatch, session)

    with pytest.raises(RuntimeError, match="Telegraph HTTP 502"):
        await create_page("tok-1", "Т", [])


async def test_create_page_raises_on_api_error(monkeypatch):
    session = _FakeSession([_FakeResponse(payload={"ok": False, "error": "ACCESS_TOKEN_INVALID"})])
    _patch_session(monkeypatch, session)

    with pytest.raises(RuntimeError, match="ACCESS_TOKEN_INVALID"):
        await create_page("tok-1", "Т", [])
