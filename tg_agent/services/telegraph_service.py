"""Telegraph (telegra.ph) page creation — lightweight aiohttp client (#1703).

``createAccount`` (once, via ``telegraph login``) yields an ``access_token``
that lives in settings under ``telegraph_access_token`` (encrypted at rest,
masked in output like every ``*_token`` key); ``createPage`` then publishes
pages. A live smoke against the real API showed ``createPage`` rejects
anonymous calls (``ACCESS_TOKEN_INVALID``) — there is no tokenless path.
Raw aiohttp in the spirit of ``provider_adapters.py``: one POST per call, no
heavy SDK. The markdown → Node conversion is a local subset converter —
Telegraph's allowed tag set is small (https://telegra.ph/api) and the content
factory's articles only need headings, paragraphs, lists, inline formatting,
quotes, code and hr.
"""

from __future__ import annotations

import json
import re
from typing import Any

import aiohttp

API_BASE = "https://api.telegra.ph"
CREATE_ACCOUNT_URL = f"{API_BASE}/createAccount"
CREATE_PAGE_URL = f"{API_BASE}/createPage"

#: Settings key holding the Telegraph ``access_token`` (secret: masked in
#: every settings surface, stored enc:v2 via ``set_secret``).
ACCESS_TOKEN_SETTING = "telegraph_access_token"

# Telegraph field limits (https://telegra.ph/api) — enforced CLI-side so a bad
# invocation fails before the network round-trip.
TITLE_MAX = 256
AUTHOR_NAME_MAX = 128
AUTHOR_URL_MAX = 512
SHORT_NAME_MAX = 32

_HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30)

_HR_RE = re.compile(r"^\s*(?:-{3,}|\*{3,}|_{3,})\s*$")
_HEADING_RE = re.compile(r"^(#{1,4})\s+(.+?)\s*#*\s*$")
_UL_ITEM_RE = re.compile(r"^\s*[-*+]\s+(.+)$")
_OL_ITEM_RE = re.compile(r"^\s*\d+[.)]\s+(.+)$")
_FENCE_RE = re.compile(r"^```\S*\s*$")
_LINK_RE = re.compile(r"\[(?P<text>[^\]]*)\]\((?P<url>[^)\s]+)\)")
_BARE_URL_RE = re.compile(r"https?://[^\s)>\]]+")


def _inline(text: str) -> list[Any]:
    """Parse inline markdown (``code``, **strong**, *em*, [text](url)) into
    Telegraph Node children; bare http(s) URLs become links so source links in
    digest-style articles stay clickable."""
    # ponytail: no backslash escapes and no _underscores_ emphasis — lone
    # markers simply stay literal (only matched pairs are parsed); add escapes
    # if articles start carrying literal asterisks.
    nodes: list[Any] = []
    buf: list[str] = []

    def flush() -> None:
        if not buf:
            return
        run = "".join(buf)
        buf.clear()
        pos = 0
        for match in _BARE_URL_RE.finditer(run):
            if match.start() > pos:
                nodes.append(run[pos : match.start()])
            nodes.append(
                {"tag": "a", "attrs": {"href": match.group(0)}, "children": [match.group(0)]}
            )
            pos = match.end()
        if pos < len(run):
            nodes.append(run[pos:])

    i = 0
    n = len(text)
    while i < n:
        if text.startswith("```", i):
            end = text.find("```", i + 3)
            if end != -1:
                flush()
                nodes.append({"tag": "code", "children": [text[i + 3 : end]]})
                i = end + 3
                continue
        if text[i] == "`":
            end = text.find("`", i + 1)
            if end != -1:
                flush()
                nodes.append({"tag": "code", "children": [text[i + 1 : end]]})
                i = end + 1
                continue
        if text.startswith("**", i):
            end = text.find("**", i + 2)
            if end != -1:
                flush()
                nodes.append({"tag": "strong", "children": _inline(text[i + 2 : end])})
                i = end + 2
                continue
        # *em* only when the marker hugs content — ``2 * 3 * 4`` stays literal.
        if text[i] == "*" and i + 1 < n and text[i + 1] != " ":
            end = text.find("*", i + 1)
            if end != -1 and text[end - 1] != " ":
                flush()
                nodes.append({"tag": "em", "children": _inline(text[i + 1 : end])})
                i = end + 1
                continue
        if text[i] == "[":
            match = _LINK_RE.match(text, i)
            if match:
                flush()
                nodes.append(
                    {
                        "tag": "a",
                        "attrs": {"href": match.group("url")},
                        "children": _inline(match.group("text")),
                    }
                )
                i = match.end()
                continue
        buf.append(text[i])
        i += 1
    flush()
    return nodes


def markdown_to_telegraph_nodes(markdown: str) -> list[dict[str, Any]]:
    """Convert a markdown subset to the Telegraph Node JSON array.

    Blocks: fenced code, hr, h1–h4 (→ h3/h4), blockquote, flat ul/ol lists,
    paragraphs. Tables (``|…|``) are not supported by Telegraph — their lines
    pass through as plain paragraph text, the caller lays them out.
    """
    nodes: list[dict[str, Any]] = []
    para: list[str] = []

    def flush_para() -> None:
        if para:
            nodes.append({"tag": "p", "children": _inline(" ".join(para))})
            para.clear()

    lines = markdown.replace("\r\n", "\n").split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]
        if _FENCE_RE.match(line):
            flush_para()
            code_lines: list[str] = []
            i += 1
            while i < len(lines) and not lines[i].lstrip().startswith("```"):
                code_lines.append(lines[i])
                i += 1
            i += 1  # skip the closing fence (or EOF)
            nodes.append(
                {"tag": "pre", "children": [{"tag": "code", "children": ["\n".join(code_lines)]}]}
            )
            continue
        if _HR_RE.match(line):
            flush_para()
            nodes.append({"tag": "hr", "children": []})
        elif heading := _HEADING_RE.match(line):
            flush_para()
            tag = "h3" if len(heading.group(1)) <= 2 else "h4"
            nodes.append({"tag": tag, "children": _inline(heading.group(2))})
        elif line.lstrip().startswith(">"):
            flush_para()
            quote: list[str] = []
            while i < len(lines) and lines[i].lstrip().startswith(">"):
                quote.append(lines[i].lstrip().lstrip(">").strip())
                i += 1
            i -= 1
            nodes.append({"tag": "blockquote", "children": _inline(" ".join(quote))})
        elif _UL_ITEM_RE.match(line) or _OL_ITEM_RE.match(line):
            flush_para()
            ordered = _OL_ITEM_RE.match(line) is not None
            item_re = _OL_ITEM_RE if ordered else _UL_ITEM_RE
            items: list[dict[str, Any]] = []
            while i < len(lines) and (match := item_re.match(lines[i])):
                items.append({"tag": "li", "children": _inline(match.group(1))})
                i += 1
            i -= 1
            nodes.append({"tag": "ol" if ordered else "ul", "children": items})
        elif not line.strip():
            flush_para()
        else:
            para.append(line.strip())
        i += 1
    flush_para()
    return nodes


async def _post_api(url: str, form: dict[str, str]) -> dict[str, Any]:
    """POST *form* to a Telegraph API method; return the full response dict.

    Raises ``RuntimeError`` on transport or API errors (``ok: false``).
    """
    async with aiohttp.ClientSession(timeout=_HTTP_TIMEOUT) as session:
        async with session.post(url, data=form) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Telegraph HTTP {resp.status}: {(await resp.text())[:200]}")
            data = await resp.json(content_type=None)
    if not data.get("ok"):
        raise RuntimeError(f"Telegraph API error: {data.get('error') or data}")
    return data


async def create_account(
    short_name: str,
    *,
    author_name: str | None = None,
    author_url: str | None = None,
) -> dict[str, Any]:
    """Create a (disposable) Telegraph account; the response carries
    ``access_token`` — the caller persists it via ``ACCESS_TOKEN_SETTING``."""
    form: dict[str, str] = {"short_name": short_name}
    if author_name:
        form["author_name"] = author_name
    if author_url:
        form["author_url"] = author_url
    return await _post_api(CREATE_ACCOUNT_URL, form)


async def create_page(
    access_token: str,
    title: str,
    content: list[dict[str, Any]],
    *,
    author_name: str | None = None,
    author_url: str | None = None,
) -> dict[str, Any]:
    """Publish a telegra.ph page under *access_token*; returns the full API
    response (``result.url`` is the public page link)."""
    form: dict[str, str] = {
        "access_token": access_token,
        "title": title,
        "content": json.dumps(content, ensure_ascii=False),
        "return_content": "false",
    }
    if author_name:
        form["author_name"] = author_name
    if author_url:
        form["author_url"] = author_url
    return await _post_api(CREATE_PAGE_URL, form)
