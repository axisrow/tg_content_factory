"""Parse_mode handling for outgoing Telegram text (drafts, captions).

Follows axisrow/telegram-mcp PR #364 (save_draft/get_drafts keep formatting,
merged 2026-11-07): ``html`` and ``md`` pre-parse into real Telegram entities,
``plain`` keeps the text verbatim, anything else is refused *before* a request
is made. Upstream's unified formatting module (their PR #367) was still open
at port time, so this stays the minimal local shape — extend here, not per
call-site, if more outgoing-text options are ever ported. See
THIRD_PARTY_NOTICES.md.
"""
from __future__ import annotations


def parse_outgoing_text(text: str, parse_mode: str | None) -> tuple[str, list | None]:
    """Return ``(text, entities)`` for the requested parse mode.

    ``None``/``plain`` keep the text verbatim with no entities; ``html`` and
    ``md``/``markdown`` pre-parse the markup into real Telegram formatting
    entities (underline exists only via html); an unknown mode raises
    ValueError before any Telegram request happens.
    """
    mode = (parse_mode or "plain").strip().lower()
    if mode == "html":
        from telethon.extensions import html

        parsed, entities = html.parse(text)
        return parsed, list(entities)
    if mode in {"md", "markdown"}:
        from telethon.extensions import markdown

        parsed, entities = markdown.parse(text)
        return parsed, list(entities)
    if mode == "plain":
        return text, None
    raise ValueError(
        f"parse_mode='{parse_mode}' не поддерживается. Используйте 'md', 'html' или 'plain'."
    )


def unparse_outgoing_html(text: str, entities: list | None) -> str:
    """Render text + entities back to HTML (Telethon's html.unparse)."""
    from telethon.extensions import html

    return html.unparse(text, entities or [])
