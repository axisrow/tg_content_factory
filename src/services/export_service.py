"""Offline orchestration for Telegram-Desktop export (issue #834).

Shared by the CLI (`export telegram`), the web route, and the agent tool so the
"gather messages → build tree" flow lives in one place (CLI/Web/Agent parity).
Media download is the worker's job (PR-3); this offline path always renders the
"not included" representation via ``offline_media_resolver``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from src.services.telegram_export_builder import (
    DEFAULT_HTML_PAGE_SIZE,
    DEFAULT_MAX_FILE_SIZE_MB,
    ExportSummary,
    TelegramExportBuilder,
    offline_media_resolver,
)

EXPORT_ROOT = Path("data/exports")
_PAGE_FETCH = 500
_MAX_LIMIT = 100_000


def default_export_dir(channel_id: int, *, now: datetime | None = None) -> Path:
    # Include a per-run HH-MM-SS suffix so two exports of the same channel on the
    # same day don't reuse one directory and leave stale files behind (Codex #937).
    stamp = (now or datetime.now(timezone.utc)).strftime("%Y-%m-%d_%H-%M-%S")
    return EXPORT_ROOT / f"ChatExport_{stamp}_{channel_id}"


async def gather_channel_messages(
    db, channel_id: int, *, date_from=None, date_to=None, limit=5000
) -> tuple[list, bool]:
    """Page a channel's messages oldest-first (Telegram-Desktop order).

    Returns ``(messages, truncated)`` — ``truncated`` is True when the channel has
    more messages than ``limit``, so the caller can record it in the manifest
    instead of silently dropping the newest history.
    """
    limit = max(1, min(int(limit), _MAX_LIMIT))
    collected: list = []
    offset = 0
    truncated = False
    while len(collected) < limit:
        page = await db.repos.messages.get_channel_messages_for_export(
            channel_id,
            date_from=date_from,
            date_to=date_to,
            limit=min(_PAGE_FETCH, limit - len(collected)),
            offset=offset,
        )
        batch = list(page.messages)
        if not batch:
            break
        collected.extend(batch)
        offset += len(batch)
        if len(collected) >= limit:
            truncated = page.has_more
            break
        if not page.has_more:
            break
    return collected, truncated


async def resolve_max_file_size_mb(db, override=None) -> int:
    if override is not None:
        return max(1, int(override))
    raw = await db.get_setting("export_max_file_size_mb")
    try:
        return max(1, int(raw)) if raw else DEFAULT_MAX_FILE_SIZE_MB
    except (TypeError, ValueError):
        return DEFAULT_MAX_FILE_SIZE_MB


async def resolve_html_page_size(db) -> int:
    raw = await db.get_setting("export_html_page_size")
    try:
        return max(1, int(raw)) if raw else DEFAULT_HTML_PAGE_SIZE
    except (TypeError, ValueError):
        return DEFAULT_HTML_PAGE_SIZE


async def run_offline_export(
    db,
    channel_id: int,
    *,
    fmt: str = "json",
    date_from=None,
    date_to=None,
    limit: int = 5000,
    out_dir: str | Path | None = None,
) -> ExportSummary | None:
    """Build a Telegram-Desktop export tree from the DB (no media download).

    Returns the ``ExportSummary`` on success, or ``None`` when the channel is
    unknown or has no matching messages.
    """
    channel = await db.get_channel_by_channel_id(int(channel_id))
    if channel is None:
        return None
    messages, truncated = await gather_channel_messages(
        db, int(channel_id), date_from=date_from, date_to=date_to, limit=limit
    )
    if not messages:
        return None
    target = Path(out_dir) if out_dir else default_export_dir(int(channel_id))
    page_size = await resolve_html_page_size(db)
    return await TelegramExportBuilder().write_export(
        target,
        channel,
        messages,
        fmt=fmt,
        media_resolver=offline_media_resolver,
        page_size=page_size,
        truncated=truncated,
    )
