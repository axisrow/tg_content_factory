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
    stamp = (now or datetime.now(timezone.utc)).strftime("%Y-%m-%d")
    return EXPORT_ROOT / f"ChatExport_{stamp}_{channel_id}"


async def gather_channel_messages(db, channel_id: int, *, date_from=None, date_to=None, limit=5000) -> list:
    """Page through stored messages, sorted chronologically by message_id.

    Telegram message_id is monotonic per channel, so ascending message_id is a
    robust chronological order (and avoids comparing naive vs aware datetimes).
    """
    limit = max(1, min(int(limit), _MAX_LIMIT))
    collected: list = []
    offset = 0
    while len(collected) < limit:
        page = await db.search_messages(
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            limit=min(_PAGE_FETCH, limit - len(collected)),
            offset=offset,
            include_filtered=True,
        )
        batch = list(page.messages)
        if not batch:
            break
        collected.extend(batch)
        offset += len(batch)
        if not page.has_more:
            break
    collected.sort(key=lambda m: (m.message_id or 0))
    return collected[:limit]


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
    messages = await gather_channel_messages(
        db, int(channel_id), date_from=date_from, date_to=date_to, limit=limit
    )
    if not messages:
        return None
    target = Path(out_dir) if out_dir else default_export_dir(int(channel_id))
    page_size = await resolve_html_page_size(db)
    return TelegramExportBuilder().write_export(
        target,
        channel,
        messages,
        fmt=fmt,
        media_resolver=offline_media_resolver,
        page_size=page_size,
    )
