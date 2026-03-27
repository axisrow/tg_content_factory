from __future__ import annotations

import html
import logging
from datetime import datetime, timezone
from email.utils import format_datetime

from fastapi import APIRouter, Request
from fastapi.responses import Response

from src.web import deps

logger = logging.getLogger(__name__)
router = APIRouter()

_RSS_CONTENT_TYPE = "application/rss+xml; charset=utf-8"
_ATOM_CONTENT_TYPE = "application/atom+xml; charset=utf-8"


def _rfc822(dt: datetime | None) -> str:
    if dt is None:
        return format_datetime(datetime.now(timezone.utc))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return format_datetime(dt)


def _iso8601(dt: datetime | None) -> str:
    if dt is None:
        return datetime.now(timezone.utc).isoformat()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


@router.get("/rss.xml", response_class=Response)
async def rss_feed(
    request: Request,
    channel_id: int | None = None,
    limit: int = 50,
) -> Response:
    """RSS 2.0 feed for collected messages."""
    db = deps.get_db(request)
    base_url = str(request.base_url).rstrip("/")
    limit = max(1, min(limit, 200))

    try:
        if channel_id is not None:
            messages = await db.get_messages(channel_id=channel_id, limit=limit)
        else:
            messages = await db.get_messages(limit=limit)
    except Exception:
        logger.exception("Failed to fetch messages for RSS feed")
        messages = []

    title = "TG Content Factory"
    link = base_url
    description = "Collected Telegram channel messages"

    items: list[str] = []
    for msg in messages:
        text = (getattr(msg, "text", "") or "").strip()
        if not text:
            continue
        msg_id = getattr(msg, "id", None) or getattr(msg, "message_id", None) or ""
        ch_id = getattr(msg, "channel_id", "")
        pub_date = _rfc822(getattr(msg, "date", None))
        item_title = text[:80].replace("\n", " ")
        item_link = f"{base_url}/search?q={html.escape(item_title[:40])}"
        guid = f"tg-msg-{ch_id}-{msg_id}"
        items.append(
            f"  <item>\n"
            f"    <title>{html.escape(item_title)}</title>\n"
            f"    <link>{html.escape(item_link)}</link>\n"
            f"    <description>{html.escape(text[:500])}</description>\n"
            f"    <pubDate>{pub_date}</pubDate>\n"
            f"    <guid isPermaLink='false'>{guid}</guid>\n"
            f"  </item>"
        )

    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<rss version="2.0">\n'
        "  <channel>\n"
        f"    <title>{html.escape(title)}</title>\n"
        f"    <link>{html.escape(link)}</link>\n"
        f"    <description>{html.escape(description)}</description>\n"
        f"    <lastBuildDate>{_rfc822(None)}</lastBuildDate>\n"
        + "\n".join(items)
        + "\n  </channel>\n</rss>"
    )
    return Response(content=xml, media_type=_RSS_CONTENT_TYPE)


@router.get("/atom.xml", response_class=Response)
async def atom_feed(
    request: Request,
    channel_id: int | None = None,
    limit: int = 50,
) -> Response:
    """Atom 1.0 feed for collected messages."""
    db = deps.get_db(request)
    base_url = str(request.base_url).rstrip("/")
    limit = max(1, min(limit, 200))

    try:
        if channel_id is not None:
            messages = await db.get_messages(channel_id=channel_id, limit=limit)
        else:
            messages = await db.get_messages(limit=limit)
    except Exception:
        logger.exception("Failed to fetch messages for Atom feed")
        messages = []

    entries: list[str] = []
    for msg in messages:
        text = (getattr(msg, "text", "") or "").strip()
        if not text:
            continue
        msg_id = getattr(msg, "id", None) or getattr(msg, "message_id", None) or ""
        ch_id = getattr(msg, "channel_id", "")
        updated = _iso8601(getattr(msg, "date", None))
        entry_title = text[:80].replace("\n", " ")
        entry_id = f"urn:tg:msg:{ch_id}:{msg_id}"
        entries.append(
            f"  <entry>\n"
            f"    <id>{html.escape(entry_id)}</id>\n"
            f"    <title>{html.escape(entry_title)}</title>\n"
            f"    <updated>{updated}</updated>\n"
            f"    <content type='text'>{html.escape(text[:1000])}</content>\n"
            f"  </entry>"
        )

    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<feed xmlns="http://www.w3.org/2005/Atom">\n'
        f"  <id>{html.escape(base_url)}/atom.xml</id>\n"
        "  <title>TG Content Factory</title>\n"
        f"  <updated>{_iso8601(None)}</updated>\n"
        f"  <link href='{html.escape(base_url)}'/>\n"
        + "\n".join(entries)
        + "\n</feed>"
    )
    return Response(content=xml, media_type=_ATOM_CONTENT_TYPE)
