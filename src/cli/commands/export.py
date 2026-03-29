from __future__ import annotations

import argparse
import asyncio
import csv
import html
import io
import json
import sys
from datetime import datetime, timezone
from email.utils import format_datetime

from src.cli import runtime


def _rfc822(dt: datetime | None) -> str:
    if dt is None:
        return format_datetime(datetime.now(timezone.utc))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return format_datetime(dt)


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        _, db = await runtime.init_db(args.config)
        try:
            channel_id = getattr(args, "channel_id", None)
            limit = max(1, min(args.limit, 10000))

            messages, _ = await db.search_messages(
                channel_id=channel_id,
                limit=limit,
            )
            if not messages:
                print("No messages found.", file=sys.stderr)
                return

            output_file = getattr(args, "output", None)
            fmt = args.export_action

            if fmt == "json":
                content = _export_json(messages)
            elif fmt == "csv":
                content = _export_csv(messages)
            elif fmt == "rss":
                content = _export_rss(messages)
            else:
                print(f"Unknown format: {fmt}", file=sys.stderr)
                return

            if output_file:
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(content)
                print(f"Exported {len(messages)} messages to {output_file}", file=sys.stderr)
            else:
                print(content, end="")
        finally:
            await db.close()

    asyncio.run(_run())


def _export_json(messages) -> str:
    items = []
    for msg in messages:
        items.append({
            "id": msg.id,
            "channel_id": msg.channel_id,
            "message_id": msg.message_id,
            "date": str(msg.date) if msg.date else None,
            "text": msg.text,
            "views": msg.views,
            "forwards": msg.forwards,
        })
    return json.dumps(items, ensure_ascii=False, indent=2)


def _export_csv(messages) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id", "channel_id", "message_id", "date", "text", "views", "forwards"])
    for msg in messages:
        writer.writerow([
            msg.id, msg.channel_id, msg.message_id,
            str(msg.date) if msg.date else "",
            msg.text or "",
            msg.views, msg.forwards,
        ])
    return buf.getvalue()


def _export_rss(messages) -> str:
    items: list[str] = []
    for msg in messages:
        text = (msg.text or "").strip()
        if not text:
            continue
        msg_id = msg.message_id or ""
        ch_id = msg.channel_id or ""
        pub_date = _rfc822(msg.date)
        item_title = text[:80].replace("\n", " ")
        guid = f"tg-msg-{ch_id}-{msg_id}"
        items.append(
            f"  <item>\n"
            f"    <title>{html.escape(item_title)}</title>\n"
            f"    <description>{html.escape(text[:500])}</description>\n"
            f"    <pubDate>{pub_date}</pubDate>\n"
            f"    <guid isPermaLink='false'>{guid}</guid>\n"
            f"  </item>"
        )

    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<rss version="2.0">\n'
        "  <channel>\n"
        "    <title>TG Content Factory Export</title>\n"
        "    <description>Exported Telegram messages</description>\n"
        f"    <lastBuildDate>{_rfc822(None)}</lastBuildDate>\n"
        + "\n".join(items)
        + "\n  </channel>\n</rss>"
    )
