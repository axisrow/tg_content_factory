from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import sys
from datetime import datetime, timezone
from email.utils import format_datetime

from src.cli import runtime
from src.utils.text_safety import csv_safe_cell, escape_xml_text


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
            if args.export_action == "telegram":
                await _run_telegram(db, args)
                return

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


async def _run_telegram(db, args: argparse.Namespace) -> None:
    from src.services.export_service import run_offline_export

    channel_id = getattr(args, "channel_id", None)
    if not channel_id:
        print("Error: --channel-id is required for telegram export.", file=sys.stderr)
        return

    if args.with_media:
        # Media download needs the live worker (owns the ClientPool); enqueue an
        # EXPORT task and optionally wait for it.
        await _enqueue_media_export(db, args, int(channel_id))
        return

    summary = await run_offline_export(
        db,
        int(channel_id),
        fmt=args.export_format,
        date_from=args.date_from,
        date_to=args.date_to,
        limit=int(args.limit),
        out_dir=args.output,
    )
    if summary is None:
        print(f"No messages found for channel {channel_id}.", file=sys.stderr)
        return
    print(
        f"Exported {summary.message_count} messages to {summary.out_dir} "
        f"(files: {', '.join(summary.files)}; media skipped: {summary.media_skipped})",
        file=sys.stderr,
    )
    if summary.truncated:
        print(
            f"Warning: channel has more than {args.limit} messages; exported the oldest "
            f"{summary.message_count} (raise --limit to include more).",
            file=sys.stderr,
        )


async def _enqueue_media_export(db, args: argparse.Namespace, channel_id: int) -> None:
    from src.models import CollectionTaskType, ExportTaskPayload

    payload = ExportTaskPayload(
        channel_id=channel_id,
        fmt=args.export_format,
        with_media=True,
        max_file_size_mb=args.max_file_size,
        date_from=args.date_from,
        date_to=args.date_to,
        limit=int(args.limit),
        out_dir=args.output,
        requested_by="cli",
    )
    task_id = await db.repos.tasks.create_generic_task(
        CollectionTaskType.EXPORT, title=f"export channel {channel_id} (media)", payload=payload
    )
    print(
        f"Enqueued media export task #{task_id}; the worker will download media and build the tree.",
        file=sys.stderr,
    )
    if args.wait:
        await _poll_export_task(db, task_id)


async def _poll_export_task(db, task_id: int, *, timeout: float = 600.0, interval: float = 2.0) -> None:
    from src.models import CollectionTaskStatus

    terminal = {CollectionTaskStatus.COMPLETED, CollectionTaskStatus.FAILED, CollectionTaskStatus.CANCELLED}
    waited = 0.0
    while waited < timeout:
        task = await db.repos.tasks.get_collection_task(task_id)
        if task and task.status in terminal:
            if task.status == CollectionTaskStatus.COMPLETED:
                print(f"Export task #{task_id} completed: {task.note or ''}", file=sys.stderr)
            else:
                detail = task.error or task.note or ""
                print(f"Export task #{task_id} {task.status.value}: {detail}", file=sys.stderr)
            return
        await asyncio.sleep(interval)
        waited += interval
    print(
        f"Export task #{task_id} still pending after {int(timeout)}s — is the worker running?",
        file=sys.stderr,
    )


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
            csv_safe_cell(msg.text or ""),
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
            f"    <title>{escape_xml_text(item_title)}</title>\n"
            f"    <description>{escape_xml_text(text[:500])}</description>\n"
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
