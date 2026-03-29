from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json

from src.cli import runtime
from src.cli.commands.common import resolve_channel
from src.models import Message


def _print_messages(messages: list[Message], fmt: str, total: int) -> None:
    if fmt == "json":
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
        print(json.dumps(items, ensure_ascii=False, indent=2))
    elif fmt == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["id", "channel_id", "message_id", "date", "text", "views", "forwards"])
        for msg in messages:
            writer.writerow([
                msg.id, msg.channel_id, msg.message_id,
                str(msg.date) if msg.date else "",
                (msg.text or "")[:500],
                msg.views, msg.forwards,
            ])
        print(buf.getvalue(), end="")
    else:
        print(f"Total: {total} messages (showing {len(messages)})\n")
        for msg in messages:
            date_str = str(msg.date)[:19] if msg.date else "—"
            text = (msg.text or "").strip()
            preview = text[:200].replace("\n", " ")
            if len(text) > 200:
                preview += "..."
            views = f"views={msg.views}" if msg.views else ""
            print(f"[{date_str}] #{msg.message_id} {views}")
            print(f"  {preview}")
            print()


def _print_live_messages(collected: list) -> None:
    for msg in reversed(collected):
        date_str = str(msg.date)[:19] if msg.date else "—"
        sender = ""
        if msg.sender:
            name = getattr(msg.sender, "first_name", None) or ""
            last = getattr(msg.sender, "last_name", None) or ""
            sender = f" {name} {last}".strip()
        text = (msg.text or "").strip()
        if not text and msg.media:
            text = f"[media: {type(msg.media).__name__}]"
        print(f"[{date_str}] #{msg.id}{sender}")
        if text:
            print(f"  {text[:500]}")
        print()


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
        pool = None
        try:
            if args.messages_action == "read":
                identifier = args.identifier

                if args.live:
                    # Live mode: read from Telegram
                    _, pool = await runtime.init_pool(config, db)
                    if not pool.clients:
                        print("No connected accounts.")
                        return
                    accounts = sorted(pool.clients.keys())
                    phone = args.phone or accounts[0]
                    if phone not in pool.clients:
                        print(f"Account {phone} not connected.")
                        return
                    result = await pool.get_native_client_by_phone(phone)
                    if result is None:
                        print(f"Client for {phone} unavailable (flood-wait or not connected).")
                        return
                    client, _ = result
                    try:
                        # Try numeric ID first
                        try:
                            entity_id = int(identifier)
                            entity = await client.get_entity(entity_id)
                        except ValueError:
                            entity = await client.get_entity(identifier)
                        kwargs = {"limit": args.limit}
                        if args.offset_id:
                            kwargs["offset_id"] = args.offset_id
                        if args.topic_id:
                            kwargs["reply_to"] = args.topic_id
                        collected = []
                        async for msg in client.iter_messages(entity, **kwargs):
                            collected.append(msg)
                        if not collected:
                            print("No messages found.")
                            return
                        _print_live_messages(collected)
                    except Exception as exc:
                        print(f"Error reading messages: {exc}")
                else:
                    # DB mode: read collected messages
                    channels = await db.get_channels()
                    ch = resolve_channel(channels, identifier)
                    if not ch:
                        print(
                            f"Channel '{identifier}' not found in DB. "
                            "Use --live to read directly from Telegram."
                        )
                        return
                    messages, total = await db.search_messages(
                        query=args.query,
                        channel_id=ch.channel_id,
                        date_from=args.date_from,
                        date_to=args.date_to,
                        limit=args.limit,
                        topic_id=args.topic_id,
                    )
                    if not messages:
                        print("No messages found.")
                        return
                    _print_messages(messages, args.output_format, total)
        finally:
            if pool:
                await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
