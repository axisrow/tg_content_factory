from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json

from src.cli import runtime
from src.cli.commands.common import resolve_channel
from src.models import Message
from src.telegram.flood_wait import HandledFloodWaitError, run_with_flood_wait
from src.telegram.reactions import (
    fetch_message_reaction_users,
    format_message_reactions,
    format_reaction_users_result,
    format_reactions_json,
    normalize_reaction_users_limit,
    parse_reactions_json,
)
from src.utils.text_safety import csv_safe_cell


def _print_messages(messages: list[Message], fmt: str, total: int, has_more: bool = False) -> None:
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
                "reactions": parse_reactions_json(msg.reactions_json),
            })
        print(json.dumps(items, ensure_ascii=False, indent=2))
    elif fmt == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["id", "channel_id", "message_id", "date", "text", "views", "forwards", "reactions"])
        for msg in messages:
            writer.writerow([
                msg.id, msg.channel_id, msg.message_id,
                str(msg.date) if msg.date else "",
                csv_safe_cell((msg.text or "")[:500]),
                msg.views, msg.forwards,
                format_reactions_json(msg.reactions_json),
            ])
        print(buf.getvalue(), end="")
    else:
        # total is a lower bound when has_more is set (#766) — show "N+".
        total_display = f"{total}+" if has_more else str(total)
        print(f"Total: {total_display} messages (showing {len(messages)})\n")
        for msg in messages:
            date_str = str(msg.date)[:19] if msg.date else "—"
            text = (msg.text or "").strip()
            preview = text[:200].replace("\n", " ")
            if len(text) > 200:
                preview += "..."
            reactions = format_reactions_json(msg.reactions_json)
            fields = [f"#{msg.message_id}"]
            if msg.views:
                fields.append(f"views={msg.views}")
            if reactions:
                fields.append(f"reactions: {reactions}")
            print(f"[{date_str}] {' '.join(fields)}")
            print(f"  {preview}")
            print()


def _print_live_messages(collected: list, reaction_users_by_message_id: dict[int, str] | None = None) -> None:
    reaction_users_by_message_id = reaction_users_by_message_id or {}
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
        reactions = format_message_reactions(msg)
        reaction_suffix = f" reactions: {reactions}" if reactions else ""
        print(f"[{date_str}] #{msg.id}{sender}{reaction_suffix}")
        if text:
            print(f"  {text[:500]}")
        reaction_users = reaction_users_by_message_id.get(msg.id)
        if reaction_users:
            print(f"  reaction users: {reaction_users}")
        print()


async def messages_read_impl(
    config_path: str,
    *,
    identifier: str,
    limit: int = 50,
    live: bool = False,
    phone: str | None = None,
    query: str = "",
    date_from: str | None = None,
    date_to: str | None = None,
    topic_id: int | None = None,
    offset_id: int | None = None,
    include_reaction_users: bool = False,
    reaction_users_limit: int | None = None,
    output_format: str = "text",
) -> None:
    """Read messages from a channel/dialog — live Telegram (``--live``) or the DB.

    Shared async body for both CLI entry points — the argparse ``run`` wrapper
    below and the Typer ``messages read`` command (``src/cli/typer_commands.py``).
    Driven through the single async-bridge ``run_async`` by its callers, so
    there is no local ``asyncio.run`` in the migrated path.
    """
    config, db = await runtime.init_db(config_path)
    pool = None
    try:
        include_reaction_users = bool(include_reaction_users)
        reaction_users_limit = normalize_reaction_users_limit(reaction_users_limit)

        if live:
            # Live mode: read from Telegram
            _, pool = await runtime.init_pool(config, db)
            if not pool.clients:
                print("No connected accounts.")
                return
            accounts = sorted(pool.clients.keys())
            phone = phone or accounts[0]
            if phone not in pool.clients:
                print(f"Account {phone} not connected.")
                return
            result = await pool.get_native_client_by_phone(phone)
            if result is None:
                print(f"Client for {phone} unavailable (flood-wait or not connected).")
                return
            client, _ = result
            try:
                # Try numeric ID first; a freshly created chat may not be in
                # the cold entity cache yet, so warm dialogs once and retry
                # via the centralized resolver.
                try:
                    entity_id = int(identifier)
                except ValueError:
                    entity_id = None
                resolve_target = entity_id if entity_id is not None else identifier
                entity = await pool.resolve_entity_with_warm(
                    client, phone, resolve_target, operation="cli_messages_read_resolve"
                )
                kwargs = {"limit": limit}
                if offset_id:
                    kwargs["offset_id"] = offset_id
                if topic_id:
                    kwargs["reply_to"] = topic_id
                collected: list = []

                async def _read_messages() -> None:
                    async for msg in client.iter_messages(entity, **kwargs):
                        collected.append(msg)

                try:
                    await run_with_flood_wait(
                        _read_messages(),
                        operation="cli_messages_read",
                        phone=phone,
                        pool=pool,
                    )
                except HandledFloodWaitError as exc:
                    print(f"Flood wait: {exc.info.detail}")
                    return
                if not collected:
                    print("No messages found.")
                    return
                reaction_users_by_message_id: dict[int, str] = {}
                if include_reaction_users:
                    async def _read_reaction_users() -> None:
                        for msg in collected:
                            if not format_message_reactions(msg):
                                continue
                            result = await fetch_message_reaction_users(
                                client,
                                entity,
                                msg.id,
                                limit=reaction_users_limit,
                            )
                            formatted = format_reaction_users_result(result)
                            if formatted:
                                reaction_users_by_message_id[msg.id] = formatted

                    try:
                        await run_with_flood_wait(
                            _read_reaction_users(),
                            operation="cli_messages_reaction_users",
                            phone=phone,
                            pool=pool,
                        )
                    except HandledFloodWaitError as exc:
                        print(f"Flood wait: {exc.info.detail}")
                        return
                _print_live_messages(collected, reaction_users_by_message_id)
            except Exception as exc:
                print(f"Error reading messages: {exc}")
        else:
            if include_reaction_users:
                print("--include-reaction-users works only with --live.")
                return
            # DB mode: read collected messages
            channels = await db.get_channels()
            ch = resolve_channel(channels, identifier)
            if not ch:
                print(
                    f"Channel '{identifier}' not found in DB. "
                    "Use --live to read directly from Telegram."
                )
                return
            page = await db.search_messages(
                query=query,
                channel_id=ch.channel_id,
                date_from=date_from,
                date_to=date_to,
                limit=limit,
                topic_id=topic_id,
            )
            messages = page.messages
            if not messages:
                print("No messages found.")
                return
            _print_messages(messages, output_format, page.total, has_more=page.has_more)
    finally:
        if pool:
            await pool.disconnect_all()
        await db.close()


def run(args: argparse.Namespace) -> None:
    if getattr(args, "messages_action", None) != "read":
        return
    asyncio.run(
        messages_read_impl(
            args.config,
            identifier=args.identifier,
            limit=getattr(args, "limit", 50),
            live=getattr(args, "live", False),
            phone=getattr(args, "phone", None),
            query=getattr(args, "query", ""),
            date_from=getattr(args, "date_from", None),
            date_to=getattr(args, "date_to", None),
            topic_id=getattr(args, "topic_id", None),
            offset_id=getattr(args, "offset_id", None),
            include_reaction_users=getattr(args, "include_reaction_users", False),
            reaction_users_limit=getattr(args, "reaction_users_limit", None),
            output_format=getattr(args, "output_format", "text"),
        )
    )
