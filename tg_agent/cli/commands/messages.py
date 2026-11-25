from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import sys
import time
from collections.abc import Callable

import typer
from telethon_floodgate import (
    HandledFloodWaitError,
    TelegramRateLimitedError,
    run_with_flood_wait,
)

from tg_agent.cli import runtime
from tg_agent.cli.commands.common import (
    _NEG_ID_POSITIONAL,
    OutputFormat,
    apply_startup,
    cli_error,
    resolve_channel,
    resolve_date_bounds,
    run_async,
)
from tg_agent.models import Message
from tg_agent.services.account_service import resolve_default_phone
from tg_agent.telegram.incoming_wait import DEFAULT_SETTLE_MS, DEFAULT_WAIT_TIMEOUT_SEC
from tg_agent.telegram.reactions import (
    fetch_message_reaction_users,
    format_message_reactions,
    format_reaction_users_result,
    format_reactions_json,
    normalize_reaction_users_limit,
    parse_reactions_json,
)
from tg_agent.telegram.utils import chat_id_and_type, marked_peer_id
from tg_agent.utils.text_safety import csv_safe_cell


def _format_topic(msg: Message) -> str:
    """``topic=1 (General)`` / ``topic=1`` без заголовка в кэше (#1552)."""
    if msg.topic_title:
        return f"topic={msg.topic_id} ({msg.topic_title})"
    return f"topic={msg.topic_id}"


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
                "topic_id": msg.topic_id,
                "topic_title": msg.topic_title,
                "views": msg.views,
                "forwards": msg.forwards,
                "reactions": parse_reactions_json(msg.reactions_json),
            })
        print(json.dumps(items, ensure_ascii=False, indent=2))
    elif fmt == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(
            ["id", "channel_id", "message_id", "date", "text",
             "topic_id", "topic_title", "views", "forwards", "reactions"]
        )
        for msg in messages:
            writer.writerow([
                msg.id, msg.channel_id, msg.message_id,
                str(msg.date) if msg.date else "",
                csv_safe_cell(msg.text or ""),
                msg.topic_id,
                csv_safe_cell(msg.topic_title or ""),
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
            if msg.topic_id is not None:
                fields.append(_format_topic(msg))
            if msg.views:
                fields.append(f"views={msg.views}")
            if reactions:
                fields.append(f"reactions: {reactions}")
            print(f"[{date_str}] {' '.join(fields)}")
            print(f"  {preview}")
            print()


def _print_live_messages(collected: list, reaction_users_by_message_id: dict[int, str] | None = None) -> None:
    reaction_users_by_message_id = reaction_users_by_message_id or {}
    # Chronological display regardless of fetch direction (#1872): the default
    # read fetches newest-first, the --after-id cursor oldest-first.
    for msg in sorted(collected, key=lambda m: m.id):
        date_str = str(msg.date)[:19] if msg.date else "—"
        sender = ""
        if msg.sender:
            name = getattr(msg.sender, "first_name", None) or ""
            last = getattr(msg.sender, "last_name", None) or ""
            sender = f" {name} {last}".strip()
        text = (msg.text or "").strip()
        reactions = format_message_reactions(msg)
        reaction_suffix = f" reactions: {reactions}" if reactions else ""
        print(f"[{date_str}] #{msg.id}{sender}{reaction_suffix}")
        if text:
            print(f"  {text[:500]}")
        media_name = type(msg.media).__name__ if msg.media else ""
        if media_name and media_name != "MessageMediaWebPage":
            # Media marker alongside caption text, like the gateway path (issue #1776);
            # web_page = link preview, not downloadable media — no marker (#1813 review)
            print(f"  [media: {media_name}]")
        reaction_users = reaction_users_by_message_id.get(msg.id)
        if reaction_users:
            print(f"  reaction users: {reaction_users}")
        print()


def _live_sender_name(msg) -> str | None:
    sender = getattr(msg, "sender", None)
    if sender is None:
        return None
    username = getattr(sender, "username", None)
    if username:
        return f"@{username}"
    name = " ".join(
        part
        for part in (getattr(sender, "first_name", None), getattr(sender, "last_name", None))
        if part
    ).strip()
    return name or getattr(sender, "title", None) or None


def _print_live_json(collected: list, *, has_more: bool, chat_type: str) -> None:
    """Live JSON envelope (#1872): records + the forward cursor.

    ``newest_id`` is the next ``--after-id`` for the loop read → process →
    repeat; ``has_more`` says the page was truncated at ``--limit``.
    """
    items = []
    for msg in sorted(collected, key=lambda m: m.id):
        out = bool(getattr(msg, "out", False))
        if out:
            addressed = False
        elif chat_type == "dm":
            # A private chat: every incoming message is addressed to the account.
            addressed = True
        else:
            # Groups/chats: Telegram marks @mentions and replies to the
            # account's messages with the `mentioned` flag.
            addressed = bool(getattr(msg, "mentioned", False))
        items.append({
            "id": msg.id,
            "out": out,
            "addressed": addressed,
            "date": msg.date.isoformat() if msg.date else None,
            "sender": _live_sender_name(msg),
            "text": msg.text if msg.text else None,
        })
    print(json.dumps({
        "messages": items,
        "newest_id": items[-1]["id"] if items else None,
        "has_more": has_more,
    }, ensure_ascii=False, indent=2))


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
    after_id: int | None = None,
    include_reaction_users: bool = False,
    reaction_users_limit: int | None = None,
    output_format: str = "text",
) -> None:
    """Read messages from a channel/dialog — live Telegram (``--live``) or the DB.

    Shared async body for both CLI entry points — the argparse ``run`` wrapper
    below and the Typer ``messages read`` command (``tg_agent/cli/typer_commands.py``).
    Driven through the single async-bridge ``run_async`` by its callers, so
    there is no local ``asyncio.run`` in the migrated path.

    ``--after-id`` is the live forward cursor (#1872): only messages with id
    strictly above it, oldest first — the mirror of the backwards
    ``--offset-id``. JSON live output is an envelope with ``newest_id`` and
    ``has_more`` so callers loop ``read --after-id <newest_id>``.
    """
    config, db = await runtime.init_db(config_path)
    pool = None
    try:
        include_reaction_users = bool(include_reaction_users)
        reaction_users_limit = normalize_reaction_users_limit(reaction_users_limit)

        # Flag-mix guards (#1872): refuse incompatible combinations instead
        # of silently ignoring a flag. Live JSON is supported; live CSV is not.
        if not live and after_id is not None:
            print(
                "--after-id works only with --live "
                "(DB reads are already newest-first). Drop --after-id or add --live.",
                file=sys.stderr,
            )
            raise typer.Exit(1)
        if live:
            if output_format == "csv":
                print(
                    "--format csv is not supported with --live "
                    "(live supports text and JSON). Drop --format or drop --live.",
                    file=sys.stderr,
                )
                raise typer.Exit(1)
            if after_id is not None and offset_id:
                print(
                    "--after-id (read forward from an id) and --offset-id "
                    "(read history before an id) are mutually exclusive.",
                    file=sys.stderr,
                )
                raise typer.Exit(1)
            if output_format == "json" and include_reaction_users:
                print(
                    "--include-reaction-users is not supported with --format json "
                    "(works with live text output).",
                    file=sys.stderr,
                )
                raise typer.Exit(1)

        if live:
            # Live mode: read from Telegram
            _, pool = await runtime.init_pool(config, db)
            if not pool.clients:
                cli_error("No connected accounts.")
            if not phone:
                phone = await resolve_default_phone(db, connected=set(pool.clients))
            if phone is None:
                cli_error("No connected accounts.")
            if phone not in pool.clients:
                cli_error(f"Account {phone} not connected.")
            result = await pool.get_native_client_by_phone(phone)
            if result is None:
                cli_error(f"Client for {phone} unavailable (flood-wait or not connected).")
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
                try:
                    entity = await pool.resolve_entity_with_warm(
                        client, phone, resolve_target, operation="cli_messages_read_resolve"
                    )
                except Exception as exc:
                    cli_error(
                        f"Cannot resolve '{identifier}' via account {phone}: {exc}. "
                        "The dialog may live on another account — retry with --phone."
                    )
                # limit+1 detects has_more exactly (#1872): a page that
                # fetched its full window plus one more message is truncated.
                kwargs = {"limit": limit + 1}
                if offset_id:
                    kwargs["offset_id"] = offset_id
                if after_id is not None:
                    # Forward cursor: ids strictly above after_id, oldest
                    # first — Telethon's forward-pagination form.
                    kwargs["min_id"] = after_id
                    kwargs["reverse"] = True
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
                    cli_error(f"Flood wait: {exc.info.detail}")
                has_more = len(collected) > limit
                collected = collected[:limit]
                if output_format == "json":
                    _, live_chat_type = chat_id_and_type(entity)
                    if not collected:
                        _print_live_json([], has_more=False, chat_type=live_chat_type)
                    else:
                        _print_live_json(collected, has_more=has_more, chat_type=live_chat_type)
                    return
                if not collected:
                    print("No messages found.")
                    return
                reaction_users_by_message_id: dict[int, str] = {}
                if include_reaction_users:
                    # Reaction-user fetches go through the bound adapter
                    # session so every dispatch pays the shared budget and
                    # the process ceiling (#1826); the surrounding
                    # iter_messages read keeps its own flood wrapper.
                    from tg_agent.telegram.backends import adapt_transport_session

                    reaction_session = adapt_transport_session(client, phone=phone, pool=pool)

                    async def _read_reaction_users() -> None:
                        for msg in collected:
                            if not format_message_reactions(msg):
                                continue
                            result = await fetch_message_reaction_users(
                                reaction_session,
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
                        cli_error(f"Flood wait: {exc.info.detail}")
                    except TelegramRateLimitedError as exc:
                        # Local budget refusal (#1826): print what was fetched
                        # and say so — partial reaction data is not complete.
                        print(
                            f"Reaction users incomplete: local rate limit ({exc}); "
                            "retry later for the rest.",
                            file=sys.stderr,
                        )
                _print_live_messages(collected, reaction_users_by_message_id)
            except typer.Exit:
                # cli_error() from the inner paths must not be re-reported as
                # "Error reading messages" by the broad handler below (#1486).
                raise
            except Exception as exc:
                cli_error(f"Error reading messages: {exc}")
        else:
            if include_reaction_users:
                print(
                    "--include-reaction-users works only with --live.",
                    file=sys.stderr,
                )
                raise typer.Exit(1)
            # DB mode: read collected messages
            channels = await db.get_channels()
            ch = resolve_channel(channels, identifier)
            if not ch:
                print(
                    f"Channel '{identifier}' not found in DB. "
                    "Use --live to read directly from Telegram.",
                    file=sys.stderr,
                )
                raise typer.Exit(1)
            page = await db.search_messages(
                query=query,
                channel_id=ch.channel_id,
                date_from=date_from,
                date_to=date_to,
                limit=limit,
                topic_id=topic_id,
            )
            messages = page.messages
            if not messages and output_format == "text":
                print("No messages found.")
                return
            # Empty json/csv still honor the declared format ([] / header-only).
            _print_messages(messages, output_format, page.total, has_more=page.has_more)
    finally:
        if pool:
            await pool.disconnect_all()
        await db.close()


_WAIT_POLL_SEC = 1.0


async def messages_wait_incoming_impl(
    config_path: str,
    *,
    chat: str,
    phone: str | None = None,
    timeout: float = 60,
    settle_ms: int = 6000,
    json_output: bool = False,
) -> None:
    """Block until an incoming message arrives in ``chat`` — the settled burst (#1871).

    ``chat`` is a chat id / @username / phone, or ``"any"`` for inbox mode.
    The event producer depends on who owns the connection: with the session-owner
    relay live (worker daemon or gateway) matches are pushed by the ``wait.messages``
    relay op — a relayed client never sees updates; without a daemon this process
    attaches the Telethon handler to its own client. ``--timeout`` bounds the wait
    for the *first* match; the settle tail may run past it (that is the debounce).
    """
    from tg_agent.telegram import incoming_wait
    from tg_agent.telegram.relay import RelayError, RelayRemoteError

    config, db = await runtime.init_db(config_path)
    pool = None
    try:
        _, pool = await runtime.init_pool(config, db)
        if not pool.clients:
            cli_error("No connected accounts.")
        if not phone:
            phone = await resolve_default_phone(db, connected=set(pool.clients))
        if phone is None:
            cli_error("No connected accounts.")
        if phone not in pool.clients:
            cli_error(f"Account {phone} not connected.")
        result = await pool.get_native_client_by_phone(phone)
        if result is None:
            cli_error(f"Client for {phone} unavailable (flood-wait or not connected).")
        client, _ = result

        target_chat_id: int | None = None
        if chat != "any":
            try:
                entity_id = int(chat)
            except ValueError:
                entity_id = None
            resolve_target = entity_id if entity_id is not None else chat
            try:
                entity = await pool.resolve_entity_with_warm(
                    client, phone, resolve_target, operation="cli_messages_wait_resolve"
                )
            except Exception as exc:
                cli_error(
                    f"Cannot resolve '{chat}' via account {phone}: {exc}. "
                    "The dialog may live on another account — retry with --phone."
                )
            target_chat_id = marked_peer_id(entity)

        queue: asyncio.Queue = asyncio.Queue()
        detach = None
        cleanup: Callable[[], None] | None = None
        if pool.relay is not None:
            connection = pool.relay
            active = True
            settle_sec = max(settle_ms, 0) / 1000.0

            def _on_push(message: dict) -> None:
                if not active or message.get("push") != incoming_wait.PUSH_KIND:
                    return
                queue.put_nowait({k: v for k, v in message.items() if k != "push"})

            connection.on_push(_on_push)

            async def _subscribe() -> None:
                try:
                    await connection.call(
                        "wait.messages",
                        phone=phone,
                        chat_id=target_chat_id,
                        timeout_sec=timeout + settle_sec + 5.0,
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001 - delivered through the queue
                    if active:
                        queue.put_nowait(exc)

            op_task = asyncio.create_task(_subscribe())

            def _stop_subscription() -> None:
                nonlocal active
                active = False
                op_task.cancel()

            cleanup = _stop_subscription
        else:
            detach = incoming_wait.attach_local_listener(
                getattr(client, "raw_client", client), phone, target_chat_id, queue
            )
        try:
            try:
                burst = await incoming_wait.wait_burst(queue, timeout=timeout, settle_ms=settle_ms)
            except incoming_wait.WaitTimeoutError:
                if json_output:
                    print(json.dumps({"event": False, "reason": "timeout"}, ensure_ascii=False))
                else:
                    print(f"No incoming message within {timeout}s.")
                raise typer.Exit(1) from None
            except RelayRemoteError as exc:
                if "unknown relay operation" in str(exc):
                    cli_error(
                        f"The session owner does not serve the wait.messages relay op: {exc}. "
                        "Restart it once: `tg-agent restart`."
                    )
                cli_error(f"messages wait failed: {exc}")
            except (RelayError, ConnectionError, OSError) as exc:
                cli_error(f"messages wait failed: the session owner went away: {exc}")
        finally:
            if cleanup is not None:
                cleanup()
            if detach is not None:
                detach()
        if json_output:
            print(json.dumps(burst.to_json(), ensure_ascii=False, indent=2))
        else:
            _print_incoming_burst(burst)
    finally:
        if pool:
            await pool.disconnect_all()
        await db.close()


def _print_incoming_burst(burst) -> None:
    print(
        f"Incoming: chat={burst.chat_id} name={burst.name or '—'} "
        f"via={burst.phone} messages={burst.message_count}"
    )
    for message_id, sender, date, text in zip(
        burst.message_ids, burst.senders, burst.dates, burst.texts, strict=True
    ):
        header = f"  #{message_id}"
        if sender:
            header += f" {sender}"
        if date:
            header += f" [{date[:19]}]"
        print(header)
        if text:
            print(f"    {text[:500]}")


async def messages_wait_impl(
    config_path: str,
    *,
    message_id: int,
    phone: str | None = None,
    chat: int | None = None,
    timeout: int = 30,
) -> None:
    """Wait until a sent message is observed deleted — local store only.

    The gateway records Telegram deletion updates as they arrive (deletion
    capture), so the verdict is read from ``observed_deletions`` with zero
    Telegram requests: exit 0 once the id shows up, exit 1 when ``--timeout``
    passes without one. Chat matching is ``ObservedDeletionsRepository.
    lookup_deletion``'s policy — this command only forwards the flags.
    """
    _, db = await runtime.init_db(config_path)
    try:
        deadline = time.monotonic() + timeout
        repo = db.repos.observed_deletions
        while True:
            found = await repo.lookup_deletion(message_id, phone=phone, chat_id=chat)
            if found is not None:
                observed_phone, observed_chat, deleted_at = found
                print(
                    f"Deleted: message_id={message_id} chat={observed_chat} "
                    f"via={observed_phone} at={deleted_at.isoformat()}"
                )
                return
            if time.monotonic() >= deadline:
                cli_error(f"Alive: no deletion recorded for message_id={message_id} within {timeout}s")
            await asyncio.sleep(_WAIT_POLL_SEC)
    finally:
        await db.close()


_SENT_CSV_COLUMNS = [
    "message_id", "phone", "chat_id", "chat_type", "recipient",
    "source", "ref", "sent_at", "deleted_at", "text",
]


def _print_sent(rows: list[dict], fmt: str) -> None:
    """Render the sent-messages registry (#1893) in text/json/csv."""
    if fmt == "json":
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return
    if fmt == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(_SENT_CSV_COLUMNS)
        for row in rows:
            writer.writerow(
                [csv_safe_cell(str(row.get(col) or "")) for col in _SENT_CSV_COLUMNS]
            )
        print(buf.getvalue(), end="")
        return
    if not rows:
        print("No sent messages.")
        return
    for row in rows:
        deleted_at = row.get("deleted_at")
        status = f"deleted={deleted_at}" if deleted_at else "alive"
        print(
            f"message_id={row['message_id']} chat={row['chat_id'] if row['chat_id'] is not None else '—'}"
            f" type={row['chat_type']} via={row['phone']} source={row['source']}"
            f" at={row['sent_at']} status={status}"
        )
        detail = ""
        if row.get("recipient"):
            detail += f" to={row['recipient']}"
        text = (row.get("text") or "").strip()
        if text:
            detail += f"  {text[:200].replace(chr(10), ' ')}{'...' if len(text) > 200 else ''}"
        if detail:
            print(f"  {detail}")


async def messages_sent_impl(
    config_path: str,
    *,
    phone: str | None = None,
    chat: int | None = None,
    source: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 200,
    output_format: str = "text",
) -> None:
    """Report the outgoing-send registry (#1893) with deletion status.

    Local store only — zero Telegram requests. ``status=deleted`` appears
    once the deletion-capture pipeline records the message; NULL means the
    message is alive or the deletion fell out of the 7-day window.
    """
    _, db = await runtime.init_db(config_path)
    try:
        rows = await db.repos.sent_messages.list(
            phone=phone,
            chat_id=chat,
            source=source,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
        )
    finally:
        await db.close()
    _print_sent(rows, output_format)


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
            after_id=getattr(args, "after_id", None),
            include_reaction_users=getattr(args, "include_reaction_users", False),
            reaction_users_limit=getattr(args, "reaction_users_limit", None),
            output_format=getattr(args, "output_format", "text"),
        )
    )


# --------------------------------------------------------------------------- #
# messages read
# --------------------------------------------------------------------------- #

messages_app = typer.Typer(no_args_is_help=True, help="Read messages from DB or live Telegram")


@messages_app.command("read", context_settings=_NEG_ID_POSITIONAL)
def messages_read(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Channel pk, channel_id, @username, or dialog ID"),
    limit: int = typer.Option(50, "--limit", help="Max messages (default: 50)"),
    live: bool = typer.Option(False, "--live", help="Read from Telegram instead of DB"),
    phone: str | None = typer.Option(
        None, "--phone", help="Account phone for --live (default: primary account)"
    ),
    query: str = typer.Option("", "--query", help="Text filter (DB only)"),
    date_from: str | None = typer.Option(None, "--date-from", help="Start date YYYY-MM-DD (DB only)"),
    date_to: str | None = typer.Option(None, "--date-to", help="End date YYYY-MM-DD (DB only)"),
    topic_id: int | None = typer.Option(None, "--topic-id", help="Forum topic ID"),
    offset_id: int | None = typer.Option(
        None, "--offset-id", help="Read messages before this message ID (--live)"
    ),
    after_id: int | None = typer.Option(
        None,
        "--after-id",
        help="Forward cursor (--live): only messages with id > N, oldest first",
    ),
    include_reaction_users: bool = typer.Option(
        False, "--include-reaction-users", help="Show users who reacted (live mode only)"
    ),
    reaction_users_limit: int = typer.Option(
        20,
        "--reaction-users-limit",
        help="Max reaction users per message for --include-reaction-users (default: 20)",
    ),
    output_format: OutputFormat = typer.Option(
        OutputFormat.text, "--format", help="Output format (default: text)"
    ),
) -> None:
    """Read messages from a channel/dialog."""
    apply_startup(ctx)
    date_from, date_to = resolve_date_bounds(date_from, date_to)
    run_async(
        messages_read_impl(
            ctx.obj.config,
            identifier=identifier,
            limit=limit,
            live=live,
            phone=phone,
            query=query,
            date_from=date_from,
            date_to=date_to,
            topic_id=topic_id,
            offset_id=offset_id,
            after_id=after_id,
            include_reaction_users=include_reaction_users,
            reaction_users_limit=reaction_users_limit,
            output_format=output_format.value,
        )
    )


@messages_app.command("wait")
def messages_wait(
    ctx: typer.Context,
    message_id: int | None = typer.Option(
        None,
        "--message-id",
        help="Deletion mode: sent message id to watch (local store, no Telegram calls)",
    ),
    phone: str | None = typer.Option(
        None, "--phone", help="Account (deletion: sender; incoming: listener); omit for the primary"
    ),
    chat: str | None = typer.Option(
        None,
        "--chat",
        help="Deletion mode: chat id as Telegram reports it (e.g. -100…). Incoming mode "
        "(long-poll): chat id / @username / phone, or 'any' for every conversation",
    ),
    timeout: int | None = typer.Option(
        None,
        "--timeout",
        help="Seconds to wait (default: 30 deletion, 60 incoming); 0 = single check",
    ),
    settle_ms: int = typer.Option(
        DEFAULT_SETTLE_MS,
        "--settle-ms",
        help="Incoming mode: keep collecting until the chat is quiet this long (default: 6000)",
    ),
    json_output: bool = typer.Option(
        False, "--json", help="Incoming mode: emit the burst as one JSON record"
    ),
) -> None:
    """Wait for an event: --message-id = deletion verdict, --chat = incoming long-poll."""
    apply_startup(ctx)
    if message_id is None and chat is None:
        cli_error(
            "Nothing to wait for: pass --message-id (deletion verdict) "
            "or --chat (incoming long-poll)."
        )
    if message_id is not None:
        # Deletion mode: --chat stays what it always was — an optional numeric
        # chat filter over the local deletion store (#1892).
        chat_id: int | None = None
        if chat is not None:
            try:
                chat_id = int(chat)
            except ValueError:
                cli_error(f"--chat in deletion mode is a numeric chat id, got {chat!r}.")
        run_async(
            messages_wait_impl(
                ctx.obj.config,
                message_id=message_id,
                phone=phone,
                chat=chat_id,
                timeout=timeout if timeout is not None else 30,
            )
        )
        return
    if timeout is None:
        timeout = DEFAULT_WAIT_TIMEOUT_SEC
    run_async(
        messages_wait_incoming_impl(
            ctx.obj.config,
            chat=chat or "any",
            phone=phone,
            timeout=timeout,
            settle_ms=settle_ms,
            json_output=json_output,
        )
    )


@messages_app.command("sent")
def messages_sent(
    ctx: typer.Context,
    phone: str | None = typer.Option(None, "--phone", help="Filter by sending account"),
    chat: int | None = typer.Option(
        None, "--chat", help="Chat id as Telegram reports it (e.g. -100…)"
    ),
    source: str | None = typer.Option(
        None,
        "--source",
        help="Sending feature: dialogs_send, dialogs_queue, dialogs_forward,"
        " dialogs_queue_forward, dm_draft, seeding, ping, agent, agent_forward,"
        " pipeline_publish, pipeline_forward, photo_publish",
    ),
    date_from: str | None = typer.Option(None, "--date-from", help="Start date YYYY-MM-DD"),
    date_to: str | None = typer.Option(None, "--date-to", help="End date YYYY-MM-DD"),
    limit: int = typer.Option(200, "--limit", help="Max rows (newest first)"),
    output_format: OutputFormat = typer.Option(
        OutputFormat.text, "--format", help="Output format (default: text)"
    ),
) -> None:
    """List outgoing sends (registry) with deletion status — local store only."""
    apply_startup(ctx)
    date_from_bound, date_to_bound = resolve_date_bounds(date_from, date_to)
    run_async(
        messages_sent_impl(
            ctx.obj.config,
            phone=phone,
            chat=chat,
            source=source,
            date_from=date_from_bound,
            date_to=date_to_bound,
            limit=limit,
            output_format=output_format.value,
        )
    )
