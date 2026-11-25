from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import time
from collections.abc import Awaitable, Callable

import typer
from telethon_floodgate import HandledFloodWaitError, TelegramRateLimitedError, is_blocking_flood_wait_until

from tg_agent.cli import runtime
from tg_agent.cli.commands.common import (
    _NEG_ID_POSITIONAL,
    OutputFormat,
    apply_startup,
    cli_error,
    run_async,
    sync_prompt,
)
from tg_agent.models import DialogMessage, TelegramCommandStatus
from tg_agent.parsers import parse_identifiers
from tg_agent.services.account_service import resolve_default_phone
from tg_agent.services.channel_service import ChannelService
from tg_agent.services.chat_history_export import export_chat_history
from tg_agent.services.telegram_actions import (
    BROADCAST_STAT_FIELDS,
    TelegramActionClientUnavailableError,
    TelegramActionMessageNotFoundError,
    TelegramActionNoMediaError,
    TelegramActionService,
)
from tg_agent.services.telegram_command_service import TelegramCommandService
from tg_agent.telegram.dm_archive import backfill_account
from tg_agent.telegram.pool_dialogs import ForumTopicsError
from tg_agent.telegram.reactions import (
    SUPPORTED_REACTION_EMOJIS_DISPLAY,
    TelegramReactionInvalidError,
    normalize_outgoing_reaction_emoji,
)
from tg_agent.telegram.rpc_guard import describe_rpc_failure
from tg_agent.telegram.transcription import default_engine
from tg_agent.utils.datetime import parse_required_datetime
from tg_agent.utils.text_safety import csv_safe_cell


async def _resolve_phone(db, pool, args) -> str | None:
    """Pick the account phone for a dialogs action, or fail loudly (#1486).

    An explicit ``--phone`` wins (must be connected). Otherwise the DB primary
    account is used when connected, falling back to the first connected phone
    when the primary is not (#1480).

    Returns the resolved phone; exits non-zero (stderr + ``typer.Exit``) when
    there are no connected accounts or the requested account is not connected —
    a dialogs action that cannot run is a failure, not a silent no-op.
    """
    if not pool.clients:
        cli_error("No connected accounts.")
    explicit = getattr(args, "phone", None)
    if explicit:
        if explicit not in pool.clients:
            cli_error(f"Account {explicit} not connected.")
        return explicit
    # pool.clients is non-empty here, and a non-empty `connected` set always
    # yields a phone from resolve_default_phone — no None case to handle.
    return await resolve_default_phone(db, connected=set(pool.clients))


def _parse_message_ids(args) -> list[int] | None:
    """Parse comma/space-separated ``args.message_ids`` into ints.

    Returns the parsed ids, or ``None`` (after printing an error) when none are
    valid.
    """
    raw_ids: list[str] = []
    for item in args.message_ids:
        raw_ids.extend(part.strip() for part in item.split(",") if part.strip())
    ids = [int(raw) for raw in raw_ids if raw.isdigit()]
    if not ids:
        print("No valid message IDs provided.")
        return None
    return ids


def _confirm_or_abort(args, *lines: str) -> bool:
    """Show a confirmation preview and read a [y/N] answer unless ``--yes``.

    Returns True to proceed, False (after printing "Aborted.") to stop.
    """
    if args.yes:
        return True
    for line in lines:
        print(line)
    answer = sync_prompt("Continue? [y/N] ").strip().lower()
    if answer != "y":
        print("Aborted.")
        return False
    return True


async def _dialogs_refresh(args, db, pool, *, channel_service_cls) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    svc = channel_service_cls(db, pool, None)  # type: ignore[arg-type]
    dialogs = await svc.get_my_dialogs(phone, refresh=True)
    if getattr(dialogs, "partial", False):
        print(
            f"WARNING: Обход диалогов завершился частично: сохранено "
            f"{getattr(dialogs, 'saved', 0)}, устаревшие записи не удалялись. "
            "Запустите обновление ещё раз, чтобы продолжить."
        )
    else:
        print(f"Dialogs refreshed: {len(dialogs)} total.")


async def _dialogs_list(args, db, pool, *, channel_service_cls) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    svc = channel_service_cls(db, pool, None)  # type: ignore[arg-type]
    dialogs = await svc.get_my_dialogs(phone)
    if not dialogs:
        print("No dialogs found.")
        return
    fmt = "{:<12} {:<40} {:<20} {:<8}"
    print(fmt.format("Type", "Title", "Username", "In DB"))
    print("-" * 84)
    for dialog in dialogs:
        print(
            fmt.format(
                dialog["channel_type"],
                dialog["title"][:40],
                ("@" + dialog["username"]) if dialog.get("username") else "",
                "Yes" if dialog.get("already_added") else "-",
            )
        )


async def _dialogs_resolve(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    try:
        entity = await pool.resolve_any_entity(args.identifier, phone=phone)
    except RuntimeError as exc:
        if "no_client" in str(exc):
            cli_error("No connected accounts.")
        cli_error(f"Error resolving entity: {exc}")
    except Exception as exc:
        cli_error(f"Error resolving entity: {exc}")
    if not entity:
        cli_error(f"Entity '{args.identifier}' not found.")
    print(f"Title: {entity['title']}")
    print(f"Type: {entity['channel_type']}")
    print(f"ID: {entity['channel_id']}")
    if entity.get("username"):
        print(f"Username: @{entity['username']}")


async def _dialogs_leave(args, db, pool, *, channel_service_cls) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return

    raw_ids: list[str] = []
    for item in args.dialog_ids:
        raw_ids.extend(part.strip() for part in item.split(",") if part.strip())
    dialog_ids: list[int] = []
    for raw in raw_ids:
        try:
            dialog_ids.append(int(raw))
        except ValueError:
            print(f"Invalid dialog ID: {raw!r}, skipping.")
    if not dialog_ids:
        print("No valid dialog IDs provided.")
        return

    svc = channel_service_cls(db, pool, None)  # type: ignore[arg-type]
    dialogs_info = await svc.get_my_dialogs(phone)
    type_map = {dialog["channel_id"]: dialog["channel_type"] for dialog in dialogs_info}
    title_map = {dialog["channel_id"]: dialog["title"] for dialog in dialogs_info}

    # Unknown ids (not in the dialog cache) fall back to "channel" -> PeerChannel,
    # not a guessed "dm" -> PeerUser. Channel ids here are bare-positive, so guessing
    # an unknown positive id as "dm" would target the wrong peer (parity with the
    # agent leave_dialogs tool).
    dialogs = [
        (channel_id, type_map.get(channel_id, "channel"))
        for channel_id in dialog_ids
    ]

    if not args.yes:
        print(f"About to leave {len(dialogs)} dialog(s):")
        for channel_id, channel_type in dialogs:
            title = title_map.get(channel_id, str(channel_id))
            print(f"  {channel_id}  {title}  ({channel_type})")
        answer = sync_prompt("Continue? [y/N] ").strip().lower()
        if answer != "y":
            print("Aborted.")
            return

    result = await TelegramActionService(pool).leave_dialogs(phone=phone, dialogs=dialogs)
    for channel_id, ok in result.results.items():
        status = "left" if ok else "failed"
        print(f"  {channel_id}: {status}")
    left = result.success_count
    failed = result.failed_count
    print(f"\nDone: {left} left, {failed} failed.")


async def _dialogs_delete(args, db, pool, *, channel_service_cls) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return

    raw_ids: list[str] = []
    for item in args.dialog_ids:
        raw_ids.extend(part.strip() for part in item.split(",") if part.strip())
    dialog_ids: list[int] = []
    for raw in raw_ids:
        try:
            dialog_ids.append(int(raw))
        except ValueError:
            print(f"Invalid dialog ID: {raw!r}, skipping.")
    if not dialog_ids:
        print("No valid dialog IDs provided.")
        return

    svc = channel_service_cls(db, pool, None)  # type: ignore[arg-type]
    dialogs_info = await svc.get_my_dialogs(phone)
    type_map = {dialog["channel_id"]: dialog["channel_type"] for dialog in dialogs_info}
    title_map = {dialog["channel_id"]: dialog["title"] for dialog in dialogs_info}

    # Unknown ids fall back to "channel" -> PeerChannel (parity with leave / the
    # agent delete_dialogs tool).
    dialogs = [
        (channel_id, type_map.get(channel_id, "channel"))
        for channel_id in dialog_ids
    ]

    if not args.yes:
        print(f"About to PERMANENTLY DELETE {len(dialogs)} dialog(s):")
        for channel_id, channel_type in dialogs:
            title = title_map.get(channel_id, str(channel_id))
            print(f"  {channel_id}  {title}  ({channel_type})")
        answer = sync_prompt("This is irreversible. Continue? [y/N] ").strip().lower()
        if answer != "y":
            print("Aborted.")
            return

    result = await TelegramActionService(pool).delete_dialogs(phone=phone, dialogs=dialogs)
    for channel_id, ok in result.results.items():
        status = "deleted" if ok else "failed"
        print(f"  {channel_id}: {status}")
    deleted = result.success_count
    failed = result.failed_count
    print(f"\nDone: {deleted} deleted, {failed} failed.")


async def _dialogs_join(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return

    if not _confirm_or_abort(args, f"Join/subscribe account {phone} to {args.target}"):
        return

    try:
        result = await TelegramActionService(pool).join_dialog(
            phone=phone,
            target=args.target,
        )
        mode = "invite" if result.via_invite else "public"
        print(f"Joined/subscribed to {result.target} as {result.phone} ({mode}).")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable (flood-wait or not connected).")
    except Exception as exc:
        cli_error(f"Error joining channel/group: {exc}")


async def _dialogs_topics(args, db, pool) -> None:
    channel_id = args.channel_id
    # A failed live read must say so (#1927): reason on stderr + non-zero exit,
    # while stored rows still print, labeled "(stored)". Empty-but-valid keeps
    # today's exit-0 output.
    live_error: str | None = None
    try:
        topics = await pool.get_forum_topics(channel_id, prefer=getattr(args, "phone", None) or None)
        from_store = False
    except ForumTopicsError as exc:
        live_error = str(exc)
        topics = await db.get_forum_topics(channel_id)
        from_store = True
    if not topics and live_error is None:
        topics = await db.get_forum_topics(channel_id)
        from_store = True

    if not topics:
        if live_error is not None:
            cli_error(f"Forum topics read failed for channel {channel_id}: {live_error}")
        print(
            f"No forum topics found for channel {channel_id}."
            " The channel may not be a forum or is not accessible."
        )
        return

    if live_error is not None:
        source = f"stored — live read failed: {live_error}"
    else:
        source = "stored" if from_store else "live"
    print(f"Forum topics for channel {channel_id} ({source}):")
    fmt = "{:<8} {:<40} {:<20} {:<26}"
    print(fmt.format("ID", "Title", "Icon", "Date"))
    print("-" * 98)
    for topic in topics:
        print(
            fmt.format(
                str(topic["id"]),
                topic["title"][:40],
                str(topic.get("icon_emoji_id") or "-")[:20],
                str(topic.get("date") or "-")[:26],
            )
        )
    if live_error is not None:
        cli_error(f"Live forum-topics read failed for channel {channel_id}: {live_error}")


async def _dialogs_send(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    recipient = args.recipient
    text = args.text
    comment_to = getattr(args, "comment_to", None)
    reply_to = getattr(args, "reply_to", None)
    # comment_to routes the text into the linked discussion group, whose reply
    # target Telethon picks itself; an explicit in-chat reply_to would fight it.
    if comment_to is not None and reply_to is not None:
        cli_error("--comment-to and --reply-to are mutually exclusive: a comment's reply target is picked by Telegram.")
        return

    headline = (
        f"Comment on post #{comment_to} in {recipient} from {phone}:"
        if comment_to is not None
        else f"Send message from {phone} to {recipient}:"
    )
    if reply_to is not None:
        headline += f" (reply to #{reply_to})"
    if not _confirm_or_abort(
        args,
        headline,
        f"  {text[:200]}{'...' if len(text) > 200 else ''}",
    ):
        return

    try:
        result = await TelegramActionService(pool, db=db).send_message(
            phone=phone,
            recipient=recipient,
            text=text,
            silent=bool(getattr(args, "silent", False)),
            comment_to=comment_to,
            reply_to=reply_to,
            parse_mode=getattr(args, "parse_mode", None),
            source="dialogs_send",
        )
        message_id = f" message_id={result.message_id}" if result.message_id is not None else ""
        print(f"Message sent to {recipient}.{message_id} via {result.phone}")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable (flood-wait or not connected).")
    except (HandledFloodWaitError, TelegramRateLimitedError) as exc:
        # Curated attribution (#1828): refusing RPC + delivery verdict + retry
        # deadline, so a resolve-side flood never reads as a send failure.
        cli_error(f"Send refused: {describe_rpc_failure(exc)}")
    except Exception as exc:
        cli_error(f"Error sending message: {exc}")


async def _dialogs_forward(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    ids = _parse_message_ids(args)
    if ids is None:
        return
    if not _confirm_or_abort(
        args,
        f"Forward {len(ids)} message(s) from {args.from_chat} to {args.to_chat}: {ids}",
    ):
        return
    try:
        fwd_result = await TelegramActionService(pool, db=db).forward_messages(
            phone=phone,
            from_chat=args.from_chat,
            to_chat=args.to_chat,
            message_ids=ids,
            source="dialogs_forward",
        )
        id_suffix = ""
        if fwd_result.message_ids:
            id_suffix = " forwarded_ids=" + ",".join(str(i) for i in fwd_result.message_ids)
        print(f"Forwarded {len(ids)} message(s) from {args.from_chat} to {args.to_chat}.{id_suffix}")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable (flood-wait or not connected).")
    except (HandledFloodWaitError, TelegramRateLimitedError) as exc:
        cli_error(f"Forward refused: {describe_rpc_failure(exc)}")
    except Exception as exc:
        cli_error(f"Error forwarding messages: {exc}")


async def _dialogs_edit_message(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    preview = args.text[:200] + ("..." if len(args.text) > 200 else "")
    if not _confirm_or_abort(
        args,
        f"Edit message #{args.message_id} in {args.chat_id}:",
        f"  {preview}",
    ):
        return
    try:
        await TelegramActionService(pool).edit_message(
            phone=phone,
            chat_id=args.chat_id,
            message_id=args.message_id,
            text=args.text,
        )
        print(f"Message #{args.message_id} edited.")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable (flood-wait or not connected).")
    except (HandledFloodWaitError, TelegramRateLimitedError) as exc:
        cli_error(f"Edit refused: {describe_rpc_failure(exc)}")
    except Exception as exc:
        cli_error(f"Error editing message: {exc}")


async def _dialogs_edit_message_media(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    if args.text is None:
        target = "(current caption preserved)"
    elif args.text == "":
        target = "(caption cleared)"
    else:
        target = args.text[:200] + ("..." if len(args.text) > 200 else "")
    preview = target
    if not _confirm_or_abort(
        args,
        f"Replace media of message #{args.message_id} in {args.chat_id} with {args.file}:",
        f"  {preview}",
    ):
        return
    try:
        await TelegramActionService(pool).edit_message_media(
            phone=phone,
            chat_id=args.chat_id,
            message_id=args.message_id,
            file=args.file,
            text=args.text,
            parse_mode=args.parse_mode,
        )
        print(f"Media of message #{args.message_id} replaced.")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable (flood-wait or not connected).")
    except Exception as exc:
        cli_error(f"Error replacing message media: {exc}")


def _print_drafts(drafts: list[dict], *, output_format: str) -> None:
    if output_format == "json":
        print(json.dumps({"drafts": drafts, "count": len(drafts)}, ensure_ascii=False))
        return
    if not drafts:
        print("No drafts found.")
        return
    for draft in drafts:
        header = f"peer_id={draft.get('peer_id', '')}"
        if draft.get("username"):
            header += f" @{draft['username']}"
        if draft.get("title"):
            header += f" {draft['title']}"
        elif draft.get("first_name"):
            header += f" {draft['first_name']}"
        print(header)
        print(f"  {draft.get('message', '')}")
        if "message_html" in draft:
            print(f"  html: {draft['message_html']}")


async def _dialogs_draft_save(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    # An empty SaveDraftRequest deletes the chat's existing draft by TL
    # semantics — possibly one typed on another device, an unrecoverable loss.
    # Refuse here so the CLI matches the agent tool's surface; clearing a draft
    # is a deliberate separate action, never a side effect of an empty arg.
    if not str(args.text).strip():
        cli_error("Empty draft text is refused: saving an empty draft would delete the existing one.")
        return
    # A draft is account-local (nobody receives it) and re-editable, so unlike
    # sends/edits it does not prompt for confirmation.
    try:
        await TelegramActionService(pool).save_draft(
            phone=phone,
            chat_id=args.chat_id,
            text=args.text,
            parse_mode=args.parse_mode,
            reply_to=args.reply_to,
            no_webpage=bool(args.no_webpage),
        )
        print(f"Draft saved to {args.chat_id}. Open the chat in Telegram to see and send it.")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable (flood-wait or not connected).")
    except Exception as exc:
        cli_error(f"Error saving draft: {exc}")


async def _dialogs_draft_list(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    try:
        result = await TelegramActionService(pool).list_drafts(phone=phone)
        _print_drafts(result.drafts, output_format=getattr(args, "format", "text"))
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable (flood-wait or not connected).")
    except Exception as exc:
        cli_error(f"Error listing drafts: {exc}")


async def _dialogs_delete_message(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    ids = _parse_message_ids(args)
    if ids is None:
        return
    if not _confirm_or_abort(
        args,
        f"Delete {len(ids)} message(s) from {args.chat_id}: {ids}",
    ):
        return
    try:
        await TelegramActionService(pool).delete_messages(
            phone=phone,
            chat_id=args.chat_id,
            message_ids=ids,
        )
        print(f"Deleted {len(ids)} message(s).")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable (flood-wait or not connected).")
    except Exception as exc:
        cli_error(f"Error deleting messages: {exc}")


async def _dialogs_react(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    # Surface only a long (non-transient) flood-wait explicitly; a
    # transient (<=60s) flood is now waited out centrally inside the
    # by-phone write resolver (get_*_client_by_phone wait_for_flood).
    acc = await pool._get_account_for_phone(phone)
    if (
        acc is not None
        and acc.flood_wait_until is not None
        and is_blocking_flood_wait_until(acc.flood_wait_until)
    ):
        cli_error(
            f"Account {phone} is flood-waited until "
            f"{acc.flood_wait_until.isoformat()}."
        )
    if args.clear:
        emoji = None
    else:
        raw_emoji = args.emoji
        if not raw_emoji:
            print("Error: emoji is required unless --clear is used.")
            raise SystemExit(2)
        try:
            emoji = normalize_outgoing_reaction_emoji(raw_emoji)
        except TelegramReactionInvalidError:
            cli_error(
                "Error: Telegram does not support this reaction emoji. "
                f"Supported reactions: {SUPPORTED_REACTION_EMOJIS_DISPLAY}"
            )
    action = (
        f"Clear reaction from message #{args.message_id} in {args.chat_id}"
        if args.clear
        else f"Send reaction {emoji!r} to message #{args.message_id} in {args.chat_id}"
    )
    if not _confirm_or_abort(args, action):
        return
    try:
        result = await TelegramActionService(pool).send_reaction(
            phone=phone,
            chat_id=args.chat_id,
            message_id=args.message_id,
            emoji=emoji,
            native=True,
            resolve_entity=True,
        )
        if args.clear:
            print(
                f"Reaction cleared from message #{args.message_id} "
                f"in {args.chat_id} (account {result.phone})"
            )
        else:
            print(
                f"Reaction {emoji!r} sent to message #{args.message_id} "
                f"in {args.chat_id} (account {result.phone})"
            )
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable (no lease could be acquired).")
    except Exception as exc:
        cli_error(f"Error sending reaction: {type(exc).__name__}: {exc}")


async def _dialogs_pin_message(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    if not _confirm_or_abort(args, f"Pin message #{args.message_id} in {args.chat_id}"):
        return
    try:
        await TelegramActionService(pool).pin_message(
            phone=phone,
            chat_id=args.chat_id,
            message_id=args.message_id,
            notify=args.notify,
        )
        print(f"Message #{args.message_id} pinned.")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable.")
    except Exception as exc:
        cli_error(f"Error pinning message: {exc}")


async def _dialogs_unpin_message(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    target = f"#{args.message_id}" if args.message_id else "all messages"
    if not _confirm_or_abort(args, f"Unpin {target} in {args.chat_id}"):
        return
    try:
        await TelegramActionService(pool).unpin_message(
            phone=phone,
            chat_id=args.chat_id,
            message_id=args.message_id,
        )
        print("Message(s) unpinned.")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable.")
    except Exception as exc:
        cli_error(f"Error unpinning message: {exc}")


async def _dialogs_download_media(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    try:
        result = await TelegramActionService(pool).download_media(
            phone=phone,
            chat_id=args.chat_id,
            message_id=args.message_id,
            output_dir=args.output_dir,
            operation_prefix="cli_dialogs_download_media",
        )
        print(f"Downloaded: {result.path}")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable.")
    except TelegramActionMessageNotFoundError:
        cli_error(f"Message #{args.message_id} not found.")
    except TelegramActionNoMediaError:
        print("No media in this message.")
    except HandledFloodWaitError as exc:
        cli_error(f"Flood wait: {exc.info.detail}")
    except Exception as exc:
        cli_error(f"Error downloading media: {exc}")


async def _dialogs_transcribe(args, db, pool) -> None:
    engine = getattr(args, "engine", None) or default_engine()
    explicit_phone = getattr(args, "phone", None)
    if explicit_phone or engine != "telegram":
        phone = await _resolve_phone(db, pool, args)
    else:
        # No explicit --phone: pass None so the service's premium lease picks a
        # premium account instead of forcing the default (possibly non-premium)
        # one into a guaranteed premium_required.
        phone = None
    if phone is None and engine != "telegram":
        return
    try:
        result = await TelegramActionService(pool).transcribe_voice(
            phone=phone,
            chat_id=args.chat_id,
            message_id=args.message_id,
            engine=engine,
            db=db,
        )
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable.")
    except TelegramActionMessageNotFoundError:
        cli_error(f"Message #{args.message_id} not found.")
    except TelegramActionNoMediaError:
        print("No media in this message.")
        return
    except HandledFloodWaitError as exc:
        cli_error(f"Flood wait: {exc.info.detail}")
    except Exception as exc:
        cli_error(f"Error transcribing: {exc}")

    status = result.get("status")
    if status == "ok":
        meta = result.get("source") or engine
        if result.get("cached"):
            meta += ", cached"
        if result.get("lang"):
            meta += f", {result['lang']}"
        print(f"[{meta}, not verbatim]")
        print(result.get("text") or "")
    elif status == "premium_required":
        print(result.get("error") or "Нужен Telegram Premium для движка telegram (или --engine groq|openai|whisper).")
    elif status == "pending":
        print("Telegram ещё не готов транскрипт (pending) — повторите вызов позже.")
    else:
        print(result.get("error") or "Transcription failed.")


def _print_dm_messages(messages: list[DialogMessage], fmt: str) -> None:
    """Render live dialog history (`DialogMessage`, not the collector's `Message`).

    Mirrors `messages._print_messages`'s text/json/csv structure so `dialogs
    read` output feels like the rest of the CLI, but maps our own fields —
    the collector model's `channel_id`/`views`/`reactions_json` don't apply
    to a live DM/group history read.
    """
    if fmt == "json":
        items = [
            {
                "id": m.id,
                "sender_id": m.sender_id,
                "sender_name": m.sender_name,
                "out": m.out,
                "date": m.date.isoformat() if m.date else None,
                "text": m.text,
                "media_type": m.media_type,
                "reply_to_id": m.reply_to_id,
                "is_forward": m.is_forward,
            }
            for m in messages
        ]
        print(json.dumps(items, ensure_ascii=False, indent=2))
    elif fmt == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["id", "sender_id", "sender_name", "out", "date", "text", "media_type", "is_forward"])
        for m in messages:
            writer.writerow([
                m.id, m.sender_id, csv_safe_cell(m.sender_name or ""), m.out,
                m.date.isoformat() if m.date else "",
                csv_safe_cell(m.text or ""),
                m.media_type or "", m.is_forward,
            ])
        print(buf.getvalue(), end="")
    else:
        print(f"Total: {len(messages)} messages\n")
        for m in messages:
            date_str = m.date.strftime("%Y-%m-%d %H:%M") if m.date else "—"
            who = "you" if m.out else (m.sender_name or m.sender_id or "?")
            text = (m.text or "").strip()
            preview = text[:200].replace("\n", " ")
            if len(text) > 200:
                preview += "..."
            media_suffix = f" [{m.media_type}]" if m.media_type else ""
            forward_suffix = " (forward)" if m.is_forward else ""
            print(f"[{date_str}] #{m.id} {who}{forward_suffix}")
            print(f"  {preview}{media_suffix}")
            print()


async def _dialogs_read(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    try:
        result = await TelegramActionService(pool).read_history(
            phone=phone,
            chat_id=args.chat_id,
            limit=args.limit,
            offset_id=args.offset_id,
        )
        _print_dm_messages(result.messages, args.format)
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable.")
    except Exception as exc:
        cli_error(f"Error reading history: {exc}")


async def _dialogs_archive_history(args, db, pool) -> None:
    """Backfill full DM history of an account into the dm_messages archive (#1453)."""
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    chat_ids: set[int] | None = None
    if args.chat_id:
        try:
            chat_ids = {int(args.chat_id)}
        except ValueError:
            cli_error(
                f"Invalid --chat-id: {args.chat_id!r} — expected a numeric Telegram chat id."
            )
    stats = await backfill_account(pool, db, phone, chat_ids=chat_ids)
    incoming, outgoing = await db.repos.dm_messages.count_by_direction(phone)
    print(
        f"Archive for {phone}: dialogs={stats['dialogs']} "
        f"archived_now={stats['archived']} errors={stats['errors']} | "
        f"total incoming={incoming} outgoing={outgoing}"
    )
    if stats.get("incomplete"):
        print(
            "ПРОГОН НЕ ЗАВЕРШЁН: насыщен history-гейт или не сошёлся свежий "
            "снимок диалогов. Запусти команду ещё раз — бэкфилл продолжится "
            "с курсоров архива."
        )


async def _dialogs_export_history(args, db, pool) -> None:
    """Telegram-Desktop-style live history export to a JSON file (#1855)."""
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    identifiers = parse_identifiers(args.chat_ids)
    if not identifiers:
        cli_error("No chat identifiers given.")
    try:
        report = await export_chat_history(
            TelegramActionService(pool),
            phone=phone,
            chat_identifiers=identifiers,
            output_path=args.output,
            resume=args.resume,
            limit=args.limit,
            offset_date=args.offset_date,
        )
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable.")
    except ValueError as exc:
        cli_error(str(exc))
    except Exception as exc:
        cli_error(f"Error exporting history: {exc}")
        return
    print(
        f"Exported {report.chats_processed} chat(s), "
        f"skipped {report.chats_skipped} (already exported), "
        f"{report.messages_exported} messages -> {report.output_path}"
    )


async def _dialogs_participants(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    try:
        result = await TelegramActionService(pool).get_participants(
            phone=phone,
            chat_id=args.chat_id,
            limit=args.limit,
            search=args.search,
        )
        participants = result.participants
        if not participants:
            print("No participants found.")
            return
        fmt = "{:<12} {:<25} {:<25} {:<25}"
        print(fmt.format("ID", "First name", "Last name", "Username"))
        print("-" * 90)
        for participant in participants:
            print(
                fmt.format(
                    str(participant.id),
                    (getattr(participant, "first_name", None) or "")[:25],
                    (getattr(participant, "last_name", None) or "")[:25],
                    ("@" + participant.username if getattr(participant, "username", None) else "")[:25],
                )
            )
        print(f"\nTotal: {len(participants)}")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable.")
    except Exception as exc:
        cli_error(f"Error fetching participants: {exc}")


async def _dialogs_edit_admin(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    if not _confirm_or_abort(args, f"Edit admin rights for {args.user_id} in {args.chat_id}"):
        return
    try:
        await TelegramActionService(pool).edit_admin(
            phone=phone,
            chat_id=args.chat_id,
            user_id=args.user_id,
            is_admin=args.is_admin,
            title=args.title or None,
        )
        print(f"Admin rights updated for {args.user_id}.")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable.")
    except Exception as exc:
        cli_error(f"Error editing admin: {exc}")


def _parse_permission_bool(value: str | bool | None, option: str) -> bool | None:
    """Reject ambiguous permission values instead of silently denying rights."""
    if value is None or isinstance(value, bool):
        return value
    normalized = value.lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise typer.BadParameter("expected true/false, yes/no, 1/0 or on/off", param_hint=option)


async def _dialogs_edit_permissions(args, db, pool) -> None:
    send_messages = _parse_permission_bool(args.send_messages, "--send-messages")
    send_media = _parse_permission_bool(args.send_media, "--send-media")
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    if not _confirm_or_abort(args, f"Edit permissions for {args.user_id} in {args.chat_id}"):
        return
    if args.send_messages is None and args.send_media is None:
        # Validated before the try so cli_error's typer.Exit is not swallowed
        # by the broad handler below (#1486).
        cli_error("Error: specify at least one flag (--send-messages or --send-media).")
    try:
        until_date = None
        if args.until_date:
            until_date = parse_required_datetime(args.until_date)
        await TelegramActionService(pool).edit_permissions(
            phone=phone,
            chat_id=args.chat_id,
            user_id=args.user_id,
            until_date=until_date,
            send_messages=send_messages,
            send_media=send_media,
        )
        print(f"Permissions updated for {args.user_id}.")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable.")
    except Exception as exc:
        cli_error(f"Error editing permissions: {exc}")


async def _dialogs_kick(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    if not _confirm_or_abort(args, f"Kick {args.user_id} from {args.chat_id}"):
        return
    try:
        await TelegramActionService(pool).kick_participant(
            phone=phone,
            chat_id=args.chat_id,
            user_id=args.user_id,
        )
        print(f"{args.user_id} kicked from {args.chat_id}.")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable.")
    except Exception as exc:
        cli_error(f"Error kicking participant: {exc}")


async def _dialogs_broadcast_stats(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    try:
        result = await TelegramActionService(pool).get_broadcast_stats(
            phone=phone,
            chat_id=args.chat_id,
        )
        stats = result.stats
        print(f"Broadcast stats for {args.chat_id}:")
        for attr in BROADCAST_STAT_FIELDS:
            val = getattr(stats, attr, None)
            if val is not None:
                current = getattr(val, "current", None)
                previous = getattr(val, "previous", None)
                if current is not None:
                    print(f"  {attr}: {current} (prev: {previous})")
                else:
                    print(f"  {attr}: {val}")
        period = getattr(stats, "period", None)
        if period is not None:
            min_date = getattr(period, "min_date", None)
            max_date = getattr(period, "max_date", None)
            print(f"  period: {min_date} — {max_date}")
        enabled_notifications = getattr(stats, "enabled_notifications", None)
        if enabled_notifications is not None:
            print(f"  enabled_notifications: {enabled_notifications}")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable.")
    except Exception as exc:
        cli_error(f"Error fetching broadcast stats: {exc}")


async def _dialogs_archive(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    try:
        await TelegramActionService(pool).set_dialog_folder(
            phone=phone,
            chat_id=args.chat_id,
            folder_id=1,
        )
        print(f"{args.chat_id} archived.")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable.")
    except Exception as exc:
        cli_error(f"Error archiving: {exc}")


async def _dialogs_unarchive(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    try:
        await TelegramActionService(pool).set_dialog_folder(
            phone=phone,
            chat_id=args.chat_id,
            folder_id=0,
        )
        print(f"{args.chat_id} unarchived.")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable.")
    except Exception as exc:
        cli_error(f"Error unarchiving: {exc}")


async def _dialogs_mark_read(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    try:
        await TelegramActionService(pool).mark_read(
            phone=phone,
            chat_id=args.chat_id,
            max_id=args.max_id,
        )
        print(f"Messages marked as read in {args.chat_id}.")
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable.")
    except Exception as exc:
        cli_error(f"Error marking messages as read: {exc}")


async def _dialogs_cache_clear(args, db, pool) -> None:
    phone = getattr(args, "phone", None)
    if phone:
        pool.invalidate_dialogs_cache(phone)
        await db.repos.dialog_cache.clear_dialogs(phone)
        print(f"Cache cleared for {phone}.")
    else:
        pool.invalidate_dialogs_cache()
        await db.repos.dialog_cache.clear_all_dialogs()
        print("Cache cleared for all accounts.")


async def _dialogs_create_channel(args, db, pool) -> None:
    phone = await _resolve_phone(db, pool, args)
    if phone is None:
        return
    is_group = args.dialogs_action == "create-group"
    noun = "group" if is_group else "channel"
    username = "" if is_group else args.username or ""
    try:
        result = await TelegramActionService(pool).create_channel(
            phone=phone,
            title=args.title,
            about=args.about or "",
            username=username,
            broadcast=not is_group,
            megagroup=is_group,
        )
    except TelegramActionClientUnavailableError:
        cli_error(f"Client for {phone} unavailable.")
    except RuntimeError as exc:
        if "Telegram returned empty response" in str(exc):
            cli_error(f"Error: Telegram returned empty response — {noun} may not have been created.")
        raise
    print(f"Created {noun} id={result.channel_id} title={args.title!r}")
    if username:
        if result.channel_username == username:
            print(f"Username set: @{result.channel_username}")
        elif result.username_error:
            print(f"Could not set username: {result.username_error}")
    if result.channel_username:
        print(f"Channel link: {result.invite_link}")


async def _dialogs_cache_status(args, db, pool) -> None:
    phones = await db.repos.dialog_cache.get_all_phones()
    now_monotonic = time.monotonic()
    # pool is None while a daemon owns the sessions (#1521): only the DB cache
    # is visible then — this process's in-memory cache would be cold anyway.
    mem_cache = pool._dialogs_cache if pool is not None else {}
    mem_ttl = pool._dialogs_cache_ttl_sec if pool is not None else 0.0

    if not phones:
        in_memory_phones = {key[0] for key in mem_cache}
        if not in_memory_phones:
            print("No cached dialogs.")
            return
        phones = sorted(in_memory_phones)

    fmt = "{:<20} {:<10} {:<28} {:<10}"
    print(fmt.format("Account", "DB entries", "DB cached at", "Mem entries"))
    print("-" * 72)
    for phone in sorted(set(phones) | {key[0] for key in mem_cache}):
        db_count = await db.repos.dialog_cache.count_dialogs(phone)
        cached_at = await db.repos.dialog_cache.get_cached_at(phone)
        cached_at_str = (
            cached_at.strftime("%Y-%m-%d %H:%M:%S UTC") if cached_at else "-"
        )
        entry = mem_cache.get((phone, "full"))
        mem_entries = (
            len(entry.dialogs)
            if entry
            and (now_monotonic - entry.fetched_at_monotonic) <= mem_ttl
            else 0
        )
        print(fmt.format(phone, str(db_count), cached_at_str, str(mem_entries)))


async def _dialogs_queue(args, db, pool) -> None:
    service = TelegramCommandService(db)
    if args.queue_action == "status":
        status_value = None
        summary = await service.summary(
            command_type=args.command_type or None,
            phone=args.phone or None,
            status=status_value,
        )
        total = sum(summary.values())
        print(
            f"Total: {total} | "
            f"pending: {summary.get(TelegramCommandStatus.PENDING, 0)} | "
            f"running: {summary.get(TelegramCommandStatus.RUNNING, 0)} | "
            f"succeeded: {summary.get(TelegramCommandStatus.SUCCEEDED, 0)} | "
            f"failed: {summary.get(TelegramCommandStatus.FAILED, 0)} | "
            f"cancelled: {summary.get(TelegramCommandStatus.CANCELLED, 0)}"
        )
        limit = max(1, min(int(args.limit or 20), 100))
        commands = await service.list(
            command_type=args.command_type or None,
            phone=args.phone or None,
            limit=limit,
        )
        if not commands:
            print("No commands in queue.")
        else:
            for cmd in commands:
                payload_phone = cmd.payload.get("phone") if isinstance(cmd.payload, dict) else None
                run_after = cmd.run_after.isoformat() if cmd.run_after else "-"
                print(
                    f"#{cmd.id} {cmd.command_type} status={cmd.status.value} "
                    # «unassigned», а не '-': команда без аккаунта в payload —
                    # наблюдаемое состояние, а не пустая клетка (#1581).
                    f"phone={payload_phone or 'unassigned'} run_after={run_after}"
                )
    elif args.queue_action == "cancel":
        if not _confirm_or_abort(args, f"Cancel queue command #{args.command_id}"):
            return
        ok = await service.cancel(int(args.command_id))
        if ok:
            print(f"Command #{args.command_id} cancelled.")
        else:
            print(
                f"Command #{args.command_id} not found or not pending "
                f"(only PENDING commands can be cancelled)."
            )
    elif args.queue_action == "clear-pending":
        filt = []
        if args.command_type:
            filt.append(f"type={args.command_type}")
        if args.phone:
            filt.append(f"phone={args.phone}")
        scope = ", ".join(filt) if filt else "ALL types and accounts"
        if not _confirm_or_abort(args, f"Bulk-cancel pending commands ({scope})"):
            return
        cancelled = await service.cancel_pending(
            command_type=args.command_type or None,
            phone=args.phone or None,
        )
        print(f"Cancelled pending commands: {cancelled}.")


# Typer options declared `str | None` (free-text "true/false": they need a third
# "not specified" state a paired boolean flag cannot express). Validated up front
# so a malformed value fails before any connection is made.
_PERMISSION_BOOL_FIELDS = frozenset({"send_messages", "send_media"})
# DB-only views: never worth a Telegram connection.
_POOL_FREE_ACTIONS = frozenset({"queue", "cache-status"})


# action -> (handler, needs_channel_service_cls). Depth-2 `queue` dispatches on
# args.queue_action inside its own handler.
_DIALOGS_HANDLERS: dict[str, tuple[Callable[..., Awaitable[None]], bool]] = {
    "refresh": (_dialogs_refresh, True),
    "list": (_dialogs_list, True),
    "resolve": (_dialogs_resolve, False),
    "leave": (_dialogs_leave, True),
    "delete": (_dialogs_delete, True),
    "join": (_dialogs_join, False),
    "topics": (_dialogs_topics, False),
    "send": (_dialogs_send, False),
    "forward": (_dialogs_forward, False),
    "edit-message": (_dialogs_edit_message, False),
    "edit-message-media": (_dialogs_edit_message_media, False),
    "draft-save": (_dialogs_draft_save, False),
    "draft-list": (_dialogs_draft_list, False),
    "delete-message": (_dialogs_delete_message, False),
    "react": (_dialogs_react, False),
    "pin-message": (_dialogs_pin_message, False),
    "unpin-message": (_dialogs_unpin_message, False),
    "download-media": (_dialogs_download_media, False),
    "transcribe": (_dialogs_transcribe, False),
    "read": (_dialogs_read, False),
    "export-history": (_dialogs_export_history, False),
    "archive-history": (_dialogs_archive_history, False),
    "participants": (_dialogs_participants, False),
    "edit-admin": (_dialogs_edit_admin, False),
    "edit-permissions": (_dialogs_edit_permissions, False),
    "kick": (_dialogs_kick, False),
    "broadcast-stats": (_dialogs_broadcast_stats, False),
    "archive": (_dialogs_archive, False),
    "unarchive": (_dialogs_unarchive, False),
    "mark-read": (_dialogs_mark_read, False),
    "cache-clear": (_dialogs_cache_clear, False),
    "create-channel": (_dialogs_create_channel, False),
    "create-group": (_dialogs_create_channel, False),
    "cache-status": (_dialogs_cache_status, False),
    "queue": (_dialogs_queue, False),
}


async def _dispatch(
    args: argparse.Namespace,
    *,
    runtime_mod=runtime,
    channel_service_cls=ChannelService,
) -> None:
    """Shared async body for every ``dialogs`` action (incl. nested ``queue``).

    Eagerly opens the db + client pool, dispatches on ``args.dialogs_action``
    (and ``args.queue_action`` for the depth-2 ``queue`` group), and always
    disconnects the pool + closes the db in ``finally``. Called by the argparse
    ``run_with_dependencies`` wrapper (which owns its own ``asyncio.run``) and,
    via ``run_async``, by the Typer command bodies in ``typer_commands.py`` —
    so the two entry points execute byte-identical logic.
    """
    if args.dialogs_action == "edit-permissions":
        for field in _PERMISSION_BOOL_FIELDS:
            _parse_permission_bool(getattr(args, field, None), "--" + field.replace("_", "-"))
    config, db = await runtime_mod.init_db(args.config)

    if args.dialogs_action in _POOL_FREE_ACTIONS:
        try:
            handler, _needs_channel_service = _DIALOGS_HANDLERS[args.dialogs_action]
            await handler(args, db, None)
        finally:
            await db.close()
        return

    # With a live daemon the pool runs on the daemon's connections (#1868).
    try:
        _, pool = await runtime_mod.init_pool(config, db)
    except BaseException:
        await db.close()
        raise
    try:
        entry = _DIALOGS_HANDLERS.get(args.dialogs_action)
        if entry is not None:
            handler, needs_channel_service = entry
            if needs_channel_service:
                await handler(args, db, pool, channel_service_cls=channel_service_cls)
            else:
                await handler(args, db, pool)
    finally:
        await pool.disconnect_all()
        await db.close()


def run_with_dependencies(
    args: argparse.Namespace,
    *,
    runtime_mod=runtime,
    channel_service_cls=ChannelService,
) -> None:
    """Run one ``dialogs`` action through :func:`_dispatch` with its own loop.

    The argparse entry point — owns the single ``asyncio.run`` per process. Kept
    as the test seam: the dialogs command tests patch ``runtime.init_db`` /
    ``init_pool`` and call this directly (some inject ``runtime_mod`` /
    ``channel_service_cls``).
    """
    asyncio.run(
        _dispatch(args, runtime_mod=runtime_mod, channel_service_cls=channel_service_cls)
    )


def run(args: argparse.Namespace) -> None:
    """Primary CLI entrypoint for Telegram dialogs management."""
    run_with_dependencies(args)


# --------------------------------------------------------------------------- #
# dialogs → list / refresh / resolve / leave / join / topics / cache-clear /
#   cache-status / send / forward / edit-message / delete-message /
#   create-channel / create-group / pin-message / react / unpin-message /
#   download-media / participants / edit-admin / edit-permissions / kick /
#   broadcast-stats / archive / unarchive / mark-read /
#   queue (NESTED depth-2: status/cancel/clear-pending)
#
# Every dialogs leaf reuses the shared async ``_dispatch`` body by
# building the argparse Namespace it dispatches on — so the Typer path executes
# the exact same (heavily tested) logic, including the mutating-command
# ``--yes`` confirmation flow and the single pool-disconnect/db-close finally.
# --------------------------------------------------------------------------- #

dialogs_app = typer.Typer(no_args_is_help=True, help="Telegram dialogs management")

# Nested depth-2 group: ``dialogs queue`` mounted via add_typer; the frozen
# ``dialogs queue <action>`` paths are the fragile Wave-4 invariant.
dialogs_queue_app = typer.Typer(
    no_args_is_help=True,
    help="Inspect and manage the Telegram command queue (reactions, sends, forwards, ...)",
)
dialogs_app.add_typer(dialogs_queue_app, name="queue")


def _run_dialogs(ctx: typer.Context, dialogs_action: str, **ns_kwargs) -> None:
    """Build the argparse Namespace a dialogs action dispatches on, then run it.

    Centralises the apply_startup → Namespace → ``_dispatch`` bridge so each leaf
    stays a thin type-hinted signature. ``ns_kwargs`` carries exactly the
    attributes the matching ``_dispatch`` branch reads off ``args``.
    """
    apply_startup(ctx)
    ns = argparse.Namespace(
        config=ctx.obj.config,
        dialogs_action=dialogs_action,
        **ns_kwargs,
    )
    run_async(_dispatch(ns))


@dialogs_app.command("list")
def dialogs_list(
    ctx: typer.Context,
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
) -> None:
    """List all dialogs for an account."""
    _run_dialogs(ctx, "list", phone=phone)


@dialogs_app.command("refresh")
def dialogs_refresh(
    ctx: typer.Context,
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
) -> None:
    """Refresh dialog cache from Telegram."""
    _run_dialogs(ctx, "refresh", phone=phone)


@dialogs_app.command("resolve", context_settings=_NEG_ID_POSITIONAL)
def dialogs_resolve(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Identifier to resolve"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
) -> None:
    """Resolve @username, t.me link, or numeric ID."""
    _run_dialogs(ctx, "resolve", identifier=identifier, phone=phone)


@dialogs_app.command("leave", context_settings=_NEG_ID_POSITIONAL)
def dialogs_leave(
    ctx: typer.Context,
    dialog_ids: list[str] = typer.Argument(..., help="Dialog IDs to leave (space- or comma-separated)"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Leave dialogs by ID."""
    _run_dialogs(ctx, "leave", dialog_ids=dialog_ids, phone=phone, yes=yes)


@dialogs_app.command("delete", context_settings=_NEG_ID_POSITIONAL)
def dialogs_delete(
    ctx: typer.Context,
    dialog_ids: list[str] = typer.Argument(..., help="Dialog IDs to delete (space- or comma-separated)"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Permanently delete dialogs by ID (DeleteChannel/DeleteChat)."""
    _run_dialogs(ctx, "delete", dialog_ids=dialog_ids, phone=phone, yes=yes)


@dialogs_app.command("join", context_settings=_NEG_ID_POSITIONAL)
def dialogs_join(
    ctx: typer.Context,
    target: str = typer.Argument(..., help="@username, t.me link, or invite link"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Join/subscribe to a channel or group."""
    _run_dialogs(ctx, "join", target=target, phone=phone, yes=yes)


@dialogs_app.command("topics")
def dialogs_topics(
    ctx: typer.Context,
    channel_id: int = typer.Option(..., "--channel-id", help="Channel ID to fetch forum topics for"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: any available)"),
) -> None:
    """List forum topics for a channel."""
    _run_dialogs(ctx, "topics", channel_id=channel_id, phone=phone)


@dialogs_app.command("cache-clear")
def dialogs_cache_clear(
    ctx: typer.Context,
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: all accounts)"),
) -> None:
    """Clear in-memory and DB dialog cache."""
    _run_dialogs(ctx, "cache-clear", phone=phone)


@dialogs_app.command("cache-status")
def dialogs_cache_status(ctx: typer.Context) -> None:
    """Show dialog cache status (entries, age)."""
    _run_dialogs(ctx, "cache-status")


@dialogs_app.command("send", context_settings=_NEG_ID_POSITIONAL)
def dialogs_send(
    ctx: typer.Context,
    recipient: str = typer.Argument(..., help="Recipient: @username, phone number, or numeric ID"),
    text: str = typer.Argument(..., help="Message text to send"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    silent: bool = typer.Option(False, "--silent", help="Send without a notification sound"),
    comment_to: int | None = typer.Option(
        None, "--comment-to", help="Comment on this channel post ID instead of sending a plain message"
    ),
    reply_to: int | None = typer.Option(
        None, "--reply-to", help="Reply to this message ID within the same chat"
    ),
    parse_mode: str | None = typer.Option(
        None, "--parse-mode", help="Text markup: plain, md or html (default: md)"
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Send a direct message to a user or chat."""
    _run_dialogs(
        ctx, "send", recipient=recipient, text=text, phone=phone, silent=silent,
        comment_to=comment_to, reply_to=reply_to, parse_mode=parse_mode, yes=yes,
    )


@dialogs_app.command("forward", context_settings=_NEG_ID_POSITIONAL)
def dialogs_forward(
    ctx: typer.Context,
    from_chat: str = typer.Argument(..., help="Source chat ID or @username"),
    to_chat: str = typer.Argument(..., help="Destination chat ID or @username"),
    message_ids: list[str] = typer.Argument(..., help="Message IDs to forward (space or comma-separated)"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Forward messages between chats."""
    _run_dialogs(
        ctx, "forward", from_chat=from_chat, to_chat=to_chat, message_ids=message_ids, phone=phone, yes=yes
    )


@dialogs_app.command("edit-message", context_settings=_NEG_ID_POSITIONAL)
def dialogs_edit_message(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    message_id: int = typer.Argument(..., help="Message ID to edit"),
    text: str = typer.Argument(..., help="New message text"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Edit a sent message."""
    _run_dialogs(ctx, "edit-message", chat_id=chat_id, message_id=message_id, text=text, phone=phone, yes=yes)


@dialogs_app.command("edit-message-media", context_settings=_NEG_ID_POSITIONAL)
def dialogs_edit_message_media(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    message_id: int = typer.Argument(..., help="Message ID to replace the media of"),
    file: str = typer.Argument(..., help="Path to the replacement media file on this machine"),
    text: str | None = typer.Option(
        None, "--text", help="New caption (omit to keep the current caption and formatting)"
    ),
    parse_mode: str | None = typer.Option(None, "--parse-mode", help="Caption format for --text: md | html | plain"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Replace the media attachment of a sent message (no repost, metrics kept)."""
    _run_dialogs(
        ctx, "edit-message-media", chat_id=chat_id, message_id=message_id, file=file,
        text=text, parse_mode=parse_mode, phone=phone, yes=yes,
    )


@dialogs_app.command("draft-save", context_settings=_NEG_ID_POSITIONAL)
def dialogs_draft_save(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    text: str = typer.Argument(..., help="Draft text (markup per --parse-mode)"),
    parse_mode: str | None = typer.Option(
        None, "--parse-mode", help="md | html | plain (default: plain; html is the only way to get underline)"
    ),
    reply_to: int | None = typer.Option(None, "--reply-to", help="Message ID the draft replies to"),
    no_webpage: bool = typer.Option(False, "--no-webpage", help="Disable link preview in the draft"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
) -> None:
    """Save a message draft (optionally formatted) to a chat's input box."""
    _run_dialogs(
        ctx, "draft-save", chat_id=chat_id, text=text, parse_mode=parse_mode,
        reply_to=reply_to, no_webpage=no_webpage, phone=phone,
    )


@dialogs_app.command("draft-list")
def dialogs_draft_list(
    ctx: typer.Context,
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    output_format: OutputFormat = typer.Option(OutputFormat.text, "--format", help="Output format (default: text)"),
) -> None:
    """List all drafts for the account (formatted drafts include message_html)."""
    _run_dialogs(ctx, "draft-list", phone=phone, format=output_format.value)


@dialogs_app.command("delete-message", context_settings=_NEG_ID_POSITIONAL)
def dialogs_delete_message(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    message_ids: list[str] = typer.Argument(..., help="Message IDs to delete (space or comma-separated)"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Delete messages from a chat."""
    _run_dialogs(ctx, "delete-message", chat_id=chat_id, message_ids=message_ids, phone=phone, yes=yes)


@dialogs_app.command("create-channel")
def dialogs_create_channel(
    ctx: typer.Context,
    title: str = typer.Option(..., "--title", help="Channel title"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    about: str = typer.Option("", "--about", help="Channel description"),
    username: str = typer.Option("", "--username", help="Public username (leave empty for private)"),
) -> None:
    """Create a new Telegram broadcast channel."""
    _run_dialogs(ctx, "create-channel", title=title, phone=phone, about=about, username=username)


@dialogs_app.command("create-group")
def dialogs_create_group(
    ctx: typer.Context,
    title: str = typer.Option(..., "--title", help="Group title"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    about: str = typer.Option("", "--about", help="Group description"),
) -> None:
    """Create a new Telegram group."""
    _run_dialogs(ctx, "create-group", title=title, phone=phone, about=about)


@dialogs_app.command("pin-message", context_settings=_NEG_ID_POSITIONAL)
def dialogs_pin_message(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    message_id: int = typer.Argument(..., help="Message ID to pin"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    notify: bool = typer.Option(False, "--notify", help="Notify members about pinned message"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Pin a message in a chat."""
    _run_dialogs(ctx, "pin-message", chat_id=chat_id, message_id=message_id, phone=phone, notify=notify, yes=yes)


@dialogs_app.command("react", context_settings=_NEG_ID_POSITIONAL)
def dialogs_react(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    message_id: int = typer.Argument(..., help="Message ID to react on"),
    emoji: str | None = typer.Argument(None, help="Reaction emoji to set; required unless --clear is used"),
    clear: bool = typer.Option(False, "--clear", help="Remove your reaction from the message"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Set or clear your reaction on a message."""
    _run_dialogs(ctx, "react", chat_id=chat_id, message_id=message_id, emoji=emoji, clear=clear, phone=phone, yes=yes)


@dialogs_app.command("unpin-message", context_settings=_NEG_ID_POSITIONAL)
def dialogs_unpin_message(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    message_id: int | None = typer.Option(None, "--message-id", help="Message ID to unpin (omit to unpin all)"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Unpin a message in a chat."""
    _run_dialogs(ctx, "unpin-message", chat_id=chat_id, message_id=message_id, phone=phone, yes=yes)


@dialogs_app.command("download-media", context_settings=_NEG_ID_POSITIONAL)
def dialogs_download_media(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    message_id: int = typer.Argument(..., help="Message ID containing media"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    output_dir: str = typer.Option(".", "--output-dir", help="Directory to save file (default: current dir)"),
) -> None:
    """Download media from a message."""
    _run_dialogs(ctx, "download-media", chat_id=chat_id, message_id=message_id, phone=phone, output_dir=output_dir)


@dialogs_app.command("transcribe", context_settings=_NEG_ID_POSITIONAL)
def dialogs_transcribe(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    message_id: int = typer.Argument(..., help="Message ID with the voice note / video note"),
    engine: str | None = typer.Option(
        None,
        "--engine",
        help="telegram (native Premium) | groq | openai | whisper (default: TELEGRAM_TRANSCRIBE_ENGINE or groq)",
    ),
    phone: str | None = typer.Option(
        None, "--phone", help="Account phone (default: primary; telegram engine: premium lease)"
    ),
) -> None:
    """Transcribe a voice note or video note into text (cached in the DB)."""
    _run_dialogs(ctx, "transcribe", chat_id=chat_id, message_id=message_id, engine=engine, phone=phone)


@dialogs_app.command("read", context_settings=_NEG_ID_POSITIONAL)
def dialogs_read(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    limit: int = typer.Option(50, "--limit", help="Max messages to fetch (default: 50)"),
    offset_id: int = typer.Option(0, "--offset-id", help="Fetch messages older than this message ID"),
    output_format: OutputFormat = typer.Option(OutputFormat.text, "--format", help="Output format (default: text)"),
) -> None:
    """Read live message history from a dialog (DM/group/channel), oldest first."""
    _run_dialogs(
        ctx, "read", chat_id=chat_id, phone=phone, limit=limit, offset_id=offset_id, format=output_format.value
    )


@dialogs_app.command("archive-history")
def dialogs_archive_history(
    ctx: typer.Context,
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    chat_id: str | None = typer.Option(None, "--chat-id", help="Backfill a single dialog by user ID"),
) -> None:
    """Backfill full DM history (both directions) into the local archive.

    Reads every personal dialog of the account page by page and stores both
    incoming and outgoing messages in the dm_messages table (no TTL).
    Resumable: re-running skips already-archived history. Next to a running
    daemon it reads on the daemon's connection (#1868).
    """
    _run_dialogs(ctx, "archive-history", phone=phone, chat_id=chat_id)


@dialogs_app.command("export-history", context_settings=_NEG_ID_POSITIONAL)
def dialogs_export_history(
    ctx: typer.Context,
    chat_ids: str = typer.Argument(..., help="Chat ID(s)/@username/t.me link — several separated by comma"),
    output: str = typer.Option(..., "--output", "-o", help="Output JSON file path"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    limit: int | None = typer.Option(None, "--limit", help="Max messages per chat (default: full history)"),
    offset_date: str | None = typer.Option(
        None, "--offset-date", help="Only messages before this ISO date (YYYY-MM-DD[THH:MM:SS])"
    ),
    resume: bool = typer.Option(
        True, "--resume/--no-resume", help="Skip chats already present in the output file (default: resume)"
    ),
) -> None:
    """Export live chat history to a structured JSON file (read-only).

    Walks each chat newest→oldest in 100-message pages through the shared
    read_history action and writes {"chats": {...}} keyed by dialog id.
    The output file doubles as resume state — re-run to skip exported chats.
    Next to a running daemon it reads on the daemon's connection (#1868);
    otherwise it connects the account itself.
    """
    _run_dialogs(
        ctx, "export-history", chat_ids=chat_ids, output=output, phone=phone,
        limit=limit, offset_date=offset_date, resume=resume,
    )


@dialogs_app.command("participants", context_settings=_NEG_ID_POSITIONAL)
def dialogs_participants(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    limit: int = typer.Option(200, "--limit", help="Max participants to fetch (default: 200)"),
    search: str = typer.Option("", "--search", help="Search query to filter participants"),
) -> None:
    """List participants of a channel/group."""
    _run_dialogs(ctx, "participants", chat_id=chat_id, phone=phone, limit=limit, search=search)


@dialogs_app.command("edit-admin", context_settings=_NEG_ID_POSITIONAL)
def dialogs_edit_admin(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    user_id: str = typer.Argument(..., help="User ID or @username to change admin rights for"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    title: str | None = typer.Option(None, "--title", help="Custom admin title"),
    is_admin: bool = typer.Option(True, "--is-admin/--no-admin", help="Promote to admin (default) / demote"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Promote or demote a user as admin."""
    _run_dialogs(
        ctx, "edit-admin", chat_id=chat_id, user_id=user_id, phone=phone,
        title=title, is_admin=is_admin, yes=yes,
    )


@dialogs_app.command("edit-permissions", context_settings=_NEG_ID_POSITIONAL)
def dialogs_edit_permissions(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    user_id: str = typer.Argument(..., help="User ID or @username"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    until_date: str | None = typer.Option(
        None, "--until-date", help="Restriction end date (ISO format, e.g. 2025-12-31)"
    ),
    send_messages: str | None = typer.Option(
        None, "--send-messages", help="Allow sending messages (true/false, yes/no, 1/0, on/off; case-insensitive)"
    ),
    send_media: str | None = typer.Option(
        None, "--send-media", help="Allow sending media (true/false, yes/no, 1/0, on/off; case-insensitive)"
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Restrict or unrestrict a user in a group."""
    _run_dialogs(
        ctx, "edit-permissions", chat_id=chat_id, user_id=user_id, phone=phone,
        until_date=until_date,
        send_messages=_parse_permission_bool(send_messages, "--send-messages"),
        send_media=_parse_permission_bool(send_media, "--send-media"),
        yes=yes,
    )


@dialogs_app.command("kick", context_settings=_NEG_ID_POSITIONAL)
def dialogs_kick(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    user_id: str = typer.Argument(..., help="User ID or @username to kick"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Kick a participant from a chat."""
    _run_dialogs(ctx, "kick", chat_id=chat_id, user_id=user_id, phone=phone, yes=yes)


@dialogs_app.command("broadcast-stats", context_settings=_NEG_ID_POSITIONAL)
def dialogs_broadcast_stats(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Channel ID or @username"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
) -> None:
    """Get broadcast statistics for a channel."""
    _run_dialogs(ctx, "broadcast-stats", chat_id=chat_id, phone=phone)


@dialogs_app.command("archive", context_settings=_NEG_ID_POSITIONAL)
def dialogs_archive(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
) -> None:
    """Archive a dialog (move to archive folder)."""
    _run_dialogs(ctx, "archive", chat_id=chat_id, phone=phone)


@dialogs_app.command("unarchive", context_settings=_NEG_ID_POSITIONAL)
def dialogs_unarchive(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
) -> None:
    """Unarchive a dialog (move to main folder)."""
    _run_dialogs(ctx, "unarchive", chat_id=chat_id, phone=phone)


@dialogs_app.command("mark-read", context_settings=_NEG_ID_POSITIONAL)
def dialogs_mark_read(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: primary account)"),
    max_id: int | None = typer.Option(None, "--max-id", help="Mark messages up to this ID as read (default: all)"),
) -> None:
    """Mark messages as read in a chat."""
    _run_dialogs(ctx, "mark-read", chat_id=chat_id, phone=phone, max_id=max_id)


# ---- nested: dialogs queue <action> --------------------------------------- #


def _run_dialogs_queue(ctx: typer.Context, queue_action: str, **ns_kwargs) -> None:
    """Bridge for the nested ``dialogs queue`` group — sets ``dialogs_action=queue``."""
    apply_startup(ctx)
    ns = argparse.Namespace(
        config=ctx.obj.config, dialogs_action="queue", queue_action=queue_action, **ns_kwargs
    )
    run_async(_dispatch(ns))


@dialogs_queue_app.command("status")
def dialogs_queue_status(
    ctx: typer.Context,
    command_type: str | None = typer.Option(None, "--command-type", help="Filter by command type, e.g. dialogs.react"),
    phone: str | None = typer.Option(None, "--phone", help="Filter by account phone"),
    limit: int = typer.Option(20, "--limit", help="Recent entries to show (1-100)"),
) -> None:
    """Show pending/running queue status."""
    _run_dialogs_queue(ctx, "status", command_type=command_type, phone=phone, limit=limit)


@dialogs_queue_app.command("cancel")
def dialogs_queue_cancel(
    ctx: typer.Context,
    command_id: int = typer.Argument(..., help="Command id from queue status"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Cancel a pending queue command by id."""
    _run_dialogs_queue(ctx, "cancel", command_id=command_id, yes=yes)


@dialogs_queue_app.command("clear-pending")
def dialogs_queue_clear_pending(
    ctx: typer.Context,
    command_type: str | None = typer.Option(None, "--command-type", help="Filter by command type, e.g. dialogs.react"),
    phone: str | None = typer.Option(None, "--phone", help="Filter by account phone"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Bulk-cancel pending queue commands (optionally filtered)."""
    _run_dialogs_queue(ctx, "clear-pending", command_type=command_type, phone=phone, yes=yes)

