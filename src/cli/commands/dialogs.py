from __future__ import annotations

import argparse
import asyncio
import time

from src.cli import runtime
from src.models import TelegramCommandStatus
from src.services.channel_service import ChannelService
from src.services.telegram_actions import (
    BROADCAST_STAT_FIELDS,
    TelegramActionClientUnavailableError,
    TelegramActionMessageNotFoundError,
    TelegramActionNoMediaError,
    TelegramActionService,
)
from src.services.telegram_command_service import TelegramCommandService
from src.telegram.flood_wait import HandledFloodWaitError, is_blocking_flood_wait_until
from src.telegram.reactions import (
    SUPPORTED_REACTION_EMOJIS_DISPLAY,
    TelegramReactionInvalidError,
    normalize_outgoing_reaction_emoji,
)
from src.utils.datetime import parse_required_datetime


def run_with_dependencies(
    args: argparse.Namespace,
    *,
    runtime_mod=runtime,
    channel_service_cls=ChannelService,
) -> None:
    async def _run() -> None:
        config, db = await runtime_mod.init_db(args.config)
        _, pool = await runtime_mod.init_pool(config, db)
        try:
            if args.dialogs_action == "refresh":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return
                svc = channel_service_cls(db, pool, None)  # type: ignore[arg-type]
                dialogs = await svc.get_my_dialogs(phone, refresh=True)
                print(f"Dialogs refreshed: {len(dialogs)} total.")

            elif args.dialogs_action == "list":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
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

            elif args.dialogs_action == "resolve":
                try:
                    entity = await pool.resolve_any_entity(args.identifier, phone=args.phone)
                except RuntimeError as exc:
                    if "no_client" in str(exc):
                        print("No connected accounts.")
                        return
                    print(f"Error resolving entity: {exc}")
                    return
                except Exception as exc:
                    print(f"Error resolving entity: {exc}")
                    return
                if not entity:
                    print(f"Entity '{args.identifier}' not found.")
                    return
                print(f"Title: {entity['title']}")
                print(f"Type: {entity['channel_type']}")
                print(f"ID: {entity['channel_id']}")
                if entity.get("username"):
                    print(f"Username: @{entity['username']}")

            elif args.dialogs_action == "leave":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
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

                dialogs = [
                    (channel_id, type_map.get(channel_id, "channel" if channel_id < 0 else "dm"))
                    for channel_id in dialog_ids
                ]

                if not args.yes:
                    print(f"About to leave {len(dialogs)} dialog(s):")
                    for channel_id, channel_type in dialogs:
                        title = title_map.get(channel_id, str(channel_id))
                        print(f"  {channel_id}  {title}  ({channel_type})")
                    answer = input("Continue? [y/N] ").strip().lower()
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

            elif args.dialogs_action == "join":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return

                if not args.yes:
                    print(f"Join/subscribe account {phone} to {args.target}")
                    answer = input("Continue? [y/N] ").strip().lower()
                    if answer != "y":
                        print("Aborted.")
                        return

                try:
                    result = await TelegramActionService(pool).join_dialog(
                        phone=phone,
                        target=args.target,
                    )
                    mode = "invite" if result.via_invite else "public"
                    print(f"Joined/subscribed to {result.target} as {result.phone} ({mode}).")
                except TelegramActionClientUnavailableError:
                    print(f"Client for {phone} unavailable (flood-wait or not connected).")
                except Exception as exc:
                    print(f"Error joining channel/group: {exc}")

            elif args.dialogs_action == "topics":
                channel_id = args.channel_id
                topics = await pool.get_forum_topics(channel_id)
                if not topics:
                    topics = await db.get_forum_topics(channel_id)
                if not topics:
                    print(
                        f"No forum topics found for channel {channel_id}."
                        " The channel may not be a forum or is not accessible."
                    )
                    return
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

            elif args.dialogs_action == "send":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return
                recipient = args.recipient
                text = args.text

                if not args.yes:
                    print(f"Send message from {phone} to {recipient}:")
                    print(f"  {text[:200]}{'...' if len(text) > 200 else ''}")
                    answer = input("Continue? [y/N] ").strip().lower()
                    if answer != "y":
                        print("Aborted.")
                        return

                try:
                    result = await TelegramActionService(pool).send_message(
                        phone=phone,
                        recipient=recipient,
                        text=text,
                    )
                    message_id = f" message_id={result.message_id}" if result.message_id is not None else ""
                    print(f"Message sent to {recipient}.{message_id}")
                except TelegramActionClientUnavailableError:
                    print(f"Client for {phone} unavailable (flood-wait or not connected).")
                except Exception as exc:
                    print(f"Error sending message: {exc}")

            elif args.dialogs_action == "forward":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return
                raw_ids: list[str] = []
                for item in args.message_ids:
                    raw_ids.extend(part.strip() for part in item.split(",") if part.strip())
                ids = [int(raw) for raw in raw_ids if raw.isdigit()]
                if not ids:
                    print("No valid message IDs provided.")
                    return
                if not args.yes:
                    print(f"Forward {len(ids)} message(s) from {args.from_chat} to {args.to_chat}: {ids}")
                    answer = input("Continue? [y/N] ").strip().lower()
                    if answer != "y":
                        print("Aborted.")
                        return
                try:
                    fwd_result = await TelegramActionService(pool).forward_messages(
                        phone=phone,
                        from_chat=args.from_chat,
                        to_chat=args.to_chat,
                        message_ids=ids,
                    )
                    id_suffix = ""
                    if fwd_result.message_ids:
                        id_suffix = " forwarded_ids=" + ",".join(str(i) for i in fwd_result.message_ids)
                    print(f"Forwarded {len(ids)} message(s) from {args.from_chat} to {args.to_chat}.{id_suffix}")
                except TelegramActionClientUnavailableError:
                    print(f"Client for {phone} unavailable (flood-wait or not connected).")
                except Exception as exc:
                    print(f"Error forwarding messages: {exc}")

            elif args.dialogs_action == "edit-message":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return
                if not args.yes:
                    preview = args.text[:200] + ("..." if len(args.text) > 200 else "")
                    print(f"Edit message #{args.message_id} in {args.chat_id}:")
                    print(f"  {preview}")
                    answer = input("Continue? [y/N] ").strip().lower()
                    if answer != "y":
                        print("Aborted.")
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
                    print(f"Client for {phone} unavailable (flood-wait or not connected).")
                except Exception as exc:
                    print(f"Error editing message: {exc}")

            elif args.dialogs_action == "delete-message":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return
                raw_ids: list[str] = []
                for item in args.message_ids:
                    raw_ids.extend(part.strip() for part in item.split(",") if part.strip())
                ids = [int(raw) for raw in raw_ids if raw.isdigit()]
                if not ids:
                    print("No valid message IDs provided.")
                    return
                if not args.yes:
                    print(f"Delete {len(ids)} message(s) from {args.chat_id}: {ids}")
                    answer = input("Continue? [y/N] ").strip().lower()
                    if answer != "y":
                        print("Aborted.")
                        return
                try:
                    await TelegramActionService(pool).delete_messages(
                        phone=phone,
                        chat_id=args.chat_id,
                        message_ids=ids,
                    )
                    print(f"Deleted {len(ids)} message(s).")
                except TelegramActionClientUnavailableError:
                    print(f"Client for {phone} unavailable (flood-wait or not connected).")
                except Exception as exc:
                    print(f"Error deleting messages: {exc}")

            elif args.dialogs_action == "react":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
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
                    print(
                        f"Account {phone} is flood-waited until "
                        f"{acc.flood_wait_until.isoformat()}."
                    )
                    return
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
                        print(
                            "Error: Telegram does not support this reaction emoji. "
                            f"Supported reactions: {SUPPORTED_REACTION_EMOJIS_DISPLAY}"
                        )
                        return
                if not args.yes:
                    action = (
                        f"Clear reaction from message #{args.message_id} in {args.chat_id}"
                        if args.clear
                        else f"Send reaction {emoji!r} to message #{args.message_id} in {args.chat_id}"
                    )
                    print(action)
                    answer = input("Continue? [y/N] ").strip().lower()
                    if answer != "y":
                        print("Aborted.")
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
                    print(f"Client for {phone} unavailable (no lease could be acquired).")
                except Exception as exc:
                    print(f"Error sending reaction: {type(exc).__name__}: {exc}")

            elif args.dialogs_action == "pin-message":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return
                if not args.yes:
                    print(f"Pin message #{args.message_id} in {args.chat_id}")
                    answer = input("Continue? [y/N] ").strip().lower()
                    if answer != "y":
                        print("Aborted.")
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
                    print(f"Client for {phone} unavailable.")
                except Exception as exc:
                    print(f"Error pinning message: {exc}")

            elif args.dialogs_action == "unpin-message":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return
                if not args.yes:
                    target = f"#{args.message_id}" if args.message_id else "all messages"
                    print(f"Unpin {target} in {args.chat_id}")
                    answer = input("Continue? [y/N] ").strip().lower()
                    if answer != "y":
                        print("Aborted.")
                        return
                try:
                    await TelegramActionService(pool).unpin_message(
                        phone=phone,
                        chat_id=args.chat_id,
                        message_id=args.message_id,
                    )
                    print("Message(s) unpinned.")
                except TelegramActionClientUnavailableError:
                    print(f"Client for {phone} unavailable.")
                except Exception as exc:
                    print(f"Error unpinning message: {exc}")

            elif args.dialogs_action == "download-media":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
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
                    print(f"Client for {phone} unavailable.")
                except TelegramActionMessageNotFoundError:
                    print(f"Message #{args.message_id} not found.")
                except TelegramActionNoMediaError:
                    print("No media in this message.")
                except HandledFloodWaitError as exc:
                    print(f"Flood wait: {exc.info.detail}")
                except Exception as exc:
                    print(f"Error downloading media: {exc}")

            elif args.dialogs_action == "participants":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
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
                    print(f"Client for {phone} unavailable.")
                except Exception as exc:
                    print(f"Error fetching participants: {exc}")

            elif args.dialogs_action == "edit-admin":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return
                if not args.yes:
                    print(f"Edit admin rights for {args.user_id} in {args.chat_id}")
                    answer = input("Continue? [y/N] ").strip().lower()
                    if answer != "y":
                        print("Aborted.")
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
                    print(f"Client for {phone} unavailable.")
                except Exception as exc:
                    print(f"Error editing admin: {exc}")

            elif args.dialogs_action == "edit-permissions":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return
                if not args.yes:
                    print(f"Edit permissions for {args.user_id} in {args.chat_id}")
                    answer = input("Continue? [y/N] ").strip().lower()
                    if answer != "y":
                        print("Aborted.")
                        return
                try:
                    if args.send_messages is None and args.send_media is None:
                        print("Error: specify at least one flag (--send-messages or --send-media).")
                        return
                    until_date = None
                    if args.until_date:
                        until_date = parse_required_datetime(args.until_date)
                    await TelegramActionService(pool).edit_permissions(
                        phone=phone,
                        chat_id=args.chat_id,
                        user_id=args.user_id,
                        until_date=until_date,
                        send_messages=(
                            args.send_messages.lower() in ("1", "true", "on")
                            if args.send_messages is not None
                            else None
                        ),
                        send_media=(
                            args.send_media.lower() in ("1", "true", "on")
                            if args.send_media is not None
                            else None
                        ),
                    )
                    print(f"Permissions updated for {args.user_id}.")
                except TelegramActionClientUnavailableError:
                    print(f"Client for {phone} unavailable.")
                except Exception as exc:
                    print(f"Error editing permissions: {exc}")

            elif args.dialogs_action == "kick":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return
                if not args.yes:
                    print(f"Kick {args.user_id} from {args.chat_id}")
                    answer = input("Continue? [y/N] ").strip().lower()
                    if answer != "y":
                        print("Aborted.")
                        return
                try:
                    await TelegramActionService(pool).kick_participant(
                        phone=phone,
                        chat_id=args.chat_id,
                        user_id=args.user_id,
                    )
                    print(f"{args.user_id} kicked from {args.chat_id}.")
                except TelegramActionClientUnavailableError:
                    print(f"Client for {phone} unavailable.")
                except Exception as exc:
                    print(f"Error kicking participant: {exc}")

            elif args.dialogs_action == "broadcast-stats":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
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
                    print(f"Client for {phone} unavailable.")
                except Exception as exc:
                    print(f"Error fetching broadcast stats: {exc}")

            elif args.dialogs_action == "archive":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return
                try:
                    await TelegramActionService(pool).set_dialog_folder(
                        phone=phone,
                        chat_id=args.chat_id,
                        folder_id=1,
                    )
                    print(f"{args.chat_id} archived.")
                except TelegramActionClientUnavailableError:
                    print(f"Client for {phone} unavailable.")
                except Exception as exc:
                    print(f"Error archiving: {exc}")

            elif args.dialogs_action == "unarchive":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return
                try:
                    await TelegramActionService(pool).set_dialog_folder(
                        phone=phone,
                        chat_id=args.chat_id,
                        folder_id=0,
                    )
                    print(f"{args.chat_id} unarchived.")
                except TelegramActionClientUnavailableError:
                    print(f"Client for {phone} unavailable.")
                except Exception as exc:
                    print(f"Error unarchiving: {exc}")

            elif args.dialogs_action == "mark-read":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return
                try:
                    await TelegramActionService(pool).mark_read(
                        phone=phone,
                        chat_id=args.chat_id,
                        max_id=args.max_id,
                    )
                    print(f"Messages marked as read in {args.chat_id}.")
                except TelegramActionClientUnavailableError:
                    print(f"Client for {phone} unavailable.")
                except Exception as exc:
                    print(f"Error marking messages as read: {exc}")

            elif args.dialogs_action == "cache-clear":
                phone = getattr(args, "phone", None)
                if phone:
                    pool.invalidate_dialogs_cache(phone)
                    await db.repos.dialog_cache.clear_dialogs(phone)
                    print(f"Cache cleared for {phone}.")
                else:
                    pool.invalidate_dialogs_cache()
                    await db.repos.dialog_cache.clear_all_dialogs()
                    print("Cache cleared for all accounts.")

            elif args.dialogs_action in {"create-channel", "create-group"}:
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
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
                    print(f"Client for {phone} unavailable.")
                    return
                except RuntimeError as exc:
                    if "Telegram returned empty response" in str(exc):
                        print(f"Error: Telegram returned empty response — {noun} may not have been created.")
                        return
                    raise
                print(f"Created {noun} id={result.channel_id} title={args.title!r}")
                if username:
                    if result.channel_username == username:
                        print(f"Username set: @{result.channel_username}")
                    elif result.username_error:
                        print(f"Could not set username: {result.username_error}")
                if result.channel_username:
                    print(f"Channel link: {result.invite_link}")

            elif args.dialogs_action == "cache-status":
                phones = await db.repos.dialog_cache.get_all_phones()
                now_monotonic = time.monotonic()

                if not phones:
                    in_memory_phones = {key[0] for key in pool._dialogs_cache}
                    if not in_memory_phones:
                        print("No cached dialogs.")
                        return
                    phones = sorted(in_memory_phones)

                fmt = "{:<20} {:<10} {:<28} {:<10}"
                print(fmt.format("Account", "DB entries", "DB cached at", "Mem entries"))
                print("-" * 72)
                for phone in sorted(set(phones) | {key[0] for key in pool._dialogs_cache}):
                    db_count = await db.repos.dialog_cache.count_dialogs(phone)
                    cached_at = await db.repos.dialog_cache.get_cached_at(phone)
                    cached_at_str = (
                        cached_at.strftime("%Y-%m-%d %H:%M:%S UTC") if cached_at else "-"
                    )
                    entry = pool._dialogs_cache.get((phone, "full"))
                    mem_entries = (
                        len(entry.dialogs)
                        if entry
                        and (now_monotonic - entry.fetched_at_monotonic) <= pool._dialogs_cache_ttl_sec
                        else 0
                    )
                    print(fmt.format(phone, str(db_count), cached_at_str, str(mem_entries)))

            elif args.dialogs_action == "queue":
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
                                f"phone={payload_phone or '-'} run_after={run_after}"
                            )
                elif args.queue_action == "cancel":
                    if not args.yes:
                        print(f"Cancel queue command #{args.command_id}")
                        answer = input("Continue? [y/N] ").strip().lower()
                        if answer != "y":
                            print("Aborted.")
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
                    if not args.yes:
                        filt = []
                        if args.command_type:
                            filt.append(f"type={args.command_type}")
                        if args.phone:
                            filt.append(f"phone={args.phone}")
                        scope = ", ".join(filt) if filt else "ALL types and accounts"
                        print(f"Bulk-cancel pending commands ({scope})")
                        answer = input("Continue? [y/N] ").strip().lower()
                        if answer != "y":
                            print("Aborted.")
                            return
                    cancelled = await service.cancel_pending(
                        command_type=args.command_type or None,
                        phone=args.phone or None,
                    )
                    print(f"Cancelled pending commands: {cancelled}.")
        finally:
            await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())


def run(args: argparse.Namespace) -> None:
    """Primary CLI entrypoint for Telegram dialogs management."""
    run_with_dependencies(args)
