from __future__ import annotations

import argparse
import asyncio
import time

from src.cli import runtime
from src.services.channel_service import ChannelService


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

                results = await svc.leave_dialogs(phone, dialogs)
                for channel_id, ok in results.items():
                    status = "left" if ok else "failed"
                    print(f"  {channel_id}: {status}")
                left = sum(1 for value in results.values() if value)
                failed = len(results) - left
                print(f"\nDone: {left} left, {failed} failed.")

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

                result = await pool.get_native_client_by_phone(phone)
                if result is None:
                    print(f"Client for {phone} unavailable (flood-wait or not connected).")
                    return
                client, _ = result
                try:
                    entity = await client.get_entity(recipient)
                    await client.send_message(entity, text)
                    print(f"Message sent to {recipient}.")
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
                result = await pool.get_native_client_by_phone(phone)
                if result is None:
                    print(f"Client for {phone} unavailable (flood-wait or not connected).")
                    return
                client, _ = result
                try:
                    from_entity = await client.get_entity(args.from_chat)
                    to_entity = await client.get_entity(args.to_chat)
                    await client.forward_messages(to_entity, ids, from_entity)
                    print(f"Forwarded {len(ids)} message(s) from {args.from_chat} to {args.to_chat}.")
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
                result = await pool.get_native_client_by_phone(phone)
                if result is None:
                    print(f"Client for {phone} unavailable (flood-wait or not connected).")
                    return
                client, _ = result
                try:
                    entity = await client.get_entity(args.chat_id)
                    await client.edit_message(entity, args.message_id, args.text)
                    print(f"Message #{args.message_id} edited.")
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
                result = await pool.get_native_client_by_phone(phone)
                if result is None:
                    print(f"Client for {phone} unavailable (flood-wait or not connected).")
                    return
                client, _ = result
                try:
                    entity = await client.get_entity(args.chat_id)
                    await client.delete_messages(entity, ids)
                    print(f"Deleted {len(ids)} message(s).")
                except Exception as exc:
                    print(f"Error deleting messages: {exc}")

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
                result = await pool.get_native_client_by_phone(phone)
                if result is None:
                    print(f"Client for {phone} unavailable.")
                    return
                client, _ = result
                try:
                    entity = await client.get_entity(args.chat_id)
                    await client.pin_message(entity, args.message_id, notify=args.notify)
                    print(f"Message #{args.message_id} pinned.")
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
                result = await pool.get_native_client_by_phone(phone)
                if result is None:
                    print(f"Client for {phone} unavailable.")
                    return
                client, _ = result
                try:
                    entity = await client.get_entity(args.chat_id)
                    await client.unpin_message(entity, args.message_id)
                    print("Message(s) unpinned.")
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
                result = await pool.get_native_client_by_phone(phone)
                if result is None:
                    print(f"Client for {phone} unavailable.")
                    return
                client, _ = result
                try:
                    entity = await client.get_entity(args.chat_id)
                    message = None
                    async for current_message in client.iter_messages(entity, ids=args.message_id):
                        message = current_message
                        break
                    if message is None:
                        print(f"Message #{args.message_id} not found.")
                        return
                    path = await client.download_media(message, file=args.output_dir)
                    if path:
                        print(f"Downloaded: {path}")
                    else:
                        print("No media in this message.")
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
                result = await pool.get_native_client_by_phone(phone)
                if result is None:
                    print(f"Client for {phone} unavailable.")
                    return
                client, _ = result
                try:
                    entity = await client.get_entity(args.chat_id)
                    participants = await client.get_participants(
                        entity,
                        limit=args.limit,
                        search=args.search,
                    )
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
                result = await pool.get_native_client_by_phone(phone)
                if result is None:
                    print(f"Client for {phone} unavailable.")
                    return
                client, _ = result
                try:
                    entity = await client.get_entity(args.chat_id)
                    user = await client.get_entity(args.user_id)
                    kwargs = {"is_admin": args.is_admin}
                    if args.title:
                        kwargs["title"] = args.title
                    await client.edit_admin(entity, user, **kwargs)
                    print(f"Admin rights updated for {args.user_id}.")
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
                result = await pool.get_native_client_by_phone(phone)
                if result is None:
                    print(f"Client for {phone} unavailable.")
                    return
                client, _ = result
                try:
                    from datetime import datetime

                    entity = await client.get_entity(args.chat_id)
                    user = await client.get_entity(args.user_id)
                    if args.send_messages is None and args.send_media is None:
                        print("Error: specify at least one flag (--send-messages or --send-media).")
                        return
                    until_date = None
                    if args.until_date:
                        until_date = datetime.fromisoformat(args.until_date)
                    kwargs = {"until_date": until_date}
                    if args.send_messages is not None:
                        kwargs["send_messages"] = args.send_messages.lower() in ("1", "true", "on")
                    if args.send_media is not None:
                        kwargs["send_media"] = args.send_media.lower() in ("1", "true", "on")
                    await client.edit_permissions(entity, user, **kwargs)
                    print(f"Permissions updated for {args.user_id}.")
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
                result = await pool.get_native_client_by_phone(phone)
                if result is None:
                    print(f"Client for {phone} unavailable.")
                    return
                client, _ = result
                try:
                    entity = await client.get_entity(args.chat_id)
                    user = await client.get_entity(args.user_id)
                    await client.kick_participant(entity, user)
                    print(f"{args.user_id} kicked from {args.chat_id}.")
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
                result = await pool.get_native_client_by_phone(phone)
                if result is None:
                    print(f"Client for {phone} unavailable.")
                    return
                client, _ = result
                try:
                    entity = await client.get_entity(args.chat_id)
                    stats = await client.get_broadcast_stats(entity)
                    print(f"Broadcast stats for {args.chat_id}:")
                    for attr in (
                        "followers",
                        "views_per_post",
                        "shares_per_post",
                        "reactions_per_post",
                        "forwards_per_post",
                    ):
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
                result = await pool.get_native_client_by_phone(phone)
                if result is None:
                    print(f"Client for {phone} unavailable.")
                    return
                client, _ = result
                try:
                    entity = await client.get_entity(args.chat_id)
                    await client.edit_folder(entity, 1)
                    print(f"{args.chat_id} archived.")
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
                result = await pool.get_native_client_by_phone(phone)
                if result is None:
                    print(f"Client for {phone} unavailable.")
                    return
                client, _ = result
                try:
                    entity = await client.get_entity(args.chat_id)
                    await client.edit_folder(entity, 0)
                    print(f"{args.chat_id} unarchived.")
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
                result = await pool.get_native_client_by_phone(phone)
                if result is None:
                    print(f"Client for {phone} unavailable.")
                    return
                client, _ = result
                try:
                    entity = await client.get_entity(args.chat_id)
                    await client.send_read_acknowledge(entity, max_id=args.max_id)
                    print(f"Messages marked as read in {args.chat_id}.")
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

            elif args.dialogs_action == "create-channel":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return
                client = pool.clients[phone]
                from telethon.tl.functions.channels import CreateChannelRequest

                result = await client(
                    CreateChannelRequest(
                        title=args.title,
                        about=args.about or "",
                        broadcast=True,
                        megagroup=False,
                    )
                )
                channel = result.chats[0] if result.chats else None
                if channel is None:
                    print("Error: Telegram returned empty response — channel may not have been created.")
                    return
                channel_id = channel.id
                channel_username = getattr(channel, "username", None) or ""
                print(f"Created channel id={channel_id} title={args.title!r}")
                if args.username and channel_id:
                    try:
                        from telethon.tl.functions.channels import UpdateUsernameRequest

                        await client(UpdateUsernameRequest(channel, args.username))
                        channel_username = args.username
                        print(f"Username set: @{channel_username}")
                    except Exception as exc:
                        print(f"Could not set username: {exc}")
                if channel_username:
                    print(f"Channel link: https://t.me/{channel_username}")

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
        finally:
            await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())


def run(args: argparse.Namespace) -> None:
    """Primary CLI entrypoint for Telegram dialogs management."""
    run_with_dependencies(args)
