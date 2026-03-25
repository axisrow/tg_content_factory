from __future__ import annotations

import argparse
import asyncio
import time

from src.cli import runtime
from src.services.channel_service import ChannelService


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
        _, pool = await runtime.init_pool(config, db)
        try:
            if args.my_telegram_action == "refresh":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return
                svc = ChannelService(db, pool, None)  # type: ignore[arg-type]
                dialogs = await svc.get_my_dialogs(phone, refresh=True)
                print(f"Dialogs refreshed: {len(dialogs)} total.")

            elif args.my_telegram_action == "list":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return
                svc = ChannelService(db, pool, None)  # type: ignore[arg-type]
                dialogs = await svc.get_my_dialogs(phone)
                if not dialogs:
                    print("No dialogs found.")
                    return
                fmt = "{:<12} {:<40} {:<20} {:<8}"
                print(fmt.format("Type", "Title", "Username", "In DB"))
                print("-" * 84)
                for d in dialogs:
                    print(
                        fmt.format(
                            d["channel_type"],
                            d["title"][:40],
                            ("@" + d["username"]) if d.get("username") else "",
                            "Yes" if d.get("already_added") else "-",
                        )
                    )
            elif args.my_telegram_action == "leave":
                accounts = sorted(pool.clients.keys())
                if not accounts:
                    print("No connected accounts.")
                    return
                phone = args.phone or accounts[0]
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return

                # Parse dialog IDs (handle comma-separated tokens within each arg)
                raw_ids: list[str] = []
                for item in args.dialog_ids:
                    raw_ids.extend(i.strip() for i in item.split(",") if i.strip())
                dialog_ids: list[int] = []
                for raw in raw_ids:
                    try:
                        dialog_ids.append(int(raw))
                    except ValueError:
                        print(f"Invalid dialog ID: {raw!r}, skipping.")
                if not dialog_ids:
                    print("No valid dialog IDs provided.")
                    return

                # Resolve channel types from the dialog cache
                svc = ChannelService(db, pool, None)  # type: ignore[arg-type]
                dialogs_info = await svc.get_my_dialogs(phone)
                type_map: dict[int, str] = {
                    d["channel_id"]: d["channel_type"] for d in dialogs_info
                }
                title_map: dict[int, str] = {d["channel_id"]: d["title"] for d in dialogs_info}

                dialogs: list[tuple[int, str]] = []
                for cid in dialog_ids:
                    ctype = type_map.get(cid, "channel" if cid < 0 else "dm")
                    dialogs.append((cid, ctype))

                if not args.yes:
                    print(f"About to leave {len(dialogs)} dialog(s):")
                    for cid, ctype in dialogs:
                        title = title_map.get(cid, str(cid))
                        print(f"  {cid}  {title}  ({ctype})")
                    answer = input("Continue? [y/N] ").strip().lower()
                    if answer != "y":
                        print("Aborted.")
                        return

                results = await svc.leave_dialogs(phone, dialogs)
                for cid, ok in results.items():
                    status = "left" if ok else "failed"
                    print(f"  {cid}: {status}")
                left = sum(1 for v in results.values() if v)
                failed = len(results) - left
                print(f"\nDone: {left} left, {failed} failed.")
            elif args.my_telegram_action == "topics":
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
                for t in topics:
                    print(
                        fmt.format(
                            str(t["id"]),
                            t["title"][:40],
                            str(t.get("icon_emoji_id") or "-")[:20],
                            str(t.get("date") or "-")[:26],
                        )
                    )

            elif args.my_telegram_action == "send":
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
                entity = await client.get_entity(recipient)
                await client.send_message(entity, text)
                print(f"Message sent to {recipient}.")

            elif args.my_telegram_action == "cache-clear":
                phone: str | None = getattr(args, "phone", None)
                if phone:
                    pool.invalidate_dialogs_cache(phone)
                    await db.repos.dialog_cache.clear_dialogs(phone)
                    print(f"Cache cleared for {phone}.")
                else:
                    pool.invalidate_dialogs_cache()
                    await db.repos.dialog_cache.clear_all_dialogs()
                    print("Cache cleared for all accounts.")

            elif args.my_telegram_action == "create-channel":
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

            elif args.my_telegram_action == "cache-status":
                phones = await db.repos.dialog_cache.get_all_phones()
                now_monotonic = time.monotonic()

                if not phones:
                    in_memory_phones = {k[0] for k in pool._dialogs_cache}
                    if not in_memory_phones:
                        print("No cached dialogs.")
                        return
                    phones = sorted(in_memory_phones)

                fmt = "{:<20} {:<10} {:<28} {:<10}"
                print(fmt.format("Account", "DB entries", "DB cached at", "Mem entries"))
                print("-" * 72)
                for ph in sorted(set(phones) | {k[0] for k in pool._dialogs_cache}):
                    db_count = await db.repos.dialog_cache.count_dialogs(ph)
                    cached_at = await db.repos.dialog_cache.get_cached_at(ph)
                    cached_at_str = (
                        cached_at.strftime("%Y-%m-%d %H:%M:%S UTC") if cached_at else "-"
                    )
                    entry = pool._dialogs_cache.get((ph, "full"))
                    mem_entries = (
                        len(entry.dialogs)
                        if entry
                        and (now_monotonic - entry.fetched_at_monotonic)
                        <= pool._dialogs_cache_ttl_sec
                        else 0
                    )
                    print(fmt.format(ph, str(db_count), cached_at_str, str(mem_entries)))
        finally:
            await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
