from __future__ import annotations

import argparse
import asyncio

from src.cli import runtime
from src.services.channel_service import ChannelService


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
        _, pool = await runtime.init_pool(config, db)
        try:
            if args.my_telegram_action == "list":
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
                    print(fmt.format(
                        d["channel_type"],
                        d["title"][:40],
                        ("@" + d["username"]) if d.get("username") else "",
                        "Yes" if d.get("already_added") else "-",
                    ))
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
                title_map: dict[int, str] = {
                    d["channel_id"]: d["title"] for d in dialogs_info
                }

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
        finally:
            await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
