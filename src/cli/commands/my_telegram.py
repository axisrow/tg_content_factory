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
        finally:
            await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
