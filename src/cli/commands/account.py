from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone

from src.cli import runtime


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        _, db = await runtime.init_db(args.config)
        try:
            if args.account_action == "list":
                accounts = await db.get_accounts()
                if not accounts:
                    print("No accounts found.")
                    return
                fmt = "{:<5} {:<16} {:<9} {:<8} {:<8}"
                print(fmt.format("ID", "Phone", "Primary", "Active", "Premium"))
                print("-" * 50)
                for acc in accounts:
                    print(fmt.format(
                        acc.id or 0,
                        acc.phone,
                        "Yes" if acc.is_primary else "No",
                        "Yes" if acc.is_active else "No",
                        "Yes" if acc.is_premium else "No",
                    ))
            elif args.account_action == "toggle":
                accounts = await db.get_accounts()
                acc = next((a for a in accounts if a.id == args.id), None)
                if not acc:
                    print(f"Account id={args.id} not found")
                    return
                new_state = not acc.is_active
                await db.set_account_active(args.id, new_state)
                print(f"Account id={args.id} ({acc.phone}): active={new_state}")
            elif args.account_action == "delete":
                await db.delete_account(args.id)
                print(f"Deleted account id={args.id}")
            elif args.account_action == "flood-status":
                accounts = await db.get_accounts()
                if not accounts:
                    print("No accounts found.")
                    return
                now = datetime.now(timezone.utc)
                fmt = "{:<16} {:<28} {:<14}"
                print(fmt.format("Phone", "Flood wait until", "Remaining"))
                print("-" * 60)
                for acc in accounts:
                    if acc.flood_wait_until is None:
                        until_str = "OK"
                        remaining_str = ""
                    else:
                        flood_until = acc.flood_wait_until
                        if flood_until.tzinfo is None:
                            flood_until = flood_until.replace(tzinfo=timezone.utc)
                        if flood_until > now:
                            delta = flood_until - now
                            remaining_str = f"{int(delta.total_seconds())}s"
                            until_str = flood_until.strftime("%Y-%m-%d %H:%M:%S UTC")
                        else:
                            until_str = "OK (expired)"
                            remaining_str = ""
                    print(fmt.format(acc.phone, until_str, remaining_str))
            elif args.account_action == "flood-clear":
                accounts = await db.get_accounts()
                acc = next((a for a in accounts if a.phone == args.phone), None)
                if not acc:
                    print(f"Account {args.phone} not found")
                    return
                await db.update_account_flood(args.phone, None)
                print(f"Flood wait cleared for {args.phone}")
        finally:
            await db.close()

    asyncio.run(_run())
