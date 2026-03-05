from __future__ import annotations

import argparse
import asyncio

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
        finally:
            await db.close()

    asyncio.run(_run())
