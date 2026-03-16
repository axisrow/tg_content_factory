from __future__ import annotations

import argparse
import asyncio

from src.cli import runtime


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
        pool = None
        try:
            if args.account_action == "info":
                _, pool = await runtime.init_pool(config, db)
                users = await pool.get_users_info()
                phone_filter = getattr(args, "phone", None)
                if phone_filter:
                    users = [u for u in users if u.phone == phone_filter]
                if not users:
                    print("No connected accounts found.")
                    return
                db_accounts = await db.get_accounts()
                active_by_phone = {a.phone: a.is_active for a in db_accounts}
                premium_by_phone = {a.phone: a.is_premium for a in db_accounts}
                fmt = "{:<16} {:<25} {:<20} {:<9} {:<8}"
                print(fmt.format("Phone", "Name", "Username", "Premium", "Active"))
                print("-" * 82)
                for u in users:
                    name = f"{u.first_name} {u.last_name}".strip() or "—"
                    username = f"@{u.username}" if u.username else "—"
                    premium = "Yes" if premium_by_phone.get(u.phone, False) else "No"
                    active = "Yes" if active_by_phone.get(u.phone, True) else "No"
                    print(fmt.format(u.phone, name[:25], username[:20], premium, active))
                return
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
            if pool:
                await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
