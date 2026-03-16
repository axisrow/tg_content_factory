from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone

from src.cli import runtime


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
        pool = None
        try:
            if args.account_action == "info":
                _, pool = await runtime.init_pool(config, db)
                users = await pool.get_users_info(include_avatar=False)
                phone_filter = getattr(args, "phone", None)
                if phone_filter:
                    users = [u for u in users if u.phone == phone_filter]
                if not users:
                    print("No connected accounts found.")
                    return
                db_accounts = await db.get_accounts()
                active_by_phone = {a.phone: a.is_active for a in db_accounts}
                fmt = "{:<16} {:<25} {:<20} {:<9} {:<8}"
                print(fmt.format("Phone", "Name", "Username", "Premium", "Active"))
                print("-" * 82)
                for u in users:
                    name = f"{u.first_name} {u.last_name}".strip() or "—"
                    username = f"@{u.username}" if u.username else "—"
                    premium = "Yes" if u.is_premium else "No"
                    active = "Yes" if active_by_phone.get(u.phone, False) else "No"
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
            if pool:
                await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
