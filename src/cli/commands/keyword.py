from __future__ import annotations

import argparse
import asyncio

from src.cli import runtime
from src.models import Keyword


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        _, db = await runtime.init_db(args.config)
        try:
            if args.keyword_action == "list":
                keywords = await db.get_keywords()
                if not keywords:
                    print("No keywords found.")
                    return
                fmt = "{:<5} {:<40} {:<8} {:<8}"
                print(fmt.format("ID", "Pattern", "Regex", "Active"))
                print("-" * 65)
                for kw in keywords:
                    print(fmt.format(
                        kw.id or 0,
                        kw.pattern[:40],
                        "Yes" if kw.is_regex else "No",
                        "Yes" if kw.is_active else "No",
                    ))
            elif args.keyword_action == "add":
                kw = Keyword(pattern=args.pattern, is_regex=args.regex)
                kid = await db.add_keyword(kw)
                print(f"Added keyword id={kid}: {args.pattern}{' (regex)' if args.regex else ''}")
            elif args.keyword_action == "delete":
                await db.delete_keyword(args.id)
                print(f"Deleted keyword id={args.id}")
            elif args.keyword_action == "toggle":
                keywords = await db.get_keywords()
                kw = next((k for k in keywords if k.id == args.id), None)
                if not kw:
                    print(f"Keyword id={args.id} not found")
                    return
                new_state = not kw.is_active
                await db.set_keyword_active(args.id, new_state)
                print(f"Keyword id={args.id}: active={new_state}")
        finally:
            await db.close()

    asyncio.run(_run())
