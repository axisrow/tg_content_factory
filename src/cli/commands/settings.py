from __future__ import annotations

import argparse
import asyncio

from src.cli import runtime


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        _config, db = await runtime.init_db(args.config)
        try:
            action = getattr(args, "settings_action", None) or "get"

            if action == "get":
                key = getattr(args, "key", None)
                if key:
                    value = await db.get_setting(key)
                    print(f"{key} = {value or '(not set)'}")
                else:
                    rows = await db.repos.settings.list_all()
                    if not rows:
                        print("No settings found.")
                        return
                    fmt = "{:<50} {}"
                    print(fmt.format("Key", "Value"))
                    print("-" * 80)
                    for k, v in rows:
                        print(fmt.format(k, v[:80] if v else ""))

            elif action == "set":
                await db.set_setting(args.key, args.value)
                print(f"Set {args.key} = {args.value}")

            elif action == "info":
                stats = await db.get_stats()
                print("System information:")
                for key, value in stats.items():
                    print(f"  {key}: {value}")
        finally:
            await db.close()

    asyncio.run(_run())
