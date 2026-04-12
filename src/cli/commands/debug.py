from __future__ import annotations

import argparse
import asyncio
import os
import resource

from src.cli import runtime
from src.cli.runtime import APP_LOG_PATH


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        _, db = await runtime.init_db(args.config)
        try:
            if args.debug_action == "logs":
                limit = args.limit
                if APP_LOG_PATH.exists():
                    with open(APP_LOG_PATH, encoding="utf-8", errors="replace") as f:
                        lines = f.readlines()
                    for line in lines[-limit:]:
                        print(line, end="")
                else:
                    print(f"No log file found at {APP_LOG_PATH}")
                    print("Tip: start the server first — logs are written to data/app.log.")

            elif args.debug_action == "memory":
                usage = resource.getrusage(resource.RUSAGE_SELF)
                print(f"Max RSS: {usage.ru_maxrss / 1024:.1f} MB")

                stats = await db.get_stats()
                print("\nDB stats:")
                for key, value in stats.items():
                    print(f"  {key}: {value}")

                db_path = db._path if hasattr(db, "_path") else "unknown"
                if db_path and db_path != ":memory:" and os.path.exists(db_path):
                    size_mb = os.path.getsize(db_path) / (1024 * 1024)
                    print(f"  DB file size: {size_mb:.1f} MB")

            elif args.debug_action == "timing":
                print("Operation timing stats:")
                print("  (no persistent timing data collected)")
                print("  Tip: use 'test benchmark' for pytest serial vs parallel benchmarks")

        finally:
            await db.close()

    asyncio.run(_run())
