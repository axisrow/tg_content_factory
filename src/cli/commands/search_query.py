from __future__ import annotations

import argparse
import asyncio

from src.cli import runtime
from src.database.bundles import SearchQueryBundle
from src.services.search_query_service import SearchQueryService


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        _, db = await runtime.init_db(args.config)
        try:
            svc = SearchQueryService(SearchQueryBundle.from_database(db))

            if args.search_query_action == "list":
                items = await svc.get_with_stats()
                if not items:
                    print("No search queries found.")
                    return
                fmt = "{:<5} {:<20} {:<30} {:<10} {:<10} {:<20}"
                print(fmt.format("ID", "Name", "Query", "Interval", "Total30d", "Last run"))
                print("-" * 100)
                for item in items:
                    sq = item["query"]
                    print(fmt.format(
                        sq.id or 0,
                        sq.name[:20],
                        sq.query[:30],
                        f"{sq.interval_minutes}m",
                        item["total_30d"],
                        (item["last_run"] or "—")[:20],
                    ))

            elif args.search_query_action == "add":
                sq_id = await svc.add(args.name, args.query, args.interval)
                print(f"Added search query id={sq_id}: {args.name}")

            elif args.search_query_action == "delete":
                await svc.delete(args.id)
                print(f"Deleted search query id={args.id}")

            elif args.search_query_action == "toggle":
                await svc.toggle(args.id)
                print(f"Toggled search query id={args.id}")

            elif args.search_query_action == "stats":
                stats = await svc.get_daily_stats(args.id, args.days)
                if not stats:
                    print("No stats found.")
                    return
                max_count = max(s.count for s in stats)
                for s in stats:
                    bar_len = int(s.count / max_count * 40) if max_count else 0
                    bar = "#" * bar_len
                    print(f"{s.day}  {bar:<40} {s.count}")

        finally:
            await db.close()

    asyncio.run(_run())
