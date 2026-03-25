from __future__ import annotations

import argparse
import asyncio

from pydantic import ValidationError

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
                fmt = "{:<5} {:<40} {:<10} {:<10} {:<20}"
                print(fmt.format("ID", "Query", "Interval", "Total30d", "Last run"))
                print("-" * 90)
                for item in items:
                    sq = item["query"]
                    print(
                        fmt.format(
                            sq.id or 0,
                            sq.query[:40],
                            f"{sq.interval_minutes}m",
                            item["total_30d"],
                            (item["last_run"] or "—")[:20],
                        )
                    )

            elif args.search_query_action == "add":
                exclude = (
                    args.exclude_patterns.replace("\\n", "\n") if args.exclude_patterns else ""
                )
                try:
                    sq_id = await svc.add(
                        args.query,
                        args.interval,
                        is_regex=args.regex,
                        is_fts=args.fts,
                        notify_on_collect=args.notify,
                        track_stats=args.track_stats,
                        exclude_patterns=exclude,
                        max_length=args.max_length,
                    )
                except ValidationError as e:
                    print(f"Error: {e.errors()[0]['msg']}")
                    return
                print(f"Added search query id={sq_id}: {args.query}")

            elif args.search_query_action == "edit":
                sq = await svc.get(args.id)
                if not sq:
                    print(f"Search query id={args.id} not found")
                    return
                notify = args.notify if args.notify is not None else sq.notify_on_collect
                tstats = args.track_stats if args.track_stats is not None else sq.track_stats
                is_fts = args.fts if args.fts is not None else sq.is_fts
                exclude = (
                    args.exclude_patterns.replace("\\n", "\n")
                    if args.exclude_patterns is not None
                    else sq.exclude_patterns
                )
                max_len = (
                    None
                    if args.max_length == -1
                    else args.max_length if args.max_length is not None else sq.max_length
                )
                try:
                    await svc.update(
                        args.id,
                        args.query if args.query else sq.query,
                        args.interval if args.interval is not None else sq.interval_minutes,
                        is_regex=args.regex if args.regex is not None else sq.is_regex,
                        is_fts=is_fts,
                        notify_on_collect=notify,
                        track_stats=tstats,
                        exclude_patterns=exclude,
                        max_length=max_len,
                    )
                except ValidationError as e:
                    print(f"Error: {e.errors()[0]['msg']}")
                    return
                print(f"Updated search query id={args.id}")

            elif args.search_query_action == "delete":
                await svc.delete(args.id)
                print(f"Deleted search query id={args.id}")

            elif args.search_query_action == "toggle":
                await svc.toggle(args.id)
                print(f"Toggled search query id={args.id}")

            elif args.search_query_action == "run":
                count = await svc.run_once(args.id)
                print(f"Search query id={args.id} executed: {count} matches found.")

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
