from __future__ import annotations

import argparse
import asyncio

from src.cli import runtime


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        _config, db = await runtime.init_db(args.config)
        try:
            action = getattr(args, "analytics_action", None) or "top"
            date_from = getattr(args, "date_from", None)
            date_to = getattr(args, "date_to", None)

            if action == "top":
                limit = getattr(args, "limit", 20)
                limit = max(1, min(limit, 100))
                rows = await db.get_top_messages(limit=limit, date_from=date_from, date_to=date_to)
                if not rows:
                    print("No messages with reactions found.")
                    return
                print(f"{'#':<4} {'Reactions':<10} {'Date':<18} {'Channel':<30} {'Text'}")
                print("-" * 90)
                for i, row in enumerate(rows, 1):
                    channel = (
                        row.get("channel_title")
                        or row.get("channel_username")
                        or str(row.get("channel_id", ""))
                    )
                    text = (row.get("text") or "")[:60].replace("\n", " ")
                    date = str(row.get("date", ""))[:16]
                    print(f"{i:<4} {row['total_reactions']:<10} {date:<18} {channel:<30} {text}")

            elif action == "content-types":
                rows = await db.get_engagement_by_media_type(date_from=date_from, date_to=date_to)
                if not rows:
                    print("No data.")
                    return
                print(f"{'Content type':<20} {'Messages':<12} {'Avg reactions'}")
                print("-" * 50)
                for row in rows:
                    print(
                        f"{row['content_type']:<20} {row['message_count']:<12}"
                        f" {row['avg_reactions']:.1f}"
                    )

            elif action == "hourly":
                rows = await db.get_hourly_activity(date_from=date_from, date_to=date_to)
                if not rows:
                    print("No data.")
                    return
                print(f"{'Hour (UTC)':<14} {'Messages':<12} {'Avg reactions'}")
                print("-" * 40)
                for row in rows:
                    print(
                        f"{row['hour']:02d}:00        {row['message_count']:<12}"
                        f" {row['avg_reactions']:.1f}"
                    )
        finally:
            await db.close()

    asyncio.run(_run())
