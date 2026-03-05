from __future__ import annotations

import argparse
import asyncio

from src.cli import runtime
from src.filters.analyzer import ChannelAnalyzer


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        _, db = await runtime.init_db(args.config)
        try:
            analyzer = ChannelAnalyzer(db)

            if not args.filter_action:
                print("Usage: filter {analyze|apply|reset}")
                return

            if args.filter_action == "analyze":
                report = await analyzer.analyze_all()
                if not report.results:
                    print("No channels found.")
                    return

                fmt = "{:<6} {:<25} {:<10} {:<10} {:<10} {:<10} {:<10} {:<15}"
                header = (
                    "ChanID", "Title", "Uniq%", "SubRatio",
                    "Cyr%", "Short%", "XDupe%", "Flags",
                )
                print(fmt.format(*header))
                print("-" * 100)
                for r in report.results:
                    flags_str = ", ".join(r.flags) if r.flags else "-"
                    print(
                        fmt.format(
                            r.channel_id,
                            (r.title or "-")[:25],
                            f"{r.uniqueness_pct:.1f}" if r.uniqueness_pct is not None else "-",
                            f"{r.subscriber_ratio:.2f}" if r.subscriber_ratio is not None else "-",
                            f"{r.cyrillic_pct:.1f}" if r.cyrillic_pct is not None else "-",
                            f"{r.short_msg_pct:.1f}" if r.short_msg_pct is not None else "-",
                            f"{r.cross_dupe_pct:.1f}" if r.cross_dupe_pct is not None else "-",
                            flags_str[:15],
                        )
                    )

                print(
                    f"\nTotal: {report.total_channels}, "
                    f"Filtered: {report.filtered_count}"
                )

            elif args.filter_action == "apply":
                report = await analyzer.analyze_all()
                count = await analyzer.apply_filters(report)
                print(f"Applied filters: {count} channels marked as filtered.")

            elif args.filter_action == "reset":
                await analyzer.reset_filters()
                print("All channel filters have been reset.")
        finally:
            await db.close()

    asyncio.run(_run())
