from __future__ import annotations

import argparse
import asyncio

from src.cli import runtime
from src.database.bundles import ChannelBundle
from src.filters.analyzer import ChannelAnalyzer
from src.services.channel_service import ChannelService
from src.services.filter_deletion_service import FilterDeletionService


def _build_deletion_service(db) -> FilterDeletionService:
    channel_bundle = ChannelBundle.from_database(db)
    channel_service = ChannelService(channel_bundle, None, queue=None)
    return FilterDeletionService(db, channel_service)


def _parse_pks(raw: str) -> list[int]:
    pks = []
    for v in raw.split(","):
        v = v.strip()
        if v:
            try:
                pks.append(int(v))
            except ValueError:
                continue
    return pks


def _print_result(result, verb: str = "Purged") -> None:
    if result.purged_count == 0:
        print("No filtered channels affected.")
    else:
        print(f"{verb} {result.purged_count} channels:")
        for title in result.purged_titles:
            print(f"  - {title}")
        if result.skipped_count:
            print(f"Skipped: {result.skipped_count}")


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        _, db = await runtime.init_db(args.config)
        try:
            analyzer = ChannelAnalyzer(db)

            if not args.filter_action:
                print("Usage: filter {analyze|apply|reset|purge|hard-delete}")
                return

            if args.filter_action == "analyze":
                report = await analyzer.analyze_all()
                count = await analyzer.apply_filters(report)
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
                    f"Filtered: {report.filtered_count}, "
                    f"Applied: {count}"
                )

            elif args.filter_action == "apply":
                report = await analyzer.analyze_all()
                count = await analyzer.apply_filters(report)
                print(f"Applied filters: {count} channels marked as filtered.")

            elif args.filter_action == "precheck":
                count = await analyzer.precheck_subscriber_ratio()
                print(
                    f"Pre-filter applied: {count} channels marked as filtered"
                    " (low_subscriber_ratio)."
                )

            elif args.filter_action == "reset":
                await analyzer.reset_filters()
                print("All channel filters have been reset.")

            elif args.filter_action == "purge":
                svc = _build_deletion_service(db)
                if hasattr(args, "pks") and args.pks:
                    pks = _parse_pks(args.pks)
                    if not pks:
                        print("No valid PKs provided.")
                        return
                    result = await svc.purge_channels_by_pks(pks)
                else:
                    result = await svc.purge_all_filtered()
                _print_result(result, "Purged messages from")

            elif args.filter_action == "hard-delete":
                svc = _build_deletion_service(db)
                if hasattr(args, "pks") and args.pks:
                    pks = _parse_pks(args.pks)
                    if not pks:
                        print("No valid PKs provided.")
                        return
                    result = await svc.hard_delete_channels_by_pks(pks)
                else:
                    channels = await db.get_channels_with_counts(
                        active_only=False, include_filtered=True,
                    )
                    pks = [
                        ch.id for ch in channels
                        if ch.is_filtered and ch.id is not None
                    ]
                    if not pks:
                        print("No filtered channels to delete.")
                        return
                    result = await svc.hard_delete_channels_by_pks(pks)
                _print_result(result, "Hard-deleted")
        finally:
            await db.close()

    asyncio.run(_run())
