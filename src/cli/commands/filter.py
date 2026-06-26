"""Shared async bodies for the ``filter`` CLI group (epic #959, Wave 3 — #1123).

Migrated off the argparse dispatcher onto the Typer ``app`` (see
``src/cli/typer_commands.py``). Each leaf sub-command is a plain ``async def
*_impl`` here — no local ``asyncio.run`` and no ``argparse.Namespace``. A thin
``run(args)`` adapter is kept for the argparse leaf audit and existing tests.
"""

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
    for pk_str in raw.split(","):
        pk_str = pk_str.strip()
        if pk_str:
            try:
                pks.append(int(pk_str))
            except ValueError:
                continue
    return pks


def _print_result(result, verb: str = "Purged") -> None:
    # Normalize to a concrete list first: a bare attribute access on some mocks/objects
    # yields a truthy non-list, which would otherwise print a bogus "Errors (0):" and
    # suppress the benign "No filtered channels affected." message (#676 review, Codex P2).
    raw_errors = getattr(result, "errors", None)
    errors = list(raw_errors) if isinstance(raw_errors, (list, tuple)) else []
    if result.purged_count == 0 and not errors:
        print("No filtered channels affected.")
    else:
        if result.purged_count:
            print(f"{verb} {result.purged_count} channels:")
            for title in result.purged_titles:
                print(f"  - {title}")
        if result.skipped_count:
            print(f"Skipped: {result.skipped_count}")
    # Real per-channel failures (not benign skips) — surface them so a CLI user can
    # tell a failure from a skip (#676 review).
    if errors:
        print(f"Errors ({len(errors)}):")
        for err in errors:
            print(f"  ✗ {err}")


async def analyze_impl(config_path: str, *, quick: bool = False, sample_size: int | None = None) -> None:
    """Analyze channels and print the uniqueness / ratio / flags report.

    quick=True samples the last ``sample_size`` messages per channel (default 300,
    #1138) and skips the cross-dupe self-join, finishing in seconds; without it the
    analysis scans the whole message history. ``sample_size`` only applies to quick.
    """
    _, db = await runtime.init_db(config_path)
    try:
        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all(quick=quick, sample_size=sample_size)
        if not report.results:
            print("No channels found.")
            return

        fmt = "{:<6} {:<25} {:<10} {:<10} {:<10} {:<10} {:<10} {:<15}"
        header = ("ChanID", "Title", "Uniq%", "SubRatio", "Cyr%", "Short%", "XDupe%", "Flags")
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
        print(f"\nTotal: {report.total_channels}, Flagged: {report.filtered_count}")
    finally:
        await db.close()


async def apply_impl(config_path: str) -> None:
    """Analyze and mark filtered channels."""
    _, db = await runtime.init_db(config_path)
    try:
        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all()
        count = await analyzer.apply_filters(report)
        print(f"Applied filters: {count} channels marked as filtered.")
    finally:
        await db.close()


async def precheck_impl(config_path: str) -> None:
    """Apply the subscriber-ratio pre-filter (no Telegram needed)."""
    _, db = await runtime.init_db(config_path)
    try:
        analyzer = ChannelAnalyzer(db)
        count = await analyzer.precheck_subscriber_ratio()
        print(
            f"Pre-filter applied: {count} channels marked as filtered"
            " (low_subscriber_ratio)."
        )
    finally:
        await db.close()


async def toggle_impl(config_path: str, *, pk: int) -> None:
    """Toggle the filter flag for a single channel."""
    _, db = await runtime.init_db(config_path)
    try:
        channel = await db.get_channel_by_pk(pk)
        if channel is None:
            print(f"Channel pk={pk} not found.")
            return
        new_state = not channel.is_filtered
        await db.set_channel_filtered(pk, new_state)
        status = "filtered" if new_state else "unfiltered"
        print(f"Channel pk={pk} ({channel.title}) marked as {status}.")
    finally:
        await db.close()


async def reset_impl(config_path: str, *, pks: str | None = None) -> None:
    """Reset the filter flag for the given PKs, or all channels if none given."""
    _, db = await runtime.init_db(config_path)
    try:
        analyzer = ChannelAnalyzer(db)
        if pks:
            pk_list = _parse_pks(pks)
            if not pk_list:
                print("No valid PKs provided.")
                return
            count = await analyzer.reset_filters_for_pks(pk_list)
            print(f"Reset filter flag for {count} channel(s).")
        else:
            await analyzer.reset_filters()
            print("All channel filters have been reset.")
    finally:
        await db.close()


async def purge_impl(config_path: str, *, pks: str | None = None, yes: bool = False) -> None:
    """Purge stored messages from filtered channels (confirmation unless --yes)."""
    _, db = await runtime.init_db(config_path)
    try:
        svc = _build_deletion_service(db)
        pk_list: list[int] | None
        if pks:
            pk_list = _parse_pks(pks)
            if not pk_list:
                print("No valid PKs provided.")
                return
            scope = f"{len(pk_list)} selected channel(s)"
        else:
            pk_list = None
            scope = "all filtered channels"
        if not yes:
            confirm = input(
                f"Purge messages from {scope}? This deletes stored messages. [y/N] "
            ).strip().lower()
            if confirm != "y":
                print("Aborted.")
                return
        if pk_list is not None:
            result = await svc.purge_channels_by_pks(pk_list)
        else:
            result = await svc.purge_all_filtered()
        _print_result(result, "Purged messages from")
    finally:
        await db.close()


async def purge_messages_impl(config_path: str, *, channel_id: int, yes: bool = False) -> None:
    """Delete all stored messages for a specific channel id (confirmation unless --yes)."""
    _, db = await runtime.init_db(config_path)
    try:
        channels = await db.get_channels()
        ch = next((c for c in channels if c.channel_id == channel_id), None)
        title = ch.title if ch else str(channel_id)
        if not yes:
            confirm = input(
                f"Delete all messages for channel {title} ({channel_id}) from DB? [y/N] "
            ).strip().lower()
            if confirm != "y":
                print("Aborted.")
                return
        count = await db.delete_messages_for_channel(channel_id)
        print(f"Deleted {count} messages for channel {title} ({channel_id}).")
    finally:
        await db.close()


async def hard_delete_impl(config_path: str, *, pks: str | None = None, yes: bool = False) -> None:
    """Hard-delete filtered channels from the DB (dev mode; irreversible)."""
    _, db = await runtime.init_db(config_path)
    try:
        dev_mode = (await db.get_setting("agent_dev_mode_enabled") or "0") == "1"
        if not dev_mode:
            print(
                "Hard-delete requires developer mode. "
                "Enable it in Settings → Developer mode."
            )
            return
        svc = _build_deletion_service(db)
        if pks:
            pk_list = _parse_pks(pks)
            if not pk_list:
                print("No valid PKs provided.")
                return
        else:
            channels = await db.get_channels_with_counts(
                active_only=False,
                include_filtered=True,
            )
            pk_list = [ch.id for ch in channels if ch.is_filtered and ch.id is not None]
            if not pk_list:
                print("No filtered channels to delete.")
                return
        if not yes:
            confirm = input(
                f"Hard-delete {len(pk_list)} channel(s) from DB? "
                "This is irreversible. Type YES to confirm: "
            )
            # The "YES" word is a deliberate stronger barrier than purge's "y" for
            # this irreversible op, but the comparison is case-insensitive to match
            # the rest of the confirm gates (#1039) — "yes"/"Yes"/"YES" all confirm;
            # "y" alone does not.
            if confirm.strip().lower() != "yes":
                print("Aborted.")
                return
        result = await svc.hard_delete_channels_by_pks(pk_list)
        _print_result(result, "Hard-deleted")
    finally:
        await db.close()


def run(args: argparse.Namespace) -> None:
    """Thin argparse adapter over the ``*_impl`` bodies (legacy dispatch path).

    The production CLI routes ``filter`` through the Typer ``app`` (#1123); this
    wrapper keeps the argparse leaf audit and command-level tests working. Args are
    read via ``getattr`` defaults so partial test Namespaces stay usable (#1117).
    """
    action = getattr(args, "filter_action", None)
    if not action:
        print("Usage: filter {analyze|apply|reset|purge|hard-delete}")
        return
    if action == "analyze":
        asyncio.run(
            analyze_impl(
                args.config,
                quick=getattr(args, "quick", False),
                sample_size=getattr(args, "sample_size", None),
            )
        )
    elif action == "apply":
        asyncio.run(apply_impl(args.config))
    elif action == "precheck":
        asyncio.run(precheck_impl(args.config))
    elif action == "toggle":
        asyncio.run(toggle_impl(args.config, pk=args.pk))
    elif action == "reset":
        asyncio.run(reset_impl(args.config, pks=getattr(args, "pks", None)))
    elif action == "purge":
        asyncio.run(
            purge_impl(args.config, pks=getattr(args, "pks", None), yes=getattr(args, "yes", False))
        )
    elif action == "purge-messages":
        asyncio.run(
            purge_messages_impl(
                args.config, channel_id=args.channel_id, yes=getattr(args, "yes", False)
            )
        )
    elif action == "hard-delete":
        asyncio.run(
            hard_delete_impl(
                args.config, pks=getattr(args, "pks", None), yes=getattr(args, "yes", False)
            )
        )
