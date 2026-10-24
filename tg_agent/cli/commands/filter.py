"""Shared async bodies for the ``filter`` CLI group (epic #959, Wave 3 — #1123).

Migrated off the argparse dispatcher onto the Typer ``app`` (see
``tg_agent/cli/typer_commands.py``). Each leaf sub-command is a plain ``async def
*_impl`` here — no local ``asyncio.run`` and no ``argparse.Namespace``. A thin
``run(args)`` adapter is kept for the argparse leaf audit and existing tests.
"""

from __future__ import annotations

import argparse
import asyncio
from typing import TYPE_CHECKING, cast

import typer

from tg_agent.cli import runtime
from tg_agent.cli.commands.common import (
    apply_startup,
    cli_error,
    run_async,
    sync_prompt,
)
from tg_agent.database.bundles import ChannelBundle
from tg_agent.filters.analyzer import ChannelAnalyzer
from tg_agent.filters.criteria import (
    AUTHOR_CONTENT_TAG,
    DEFAULT_QUICK_SAMPLE_SIZE,
    OPERATOR_STICKY_FLAGS,
)
from tg_agent.services.channel_service import ChannelService
from tg_agent.services.filter_deletion_service import FilterDeletionService

if TYPE_CHECKING:
    from tg_agent.telegram.client_pool import ClientPool


def _build_deletion_service(db) -> FilterDeletionService:
    channel_bundle = ChannelBundle.from_database(db)
    channel_service = ChannelService(channel_bundle, cast("ClientPool", None), queue=None)
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


async def analyze_impl(
    config_path: str,
    *,
    quick: bool = False,
    sample_size: int | None = None,
    rebuild: bool = False,
) -> None:
    """Analyze channels and print the uniqueness / ratio / flags report.

    quick=True samples the last ``sample_size`` messages per channel (default 300,
    #1138) and skips the cross-dupe self-join, finishing in seconds; without it the
    analysis scans the whole message history. ``sample_size`` only applies to quick.

    rebuild=True пересчитывает накопительный справочник текст-статистики из всей
    истории (#1679) и включает быстрый путь анализатора — ручной точный режим
    (дефолтный запуск после backfill идёт по справочнику за секунды).
    """
    _, db = await runtime.init_db(config_path)
    try:
        if rebuild:
            import time as _time

            t0 = _time.monotonic()
            await db.filter_repo.rebuild_text_stats()
            await db.filter_repo.set_stats_populated(True)
            print(f"Stats cache rebuilt in {_time.monotonic() - t0:.1f}s — fast path enabled.")
        analyzer = ChannelAnalyzer(db)
        report = await analyzer.analyze_all(quick=quick, sample_size=sample_size)
        if not report.results:
            print("No channels found.")
            return

        fmt = "{:<6} {:<25} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<10} {:<9} {:<30}"
        header = (
            "ChanID", "Title", "Uniq%", "SubRatio", "Cyr%", "Short%", "XDupe%", "Auto%",
            "Views%", "Fwd%", "msgs/24h", "Flags",
        )
        print(fmt.format(*header))
        print("-" * 145)
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
                    f"{r.auto_feed_pct:.1f}" if r.auto_feed_pct is not None else "-",
                    f"{r.views_ratio_pct:.1f}" if r.views_ratio_pct is not None else "-",
                    f"{r.forward_pct:.1f}" if r.forward_pct is not None else "-",
                    str(r.volume_24h) if r.volume_24h is not None else "-",
                    flags_str[:30],
                )
            )
        print(f"\nTotal: {report.total_channels}, Flagged: {report.filtered_count}")
    finally:
        await db.close()


async def apply_impl(config_path: str, *, whitelist: bool = False) -> None:
    """Analyze and mark filtered channels.

    whitelist=True switches to the whitelist mode (#1490): channels WITHOUT the
    «авторский контент» trust tag get filtered (no_author_content), tagged ones
    are unfiltered — metrics/analyze are not used, the verdict is tag-only.
    """
    _, db = await runtime.init_db(config_path)
    try:
        analyzer = ChannelAnalyzer(db)
        if whitelist:
            count = await analyzer.apply_whitelist()
            print(
                f"Whitelist applied: {count} channels filtered without the "
                "«авторский контент» trust tag."
            )
            return
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


async def suspects_impl(config_path: str) -> None:
    """Whitelist↔blacklist contradiction report: «авторский контент» + flags (#1490).

    Read-only: errors of the filter surface here for a human decision; behavior
    (is_filtered / collection) is not changed by this command.
    """
    _, db = await runtime.init_db(config_path)
    try:
        suspects = await db.repos.channels.fetch_suspect_channels(AUTHOR_CONTENT_TAG)
        if not suspects:
            print("No suspects: no channel carries the author-content tag together with filter flags.")
            return
        print(f"Suspects (author-content tag + filter flags): {len(suspects)}")
        fmt = "{:<6} {:<22} {:<30} {:<9} {:<7}"
        print(fmt.format("Pk", "Username", "Title", "Filtered", "Active") + " Flags")
        print("-" * 110)
        for s in suspects:
            # Flags are the decision evidence: never truncate (Codex P2 #1493) —
            # a mid-flag slice would hide reasons from the human reviewing.
            print(
                fmt.format(
                    s["pk"],
                    f"@{(s['username'] or '-')}"[:22],
                    (s["title"] or "-")[:30],
                    "yes" if s["is_filtered"] else "no",
                    "yes" if s["is_active"] else "no",
                )
                + f" {s['filter_flags'] or '-'}"
            )
    finally:
        await db.close()


async def provenance_backfill_impl(config_path: str) -> None:
    """Show private channels left filtered by the provenance migration.

    Database initialization runs the idempotent backfill.  This explicit CLI
    view gives the owner the required list for deciding which private channels
    may be unfiltered; it intentionally performs no filter-state changes.
    """
    _, db = await runtime.init_db(config_path)
    try:
        channels = await db.get_channels(active_only=False, include_filtered=True)
        filtered_private = [
            ch
            for ch in channels
            if ch.username_state == "known" and not ch.username and ch.is_filtered
        ]
        if not filtered_private:
            print("No filtered private channels require an explicit decision.")
            return
        print(
            "Filtered private channels require an explicit owner decision "
            f"({len(filtered_private)}):"
        )
        for channel in filtered_private:
            print(
                f"  {channel.channel_id}\t{channel.title or 'Без названия'} "
                f"(pk={channel.id})"
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
        await db.set_channel_filtered(pk, new_state, origin="human", actor="cli")
        status = "filtered" if new_state else "unfiltered"
        print(f"Channel pk={pk} ({channel.title}) marked as {status}.")
    finally:
        await db.close()


async def flag_impl(
    config_path: str,
    *,
    pk: int,
    set_flag: str | None = None,
    unset_flag: str | None = None,
) -> None:
    """Set/unset an operator sticky filter flag (#1499) — durable across apply cycles."""
    if (set_flag is None) == (unset_flag is None):
        cli_error("Usage: filter flag <pk> --set manual | --unset manual (exactly one action).")
    action, flag = ("set", set_flag) if set_flag is not None else ("unset", unset_flag)
    if flag not in OPERATOR_STICKY_FLAGS:
        valid = ", ".join(sorted(OPERATOR_STICKY_FLAGS))
        cli_error(f"Unknown operator flag '{flag}'. Sticky flags settable by hand: {valid}.")
    _, db = await runtime.init_db(config_path)
    try:
        applied = await db.repos.channels.merge_filter_flag(pk, flag, add=action == "set")
        if applied is False:
            print(f"Channel pk={pk} not found.")
        elif applied is None:
            print(f"Channel pk={pk}: flag '{flag}' was not set; nothing changed.")
        elif action == "set":
            print(f"Channel pk={pk}: sticky flag '{flag}' set (is_filtered=1, origin=human).")
        else:
            print(f"Channel pk={pk}: sticky flag '{flag}' unset (back under auto verdicts).")
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
            count = await analyzer.reset_filters_for_pks(
                pk_list, origin="human", actor="cli"
            )
            print(f"Reset filter flag for {count} channel(s).")
        else:
            await analyzer.reset_filters(origin="human", actor="cli")
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
            confirm = sync_prompt(
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
            confirm = sync_prompt(
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
            confirm = sync_prompt(
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
        print("Usage: filter {analyze|apply|reset|precheck|toggle|flag|purge|hard-delete|suspects}")
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
        asyncio.run(apply_impl(args.config, whitelist=getattr(args, "whitelist", False)))
    elif action == "precheck":
        asyncio.run(precheck_impl(args.config))
    elif action == "suspects":
        asyncio.run(suspects_impl(args.config))
    elif action == "toggle":
        asyncio.run(toggle_impl(args.config, pk=args.pk))
    elif action == "flag":
        asyncio.run(
            flag_impl(
                args.config,
                pk=args.pk,
                set_flag=getattr(args, "set_flag", None),
                unset_flag=getattr(args, "unset_flag", None),
            )
        )
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


# --------------------------------------------------------------------------- #
# filter → analyze / apply / reset / precheck / toggle / flag / purge
#          / purge-messages / hard-delete
# --------------------------------------------------------------------------- #

filter_app = typer.Typer(no_args_is_help=True, help="Channel content filter")


@filter_app.command("analyze")
def filter_analyze(
    ctx: typer.Context,
    quick: bool = typer.Option(
        False,
        "--quick",
        help="Sample the last N messages/channel + skip cross-dupe analysis (seconds on large DBs)",
    ),
    sample_size: int = typer.Option(
        DEFAULT_QUICK_SAMPLE_SIZE,
        "--sample-size",
        help=(
            f"Messages/channel to sample in --quick mode "
            f"(default: {DEFAULT_QUICK_SAMPLE_SIZE}; ignored without --quick)"
        ),
    ),
    rebuild: bool = typer.Option(
        False,
        "--rebuild",
        help=(
            "Recompute the incremental stats cache from full history (#1679) "
            "and enable the fast analyze path (manual exact mode)"
        ),
    ),
) -> None:
    """Analyze channels and show report."""
    apply_startup(ctx)
    run_async(analyze_impl(ctx.obj.config, quick=quick, sample_size=sample_size, rebuild=rebuild))


@filter_app.command("apply")
def filter_apply(
    ctx: typer.Context,
    whitelist: bool = typer.Option(
        False,
        "--whitelist",
        help=(
            "Whitelist mode (#1490): filter channels WITHOUT the «авторский контент» "
            "tag (no_author_content), unfilter tagged ones; analyze is not run"
        ),
    ),
) -> None:
    """Analyze and mark filtered channels."""
    apply_startup(ctx)
    run_async(apply_impl(ctx.obj.config, whitelist=whitelist))


@filter_app.command("reset")
def filter_reset(
    ctx: typer.Context,
    pks: str | None = typer.Option(None, "--pks", help="Comma-separated PKs (default: all)"),
) -> None:
    """Reset channel filter flag."""
    apply_startup(ctx)
    run_async(reset_impl(ctx.obj.config, pks=pks))


@filter_app.command("precheck")
def filter_precheck(ctx: typer.Context) -> None:
    """Apply pre-filter by subscriber ratio (no Telegram needed)."""
    apply_startup(ctx)
    run_async(precheck_impl(ctx.obj.config))


@filter_app.command("suspects")
def filter_suspects(ctx: typer.Context) -> None:
    """Channels tagged «авторский контент» that also carry filter flags (#1490)."""
    apply_startup(ctx)
    run_async(suspects_impl(ctx.obj.config))


@filter_app.command("backfill-private")
def filter_backfill_private(ctx: typer.Context) -> None:
    """Show filtered private channels requiring an owner decision."""
    apply_startup(ctx)
    run_async(provenance_backfill_impl(ctx.obj.config))


@filter_app.command("toggle")
def filter_toggle(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Channel primary key"),
) -> None:
    """Toggle filter for a single channel."""
    apply_startup(ctx)
    run_async(toggle_impl(ctx.obj.config, pk=pk))


@filter_app.command("flag")
def filter_flag(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Channel primary key"),
    set_flag: str | None = typer.Option(None, "--set", help="Sticky flag to set (manual)"),
    unset_flag: str | None = typer.Option(None, "--unset", help="Sticky flag to unset (manual)"),
) -> None:
    """Set/unset an operator sticky filter flag (#1499)."""
    apply_startup(ctx)
    run_async(
        flag_impl(ctx.obj.config, pk=pk, set_flag=set_flag, unset_flag=unset_flag)
    )


@filter_app.command("purge")
def filter_purge(
    ctx: typer.Context,
    pks: str | None = typer.Option(None, "--pks", help="Comma-separated PKs (default: all)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Purge messages from filtered channels."""
    apply_startup(ctx)
    run_async(purge_impl(ctx.obj.config, pks=pks, yes=yes))


@filter_app.command("purge-messages")
def filter_purge_messages(
    ctx: typer.Context,
    channel_id: int = typer.Option(..., "--channel-id", help="Channel ID whose messages to delete"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Delete messages for a specific channel from DB."""
    apply_startup(ctx)
    run_async(purge_messages_impl(ctx.obj.config, channel_id=channel_id, yes=yes))


@filter_app.command("hard-delete")
def filter_hard_delete(
    ctx: typer.Context,
    pks: str | None = typer.Option(None, "--pks", help="Comma-separated PKs (default: all)"),
    yes: bool = typer.Option(False, "--yes", help="Skip confirmation prompt"),
) -> None:
    """Hard-delete filtered channels from DB (dev/testing)."""
    apply_startup(ctx)
    run_async(hard_delete_impl(ctx.obj.config, pks=pks, yes=yes))
