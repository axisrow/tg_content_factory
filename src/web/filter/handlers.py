"""Application orchestration for the filter web domain."""

from __future__ import annotations

import logging

from fastapi import Request

from src.filters.analyzer import ChannelAnalyzer
from src.filters.models import FilterReport
from src.web import deps
from src.web.filter.forms import (
    HARD_DELETE_ALL_CONFIRM_PHRASE,
    parse_confirm_pairs,
    parse_pks,
    parse_snapshot,
)
from src.web.filter.responses import (
    FilterRedirect,
    FilterTemplate,
    channels_redirect,
    manage_redirect,
)

logger = logging.getLogger(__name__)


async def _dev_mode_enabled(request: Request) -> bool:
    return (await deps.get_db(request).repos.settings.get_setting("agent_dev_mode_enabled") or "0") == "1"


async def filter_manage(request: Request) -> FilterTemplate:
    db = deps.get_db(request)
    channels = await db.get_channels_with_counts(active_only=False, include_filtered=True)
    filtered = [ch for ch in channels if ch.is_filtered]
    total_channels = len(channels)
    dev_mode = await _dev_mode_enabled(request)
    pending_rename_count = await db.count_pending_rename_events()
    return FilterTemplate(
        "filter_manage.html",
        {
            "channels": filtered,
            "total": len(filtered),
            "total_channels": total_channels,
            "dev_mode": dev_mode,
            "pending_rename_count": pending_rename_count,
        },
    )


async def purge_selected_filtered(request: Request) -> FilterRedirect:
    form = await request.form()
    pks = parse_pks(form)
    if not pks:
        return manage_redirect(error="no_filtered_channels")
    svc = deps.filter_deletion_service(request)
    result = await svc.purge_channels_by_pks(pks)
    # A purge that hit an exception increments skipped_count AND records an error
    # message. Surface real failures instead of always reporting success (#676);
    # benign skips (a pk that is no longer filtered) carry no error and stay quiet.
    if result.errors:
        return manage_redirect(error="purge_partial")
    return manage_redirect(msg="purged_selected")


async def purge_all_filtered(request: Request) -> FilterRedirect:
    svc = deps.filter_deletion_service(request)
    result = await svc.purge_all_filtered()
    if result.purged_count == 0 and not result.errors:
        return manage_redirect(error="no_filtered_channels")
    # Same partial-failure surfacing as purge_selected_filtered (#676 review): a real
    # per-channel exception must not be hidden behind a success message just because
    # other channels purged fine.
    if result.errors:
        return manage_redirect(error="purge_partial")
    return manage_redirect(msg="purged_all_filtered", count=result.purged_count)


async def hard_delete_selected(request: Request) -> FilterRedirect:
    if not await _dev_mode_enabled(request):
        return manage_redirect(error="dev_mode_required_for_hard_delete")
    form = await request.form()
    pks = parse_pks(form)
    if not pks:
        return manage_redirect(error="no_filtered_channels")
    svc = deps.filter_deletion_service(request)
    result = await svc.hard_delete_channels_by_pks(pks)
    # Codex round 9 follow-up: surface partial failures on the selected path
    # too. hard_delete_channels_by_pks catches per-channel exceptions into
    # skipped_count, and child-data rollback (introduced in the round-9
    # atomicity fix on delete_channel) means a "skipped" row keeps its data
    # — but only when the FK violation lets us roll back. Either way the
    # admin needs to see the exact count breakdown.
    if result.skipped_count or result.purged_count != len(pks):
        return manage_redirect(
            error="hard_delete_partial",
            purged=result.purged_count,
            skipped=result.skipped_count,
            expected=len(pks),
        )
    return manage_redirect(msg="deleted_filtered", count=result.purged_count)


async def hard_delete_all(request: Request) -> FilterRedirect:
    if not await _dev_mode_enabled(request):
        return manage_redirect(error="dev_mode_required_for_hard_delete")
    form = await request.form()
    # Server-side confirmation defends against direct POSTs, stale pages,
    # resubmits, and same-count stale swaps. Two interlocking checks:
    #   1. confirm phrase — defeats blind direct POSTs.
    #   2. confirm_pks snapshot of (pk, channel_id) pairs — exact rendered
    #      set with stable identities (Codex round 7). channel_id is the
    #      Telegram-assigned ID, not the SQLite rowid, so a PK reused after
    #      a delete+insert race resolves to a different channel_id and the
    #      comparison rejects. Duplicates are rejected at parse time.
    #      Delete then runs on the unique PK list extracted from the
    #      validated snapshot.
    confirm = (form.get("confirm") or "").strip()
    if confirm != HARD_DELETE_ALL_CONFIRM_PHRASE:
        return manage_redirect(error="hard_delete_confirm_required")
    confirm_pks_raw = form.get("confirm_pks")
    if confirm_pks_raw is None:
        return manage_redirect(error="hard_delete_confirm_required")
    confirmed_pairs = parse_confirm_pairs(str(confirm_pks_raw))
    if confirmed_pairs is None:
        return manage_redirect(error="hard_delete_confirm_required")
    db = deps.get_db(request)
    channels = await db.get_channels_with_counts(active_only=False, include_filtered=True)
    current_pairs = {
        (ch.id, ch.channel_id)
        for ch in channels
        if ch.is_filtered and ch.id is not None and ch.channel_id is not None
    }
    if not current_pairs:
        return manage_redirect(error="no_filtered_channels")
    if set(confirmed_pairs) != current_pairs:
        return manage_redirect(error="hard_delete_set_changed")
    svc = deps.filter_deletion_service(request)
    # Pass only the unique PKs from the validated snapshot to the service.
    # The set comparison above already proved each (pk, channel_id) pair
    # matches the current filtered row, so the PK list is canonical.
    confirmed_pks = [pk for pk, _ in confirmed_pairs]
    result = await svc.hard_delete_channels_by_pks(confirmed_pks)
    # Partial-failure surfacing (Codex round 8): the deletion service catches
    # per-channel exceptions and increments skipped_count, but commits are
    # per-channel. A partial result means rows are already gone irreversibly
    # while others remain — admin needs to see the discrepancy instead of a
    # blanket "deleted" message.
    if result.skipped_count or result.purged_count != len(confirmed_pks):
        return manage_redirect(
            error="hard_delete_partial",
            purged=result.purged_count,
            skipped=result.skipped_count,
            expected=len(confirmed_pks),
        )
    return manage_redirect(msg="deleted_filtered", count=result.purged_count)


async def analyze_channels(request: Request) -> FilterRedirect:
    db = deps.get_db(request)
    analyzer = ChannelAnalyzer(db)
    report = await analyzer.analyze_all()
    await analyzer.apply_filters(report)

    purged_count = 0
    auto_delete = await db.repos.settings.get_setting("auto_delete_filtered")
    if auto_delete == "1" and report.filtered_count > 0:
        channels = await db.get_channels_with_counts(active_only=False, include_filtered=True)
        pk_map = {ch.channel_id: ch.id for ch in channels if ch.id is not None}
        filtered_pks = [
            pk_map[r.channel_id] for r in report.results if r.is_filtered and r.channel_id in pk_map
        ]
        if filtered_pks:
            svc = deps.filter_deletion_service(request)
            result = await svc.purge_channels_by_pks(filtered_pks)
            purged_count = result.purged_count
            # Auto-purge runs in the background of analysis; there's no dedicated flash
            # slot for it, but a partial failure must not vanish (#676 review).
            if result.errors:
                logger.warning(
                    "analyze_channels auto-purge: %d channel(s) failed: %s",
                    len(result.errors),
                    "; ".join(result.errors),
                )

    msg = "purged_all_filtered" if purged_count else "filter_applied"
    return manage_redirect(msg=msg)


async def apply_filters(request: Request) -> FilterRedirect:
    db = deps.get_db(request)
    analyzer = ChannelAnalyzer(db)

    form = await request.form()
    if form.get("snapshot") != "1":
        return channels_redirect(error="filter_snapshot_required")
    snapshot_results = parse_snapshot(form.getlist("selected"))
    report = FilterReport(
        results=snapshot_results,
        total_channels=len(snapshot_results),
        filtered_count=len(snapshot_results),
    )
    count = await analyzer.apply_filters(report)
    return channels_redirect(msg="filter_applied", count=count)


async def has_stats(request: Request) -> dict:
    db = deps.get_db(request)
    channels = await db.repos.channels.get_channels(active_only=True, include_filtered=False)
    if not channels:
        return {"has_stats": True}
    stats_map = await db.get_latest_stats_for_all()
    for ch in channels:
        if ch.channel_id not in stats_map:
            return {"has_stats": False}
    return {"has_stats": True}


async def precheck_subscriber_ratio(request: Request) -> FilterRedirect:
    db = deps.get_db(request)
    analyzer = ChannelAnalyzer(db)
    count = await analyzer.precheck_subscriber_ratio()
    return manage_redirect(msg="precheck_done", count=count)


async def reset_filters(request: Request) -> FilterRedirect:
    db = deps.get_db(request)
    analyzer = ChannelAnalyzer(db)
    await analyzer.reset_filters()
    return manage_redirect(msg="filter_reset")


async def reset_filters_selected(request: Request) -> FilterRedirect:
    db = deps.get_db(request)
    form = await request.form()
    pks = parse_pks(form)
    if not pks:
        return manage_redirect(error="no_filtered_channels")
    analyzer = ChannelAnalyzer(db)
    count = await analyzer.reset_filters_for_pks(pks)
    return manage_redirect(msg="filter_reset_selected", count=count)


async def purge_channel_messages(request: Request, channel_id: int) -> FilterRedirect:
    db = deps.get_db(request)
    channel = await db.get_channel_by_channel_id(channel_id)
    if not channel or not channel.is_filtered:
        return channels_redirect(error="not_filtered")
    deleted = await db.delete_messages_for_channel(channel_id)
    return channels_redirect(msg="purged", count=deleted)


async def toggle_channel_filter(request: Request, pk: int) -> FilterRedirect:
    db = deps.get_db(request)
    channel = await db.get_channel_by_pk(pk)
    if not channel:
        return channels_redirect(msg="channel_not_found")
    await db.set_channel_filtered(pk, not channel.is_filtered)
    return channels_redirect(msg="filter_toggled")
