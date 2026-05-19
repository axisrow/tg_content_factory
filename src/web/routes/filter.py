import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.filters.analyzer import ChannelAnalyzer
from src.filters.criteria import VALID_FLAGS
from src.filters.models import ChannelFilterResult, FilterReport
from src.web import deps

logger = logging.getLogger(__name__)

router = APIRouter()


async def _dev_mode_enabled(request: Request) -> bool:
    return (await deps.get_db(request).get_setting("agent_dev_mode_enabled") or "0") == "1"


def _parse_snapshot(values: list[str]) -> list[ChannelFilterResult]:
    deduped: dict[int, list[str]] = {}
    for value in values:
        channel_id_str, sep, flags_csv = value.partition("|")
        if not sep:
            continue
        try:
            channel_id = int(channel_id_str)
        except ValueError:
            continue
        flags = [f for f in (f.strip() for f in flags_csv.split(",")) if f in VALID_FLAGS]
        if not flags:
            continue
        deduped[channel_id] = flags
    return [
        ChannelFilterResult(channel_id=channel_id, flags=flags, is_filtered=True)
        for channel_id, flags in deduped.items()
    ]


def _parse_pks(form, field: str = "pks") -> list[int]:
    pks = []
    for v in form.getlist(field):
        try:
            pks.append(int(v))
        except (ValueError, TypeError):
            continue
    return pks


@router.get("/filter/manage", response_class=HTMLResponse)
async def filter_manage(request: Request):
    db = deps.get_db(request)
    channels = await db.get_channels_with_counts(active_only=False, include_filtered=True)
    filtered = [ch for ch in channels if ch.is_filtered]
    dev_mode = await _dev_mode_enabled(request)
    pending_rename_count = await db.count_pending_rename_events()
    return deps.get_templates(request).TemplateResponse(
        request,
        "filter_manage.html",
        {
            "channels": filtered,
            "total": len(filtered),
            "dev_mode": dev_mode,
            "pending_rename_count": pending_rename_count,
        },
    )


@router.post("/filter/purge-selected")
async def purge_selected_filtered(request: Request):
    form = await request.form()
    pks = _parse_pks(form)
    if not pks:
        return RedirectResponse(
            url="/channels/filter/manage?error=no_filtered_channels",
            status_code=303,
        )
    svc = deps.filter_deletion_service(request)
    await svc.purge_channels_by_pks(pks)
    return RedirectResponse(
        url="/channels/filter/manage?msg=purged_selected",
        status_code=303,
    )


@router.post("/filter/purge-all")
async def purge_all_filtered(request: Request):
    svc = deps.filter_deletion_service(request)
    result = await svc.purge_all_filtered()
    if result.purged_count == 0:
        return RedirectResponse(
            url="/channels/filter/manage?error=no_filtered_channels",
            status_code=303,
        )
    return RedirectResponse(
        url=(f"/channels/filter/manage?msg=purged_all_filtered" f"&count={result.purged_count}"),
        status_code=303,
    )


@router.post("/filter/hard-delete-selected")
async def hard_delete_selected(request: Request):
    if not await _dev_mode_enabled(request):
        return RedirectResponse(
            url="/channels/filter/manage?error=dev_mode_required_for_hard_delete",
            status_code=303,
        )
    form = await request.form()
    pks = _parse_pks(form)
    if not pks:
        return RedirectResponse(
            url="/channels/filter/manage?error=no_filtered_channels",
            status_code=303,
        )
    svc = deps.filter_deletion_service(request)
    result = await svc.hard_delete_channels_by_pks(pks)
    return RedirectResponse(
        url=(f"/channels/filter/manage?msg=deleted_filtered" f"&count={result.purged_count}"),
        status_code=303,
    )


HARD_DELETE_ALL_CONFIRM_PHRASE = "DELETE_ALL_FILTERED"


def _parse_confirm_pairs(raw: str) -> list[tuple[int, int]] | None:
    """Parse the hard-delete-all snapshot as ``pk:channel_id`` pairs.

    Each token must be ``<pk>:<channel_id>`` where both are integers.
    Duplicate ``pk`` values (or duplicate ``channel_id`` values) are rejected
    so a crafted ``"1:1001,1:1001"`` cannot smuggle a delete past the set
    comparison. Empty/whitespace input is treated as an empty snapshot so
    the no_filtered_channels branch stays reachable through the normal form
    flow. Returns ``None`` when any token is malformed.

    Binding to ``channel_id`` (the Telegram-assigned identifier, not the
    SQLite rowid) guards against PK reuse: if the rendered row is deleted
    and a new row is inserted between render and submit, the new row will
    likely have a different ``channel_id`` and the comparison will reject.
    """
    tokens = [tok.strip() for tok in (raw or "").split(",") if tok.strip()]
    pairs: list[tuple[int, int]] = []
    seen_pks: set[int] = set()
    seen_chids: set[int] = set()
    for tok in tokens:
        parts = tok.split(":")
        if len(parts) != 2:
            return None
        try:
            pk = int(parts[0])
            chid = int(parts[1])
        except ValueError:
            return None
        if pk in seen_pks or chid in seen_chids:
            return None
        seen_pks.add(pk)
        seen_chids.add(chid)
        pairs.append((pk, chid))
    return pairs


@router.post("/filter/hard-delete-all")
async def hard_delete_all(request: Request):
    if not await _dev_mode_enabled(request):
        return RedirectResponse(
            url="/channels/filter/manage?error=dev_mode_required_for_hard_delete",
            status_code=303,
        )
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
        return RedirectResponse(
            url="/channels/filter/manage?error=hard_delete_confirm_required",
            status_code=303,
        )
    confirm_pks_raw = form.get("confirm_pks")
    if confirm_pks_raw is None:
        return RedirectResponse(
            url="/channels/filter/manage?error=hard_delete_confirm_required",
            status_code=303,
        )
    confirmed_pairs = _parse_confirm_pairs(str(confirm_pks_raw))
    if confirmed_pairs is None:
        return RedirectResponse(
            url="/channels/filter/manage?error=hard_delete_confirm_required",
            status_code=303,
        )
    db = deps.get_db(request)
    channels = await db.get_channels_with_counts(active_only=False, include_filtered=True)
    current_pairs = {
        (ch.id, ch.channel_id)
        for ch in channels
        if ch.is_filtered and ch.id is not None and ch.channel_id is not None
    }
    if not current_pairs:
        return RedirectResponse(
            url="/channels/filter/manage?error=no_filtered_channels",
            status_code=303,
        )
    if set(confirmed_pairs) != current_pairs:
        return RedirectResponse(
            url="/channels/filter/manage?error=hard_delete_set_changed",
            status_code=303,
        )
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
        return RedirectResponse(
            url=(
                "/channels/filter/manage?error=hard_delete_partial"
                f"&purged={result.purged_count}"
                f"&skipped={result.skipped_count}"
                f"&expected={len(confirmed_pks)}"
            ),
            status_code=303,
        )
    return RedirectResponse(
        url=(f"/channels/filter/manage?msg=deleted_filtered" f"&count={result.purged_count}"),
        status_code=303,
    )


@router.post("/filter/analyze")
async def analyze_channels(request: Request):
    db = deps.get_db(request)
    analyzer = ChannelAnalyzer(db)
    report = await analyzer.analyze_all()
    await analyzer.apply_filters(report)

    purged_count = 0
    auto_delete = await db.get_setting("auto_delete_filtered")
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

    msg = "filter_applied"
    if purged_count:
        msg = "purged_all_filtered"
    return RedirectResponse(
        url=f"/channels/filter/manage?msg={msg}",
        status_code=303,
    )


@router.post("/filter/apply")
async def apply_filters(request: Request):
    db = deps.get_db(request)
    analyzer = ChannelAnalyzer(db)

    form = await request.form()
    if form.get("snapshot") != "1":
        return RedirectResponse(url="/channels?error=filter_snapshot_required", status_code=303)
    snapshot_results = _parse_snapshot(form.getlist("selected"))
    report = FilterReport(
        results=snapshot_results,
        total_channels=len(snapshot_results),
        filtered_count=len(snapshot_results),
    )

    count = await analyzer.apply_filters(report)
    return RedirectResponse(url=f"/channels?msg=filter_applied&count={count}", status_code=303)


@router.get("/filter/has-stats")
async def has_stats(request: Request):
    db = deps.get_db(request)
    channels = await db.get_channels(active_only=True, include_filtered=False)
    if not channels:
        return {"has_stats": True}

    stats_map = await db.get_latest_stats_for_all()
    for ch in channels:
        if ch.channel_id not in stats_map:
            return {"has_stats": False}

    return {"has_stats": True}


@router.post("/filter/precheck")
async def precheck_subscriber_ratio(request: Request):
    db = deps.get_db(request)
    analyzer = ChannelAnalyzer(db)
    count = await analyzer.precheck_subscriber_ratio()
    return RedirectResponse(
        url=f"/channels/filter/manage?msg=precheck_done&count={count}",
        status_code=303,
    )


@router.post("/filter/reset")
async def reset_filters(request: Request):
    db = deps.get_db(request)
    analyzer = ChannelAnalyzer(db)
    await analyzer.reset_filters()
    return RedirectResponse(url="/channels/filter/manage?msg=filter_reset", status_code=303)


@router.post("/filter/reset-selected")
async def reset_filters_selected(request: Request):
    db = deps.get_db(request)
    form = await request.form()
    pks = _parse_pks(form)
    if not pks:
        return RedirectResponse(
            url="/channels/filter/manage?error=no_filtered_channels",
            status_code=303,
        )
    analyzer = ChannelAnalyzer(db)
    count = await analyzer.reset_filters_for_pks(pks)
    return RedirectResponse(
        url=f"/channels/filter/manage?msg=filter_reset_selected&count={count}",
        status_code=303,
    )


@router.post("/{channel_id}/purge-messages")
async def purge_channel_messages(request: Request, channel_id: int):
    db = deps.get_db(request)
    channel = await db.get_channel_by_channel_id(channel_id)
    if not channel or not channel.is_filtered:
        return RedirectResponse(url="/channels?error=not_filtered", status_code=303)
    deleted = await db.delete_messages_for_channel(channel_id)
    return RedirectResponse(url=f"/channels?msg=purged&count={deleted}", status_code=303)


@router.post("/{pk}/filter-toggle")
async def toggle_channel_filter(request: Request, pk: int):
    db = deps.get_db(request)
    channel = await db.get_channel_by_pk(pk)
    if not channel:
        return RedirectResponse(url="/channels?msg=channel_not_found", status_code=303)
    await db.set_channel_filtered(pk, not channel.is_filtered)
    return RedirectResponse(url="/channels?msg=filter_toggled", status_code=303)
