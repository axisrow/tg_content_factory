import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.filters.analyzer import ChannelAnalyzer
from src.filters.criteria import VALID_FLAGS
from src.filters.models import ChannelFilterResult, FilterReport
from src.web import deps

logger = logging.getLogger(__name__)

router = APIRouter()


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


@router.post("/filter/analyze", response_class=HTMLResponse)
async def analyze_channels(request: Request):
    db = deps.get_db(request)
    analyzer = ChannelAnalyzer(db)
    report = await analyzer.analyze_all()
    await analyzer.apply_filters(report)
    return deps.get_templates(request).TemplateResponse(
        request,
        "filter_report.html",
        {"report": report},
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
    return RedirectResponse(
        url=f"/channels?msg=filter_applied&count={count}", status_code=303
    )


@router.post("/filter/reset")
async def reset_filters(request: Request):
    db = deps.get_db(request)
    analyzer = ChannelAnalyzer(db)
    await analyzer.reset_filters()
    return RedirectResponse(url="/channels?msg=filter_reset", status_code=303)


@router.post("/{pk}/filter-toggle")
async def toggle_channel_filter(request: Request, pk: int):
    db = deps.get_db(request)
    channel = await db.get_channel_by_pk(pk)
    if not channel:
        return RedirectResponse(url="/channels?msg=channel_not_found", status_code=303)
    await db.set_channel_filtered(pk, not channel.is_filtered)
    return RedirectResponse(url="/channels?msg=filter_toggled", status_code=303)
