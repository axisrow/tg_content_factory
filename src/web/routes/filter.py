import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.filters.analyzer import ChannelAnalyzer
from src.web import deps

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/filter/analyze", response_class=HTMLResponse)
async def analyze_channels(request: Request):
    db = deps.get_db(request)
    assert db.db is not None
    analyzer = ChannelAnalyzer(db.db)
    report = await analyzer.analyze_all()
    return deps.get_templates(request).TemplateResponse(
        request,
        "filter_report.html",
        {"report": report},
    )


@router.post("/filter/apply")
async def apply_filters(request: Request):
    db = deps.get_db(request)
    assert db.db is not None
    analyzer = ChannelAnalyzer(db.db)
    report = await analyzer.analyze_all()
    count = await analyzer.apply_filters(report)
    return RedirectResponse(
        url=f"/channels?msg=filter_applied&count={count}", status_code=303
    )


@router.post("/filter/reset")
async def reset_filters(request: Request):
    db = deps.get_db(request)
    assert db.db is not None
    analyzer = ChannelAnalyzer(db.db)
    await analyzer.reset_filters()
    return RedirectResponse(url="/channels?msg=filter_reset", status_code=303)


@router.post("/{pk}/filter-toggle")
async def toggle_channel_filter(request: Request, pk: int):
    db = deps.get_db(request)
    channel = await db.get_channel_by_pk(pk)
    if channel:
        await db.set_channel_filtered(pk, not channel.is_filtered)
    return RedirectResponse(url="/channels?msg=channel_toggled", status_code=303)
