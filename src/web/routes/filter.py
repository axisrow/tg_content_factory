import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from src.web.filter import handlers
from src.web.filter.responses import filter_response

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/filter/manage", response_class=HTMLResponse)
async def filter_manage(request: Request):
    return filter_response(request, await handlers.filter_manage(request))


@router.get("/filter/manage/fragments/table", response_class=HTMLResponse)
async def filter_manage_table(request: Request):
    return filter_response(request, await handlers.filter_manage_table(request))


@router.post("/filter/purge-selected")
async def purge_selected_filtered(request: Request):
    return filter_response(request, await handlers.purge_selected_filtered(request))


@router.post("/filter/purge-all")
async def purge_all_filtered(request: Request):
    return filter_response(request, await handlers.purge_all_filtered(request))


@router.post("/filter/hard-delete-selected")
async def hard_delete_selected(request: Request):
    return filter_response(request, await handlers.hard_delete_selected(request))


@router.post("/filter/hard-delete-all")
async def hard_delete_all(request: Request):
    return filter_response(request, await handlers.hard_delete_all(request))


@router.post("/filter/analyze")
async def analyze_channels(request: Request):
    return filter_response(request, await handlers.analyze_channels(request))


@router.get("/filter/analyze/status")
async def analyze_status(request: Request):
    return await handlers.analyze_status(request)


@router.post("/filter/apply")
async def apply_filters(request: Request):
    return filter_response(request, await handlers.apply_filters(request))


@router.get("/filter/has-stats")
async def has_stats(request: Request):
    return filter_response(request, await handlers.has_stats(request))


@router.post("/filter/precheck")
async def precheck_subscriber_ratio(request: Request):
    return filter_response(request, await handlers.precheck_subscriber_ratio(request))


@router.post("/filter/reset")
async def reset_filters(request: Request):
    return filter_response(request, await handlers.reset_filters(request))


@router.post("/filter/reset-selected")
async def reset_filters_selected(request: Request):
    return filter_response(request, await handlers.reset_filters_selected(request))


@router.post("/{channel_id}/purge-messages")
async def purge_channel_messages(request: Request, channel_id: int):
    return filter_response(request, await handlers.purge_channel_messages(request, channel_id))


@router.post("/{pk}/filter-toggle")
async def toggle_channel_filter(request: Request, pk: int):
    return filter_response(request, await handlers.toggle_channel_filter(request, pk))
