from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from src.web import deps

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: str = Query(""),
    channel_id: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
    mode: str = Query("local"),
    page: int = Query(1),
):
    result = None
    limit = 50
    offset = (page - 1) * limit
    channel_id_int: int | None = int(channel_id) if channel_id else None

    service = deps.search_service(request)

    if q:
        result = await service.search(
            mode=mode,
            query=q,
            limit=limit,
            channel_id=channel_id_int,
            date_from=date_from or None,
            date_to=date_to or None,
            offset=offset,
        )

    db = deps.get_db(request)
    channels = await db.get_channels()
    ai_enabled = deps.get_ai_search(request).enabled
    search_quota = await service.check_quota()

    total_pages = 0
    if result and result.total > 0:
        total_pages = (result.total + limit - 1) // limit

    return deps.get_templates(request).TemplateResponse(
        request,
        "search.html",
        {
            "result": result,
            "channels": channels,
            "q": q,
            "channel_id": channel_id_int,
            "date_from": date_from,
            "date_to": date_to,
            "mode": mode,
            "page": page,
            "total_pages": total_pages,
            "ai_enabled": ai_enabled,
            "search_quota": search_quota,
        },
    )
