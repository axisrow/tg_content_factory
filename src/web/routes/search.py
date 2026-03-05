from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

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

    engine = request.app.state.search_engine

    if q:
        if mode == "ai":
            ai_search = request.app.state.ai_search
            result = await ai_search.search(q)
        elif mode == "telegram":
            result = await engine.search_telegram(q, limit=limit)
        elif mode == "my_chats":
            result = await engine.search_my_chats(q, limit=limit)
        elif mode == "channel":
            result = await engine.search_in_channel(channel_id_int, q, limit=limit)
        else:
            result = await engine.search_local(
                query=q,
                channel_id=channel_id_int,
                date_from=date_from or None,
                date_to=date_to or None,
                limit=limit,
                offset=offset,
            )

    db = request.app.state.db
    channels = await db.get_channels()
    ai_enabled = request.app.state.ai_search.enabled

    # Fetch search quota for Premium mode display
    search_quota = await engine.check_search_quota()

    total_pages = 0
    if result and result.total > 0:
        total_pages = (result.total + limit - 1) // limit

    return request.app.state.templates.TemplateResponse(
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
