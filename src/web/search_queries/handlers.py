"""Application orchestration for the search-queries web domain."""

from __future__ import annotations

from fastapi import Request
from pydantic import ValidationError

from src.web import deps
from src.web.search_queries.forms import SearchQueryForm
from src.web.search_queries.responses import SearchQueryRedirect, SearchQueryTemplate


async def _sync_scheduler(request: Request) -> None:
    scheduler = deps.get_scheduler(request)
    if scheduler.is_running:
        await scheduler.sync_search_query_jobs()


async def search_queries_page(request: Request) -> SearchQueryTemplate:
    svc = deps.search_query_service(request)
    items = await svc.get_with_stats()
    return SearchQueryTemplate("search_queries.html", {"items": items})


async def add_search_query(request: Request, form: SearchQueryForm) -> SearchQueryRedirect:
    if not form.query.strip():
        return SearchQueryRedirect(error="invalid_value")
    svc = deps.search_query_service(request)
    try:
        await svc.add(
            form.query,
            form.interval_minutes,
            is_regex=form.is_regex,
            is_fts=form.is_fts,
            notify_on_collect=form.notify_on_collect,
            track_stats=form.track_stats,
            exclude_patterns=form.exclude_patterns,
            max_length=form.max_length,
            chat_filter=form.chat_filter,
        )
    except ValidationError:
        return SearchQueryRedirect(error="invalid_value")
    chat_validation = await svc.validate_chat_filter(form.chat_filter)
    await _sync_scheduler(request)
    return SearchQueryRedirect(msg="sq_added", extra={"warning": chat_validation.warning_text() or None})


async def toggle_search_query(request: Request, sq_id: int) -> SearchQueryRedirect:
    await deps.search_query_service(request).toggle(sq_id)
    await _sync_scheduler(request)
    return SearchQueryRedirect(msg="sq_toggled")


async def edit_search_query(request: Request, sq_id: int, form: SearchQueryForm) -> SearchQueryRedirect:
    if not form.query.strip():
        return SearchQueryRedirect(error="invalid_value")
    svc = deps.search_query_service(request)
    try:
        await svc.update(
            sq_id,
            form.query,
            form.interval_minutes,
            is_regex=form.is_regex,
            is_fts=form.is_fts,
            notify_on_collect=form.notify_on_collect,
            track_stats=form.track_stats,
            exclude_patterns=form.exclude_patterns,
            max_length=form.max_length,
            chat_filter=form.chat_filter,
        )
    except ValidationError:
        return SearchQueryRedirect(error="invalid_value")
    chat_validation = await svc.validate_chat_filter(form.chat_filter)
    await _sync_scheduler(request)
    return SearchQueryRedirect(msg="sq_edited", extra={"warning": chat_validation.warning_text() or None})


async def delete_search_query(request: Request, sq_id: int) -> SearchQueryRedirect:
    await deps.search_query_service(request).delete(sq_id)
    await _sync_scheduler(request)
    return SearchQueryRedirect(msg="sq_deleted")


async def run_search_query(request: Request, sq_id: int) -> SearchQueryRedirect:
    await deps.search_query_service(request).run_once(sq_id)
    return SearchQueryRedirect(msg="sq_run")
