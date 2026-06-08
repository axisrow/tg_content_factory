"""Application orchestration for the channels web domain."""

from __future__ import annotations

import logging
import sqlite3

from fastapi import Request

from src.web import deps
from src.web.channels.forms import parse_channel_ids, parse_tags
from src.web.channels.responses import ChannelsJson, ChannelsRedirect, ChannelsTemplate

logger = logging.getLogger(__name__)


async def _enqueue_channel_command(request: Request, command_type: str, payload: dict) -> ChannelsRedirect:
    command_id = await deps.telegram_command_service(request).enqueue(
        command_type,
        payload=payload,
        requested_by="web:channels",
    )
    return ChannelsRedirect(extra={"command_id": command_id})


async def channels_list(request: Request) -> ChannelsTemplate:
    service = deps.channel_service(request)
    db = deps.get_db(request)
    show_all = request.query_params.get("view") == "all"
    channels, latest_stats, prev_subscriber_counts = await service.list_for_page(
        include_filtered=show_all
    )
    active_count = await db.repos.channels.count_channels(active_only=True, include_filtered=False)
    total_count = await db.repos.channels.count_channels()
    return ChannelsTemplate(
        "channels.html",
        {
            "channels": channels,
            "latest_stats": latest_stats,
            "prev_subscriber_counts": prev_subscriber_counts,
            "error": request.query_params.get("error"),
            "msg": request.query_params.get("msg"),
            "show_all": show_all,
            "active_count": active_count,
            "total_count": total_count,
        },
    )


async def add_channel(request: Request, identifier: str) -> ChannelsRedirect:
    if not identifier.strip():
        return ChannelsRedirect(error="resolve")
    return await _enqueue_channel_command(
        request,
        "channels.add_identifier",
        {"identifier": identifier.strip()},
    )


async def get_dialogs(request: Request) -> ChannelsJson:
    service = deps.channel_service(request)
    dialogs = await service.get_dialogs_with_added_flags()
    return ChannelsJson(dialogs)


async def add_bulk(request: Request) -> ChannelsRedirect:
    form = await request.form()
    service = deps.channel_service(request)
    await service.add_bulk_by_dialog_ids(parse_channel_ids(form))
    return ChannelsRedirect(msg="channels_added")


async def toggle_channel(request: Request, pk: int) -> ChannelsRedirect:
    await deps.channel_service(request).toggle(pk)
    return ChannelsRedirect(msg="channel_toggled")


async def delete_channel(request: Request, pk: int) -> ChannelsRedirect:
    try:
        await deps.channel_service(request).delete(pk)
    except sqlite3.IntegrityError:
        return ChannelsRedirect(error="channel_in_pipeline")
    return ChannelsRedirect(msg="channel_deleted")


async def refresh_channel_types(request: Request) -> ChannelsRedirect:
    return await _enqueue_channel_command(request, "channels.refresh_types", {})


async def refresh_channel_meta(request: Request) -> ChannelsRedirect:
    return await _enqueue_channel_command(request, "channels.refresh_meta", {})


async def list_tags(request: Request) -> ChannelsJson:
    db = deps.get_db(request)
    tags = await db.repos.channels.list_all_tags()
    return ChannelsJson({"tags": tags})


async def create_tag(request: Request, name: str) -> ChannelsRedirect:
    if not name.strip():
        return ChannelsRedirect(error="missing_fields")
    db = deps.get_db(request)
    await db.repos.channels.create_tag(name.strip())
    return ChannelsRedirect(msg="tag_created")


async def delete_tag(request: Request, name: str) -> ChannelsJson:
    db = deps.get_db(request)
    await db.repos.channels.delete_tag(name)
    return ChannelsJson({"ok": True})


async def get_channel_tags(request: Request, pk: int) -> ChannelsJson:
    db = deps.get_db(request)
    tags = await db.repos.channels.get_channel_tags(pk)
    return ChannelsJson({"tags": tags})


async def set_channel_tags(request: Request, pk: int) -> ChannelsRedirect:
    db = deps.get_db(request)
    form = await request.form()
    await db.repos.channels.set_channel_tags(pk, parse_tags(form.get("tags", "")))
    return ChannelsRedirect(msg="tags_updated")
