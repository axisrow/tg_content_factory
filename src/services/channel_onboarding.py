from __future__ import annotations

import inspect
import logging
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from src.models import Channel, StatsAllTaskPayload

logger = logging.getLogger(__name__)

CreateStatsTask = Callable[[StatsAllTaskPayload], Awaitable[int]]


def clean_channel_meta(meta: dict | None) -> dict:
    if not meta:
        return {"about": None, "linked_chat_id": None, "has_comments": False}
    about = meta.get("about")
    linked_chat_id = meta.get("linked_chat_id")
    if not isinstance(about, str):
        about = None
    if isinstance(linked_chat_id, bool) or not isinstance(linked_chat_id, int):
        linked_chat_id = None
    has_comments = meta.get("has_comments")
    if not isinstance(has_comments, bool):
        has_comments = linked_chat_id is not None
    return {
        "about": about,
        "linked_chat_id": linked_chat_id,
        "has_comments": has_comments,
    }


async def fetch_channel_meta(pool: Any, channel_id: int, channel_type: str | None) -> dict | None:
    fetcher = getattr(pool, "fetch_channel_meta", None)
    if not callable(fetcher):
        return None
    try:
        return await fetcher(channel_id, channel_type)
    except Exception as exc:
        logger.warning("Failed to fetch channel metadata for %s: %s", channel_id, exc)
        return None


def channel_from_resolved_info(info: dict[str, Any], meta: dict | None = None) -> Channel:
    clean_meta = clean_channel_meta(meta)
    return Channel(
        channel_id=int(info["channel_id"]),
        title=info.get("title"),
        username=info.get("username"),
        channel_type=info.get("channel_type"),
        is_active=not info.get("deactivate", False),
        about=clean_meta["about"],
        linked_chat_id=clean_meta["linked_chat_id"],
        has_comments=clean_meta["has_comments"],
        created_at=info.get("created_at"),
    )


def channel_with_meta(channel: Channel, meta: dict | None = None) -> Channel:
    clean_meta = clean_channel_meta(meta)
    if clean_meta["about"] is None and clean_meta["linked_chat_id"] is None and not clean_meta["has_comments"]:
        return channel
    return channel.model_copy(
        update={
            "about": clean_meta["about"],
            "linked_chat_id": clean_meta["linked_chat_id"],
            "has_comments": clean_meta["has_comments"],
        }
    )


async def get_existing_channel(store: Any, channel_id: int) -> Channel | None:
    for name in ("get_by_channel_id", "get_channel_by_channel_id"):
        getter = getattr(store, name, None)
        if not callable(getter):
            continue
        try:
            result = getter(channel_id)
            if inspect.isawaitable(result):
                return await result
        except (AttributeError, TypeError):
            continue
    return None


async def enqueue_stats_for_new_channels(
    create_stats_task: CreateStatsTask | None,
    channel_ids: Iterable[int],
    *,
    context: str,
) -> int | None:
    if create_stats_task is None:
        return None
    unique_ids = list(dict.fromkeys(int(channel_id) for channel_id in channel_ids))
    if not unique_ids:
        return None
    try:
        return await create_stats_task(StatsAllTaskPayload(channel_ids=unique_ids))
    except Exception as exc:
        logger.warning("Failed to enqueue stats task after %s: %s", context, exc)
        return None
