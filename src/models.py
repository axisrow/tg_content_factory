from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class Account(BaseModel):
    id: int | None = None
    phone: str
    session_string: str
    is_primary: bool = False
    is_active: bool = True
    is_premium: bool = False
    flood_wait_until: datetime | None = None
    created_at: datetime | None = None


class TelegramUserInfo(BaseModel):
    phone: str
    first_name: str = ""
    last_name: str = ""
    username: str | None = None
    is_primary: bool = False
    avatar_base64: str | None = None  # "data:image/jpeg;base64,..."


class Channel(BaseModel):
    id: int | None = None
    channel_id: int
    title: str | None = None
    username: str | None = None
    channel_type: str | None = None  # "channel" | "group"
    is_active: bool = True
    is_filtered: bool = False
    filter_flags: str = ""
    last_collected_id: int = 0
    added_at: datetime | None = None
    message_count: int = 0


class Message(BaseModel):
    id: int | None = None
    channel_id: int
    message_id: int
    sender_id: int | None = None
    sender_name: str | None = None
    text: str | None = None
    media_type: str | None = None
    date: datetime
    collected_at: datetime | None = None
    channel_title: str | None = None
    channel_username: str | None = None


class Keyword(BaseModel):
    id: int | None = None
    pattern: str
    is_regex: bool = False
    is_active: bool = True


class CollectionTask(BaseModel):
    id: int | None = None
    channel_id: int
    channel_title: str | None = None
    status: str = "pending"  # pending / running / completed / failed / cancelled
    messages_collected: int = 0
    error: str | None = None
    run_after: datetime | None = None
    payload: dict[str, Any] | None = None
    parent_task_id: int | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


class ChannelStats(BaseModel):
    id: int | None = None
    channel_id: int
    subscriber_count: int | None = None
    avg_views: float | None = None
    avg_reactions: float | None = None
    avg_forwards: float | None = None
    collected_at: datetime | None = None


class SearchResult(BaseModel):
    messages: list[Message]
    total: int
    query: str
    ai_summary: str | None = None
    error: str | None = None
