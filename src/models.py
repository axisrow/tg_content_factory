from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, model_validator


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
    is_premium: bool = False
    avatar_base64: str | None = None  # "data:image/jpeg;base64,..."


class Channel(BaseModel):
    id: int | None = None
    channel_id: int
    title: str | None = None
    username: str | None = None
    channel_type: str | None = None  # "channel"|"supergroup"|"gigagroup"|"group"|"unavailable"
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
    topic_id: int | None = None
    reactions_json: str | None = None
    date: datetime
    collected_at: datetime | None = None
    channel_title: str | None = None
    channel_username: str | None = None


class CollectionTaskStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class CollectionTaskType(StrEnum):
    CHANNEL_COLLECT = "channel_collect"
    STATS_ALL = "stats_all"
    SQ_STATS = "sq_stats"
    PHOTO_DUE = "photo_due"
    PHOTO_AUTO = "photo_auto"


class StatsAllTaskPayload(BaseModel):
    task_kind: str = CollectionTaskType.STATS_ALL.value
    channel_ids: list[int]
    next_index: int = 0
    batch_size: int = 20
    channels_ok: int = 0
    channels_err: int = 0


class SqStatsTaskPayload(BaseModel):
    task_kind: str = CollectionTaskType.SQ_STATS.value
    sq_id: int


class CollectionTask(BaseModel):
    id: int | None = None
    channel_id: int | None = None
    channel_title: str | None = None
    channel_username: str | None = None
    task_type: CollectionTaskType = CollectionTaskType.CHANNEL_COLLECT
    status: CollectionTaskStatus = CollectionTaskStatus.PENDING
    messages_collected: int = 0
    error: str | None = None
    note: str | None = None
    run_after: datetime | None = None
    payload: dict[str, Any] | StatsAllTaskPayload | SqStatsTaskPayload | None = None
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


class NotificationBot(BaseModel):
    id: int = 0
    tg_user_id: int
    tg_username: str | None = None
    bot_id: int | None = None
    bot_username: str
    bot_token: str
    created_at: datetime | None = None


class SearchQuery(BaseModel):
    id: int | None = None
    name: str = ""
    query: str
    is_regex: bool = False
    is_fts: bool = False
    is_active: bool = True
    notify_on_collect: bool = False
    track_stats: bool = True
    interval_minutes: int = Field(60, ge=1)
    exclude_patterns: str = ""
    max_length: int | None = None
    created_at: datetime | None = None

    @model_validator(mode="after")
    def check_mode_exclusive(self) -> "SearchQuery":
        if self.is_regex and self.is_fts:
            raise ValueError("is_regex and is_fts are mutually exclusive")
        return self

    @model_validator(mode="after")
    def default_name_to_query(self) -> "SearchQuery":
        if not self.name:
            self.name = self.query
        return self

    @property
    def exclude_patterns_list(self) -> list[str]:
        if not self.exclude_patterns:
            return []
        return [p.strip() for p in self.exclude_patterns.splitlines() if p.strip()]


class SearchQueryDailyStat(BaseModel):
    day: str  # "2026-03-07"
    count: int


class SearchResult(BaseModel):
    messages: list[Message]
    total: int
    query: str
    ai_summary: str | None = None
    error: str | None = None


class PhotoSendMode(StrEnum):
    ALBUM = "album"
    SEPARATE = "separate"


class PhotoBatchStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SCHEDULED = "scheduled"


class PhotoBatch(BaseModel):
    id: int | None = None
    phone: str
    target_dialog_id: int
    target_title: str | None = None
    target_type: str | None = None
    send_mode: PhotoSendMode = PhotoSendMode.ALBUM
    caption: str | None = None
    status: PhotoBatchStatus = PhotoBatchStatus.PENDING
    error: str | None = None
    created_at: datetime | None = None
    last_run_at: datetime | None = None


class PhotoBatchItem(BaseModel):
    id: int | None = None
    batch_id: int | None = None
    phone: str
    target_dialog_id: int
    target_title: str | None = None
    target_type: str | None = None
    file_paths: list[str]
    send_mode: PhotoSendMode = PhotoSendMode.ALBUM
    caption: str | None = None
    schedule_at: datetime | None = None
    status: PhotoBatchStatus = PhotoBatchStatus.PENDING
    error: str | None = None
    telegram_message_ids: list[int] = Field(default_factory=list)
    created_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


class PhotoAutoUploadJob(BaseModel):
    id: int | None = None
    phone: str
    target_dialog_id: int
    target_title: str | None = None
    target_type: str | None = None
    folder_path: str
    send_mode: PhotoSendMode = PhotoSendMode.ALBUM
    caption: str | None = None
    interval_minutes: int = Field(60, ge=1)
    is_active: bool = True
    error: str | None = None
    last_run_at: datetime | None = None
    last_seen_marker: str | None = None
    created_at: datetime | None = None
