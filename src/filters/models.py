from __future__ import annotations

from pydantic import BaseModel, Field


class ChannelFilterResult(BaseModel):
    channel_id: int
    title: str | None = None
    username: str | None = None
    message_count: int = 0
    flags: list[str] = Field(default_factory=list)
    uniqueness_pct: float | None = None
    subscriber_ratio: float | None = None
    cyrillic_pct: float | None = None
    short_msg_pct: float | None = None
    cross_dupe_pct: float | None = None
    is_filtered: bool = False


class FilterReport(BaseModel):
    results: list[ChannelFilterResult] = Field(default_factory=list)
    total_channels: int = 0
    filtered_count: int = 0
