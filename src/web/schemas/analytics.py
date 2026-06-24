"""REST response schemas for analytics endpoints (#1070).

Field names mirror the JSON keys produced by ``src/web/routes/analytics.py``
(``dataclasses.asdict`` of the analytics dataclasses, ``model_dump`` of Pydantic
models, or hand-built dicts / raw SQL rows). Nullable where the source query can
return ``NULL``.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

# ── Content ──────────────────────────────────────────────────────────────


class ContentSummary(BaseModel):
    """Aggregate generation/publication counters across all pipelines."""

    total_generations: int
    total_published: int
    total_pending: int
    total_rejected: int
    pipelines_count: int


class ContentTypeStat(BaseModel):
    """Engagement grouped by media/content type."""

    content_type: str = Field(..., description="Media type, or 'text' for plain messages.")
    message_count: int
    avg_reactions: float


class PipelineStat(BaseModel):
    """Per-pipeline generation/publication statistics."""

    pipeline_id: int
    pipeline_name: str
    total_generations: int
    total_published: int
    total_rejected: int
    pending_moderation: int
    success_rate: float


class DailyStat(BaseModel):
    """Daily generation/publication/rejection counts."""

    date: str = Field(..., description="Day in YYYY-MM-DD form.")
    generations: int
    publications: int
    rejections: int


# ── Trends ───────────────────────────────────────────────────────────────


class TrendingTopicItem(BaseModel):
    keyword: str
    count: int


class TrendingChannelItem(BaseModel):
    channel_id: int
    title: str | None = None
    username: str | None = None
    avg_views: float
    message_count: int


class TrendingEmojiItem(BaseModel):
    emoji: str
    count: int


# ── Channel analytics ────────────────────────────────────────────────────


class ChannelOverviewResponse(BaseModel):
    """Summary card for a single channel (``ChannelOverview`` dataclass)."""

    channel_id: int
    title: str | None = None
    username: str | None = None
    subscriber_count: int | None = None
    subscriber_delta: int | None = None
    subscriber_delta_week: int | None = None
    subscriber_delta_month: int | None = None
    err: float | None = None
    err24: float | None = None
    total_posts: int = 0
    posts_today: int = 0
    posts_week: int = 0
    posts_month: int = 0
    avg_views: float | None = None
    avg_forwards: float | None = None
    avg_reactions: float | None = None


class ChannelRatingItem(BaseModel):
    """One channel quality rating (``ChannelRating`` model, dumped as JSON)."""

    channel_id: int
    title: str | None = None
    username: str | None = None
    useful: str = Field(..., description="'useful' or 'useless'.")
    genre: str = Field(..., description="ad | infobiz | aggregator | copy | original.")
    confidence: float = 0.0
    reason: str | None = None
    emoji_trash_score: float | None = None
    flag_count: int = 0
    n_total: int = 0
    updated_at: datetime | None = None


# ── Messages / misc ──────────────────────────────────────────────────────


class TopMessageItem(BaseModel):
    """Top message by total reactions (raw joined row)."""

    id: int = Field(..., description="DB primary key of the message row.")
    channel_id: int
    message_id: int = Field(..., description="Telegram message id.")
    text: str | None = None
    media_type: str | None = None
    date: str | None = None
    reactions_json: str | None = None
    channel_title: str | None = None
    channel_username: str | None = None
    total_reactions: int = 0


class HourlyMessageItem(BaseModel):
    """Message distribution by hour-of-day."""

    hour: int = Field(..., ge=0, le=23)
    message_count: int
    avg_reactions: float


class MessageVelocityItem(BaseModel):
    """Daily message count over a period."""

    date: str = Field(..., description="Day in YYYY-MM-DD form.")
    count: int


class PeakHourItem(BaseModel):
    """Message count per hour-of-day over a period."""

    hour: int = Field(..., ge=0, le=23)
    count: int
