"""REST response schemas for pipeline endpoints (#1070).

Mirrors the JSON payloads built in ``src/web/pipelines/handlers.py``. Routes
return ``JSONResponse`` directly, so these models document — they do not
serialize.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ChannelSearchItem(BaseModel):
    """One entry in the searchable channel picker (``GET /pipelines/api/channels/search``)."""

    value: int = Field(..., description="Telegram channel id.")
    title: str
    username: str = Field("", description="Channel @username, empty when none.")
    group: str = Field("channel", description="Picker group label.")


class PipelineDetailResponse(BaseModel):
    """Pipeline configuration detail (``GET /pipelines/{id}/show``)."""

    id: int
    name: str
    is_active: bool
    llm_model: str | None = None
    publish_mode: str
    generation_backend: str
    generate_interval_minutes: int
    source_ids: list[int] = Field(default_factory=list)
    source_titles: list[str] = Field(default_factory=list)
    target_refs: list[Any] = Field(default_factory=list)


class PipelineRunSummary(BaseModel):
    """Compact run row used in run-history and queue listings."""

    id: int
    pipeline_id: int
    status: str
    moderation_status: str
    created_at: str | None = None
    published_at: str | None = None


class PipelineRunsResponse(BaseModel):
    """Run history for a pipeline (``GET /pipelines/{id}/runs``)."""

    pipeline_id: int
    runs: list[PipelineRunSummary] = Field(default_factory=list)


class PipelineRunDetailResponse(PipelineRunSummary):
    """Full run detail (``GET /pipelines/{id}/runs/{run_id}``)."""

    generated_text: str | None = None
    image_url: str | None = Field(None, description="Image URL; re-signed for S3-backed media.")
    quality_score: float | None = None


class PipelineQueueItem(PipelineRunSummary):
    """A run awaiting moderation, with a short text preview."""

    preview: str = Field("", description="First 100 chars of the generated text.")


class PipelineQueueResponse(BaseModel):
    """Moderation queue for a pipeline (``GET /pipelines/{id}/queue``)."""

    pipeline_id: int
    queue: list[PipelineQueueItem] = Field(default_factory=list)


class PipelineTemplateItem(BaseModel):
    """A pipeline template definition (``GET /pipelines/templates/json``)."""

    id: int | None = None
    name: str
    description: str | None = None
    category: str | None = None
    template_json: dict[str, Any] = Field(
        default_factory=dict,
        description="Serialized PipelineGraph (nodes/edges).",
    )
