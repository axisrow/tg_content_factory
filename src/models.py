from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, model_validator

from src.telegram.flood_wait import FloodWaitInfo


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
    about: str | None = None
    linked_chat_id: int | None = None
    has_comments: bool = False
    last_collected_id: int = 0
    added_at: datetime | None = None
    created_at: datetime | None = None
    message_count: int = 0
    tags: list[str] = []


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
    views: int | None = None
    forwards: int | None = None
    reply_count: int | None = None
    date: datetime
    collected_at: datetime | None = None
    detected_lang: str | None = None
    translation_en: str | None = None
    translation_custom: str | None = None
    forward_from_channel_id: int | None = None
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
    PIPELINE_RUN = "pipeline_run"
    CONTENT_GENERATE = "content_generate"
    CONTENT_PUBLISH = "content_publish"
    TRANSLATE_BATCH = "translate_batch"


class ContentGenerateTaskPayload(BaseModel):
    task_kind: str = "content_generate"
    pipeline_id: int


class ContentPublishTaskPayload(BaseModel):
    task_kind: str = "content_publish"
    pipeline_id: int | None = None


class StatsAllTaskPayload(BaseModel):
    task_kind: str = CollectionTaskType.STATS_ALL.value
    channel_ids: list[int]
    next_index: int = 0
    channels_ok: int = 0
    channels_err: int = 0


class SqStatsTaskPayload(BaseModel):
    task_kind: str = CollectionTaskType.SQ_STATS.value
    sq_id: int


class TranslateBatchTaskPayload(BaseModel):
    task_kind: str = "translate_batch"
    target_lang: str = "en"
    source_filter: list[str] = []
    batch_size: int = 20
    last_processed_id: int = 0


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
    payload: (
        dict[str, Any]
        | StatsAllTaskPayload
        | SqStatsTaskPayload
        | PipelineRunTaskPayload
        | ContentGenerateTaskPayload
        | ContentPublishTaskPayload
        | TranslateBatchTaskPayload
        | None
    ) = None
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


class PipelinePublishMode(StrEnum):
    AUTO = "auto"
    MODERATED = "moderated"


class PipelineGenerationBackend(StrEnum):
    CHAIN = "chain"
    AGENT = "agent"
    DEEP_AGENTS = "deep_agents"


class PipelineNodeType(StrEnum):
    SOURCE = "source"
    RETRIEVE_CONTEXT = "retrieve_context"
    LLM_GENERATE = "llm_generate"
    LLM_REFINE = "llm_refine"
    IMAGE_GENERATE = "image_generate"
    PUBLISH = "publish"
    NOTIFY = "notify"
    FILTER = "filter"
    DELAY = "delay"
    REACT = "react"
    FORWARD = "forward"
    DELETE_MESSAGE = "delete_message"
    FETCH_MESSAGES = "fetch_messages"
    CONDITION = "condition"
    SEARCH_QUERY_TRIGGER = "search_query_trigger"


class PipelineNode(BaseModel):
    id: str
    type: PipelineNodeType
    name: str
    config: dict[str, Any] = Field(default_factory=dict)
    position: dict[str, float] = Field(default_factory=lambda: {"x": 0.0, "y": 0.0})


class PipelineEdge(BaseModel):
    model_config = {"populate_by_name": True}

    from_node: str = Field(alias="from")
    to_node: str = Field(alias="to")
    condition: str | None = None


class PipelineGraph(BaseModel):
    nodes: list[PipelineNode] = Field(default_factory=list)
    edges: list[PipelineEdge] = Field(default_factory=list)

    def to_json(self) -> str:
        import json
        return json.dumps(
            {
                "nodes": [n.model_dump() for n in self.nodes],
                "edges": [
                    {"from": e.from_node, "to": e.to_node, **({"condition": e.condition} if e.condition else {})}
                    for e in self.edges
                ],
            },
            ensure_ascii=False,
        )

    @classmethod
    def from_json(cls, data: str | dict) -> "PipelineGraph":
        import json
        if isinstance(data, str):
            data = json.loads(data)
        nodes = [PipelineNode.model_validate(n) for n in data.get("nodes", [])]
        edges = [PipelineEdge.model_validate(e) for e in data.get("edges", [])]
        return cls(nodes=nodes, edges=edges)


class PipelineTemplate(BaseModel):
    id: int | None = None
    name: str
    description: str = ""
    category: str = ""
    template_json: PipelineGraph
    is_builtin: bool = False
    created_at: datetime | None = None


class ContentPipeline(BaseModel):
    id: int | None = None
    name: str
    prompt_template: str
    llm_model: str | None = None
    image_model: str | None = None
    publish_mode: PipelinePublishMode = PipelinePublishMode.MODERATED
    generation_backend: PipelineGenerationBackend = PipelineGenerationBackend.CHAIN
    is_active: bool = True
    last_generated_id: int = 0
    generate_interval_minutes: int = Field(60, ge=1)
    publish_times: str | None = None  # JSON array of "HH:MM" times, e.g. '["09:00", "18:00"]'
    refinement_steps: list[dict] = []  # list of {name, prompt} dicts; {text} in prompt is replaced
    pipeline_json: PipelineGraph | None = None  # node-based DAG config (issue #343)
    created_at: datetime | None = None


class PipelineSource(BaseModel):
    id: int | None = None
    pipeline_id: int
    channel_id: int
    created_at: datetime | None = None


class PipelineTarget(BaseModel):
    id: int | None = None
    pipeline_id: int
    phone: str
    dialog_id: int
    title: str | None = None
    dialog_type: str | None = None
    created_at: datetime | None = None


class PipelineRunTaskPayload(BaseModel):
    task_kind: str = CollectionTaskType.PIPELINE_RUN.value
    pipeline_id: int
    dry_run: bool = False
    since_hours: float = 24.0


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
    flood_wait: FloodWaitInfo | None = None


class GenerationRun(BaseModel):
    id: int | None = None
    pipeline_id: int | None = None
    status: str = "pending"
    prompt: str | None = None
    generated_text: str | None = None
    metadata: dict | None = None
    image_url: str | None = None
    moderation_status: str = "pending"
    quality_score: float | None = None
    quality_issues: list[str] | None = None
    variants: list[str] | None = None
    selected_variant: int | None = None
    published_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


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


class GeneratedImage(BaseModel):
    id: int | None = None
    prompt: str
    model: str | None = None
    image_url: str | None = None
    local_path: str | None = None
    created_at: str | None = None


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
