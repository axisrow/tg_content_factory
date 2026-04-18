"""Shared factories for pipeline-run related test fixtures.

Used across executor, dispatcher, CLI, web route, and agent tool tests so that
scenario drift cannot make the same "run" mean different things in different
layers (issue #463).

All factories produce real Pydantic models (not SimpleNamespace) so type/field
drift surfaces as a test failure rather than silently passing.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    GenerationRun,
    PipelineRunTaskPayload,
)
from src.services.pipeline_result import (
    RESULT_KIND_GENERATED_ITEMS,
    RESULT_KIND_PROCESSED_MESSAGES,
)


def _default_created_at() -> datetime:
    return datetime(2026, 4, 19, 12, 0, 0, tzinfo=timezone.utc)


def _build_citations(count: int) -> list[dict[str, Any]]:
    return [
        {
            "channel_id": -1000 - i,
            "channel_title": f"Channel {i}",
            "message_id": 100 + i,
            "date": "2026-04-19T10:00:00+00:00",
        }
        for i in range(count)
    ]


def make_generation_run(
    *,
    run_id: int | None = 1,
    pipeline_id: int = 1,
    text: str = "Generated content",
    citations_count: int = 2,
    status: str = "completed",
    moderation_status: str = "pending",
    quality_score: float | None = None,
    created_at: datetime | None = None,
) -> GenerationRun:
    """Factory for a *pure generation* run — result_kind=generated_items."""
    citations = _build_citations(citations_count)
    metadata: dict[str, Any] = {
        "citations": citations,
        "result_kind": RESULT_KIND_GENERATED_ITEMS,
        "result_count": citations_count,
    }
    ts = created_at or _default_created_at()
    return GenerationRun(
        id=run_id,
        pipeline_id=pipeline_id,
        status=status,
        generated_text=text,
        metadata=metadata,
        moderation_status=moderation_status,
        quality_score=quality_score,
        created_at=ts,
        updated_at=ts,
    )


def make_action_only_run(
    *,
    run_id: int | None = 1,
    pipeline_id: int = 1,
    action_counts: dict[str, int] | None = None,
    status: str = "completed",
    moderation_status: str = "pending",
    created_at: datetime | None = None,
) -> GenerationRun:
    """Factory for an *action-only* run (no text, no citations).

    result_kind=processed_messages, result_count=sum(action_counts).
    """
    counts = dict(action_counts or {"react": 3})
    total = sum(max(0, int(v)) for v in counts.values())
    metadata: dict[str, Any] = {
        "citations": [],
        "action_counts": counts,
        "result_kind": RESULT_KIND_PROCESSED_MESSAGES,
        "result_count": total,
    }
    ts = created_at or _default_created_at()
    return GenerationRun(
        id=run_id,
        pipeline_id=pipeline_id,
        status=status,
        generated_text="",
        metadata=metadata,
        moderation_status=moderation_status,
        created_at=ts,
        updated_at=ts,
    )


def make_mixed_run(
    *,
    run_id: int | None = 1,
    pipeline_id: int = 1,
    text: str = "Generated with actions",
    citations_count: int = 2,
    action_counts: dict[str, int] | None = None,
    status: str = "completed",
    moderation_status: str = "pending",
    created_at: datetime | None = None,
) -> GenerationRun:
    """Factory for a *mixed* run (generation + actions).

    Per issue #463 decision: generation semantics wins (result_kind=generated_items,
    result_count=citations_count), but metadata.action_counts is ALWAYS present so
    UI/tests can surface both dimensions.
    """
    counts = dict(action_counts or {"react": 4})
    citations = _build_citations(citations_count)
    metadata: dict[str, Any] = {
        "citations": citations,
        "action_counts": counts,
        "result_kind": RESULT_KIND_GENERATED_ITEMS,
        "result_count": citations_count,
    }
    ts = created_at or _default_created_at()
    return GenerationRun(
        id=run_id,
        pipeline_id=pipeline_id,
        status=status,
        generated_text=text,
        metadata=metadata,
        moderation_status=moderation_status,
        created_at=ts,
        updated_at=ts,
    )


def make_pipeline_run_task(
    *,
    task_id: int | None = 1,
    pipeline_id: int = 1,
    run_id: int | None = 1,
    status: CollectionTaskStatus = CollectionTaskStatus.COMPLETED,
    messages_collected: int = 0,
    note: str | None = None,
) -> CollectionTask:
    """Factory for a CollectionTask representing a completed pipeline_run."""
    effective_note = note if note is not None else (
        f"Pipeline run id={run_id}" if run_id is not None else None
    )
    return CollectionTask(
        id=task_id,
        task_type=CollectionTaskType.PIPELINE_RUN,
        status=status,
        payload=PipelineRunTaskPayload(pipeline_id=pipeline_id),
        messages_collected=messages_collected,
        note=effective_note,
        created_at=_default_created_at(),
        completed_at=_default_created_at(),
    )
