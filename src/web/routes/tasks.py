"""Interop task REST API (#961, part of #829).

Lets an external tg_messenger worker create, poll, atomically claim, and report
back on interop tasks (dm_reply / chat_answer / fetch_dialogs / fetch_history).

Auth: mounted behind the existing web auth middleware (HTTP Basic with WEB_PASS
for non-browser clients), so no separate gate here.

Gating: only EXTERNAL_INTEROP_TASK_TYPES may be created or claimed through this
API. The factory's own task types stay internal — an external worker can neither
inject them nor steal them.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field, field_validator

from src.models import EXTERNAL_INTEROP_TASK_TYPES, CollectionTaskStatus, CollectionTaskType
from src.utils.json import safe_json_dumps
from src.web import deps

router = APIRouter()

_ALLOWED_VALUES = {t.value for t in EXTERNAL_INTEROP_TASK_TYPES}
# Cap JSON bodies so an authenticated client can't bloat the SQLite TEXT column
# (or stress JSON (de)serialization) with a multi-MB payload (#961 review).
_MAX_PAYLOAD_BYTES = 64 * 1024


def _check_payload_size(value: dict) -> dict:
    if len(safe_json_dumps(value).encode("utf-8")) > _MAX_PAYLOAD_BYTES:
        raise ValueError(f"payload exceeds {_MAX_PAYLOAD_BYTES} bytes")
    return value


class CreateTaskRequest(BaseModel):
    type: str
    payload: dict = Field(default_factory=dict)

    _size = field_validator("payload")(_check_payload_size)


class ClaimRequest(BaseModel):
    types: list[str] = Field(default_factory=list)


class CompleteRequest(BaseModel):
    result_payload: dict = Field(default_factory=dict)

    _size = field_validator("result_payload")(_check_payload_size)


class FailRequest(BaseModel):
    error: str


def _require_external(type_value: str) -> CollectionTaskType:
    if type_value not in _ALLOWED_VALUES:
        raise HTTPException(
            status_code=403,
            detail=f"Task type '{type_value}' is not claimable via the interop API",
        )
    return CollectionTaskType(type_value)


def _task_json(task) -> dict:
    return task.model_dump(mode="json")


@router.post("")
async def create_task(request: Request, body: CreateTaskRequest):
    task_type = _require_external(body.type)
    tasks = deps.get_db(request).repos.tasks
    task_id = await tasks.create_generic_task(task_type, payload=body.payload)
    return JSONResponse({"id": task_id}, status_code=201)


@router.post("/claim")
async def claim_task(request: Request, body: ClaimRequest):
    requested = body.types or list(_ALLOWED_VALUES)
    for type_value in requested:
        _require_external(type_value)
    tasks = deps.get_db(request).repos.tasks
    # Types are already gated to the external allow-list above; the dispatcher's
    # HANDLED_TYPES never includes them, so this claim only ever races other
    # external workers, not the factory's own dispatcher.
    task = await tasks.claim_next_due_generic_task(datetime.now(timezone.utc), requested)
    if task is None:
        return Response(status_code=204)
    return JSONResponse(_task_json(task))


@router.post("/requeue-running")
async def requeue_running(request: Request):
    """Recover orphaned interop tasks stuck in RUNNING — e.g. the external worker
    crashed mid-task. The factory's own dispatcher never owns these types, so it
    won't requeue them on its startup; the external worker calls this on boot to
    reset its interop tasks RUNNING→PENDING so they can be re-claimed (#961 review)."""
    tasks = deps.get_db(request).repos.tasks
    count = await tasks.requeue_running_generic_tasks_on_startup(
        datetime.now(timezone.utc), list(_ALLOWED_VALUES)
    )
    return JSONResponse({"requeued": count})


async def _load_external_task(tasks, task_id: int):
    """Fetch a task and gate it to the external interop allow-list.

    Without this, an authenticated external worker could read or complete the
    factory's *internal* tasks (channel_collect, pipeline_run, …) by id — info
    disclosure + lifecycle poisoning (review on #961).
    """
    task = await tasks.get_collection_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    _require_external(task.task_type.value)
    return task


@router.get("/{task_id}")
async def get_task(request: Request, task_id: int):
    tasks = deps.get_db(request).repos.tasks
    task = await _load_external_task(tasks, task_id)
    return JSONResponse(_task_json(task))


@router.post("/{task_id}/complete")
async def complete_task(request: Request, task_id: int, body: CompleteRequest):
    tasks = deps.get_db(request).repos.tasks
    await _load_external_task(tasks, task_id)
    # required_status=RUNNING: only a claimed (RUNNING) task may be completed, so an
    # external worker can't skip the atomic claim or replay-complete a finished task.
    updated = await tasks.update_collection_task(
        task_id,
        CollectionTaskStatus.COMPLETED,
        result_payload=body.result_payload,
        required_status=CollectionTaskStatus.RUNNING,
    )
    if not updated:
        raise HTTPException(status_code=409, detail="Task is not in RUNNING state")
    return JSONResponse({"ok": True})


@router.post("/{task_id}/fail")
async def fail_task(request: Request, task_id: int, body: FailRequest):
    tasks = deps.get_db(request).repos.tasks
    await _load_external_task(tasks, task_id)
    updated = await tasks.update_collection_task(
        task_id,
        CollectionTaskStatus.FAILED,
        error=body.error,
        required_status=CollectionTaskStatus.RUNNING,
    )
    if not updated:
        raise HTTPException(status_code=409, detail="Task is not in RUNNING state")
    return JSONResponse({"ok": True})
