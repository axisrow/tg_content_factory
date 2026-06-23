"""Unified jobs read-model (#963).

Aggregates the four heterogeneous background-work sources into one normalized
``JobView`` list so the panel (#964/#965) can render every job in a single
table. Read-only: it reads DB rows + runtime snapshots, never writes.

Sources:
- ``collection_tasks``     (CHANNEL_COLLECT + generic dispatcher tasks)
- ``telegram_commands``    (TelegramCommandDispatcher)
- ``photo_batch_items`` / ``photo_auto_upload_jobs`` (photo loader)
- APScheduler jobs from the ``scheduler_jobs`` runtime snapshot

Runtime state is derived deterministically from each row's status plus two
runtime snapshots (``collection_queue_status`` for the pause-gate, and each
source's own deferral signals for flood-wait), so the result is unit-testable
without a live worker.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Iterable

from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    JobRuntimeState,
    JobSource,
    JobView,
    PhotoAutoUploadJob,
    PhotoBatchItem,
    PhotoBatchStatus,
    TelegramCommand,
    TelegramCommandStatus,
)
from src.utils.datetime import normalize_utc

if TYPE_CHECKING:
    from src.database.facade import Database

# Terminal/1:1 status → runtime-state tables (the non-terminal cases that need
# runtime context — pause-gate, flood-wait, scheduled — stay explicit below).
_CT_TERMINAL = {
    CollectionTaskStatus.COMPLETED: JobRuntimeState.COMPLETED,
    CollectionTaskStatus.FAILED: JobRuntimeState.FAILED,
    CollectionTaskStatus.CANCELLED: JobRuntimeState.CANCELLED,
}
_TG_TERMINAL = {
    TelegramCommandStatus.SUCCEEDED: JobRuntimeState.COMPLETED,
    TelegramCommandStatus.FAILED: JobRuntimeState.FAILED,
    TelegramCommandStatus.CANCELLED: JobRuntimeState.CANCELLED,
}
_PHOTO_ITEM_STATE = {
    PhotoBatchStatus.RUNNING: JobRuntimeState.RUNNING,
    PhotoBatchStatus.PENDING: JobRuntimeState.PENDING,
    PhotoBatchStatus.SCHEDULED: JobRuntimeState.SCHEDULED,
    PhotoBatchStatus.COMPLETED: JobRuntimeState.COMPLETED,
    PhotoBatchStatus.FAILED: JobRuntimeState.FAILED,
    PhotoBatchStatus.CANCELLED: JobRuntimeState.CANCELLED,
}

# UTC-aware floor for jobs without a timestamp (e.g. scheduler jobs) so they sort
# last; kept aware to match ``normalize_utc`` keys in the sort below.
_NO_TIMESTAMP_SENTINEL = datetime.min.replace(tzinfo=timezone.utc)


def _future(dt: datetime | None, now: datetime) -> bool:
    return dt is not None and dt > now


# Per-source fetch bound used when a runtime_state filter is active (see list_jobs).
_FILTER_FETCH_CAP = 500


class JobsReadModel:
    def __init__(self, db: "Database") -> None:
        self._db = db

    async def list_jobs(
        self,
        *,
        sources: Iterable[JobSource] | None = None,
        statuses: Iterable[JobRuntimeState] | None = None,
        limit: int = 100,
        now: datetime | None = None,
    ) -> list[JobView]:
        now = now or datetime.now(timezone.utc)
        wanted_sources = set(sources) if sources is not None else None
        wanted_states = set(statuses) if statuses is not None else None

        paused, active_ids = await self._queue_runtime()
        jobs: list[JobView] = []

        # runtime_state is derived (not a DB column), so per-source state filtering
        # can't be pushed into SQL. When filtering, fetch a larger per-source batch
        # so matching rows aren't truncated away by the per-source limit before the
        # state filter runs (review on #963); the final slice still caps at `limit`.
        fetch_limit = limit if wanted_states is None else max(limit, _FILTER_FETCH_CAP)

        if self._want(JobSource.COLLECTION_TASK, wanted_sources):
            for task in await self._db.repos.tasks.get_collection_tasks(limit=fetch_limit):
                jobs.append(self._from_collection_task(task, now, paused, active_ids))
        if self._want(JobSource.TELEGRAM_COMMAND, wanted_sources):
            for cmd in await self._db.repos.telegram_commands.list_commands(limit=fetch_limit):
                jobs.append(self._from_telegram_command(cmd, now))
        if self._want(JobSource.PHOTO_BATCH_ITEM, wanted_sources):
            for item in await self._db.repos.photo_loader.list_items(limit=fetch_limit):
                jobs.append(self._from_photo_item(item))
        if self._want(JobSource.PHOTO_AUTO_JOB, wanted_sources):
            for auto in await self._db.repos.photo_loader.list_auto_jobs():
                jobs.append(self._from_photo_auto(auto))
        if self._want(JobSource.SCHEDULER_JOB, wanted_sources):
            jobs.extend(await self._scheduler_jobs())

        if wanted_states is not None:
            jobs = [j for j in jobs if j.runtime_state in wanted_states]

        # Newest activity first; jobs without timestamps (scheduler) sort last.
        # ``created_at`` mixes naive (SQLite ``datetime('now')``) and aware values
        # across sources; normalise every key to UTC-aware so ``sort`` never raises
        # ``TypeError`` on a naive-vs-aware comparison (the ``None``-sentinel is aware).
        jobs.sort(key=lambda j: (normalize_utc(j.created_at) or _NO_TIMESTAMP_SENTINEL), reverse=True)
        # ``limit`` is the unified cap; each source is also fetched with it as a
        # per-source bound, so the final slice honours the documented contract.
        return jobs[:limit]

    @staticmethod
    def _want(source: JobSource, wanted: set[JobSource] | None) -> bool:
        return wanted is None or source in wanted

    async def _queue_runtime(self) -> tuple[bool, set[int]]:
        snap = await self._db.repos.runtime_snapshots.get_snapshot("collection_queue_status")
        if snap is None:
            return False, set()
        payload = snap.payload or {}
        active = {int(i) for i in payload.get("active_task_ids", []) if i is not None}
        return bool(payload.get("paused", False)), active

    async def _scheduler_jobs(self) -> list[JobView]:
        snap = await self._db.repos.runtime_snapshots.get_snapshot("scheduler_jobs")
        if snap is None:
            return []
        # A scheduler job can be toggled off via the scheduler_job_disabled:<id>
        # setting; such jobs are still listed by get_potential_jobs but must show as
        # INACTIVE, not SCHEDULED (review on #963). One batched prefix read.
        disabled_map = await self._db.repos.settings.get_settings_by_prefix(
            "scheduler_job_disabled:"
        )
        out: list[JobView] = []
        for entry in (snap.payload or {}).get("jobs", []):
            job_id = str(entry.get("job_id", "?"))
            interval = entry.get("interval_minutes")
            disabled = disabled_map.get(f"scheduler_job_disabled:{job_id}") == "1"
            out.append(
                JobView(
                    source=JobSource.SCHEDULER_JOB,
                    id=f"scheduler_job:{job_id}",
                    job_type=job_id,
                    runtime_state=(
                        JobRuntimeState.INACTIVE if disabled else JobRuntimeState.SCHEDULED
                    ),
                    summary=f"every {interval}m" if interval is not None else "scheduled",
                    created_at=snap.updated_at,
                )
            )
        return out

    @staticmethod
    def _from_collection_task(
        task: CollectionTask, now: datetime, paused: bool, active_ids: set[int]
    ) -> JobView:
        status = task.status
        if status == CollectionTaskStatus.RUNNING:
            state = JobRuntimeState.RUNNING
        elif status == CollectionTaskStatus.PENDING:
            # active_ids (live queue snapshot) only *upgrades* a PENDING row to
            # RUNNING — the row's status hasn't flipped yet. It must NOT override a
            # terminal status: a COMPLETED/FAILED task whose id lingers in a stale
            # snapshot would otherwise show as RUNNING (review on #963).
            if task.id in active_ids:
                state = JobRuntimeState.RUNNING
            elif paused:
                state = JobRuntimeState.PAUSE_GATE
            elif _future(task.run_after, now):
                state = JobRuntimeState.SCHEDULED
            else:
                state = JobRuntimeState.PENDING
        else:
            state = _CT_TERMINAL[status]
        summary = task.channel_title or task.channel_username or task.task_type.value
        return JobView(
            source=JobSource.COLLECTION_TASK,
            id=f"collection_task:{task.id}",
            raw_id=task.id,
            job_type=task.task_type.value,
            status=status.value,
            runtime_state=state,
            summary=summary,
            run_after=task.run_after,
            created_at=task.created_at,
            started_at=task.started_at,
            finished_at=task.completed_at,
            error=task.error,
            note=task.note,
        )

    @staticmethod
    def _from_telegram_command(cmd: TelegramCommand, now: datetime) -> JobView:
        status = cmd.status
        if status == TelegramCommandStatus.RUNNING:
            state = JobRuntimeState.RUNNING
        elif status == TelegramCommandStatus.PENDING:
            if (cmd.result_payload or {}).get("state") == "waiting_flood_wait":
                state = JobRuntimeState.FLOOD_WAIT
            elif _future(cmd.run_after, now):
                state = JobRuntimeState.SCHEDULED
            else:
                state = JobRuntimeState.PENDING
        else:
            state = _TG_TERMINAL[status]
        return JobView(
            source=JobSource.TELEGRAM_COMMAND,
            id=f"telegram_command:{cmd.id}",
            raw_id=cmd.id,
            job_type=cmd.command_type,
            status=status.value,
            runtime_state=state,
            summary=cmd.command_type,
            run_after=cmd.run_after,
            created_at=cmd.created_at,
            started_at=cmd.started_at,
            finished_at=cmd.finished_at,
            error=cmd.error,
        )

    @staticmethod
    def _from_photo_item(item: PhotoBatchItem) -> JobView:
        state = _PHOTO_ITEM_STATE[item.status]
        count = len(item.file_paths)
        return JobView(
            source=JobSource.PHOTO_BATCH_ITEM,
            id=f"photo_batch_item:{item.id}",
            raw_id=item.id,
            job_type="photo_send",
            status=item.status.value,
            runtime_state=state,
            summary=f"{count} photo(s) → {item.target_title or item.target_dialog_id}",
            run_after=item.schedule_at,
            created_at=item.created_at,
            started_at=item.started_at,
            finished_at=item.completed_at,
            error=item.error,
        )

    @staticmethod
    def _from_photo_auto(auto: PhotoAutoUploadJob) -> JobView:
        state = JobRuntimeState.SCHEDULED if auto.is_active else JobRuntimeState.INACTIVE
        return JobView(
            source=JobSource.PHOTO_AUTO_JOB,
            id=f"photo_auto_job:{auto.id}",
            raw_id=auto.id,
            job_type="photo_auto",
            status="active" if auto.is_active else "inactive",
            runtime_state=state,
            summary=f"{auto.folder_path} every {auto.interval_minutes}m → "
            f"{auto.target_title or auto.target_dialog_id}",
            created_at=auto.created_at,
            started_at=auto.last_run_at,
            error=auto.error,
        )
