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
from typing import TYPE_CHECKING, AsyncIterator, Iterable

from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    JobRuntimeState,
    JobSource,
    JobView,
    PhotoAutoUploadJob,
    PhotoBatch,
    PhotoBatchItem,
    PhotoBatchStatus,
    PhotoBatchView,
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
    PhotoBatchStatus.HELD: JobRuntimeState.PENDING,
    PhotoBatchStatus.RUNNING: JobRuntimeState.RUNNING,
    PhotoBatchStatus.PENDING: JobRuntimeState.PENDING,
    PhotoBatchStatus.SCHEDULED: JobRuntimeState.SCHEDULED,
    PhotoBatchStatus.COMPLETED: JobRuntimeState.COMPLETED,
    PhotoBatchStatus.FAILED: JobRuntimeState.FAILED,
    PhotoBatchStatus.CANCELLED: JobRuntimeState.CANCELLED,
}
# An item counts as "processed" for the progress bar once it reaches any terminal
# state — succeeded OR failed OR cancelled — so the bar always reaches N/N (#1152).
_PHOTO_TERMINAL_STATUSES = frozenset(
    status
    for status, state in _PHOTO_ITEM_STATE.items()
    if state in {JobRuntimeState.COMPLETED, JobRuntimeState.FAILED, JobRuntimeState.CANCELLED}
)

# UTC-aware floor for jobs without a timestamp (e.g. scheduler jobs) so they sort
# last; kept aware to match ``normalize_utc`` keys in the sort below.
_NO_TIMESTAMP_SENTINEL = datetime.min.replace(tzinfo=timezone.utc)


def _future(dt: datetime | None, now: datetime) -> bool:
    return dt is not None and dt > now


# Per-source fetch bound used when a runtime_state filter is active (see list_jobs).
_FILTER_FETCH_CAP = 500
_PAGINATION_FETCH_BATCH = 500


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
        """Return the newest jobs up to ``limit`` (legacy non-paginated API)."""
        fetch_limit = limit if statuses is None else max(limit, _FILTER_FETCH_CAP)
        jobs = await self._collect_jobs(
            sources=sources,
            statuses=statuses,
            fetch_limit=fetch_limit,
            now=now,
        )
        return jobs[:limit]

    async def list_jobs_paginated(
        self,
        *,
        sources: Iterable[JobSource] | None = None,
        statuses: Iterable[JobRuntimeState] | None = None,
        page: int = 1,
        limit: int = 100,
        now: datetime | None = None,
    ) -> tuple[list[JobView], int]:
        """Return one globally sorted page without materializing full history tables."""
        page = max(1, page)
        limit = max(1, limit)
        now = now or datetime.now(timezone.utc)
        wanted_sources = set(sources) if sources is not None else None
        wanted_states = set(statuses) if statuses is not None else None
        offset = (page - 1) * limit

        paused, active_ids = await self._queue_runtime()
        scheduler_jobs = (
            await self._scheduler_jobs()
            if self._want(JobSource.SCHEDULER_JOB, wanted_sources)
            else []
        )
        total = (
            await self._count_unfiltered_jobs(wanted_sources, len(scheduler_jobs))
            if wanted_states is None
            else None
        )
        if total is not None and offset >= total:
            return [], total

        iterators = self._paginated_source_iterators(
            wanted_sources=wanted_sources,
            now=now,
            paused=paused,
            active_ids=active_ids,
            scheduler_jobs=scheduler_jobs,
        )
        heads: list[JobView | None] = []
        for iterator in iterators:
            heads.append(await anext(iterator, None))

        jobs: list[JobView] = []
        matched = 0
        while any(job is not None for job in heads):
            source_index = max(
                (index for index, job in enumerate(heads) if job is not None),
                key=lambda index: self._job_sort_key(heads[index]),
            )
            job = heads[source_index]
            heads[source_index] = await anext(iterators[source_index], None)
            if job is None or (wanted_states is not None and job.runtime_state not in wanted_states):
                continue
            if matched >= offset and len(jobs) < limit:
                jobs.append(job)
            matched += 1
            # Without a derived-state filter, SQL counts already provide the exact
            # total, so stop once this page is filled instead of scanning history.
            if total is not None and len(jobs) == limit:
                break

        return jobs, total if total is not None else matched

    @staticmethod
    def _job_sort_key(job: JobView | None) -> datetime:
        if job is None:
            return _NO_TIMESTAMP_SENTINEL
        return normalize_utc(job.created_at) or _NO_TIMESTAMP_SENTINEL

    async def _count_unfiltered_jobs(
        self,
        wanted_sources: set[JobSource] | None,
        scheduler_count: int,
    ) -> int:
        total = 0
        if self._want(JobSource.COLLECTION_TASK, wanted_sources):
            total += await self._db.repos.tasks.count_collection_tasks()
        if self._want(JobSource.TELEGRAM_COMMAND, wanted_sources):
            total += await self._db.repos.telegram_commands.count_commands()
        if self._want(JobSource.PHOTO_BATCH_ITEM, wanted_sources):
            total += await self._db.repos.photo_loader.count_items()
        if self._want(JobSource.PHOTO_AUTO_JOB, wanted_sources):
            total += await self._db.repos.photo_loader.count_auto_jobs()
        return total + scheduler_count

    def _paginated_source_iterators(
        self,
        *,
        wanted_sources: set[JobSource] | None,
        now: datetime,
        paused: bool,
        active_ids: set[int],
        scheduler_jobs: list[JobView],
    ) -> list[AsyncIterator[JobView]]:
        iterators: list[AsyncIterator[JobView]] = []
        if self._want(JobSource.COLLECTION_TASK, wanted_sources):
            iterators.append(self._iter_collection_jobs(now, paused, active_ids))
        if self._want(JobSource.TELEGRAM_COMMAND, wanted_sources):
            iterators.append(self._iter_telegram_jobs(now))
        if self._want(JobSource.PHOTO_BATCH_ITEM, wanted_sources):
            iterators.append(self._iter_photo_item_jobs())
        if self._want(JobSource.PHOTO_AUTO_JOB, wanted_sources):
            iterators.append(self._iter_photo_auto_jobs())
        if scheduler_jobs:
            iterators.append(self._iter_job_list(scheduler_jobs))
        return iterators

    async def _iter_collection_jobs(
        self,
        now: datetime,
        paused: bool,
        active_ids: set[int],
    ) -> AsyncIterator[JobView]:
        offset = 0
        while True:
            rows = await self._db.repos.tasks.get_collection_tasks(
                limit=_PAGINATION_FETCH_BATCH,
                offset=offset,
            )
            for task in rows:
                yield self._from_collection_task(task, now, paused, active_ids)
            if len(rows) < _PAGINATION_FETCH_BATCH:
                return
            offset += len(rows)

    async def _iter_telegram_jobs(self, now: datetime) -> AsyncIterator[JobView]:
        offset = 0
        while True:
            rows = await self._db.repos.telegram_commands.list_commands(
                limit=_PAGINATION_FETCH_BATCH,
                offset=offset,
            )
            for command in rows:
                yield self._from_telegram_command(command, now)
            if len(rows) < _PAGINATION_FETCH_BATCH:
                return
            offset += len(rows)

    async def _iter_photo_item_jobs(self) -> AsyncIterator[JobView]:
        offset = 0
        while True:
            rows = await self._db.repos.photo_loader.list_items(
                limit=_PAGINATION_FETCH_BATCH,
                offset=offset,
            )
            for item in rows:
                yield self._from_photo_item(item)
            if len(rows) < _PAGINATION_FETCH_BATCH:
                return
            offset += len(rows)

    async def _iter_photo_auto_jobs(self) -> AsyncIterator[JobView]:
        jobs = [self._from_photo_auto(job) for job in await self._db.repos.photo_loader.list_auto_jobs()]
        jobs.sort(key=self._job_sort_key, reverse=True)
        for job in jobs:
            yield job

    async def _iter_job_list(self, jobs: list[JobView]) -> AsyncIterator[JobView]:
        jobs.sort(key=self._job_sort_key, reverse=True)
        for job in jobs:
            yield job

    async def _collect_jobs(
        self,
        *,
        sources: Iterable[JobSource] | None,
        statuses: Iterable[JobRuntimeState] | None,
        fetch_limit: int,
        now: datetime | None,
    ) -> list[JobView]:
        now = now or datetime.now(timezone.utc)
        wanted_sources = set(sources) if sources is not None else None
        wanted_states = set(statuses) if statuses is not None else None

        paused, active_ids = await self._queue_runtime()
        jobs: list[JobView] = []

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
        return jobs

    async def list_photo_batches(self, *, limit: int = 50) -> list[PhotoBatchView]:
        batches = await self._db.repos.photo_loader.list_batches(limit=limit)
        return [await self._from_photo_batch(batch) for batch in batches]

    async def get_photo_batch(self, batch_id: int) -> PhotoBatchView | None:
        batch = await self._db.repos.photo_loader.get_batch(batch_id)
        if batch is None:
            return None
        return await self._from_photo_batch(batch)

    async def _from_photo_batch(self, batch: PhotoBatch) -> PhotoBatchView:
        counts = (
            await self._db.repos.photo_loader.count_items_by_batch_status(batch.id)
            if batch.id is not None
            else {}
        )
        # "Progress" means how many items reached a terminal state — not how many
        # SUCCEEDED. Counting only COMPLETED froze the bar at e.g. 2/3 forever once
        # any item FAILED, even though the batch was fully terminal (#1152 follow-up).
        return PhotoBatchView(
            **batch.model_dump(),
            completed_items=sum(counts.get(status, 0) for status in _PHOTO_TERMINAL_STATUSES),
            total_items=sum(counts.values()),
        )

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
