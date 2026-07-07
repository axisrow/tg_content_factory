"""Shared helper for enqueuing scheduler control commands from web routes.

In web-mode ``deps.get_scheduler`` returns ``SnapshotSchedulerManager`` — a
read-only shim whose ``sync_*``/``update_interval`` methods are no-ops (the live
``SchedulerManager`` lives in the separate worker container). The only way a web
mutation reaches the running scheduler is by enqueuing a ``scheduler.reconcile``
telegram command, which the worker's ``_handle_scheduler_reconcile`` picks up and
which re-registers search-query/pipeline jobs and re-reads the collect interval.

``scheduler/handlers.py`` already does this for its own routes; this helper lets
the search-query, pipeline, and settings domains do the same without importing
across handler modules. ``TelegramCommandService.enqueue`` deduplicates on
``(command_type, payload)``, so repeated reconcile requests collapse into one
pending command instead of piling up a backlog.
"""

from __future__ import annotations

from fastapi import Request

from src.web import deps


async def enqueue_scheduler_reconcile(request: Request, *, requested_by: str) -> int:
    """Enqueue a ``scheduler.reconcile`` command; return its command id."""
    return await deps.telegram_command_service(request).enqueue(
        "scheduler.reconcile",
        payload={},
        requested_by=requested_by,
    )
