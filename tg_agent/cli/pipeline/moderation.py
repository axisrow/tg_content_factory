"""Pipeline CLI moderation operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tg_agent.cli import runtime
from tg_agent.cli.commands.common import cli_error

if TYPE_CHECKING:
    from tg_agent.telegram.client_pool import ClientPool
from tg_agent.cli.pipeline import requests, responses
from tg_agent.cli.pipeline.context import PipelineServices, default_services
from tg_agent.services.pipeline_service import (
    PipelineService,
)


async def _pipeline_queue(args: requests.PipelineQueueCommand, db, config, svc: PipelineService) -> None:
    pipeline = await svc.get(args.id)
    if pipeline is None:
        cli_error(f"Pipeline id={args.id} not found")
    runs = await db.repos.generation_runs.list_pending_moderation(
        pipeline_id=args.id,
        limit=args.limit,
    )
    return responses.render_pipeline_queue(args, runs)


async def _pipeline_moderation_list(
    args: requests.PipelineModerationListCommand, db, config, svc: PipelineService
) -> None:
    runs = await db.repos.generation_runs.list_pending_moderation(
        pipeline_id=args.pipeline_id,
        limit=args.limit,
    )
    return responses.render_pipeline_moderation_list(runs)


async def _pipeline_moderation_view(
    args: requests.PipelineModerationViewCommand, db, config, svc: PipelineService
) -> None:
    run = await db.repos.generation_runs.get(args.run_id)
    return responses.render_pipeline_moderation_view(args, run)


async def _pipeline_approve(args: requests.PipelineApproveCommand, db, config, svc: PipelineService) -> None:
    run = await db.repos.generation_runs.get(args.run_id)
    if run is None:
        cli_error(f"Run id={args.run_id} not found")
    await db.repos.generation_runs.set_moderation_status(args.run_id, "approved", origin="human", actor="cli")
    print(f"Approved run id={args.run_id}")


async def _pipeline_reject(args: requests.PipelineRejectCommand, db, config, svc: PipelineService) -> None:
    run = await db.repos.generation_runs.get(args.run_id)
    if run is None:
        cli_error(f"Run id={args.run_id} not found")
    await db.repos.generation_runs.set_moderation_status(args.run_id, "rejected", origin="human", actor="cli")
    print(f"Rejected run id={args.run_id}")


async def _pipeline_resolve_unconfirmed(
    args: requests.PipelineResolveUnconfirmedCommand, db, config, svc: PipelineService
) -> None:
    run = await db.repos.generation_runs.get(args.run_id)
    if run is None:
        cli_error(f"Run id={args.run_id} not found")
    delivered = args.action == "delivered"
    ok = await db.repos.generation_runs.resolve_unconfirmed_target(args.run_id, args.target, delivered=delivered)
    if not ok:
        cli_error(f"Target {args.target} is not unconfirmed for run id={args.run_id}")
    print(f"Resolved unconfirmed target {args.target} as {args.action} for run id={args.run_id}")


async def _bulk_moderate(
    args: requests.PipelineBulkApproveCommand | requests.PipelineBulkRejectCommand, db, status: str, label: str
) -> None:
    # Atomic batch (#1041): resolve which ids exist (preserving the
    # skip-missing behaviour), then flip them all in one transaction
    # so a mid-batch failure rolls back instead of half-applying.
    existing_ids: list[int] = []
    missing_ids: list[int] = []
    for run_id in args.run_ids:
        run = await db.repos.generation_runs.get(run_id)
        if run is None:
            missing_ids.append(run_id)
            continue
        existing_ids.append(run_id)
    skipped = await db.repos.generation_runs.set_moderation_status_bulk(
        existing_ids, status, origin="human", actor="cli"
    )
    # An operator (human-origin) decision is refused only for runs already
    # publishing/published (#1234) — report them instead of counting them.
    print(f"Bulk {label}: {len(existing_ids) - len(skipped)}/{len(args.run_ids)}")
    if missing_ids or skipped:
        # The summary stays on stdout; what was left undone goes to stderr with
        # exit 1, so a caller never takes a partial batch for a complete one (#1951).
        reasons = [f"run id={run_id} not found" for run_id in missing_ids]
        reasons += [f"run id={run_id} is publishing/published" for run_id in skipped]
        cli_error(f"{len(reasons)} of {len(args.run_ids)} run(s) were not {label}: " + "; ".join(reasons))


async def _pipeline_bulk_approve(args: requests.PipelineBulkApproveCommand, db, config, svc: PipelineService) -> None:
    await _bulk_moderate(args, db, "approved", "approved")


async def _pipeline_bulk_reject(args: requests.PipelineBulkRejectCommand, db, config, svc: PipelineService) -> None:
    await _bulk_moderate(args, db, "rejected", "rejected")


async def _pipeline_publish(
    args: requests.PipelinePublishCommand, db, config, svc: PipelineService, *, services: PipelineServices | None = None
) -> ClientPool | None:
    services = services or default_services()
    run = await db.repos.generation_runs.get(args.run_id)
    if run is None:
        cli_error(f"Run id={args.run_id} not found")
    if run.pipeline_id is None:
        cli_error(f"Run id={args.run_id} has no pipeline")

    pipeline = await svc.get(run.pipeline_id)
    if pipeline is None:
        cli_error(f"Pipeline id={run.pipeline_id} not found")

    _, pool = await runtime.init_pool(config, db)
    if not pool.clients:
        # The dispatcher disconnects only a pool a handler returns; a failure
        # leaves by exception, so the pool is closed here.
        await pool.disconnect_all()
        cli_error("Нет доступных аккаунтов Telegram.")

    publish_service = services.publish_service(db, pool)
    results = await publish_service.publish_run(run, pipeline)
    if not results or not all(result.success for result in results):
        failed = "\n".join(
            f"  - {result.error or 'Unknown publish error'}" for result in results if not result.success
        )
        await pool.disconnect_all()
        cli_error(f"Failed to publish run id={args.run_id}" + (f"\n{failed}" if failed else ""))

    print(f"Published run id={args.run_id} to {len(results)} target(s)")
    for r in results:
        if r.message_id is not None:
            phone_part = f" phone={r.phone}" if r.phone else ""
            dialog_part = f" dialog_id={r.dialog_id}" if r.dialog_id is not None else ""
            print(f"  published_message_id={r.message_id}{phone_part}{dialog_part}")
    return pool


HANDLERS = {
    "queue": _pipeline_queue,
    "moderation-list": _pipeline_moderation_list,
    "moderation-view": _pipeline_moderation_view,
    "approve": _pipeline_approve,
    "reject": _pipeline_reject,
    "resolve-unconfirmed": _pipeline_resolve_unconfirmed,
    "bulk-approve": _pipeline_bulk_approve,
    "bulk-reject": _pipeline_bulk_reject,
    "publish": _pipeline_publish,
}
