from __future__ import annotations

import logging

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.services.publish_service import PublishService  # noqa: F401
from src.web import deps
from src.web.query_params import parse_clamped_int, parse_optional_int

logger = logging.getLogger(__name__)
router = APIRouter()


def _moderation_redirect(code: str, *, error: bool = False) -> RedirectResponse:
    key = "error" if error else "msg"
    return RedirectResponse(url=f"/moderation?{key}={code}", status_code=303)


@router.get("/", response_class=HTMLResponse)
async def moderation_queue_page(request: Request):
    # Lazyload skeleton (#948): the heavy queue (pending-moderation table +
    # pipeline relations) loads via the /moderation/fragments/table fragment.
    return deps.get_templates(request).TemplateResponse(request, "moderation.html", {})


@router.get("/fragments/table", response_class=HTMLResponse)
async def moderation_table_fragment(
    request: Request,
    pipeline_id: str | None = None,
    limit: str | None = None,
    offset: str | None = None,
):
    # The filter form submits empty values (?pipeline_id=) and pagination links
    # may carry "None" — parse leniently instead of letting FastAPI 422 (#779).
    parsed_pipeline_id = parse_optional_int(pipeline_id)
    parsed_limit = parse_clamped_int(limit, default=50, minimum=1, maximum=200)
    parsed_offset = max(0, parse_optional_int(offset, 0) or 0)

    db = deps.get_db(request)
    pending_runs = await db.repos.generation_runs.list_pending_moderation(
        pipeline_id=parsed_pipeline_id,
        limit=parsed_limit,
        offset=parsed_offset,
    )

    pipelines_svc = deps.pipeline_service(request)
    pipelines = await pipelines_svc.get_with_relations()

    return deps.get_templates(request).TemplateResponse(
        request,
        "moderation_content.html",
        {
            "pending_runs": pending_runs,
            "pipelines": pipelines,
            "selected_pipeline_id": parsed_pipeline_id,
            "limit": parsed_limit,
            "offset": parsed_offset,
        },
    )


@router.get("/{run_id}/view", response_class=HTMLResponse)
async def view_run(request: Request, run_id: int):
    db = deps.get_db(request)
    run = await db.repos.generation_runs.get(run_id)
    if run is None:
        return _moderation_redirect("run_not_found", error=True)

    # Re-sign the S3 image URL so a run viewed >7 days after generation doesn't
    # render a dead/403 presigned link (#869/#873/#874). Passes through non-S3 URLs.
    from src.services.s3_store import refresh_s3_url

    image_url = await refresh_s3_url(run.image_url)
    return deps.get_templates(request).TemplateResponse(
        request,
        "moderation/view.html",
        {"run": run, "image_url": image_url},
    )


@router.post("/{run_id}/approve")
async def approve_run(request: Request, run_id: int):
    db = deps.get_db(request)
    run = await db.repos.generation_runs.get(run_id)
    if run is None:
        return _moderation_redirect("run_not_found", error=True)

    await db.repos.generation_runs.set_moderation_status(run_id, "approved")
    return _moderation_redirect("run_approved")


@router.post("/{run_id}/reject")
async def reject_run(request: Request, run_id: int):
    db = deps.get_db(request)
    run = await db.repos.generation_runs.get(run_id)
    if run is None:
        return _moderation_redirect("run_not_found", error=True)

    await db.repos.generation_runs.set_moderation_status(run_id, "rejected")
    return _moderation_redirect("run_rejected")


@router.post("/{run_id}/publish")
async def publish_run(request: Request, run_id: int):
    db = deps.get_db(request)
    run = await db.repos.generation_runs.get(run_id)
    if run is None:
        return _moderation_redirect("run_not_found", error=True)

    if run.pipeline_id is None:
        return _moderation_redirect("pipeline_invalid", error=True)

    pipeline = await deps.pipeline_service(request).get(run.pipeline_id)
    if pipeline is None:
        return _moderation_redirect("pipeline_invalid", error=True)

    if run.moderation_status != "approved":
        return _moderation_redirect("run_not_approved", error=True)

    command_id = await deps.telegram_command_service(request).enqueue(
        "moderation.publish_run",
        payload={"run_id": run_id, "pipeline_id": run.pipeline_id},
        requested_by="web:moderation",
    )
    return RedirectResponse(url=f"/moderation?command_id={command_id}", status_code=303)


@router.post("/bulk-approve")
async def bulk_approve(request: Request, run_ids: list[int] = Form(default=[])):
    db = deps.get_db(request)
    for run_id in run_ids:
        run = await db.repos.generation_runs.get(run_id)
        if run is not None:
            await db.repos.generation_runs.set_moderation_status(run_id, "approved")

    return _moderation_redirect("runs_approved")


@router.post("/bulk-reject")
async def bulk_reject(request: Request, run_ids: list[int] = Form(default=[])):
    db = deps.get_db(request)
    for run_id in run_ids:
        run = await db.repos.generation_runs.get(run_id)
        if run is not None:
            await db.repos.generation_runs.set_moderation_status(run_id, "rejected")

    return _moderation_redirect("runs_rejected")
