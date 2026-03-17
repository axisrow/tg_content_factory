from __future__ import annotations

import logging

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.web import deps

logger = logging.getLogger(__name__)
router = APIRouter()


def _moderation_redirect(code: str, *, error: bool = False) -> RedirectResponse:
    key = "error" if error else "msg"
    return RedirectResponse(url=f"/moderation?{key}={code}", status_code=303)


@router.get("/", response_class=HTMLResponse)
async def moderation_queue_page(
    request: Request,
    pipeline_id: int | None = None,
    limit: int = 50,
    offset: int = 0,
):
    db = deps.get_db(request)
    pending_runs = await db.repos.generation_runs.list_pending_moderation(
        pipeline_id=pipeline_id,
        limit=limit,
        offset=offset,
    )
    
    pipelines_svc = deps.pipeline_service(request)
    pipelines = await pipelines_svc.get_with_relations()
    
    return deps.get_templates(request).TemplateResponse(
        request,
        "moderation.html",
        {
            "pending_runs": pending_runs,
            "pipelines": pipelines,
            "selected_pipeline_id": pipeline_id,
            "limit": limit,
            "offset": offset,
        },
    )


@router.get("/{run_id}/view", response_class=HTMLResponse)
async def view_run(request: Request, run_id: int):
    db = deps.get_db(request)
    run = await db.repos.generation_runs.get(run_id)
    if run is None:
        return _moderation_redirect("run_not_found", error=True)
    
    return deps.get_templates(request).TemplateResponse(
        request,
        "moderation/view.html",
        {"run": run},
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
    
    if run.moderation_status != "approved":
        return _moderation_redirect("run_not_approved", error=True)
    
    await db.repos.generation_runs.set_published_at(run_id)
    return _moderation_redirect("run_published")


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
