from __future__ import annotations

from urllib.parse import quote

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import ValidationError

from src.models import PipelinePublishMode, PipelineTarget
from src.web import deps

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def pipelines_page(request: Request, phone: str | None = None):
    svc = deps.pipeline_service(request)
    pipelines = await svc.list()

    pool = deps.get_pool(request)
    accounts = sorted(pool.clients.keys())
    selected_phone = phone if phone in pool.clients else (accounts[0] if accounts else None)

    channels = await deps.get_channel_bundle(request).list_channels()

    dialogs: list[dict] = []
    if selected_phone:
        dialogs = await deps.channel_service(request).get_my_dialogs(selected_phone)

    return deps.get_templates(request).TemplateResponse(
        request,
        "pipelines.html",
        {
            "pipelines": pipelines,
            "accounts": accounts,
            "selected_phone": selected_phone,
            "channels": channels,
            "dialogs": dialogs,
        },
    )


@router.post("/add")
async def add_pipeline(
    request: Request,
    name: str = Form(...),
    phone: str = Form(...),
    source_channel_ids: list[str] = Form(default=[]),
    target_dialog_ids: list[str] = Form(default=[]),
    prompt_template: str = Form(""),
    llm_model: str = Form(""),
    publish_mode: str = Form("draft"),
):
    svc = deps.pipeline_service(request)
    pool = deps.get_pool(request)
    if phone not in pool.clients:
        return RedirectResponse(url="/pipelines?error=invalid_account", status_code=303)
    sources = [int(x) for x in source_channel_ids if x.lstrip("-").isdigit()]
    targets = []
    for tid in target_dialog_ids:
        if tid.lstrip("-").isdigit():
            targets.append(PipelineTarget(dialog_id=int(tid)))
    try:
        mode = PipelinePublishMode(publish_mode)
    except ValueError:
        mode = PipelinePublishMode.DRAFT
    try:
        await svc.add(
            name,
            phone,
            source_channel_ids=sources,
            targets=targets,
            prompt_template=prompt_template or None,
            llm_model=llm_model or None,
            publish_mode=mode,
        )
    except ValidationError:
        return RedirectResponse(url="/pipelines?error=invalid_value", status_code=303)
    return RedirectResponse(url="/pipelines?msg=pipeline_added", status_code=303)


@router.post("/{pipeline_id}/edit")
async def edit_pipeline(
    request: Request,
    pipeline_id: int,
    name: str = Form(...),
    phone: str = Form(...),
    source_channel_ids: list[str] = Form(default=[]),
    target_dialog_ids: list[str] = Form(default=[]),
    prompt_template: str = Form(""),
    llm_model: str = Form(""),
    publish_mode: str = Form("draft"),
):
    svc = deps.pipeline_service(request)
    pool = deps.get_pool(request)
    if phone not in pool.clients:
        return RedirectResponse(url="/pipelines?error=invalid_account", status_code=303)
    sources = [int(x) for x in source_channel_ids if x.lstrip("-").isdigit()]
    targets = []
    for tid in target_dialog_ids:
        if tid.lstrip("-").isdigit():
            targets.append(PipelineTarget(dialog_id=int(tid)))
    try:
        mode = PipelinePublishMode(publish_mode)
    except ValueError:
        mode = PipelinePublishMode.DRAFT
    try:
        updated = await svc.update(
            pipeline_id,
            name,
            phone,
            source_channel_ids=sources,
            targets=targets,
            prompt_template=prompt_template or None,
            llm_model=llm_model or None,
            publish_mode=mode,
        )
    except ValidationError:
        return RedirectResponse(url="/pipelines?error=invalid_value", status_code=303)
    if not updated:
        return RedirectResponse(url="/pipelines?error=not_found", status_code=303)
    return RedirectResponse(url="/pipelines?msg=pipeline_edited", status_code=303)


@router.post("/{pipeline_id}/toggle")
async def toggle_pipeline(request: Request, pipeline_id: int):
    svc = deps.pipeline_service(request)
    await svc.toggle(pipeline_id)
    return RedirectResponse(url="/pipelines?msg=pipeline_toggled", status_code=303)


@router.post("/{pipeline_id}/delete")
async def delete_pipeline(request: Request, pipeline_id: int):
    svc = deps.pipeline_service(request)
    await svc.delete(pipeline_id)
    return RedirectResponse(url="/pipelines?msg=pipeline_deleted", status_code=303)


@router.post("/refresh")
async def pipelines_refresh(request: Request, phone: str = Form(...)):
    await deps.channel_service(request).get_my_dialogs(phone, refresh=True)
    return RedirectResponse(
        url=f"/pipelines?phone={quote(phone, safe='')}",
        status_code=303,
    )
