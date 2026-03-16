from __future__ import annotations

from urllib.parse import quote

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.agent.prompt_template import ALLOWED_TEMPLATE_VARIABLES
from src.models import PipelineGenerationBackend, PipelinePublishMode
from src.services.pipeline_service import (
    PipelineService,
    PipelineTargetRef,
    PipelineValidationError,
)
from src.web import deps

router = APIRouter()


def _pipeline_redirect(
    code: str,
    *,
    error: bool = False,
    phone: str | None = None,
) -> RedirectResponse:
    key = "error" if error else "msg"
    suffix = f"&phone={quote(phone, safe='')}" if phone else ""
    return RedirectResponse(url=f"/pipelines?{key}={quote(code, safe='')}{suffix}", status_code=303)


def _target_refs(values: list[str]) -> list[PipelineTargetRef]:
    refs: list[PipelineTargetRef] = []
    for value in values:
        phone, separator, raw_dialog_id = value.partition("|")
        if not separator:
            raise PipelineValidationError("Некорректный формат цели pipeline.")
        try:
            dialog_id = int(raw_dialog_id)
        except ValueError as exc:
            raise PipelineValidationError("Некорректный dialog id для pipeline target.") from exc
        refs.append(PipelineTargetRef(phone=phone, dialog_id=dialog_id))
    return refs


async def _page_context(request: Request) -> dict:
    svc = deps.pipeline_service(request)
    channels = await deps.get_channel_bundle(request).list_channels(include_filtered=True)
    accounts = await deps.get_account_bundle(request).list_accounts()
    selected_phone = request.query_params.get("phone") or (accounts[0].phone if accounts else "")
    if selected_phone:
        refresh = request.query_params.get("refresh") == "1"
        try:
            await deps.channel_service(request).get_my_dialogs(selected_phone, refresh=refresh)
        except Exception:
            pass
    cached_dialogs = await svc.list_cached_dialogs_by_phone()
    return {
        "items": await svc.get_with_relations(),
        "channels": channels,
        "accounts": accounts,
        "cached_dialogs": cached_dialogs,
        "selected_phone": selected_phone,
        "prompt_variables": sorted(ALLOWED_TEMPLATE_VARIABLES),
        "publish_modes": list(PipelinePublishMode),
        "generation_backends": list(PipelineGenerationBackend),
    }


@router.get("/", response_class=HTMLResponse)
async def pipelines_page(request: Request):
    return deps.get_templates(request).TemplateResponse(
        request,
        "pipelines.html",
        await _page_context(request),
    )


@router.post("/add")
async def add_pipeline(
    request: Request,
    name: str = Form(...),
    prompt_template: str = Form(...),
    source_channel_ids: list[int] = Form(...),
    target_refs: list[str] = Form(...),
    llm_model: str = Form(""),
    image_model: str = Form(""),
    publish_mode: str = Form(PipelinePublishMode.MODERATED.value),
    generation_backend: str = Form(PipelineGenerationBackend.CHAIN.value),
    generate_interval_minutes: int = Form(60),
    is_active: bool = Form(False),
):
    svc: PipelineService = deps.pipeline_service(request)
    phone = request.query_params.get("phone")
    try:
        await svc.add(
            name=name,
            prompt_template=prompt_template,
            source_channel_ids=source_channel_ids,
            target_refs=_target_refs(target_refs),
            llm_model=llm_model,
            image_model=image_model,
            publish_mode=publish_mode,
            generation_backend=generation_backend,
            generate_interval_minutes=generate_interval_minutes,
            is_active=is_active,
        )
    except PipelineValidationError as exc:
        return _pipeline_redirect(str(exc), error=True, phone=phone)
    return _pipeline_redirect("pipeline_added", phone=phone)


@router.post("/{pipeline_id}/edit")
async def edit_pipeline(
    request: Request,
    pipeline_id: int,
    name: str = Form(...),
    prompt_template: str = Form(...),
    source_channel_ids: list[int] = Form(...),
    target_refs: list[str] = Form(...),
    llm_model: str = Form(""),
    image_model: str = Form(""),
    publish_mode: str = Form(PipelinePublishMode.MODERATED.value),
    generation_backend: str = Form(PipelineGenerationBackend.CHAIN.value),
    generate_interval_minutes: int = Form(60),
    is_active: bool = Form(False),
):
    svc: PipelineService = deps.pipeline_service(request)
    phone = request.query_params.get("phone")
    existing = await svc.get(pipeline_id)
    if existing is None:
        return _pipeline_redirect("pipeline_invalid", error=True, phone=phone)
    try:
        ok = await svc.update(
            pipeline_id,
            name=name,
            prompt_template=prompt_template,
            source_channel_ids=source_channel_ids,
            target_refs=_target_refs(target_refs),
            llm_model=llm_model,
            image_model=image_model,
            publish_mode=publish_mode,
            generation_backend=generation_backend,
            generate_interval_minutes=generate_interval_minutes,
            is_active=is_active,
            last_generated_id=existing.last_generated_id,
        )
    except PipelineValidationError as exc:
        return _pipeline_redirect(str(exc), error=True, phone=phone)
    if not ok:
        return _pipeline_redirect("pipeline_invalid", error=True, phone=phone)
    return _pipeline_redirect("pipeline_edited", phone=phone)


@router.post("/{pipeline_id}/toggle")
async def toggle_pipeline(request: Request, pipeline_id: int):
    phone = request.query_params.get("phone")
    ok = await deps.pipeline_service(request).toggle(pipeline_id)
    if not ok:
        return _pipeline_redirect("pipeline_invalid", error=True, phone=phone)
    return _pipeline_redirect("pipeline_toggled", phone=phone)


@router.post("/{pipeline_id}/delete")
async def delete_pipeline(request: Request, pipeline_id: int):
    phone = request.query_params.get("phone")
    await deps.pipeline_service(request).delete(pipeline_id)
    return _pipeline_redirect("pipeline_deleted", phone=phone)
