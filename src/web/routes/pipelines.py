from __future__ import annotations

import logging
from urllib.parse import quote

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.models import Pipeline, PipelineTarget
from src.web import deps

router = APIRouter()
logger = logging.getLogger(__name__)


def _redirect(phone: str | None, code: str, *, error: bool = False) -> RedirectResponse:
    key = "error" if error else "msg"
    query = [f"{key}={code}"]
    if phone:
        query.append(f"phone={quote(phone, safe='')}")
    return RedirectResponse(url=f"/pipelines?{'&'.join(query)}", status_code=303)


def _pick_phone(request: Request, phone: str | None) -> tuple[list[str], str | None]:
    accounts = sorted(deps.get_pool(request).clients.keys())
    selected_phone = phone if phone in accounts else (accounts[0] if accounts else None)
    return accounts, selected_phone


def _parse_id_list(values: list[str]) -> list[int]:
    parsed: list[int] = []
    seen: set[int] = set()
    for value in values:
        try:
            parsed_value = int(str(value).strip())
        except (TypeError, ValueError):
            continue
        if parsed_value in seen:
            continue
        seen.add(parsed_value)
        parsed.append(parsed_value)
    return parsed


async def _load_target_context(
    request: Request,
    phone: str | None,
) -> tuple[list[dict], object | None]:
    dialogs: list[dict] = []
    dialogs_cached_at = None
    if phone:
        dialogs = await deps.channel_service(request).get_my_dialogs(phone)
        dialogs_cached_at = await deps.get_db(request).repos.dialog_cache.get_cached_at(phone)
    selectable = [
        dialog
        for dialog in dialogs
        if str(dialog.get("channel_type", "")).strip() != "bot"
    ]
    return selectable, dialogs_cached_at


async def _parse_pipeline_form(request: Request) -> tuple[Pipeline | None, str | None]:
    form = await request.form()
    phone = str(form.get("phone") or "").strip()
    name = str(form.get("name") or "").strip()
    prompt_template = str(form.get("prompt_template") or "").strip()
    llm_model = str(form.get("llm_model") or "").strip()
    source_channel_ids = _parse_id_list(form.getlist("source_channel_ids"))
    target_dialog_ids = _parse_id_list(form.getlist("target_dialog_ids"))

    accounts = set(deps.get_pool(request).clients.keys())
    if phone not in accounts or not name or not prompt_template or not llm_model:
        return None, "invalid_value"

    _, channels = await deps.pipeline_service(request).list_for_page(phone=phone)
    available_source_ids = {channel.channel_id for channel in channels}
    source_channel_ids = [
        channel_id for channel_id in source_channel_ids if channel_id in available_source_ids
    ]
    if not source_channel_ids:
        return None, "invalid_value"

    dialogs, _ = await _load_target_context(request, phone)
    dialogs_by_id = {int(dialog["channel_id"]): dialog for dialog in dialogs}
    targets: list[PipelineTarget] = []
    for dialog_id in target_dialog_ids:
        dialog = dialogs_by_id.get(dialog_id)
        if dialog is None:
            continue
        targets.append(
            PipelineTarget(
                dialog_id=dialog_id,
                title=dialog.get("title"),
                dialog_type=dialog.get("channel_type"),
            )
        )
    if not targets:
        return None, "invalid_value"

    return (
        Pipeline(
            name=name,
            phone=phone,
            source_channel_ids=source_channel_ids,
            targets=targets,
            prompt_template=prompt_template,
            llm_model=llm_model,
        ),
        None,
    )


@router.get("/", response_class=HTMLResponse)
async def pipelines_page(request: Request, phone: str | None = None):
    accounts, selected_phone = _pick_phone(request, phone)
    pipelines, source_channels = await deps.pipeline_service(request).list_for_page(
        phone=selected_phone
    )
    dialogs, dialogs_cached_at = await _load_target_context(request, selected_phone)
    source_channel_map = {
        channel.channel_id: channel.title or str(channel.channel_id) for channel in source_channels
    }
    return deps.get_templates(request).TemplateResponse(
        request,
        "pipelines.html",
        {
            "accounts": accounts,
            "selected_phone": selected_phone,
            "pipelines": pipelines,
            "source_channels": source_channels,
            "dialogs": dialogs,
            "dialogs_cached_at": dialogs_cached_at,
            "source_channel_map": source_channel_map,
        },
    )


@router.post("/refresh")
async def pipelines_refresh(request: Request, phone: str = Form(...)):
    await deps.channel_service(request).get_my_dialogs(phone, refresh=True)
    return RedirectResponse(url=f"/pipelines?phone={quote(phone, safe='')}", status_code=303)


@router.post("/add")
async def add_pipeline(request: Request):
    pipeline, error = await _parse_pipeline_form(request)
    if error or pipeline is None:
        phone = str((await request.form()).get("phone") or "").strip() or None
        return _redirect(phone, error or "invalid_value", error=True)
    await deps.pipeline_service(request).add(pipeline)
    return _redirect(pipeline.phone, "pipeline_added")


@router.post("/{pipeline_id}/edit")
async def edit_pipeline(request: Request, pipeline_id: int):
    existing = await deps.pipeline_service(request).get_by_id(pipeline_id)
    phone = existing.phone if existing is not None else None
    if existing is None:
        return _redirect(phone, "invalid_value", error=True)
    pipeline, error = await _parse_pipeline_form(request)
    if error or pipeline is None:
        submitted_phone = str((await request.form()).get("phone") or "").strip()
        phone = submitted_phone or existing.phone
        return _redirect(phone, error or "invalid_value", error=True)
    await deps.pipeline_service(request).update(pipeline_id, pipeline)
    return _redirect(pipeline.phone, "pipeline_edited")


@router.post("/{pipeline_id}/delete")
async def delete_pipeline(request: Request, pipeline_id: int):
    existing = await deps.pipeline_service(request).get_by_id(pipeline_id)
    await deps.pipeline_service(request).delete(pipeline_id)
    return _redirect(existing.phone if existing is not None else None, "pipeline_deleted")
