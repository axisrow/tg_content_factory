from __future__ import annotations

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse

from src.models import PipelineGenerationBackend, PipelinePublishMode
from src.web.pipelines import handlers
from src.web.pipelines.forms import (
    CreateWizardForm,
    PipelineCreateForm,
    PipelineEditForm,
    PipelineGenerateForm,
    PipelineImportForm,
    PipelineRunForm,
    PipelineTemplateCreateForm,
)
from src.web.pipelines.responses import pipeline_response

router = APIRouter()


@router.get("/api/channels/search", response_class=JSONResponse)
async def api_channels_search(request: Request, q: str = ""):
    return pipeline_response(request, await handlers.api_channels_search(request, q=q))


@router.get("/", response_class=HTMLResponse)
async def pipelines_page(request: Request):
    return pipeline_response(request, await handlers.pipelines_page(request))


@router.get("/create", response_class=HTMLResponse)
async def create_wizard_page(request: Request):
    return pipeline_response(request, await handlers.create_wizard_page(request))


@router.post("/create-wizard")
async def create_wizard_submit(
    request: Request,
    name: str = Form(""),
    pipeline_json: str = Form(""),
    source_channel_ids: list[int] = Form(default=[]),
    target_refs: list[str] = Form(default=[]),
    generate_interval_minutes: int = Form(60),
    is_active: str = Form(""),
    run_after: str = Form(""),
    since_value: int = Form(24),
    since_unit: str = Form("h"),
    account_phone: str = Form(""),
):
    form = CreateWizardForm(
        name=name,
        pipeline_json=pipeline_json,
        source_channel_ids=source_channel_ids,
        target_refs=target_refs,
        generate_interval_minutes=generate_interval_minutes,
        is_active=is_active,
        run_after=run_after,
        since_value=since_value,
        since_unit=since_unit,
        account_phone=account_phone,
    )
    result = await handlers.create_wizard_submit(
        request,
        name=form.name,
        pipeline_json=form.pipeline_json,
        source_channel_ids=form.source_channel_ids,
        target_refs=form.target_refs,
        generate_interval_minutes=form.generate_interval_minutes,
        is_active=form.is_active,
        run_after=form.run_after,
        since_value=form.since_value,
        since_unit=form.since_unit,
        account_phone=form.account_phone,
    )
    return pipeline_response(request, result)


@router.post("/add")
async def add_pipeline(
    request: Request,
    name: str = Form(""),
    prompt_template: str = Form(""),
    source_channel_ids: list[int] = Form(default=[]),
    target_refs: list[str] = Form(default=[]),
    llm_model: str = Form(""),
    image_model: str = Form(""),
    publish_mode: str = Form(PipelinePublishMode.MODERATED.value),
    generation_backend: str = Form(PipelineGenerationBackend.CHAIN.value),
    generate_interval_minutes: int = Form(60),
    is_active: bool = Form(False),
):
    form = PipelineCreateForm(
        name=name,
        prompt_template=prompt_template,
        source_channel_ids=source_channel_ids,
        target_refs=target_refs,
        llm_model=llm_model,
        image_model=image_model,
        publish_mode=publish_mode,
        generation_backend=generation_backend,
        generate_interval_minutes=generate_interval_minutes,
        is_active=is_active,
    )
    result = await handlers.add_pipeline(
        request,
        name=form.name,
        prompt_template=form.prompt_template,
        source_channel_ids=form.source_channel_ids,
        target_refs=form.target_refs,
        llm_model=form.llm_model,
        image_model=form.image_model,
        publish_mode=form.publish_mode,
        generation_backend=form.generation_backend,
        generate_interval_minutes=form.generate_interval_minutes,
        is_active=form.is_active,
    )
    return pipeline_response(request, result)


@router.post("/{pipeline_id}/edit")
async def edit_pipeline(
    request: Request,
    pipeline_id: int,
    name: str = Form(""),
    prompt_template: str = Form(""),
    source_channel_ids: list[int] = Form(default=[]),
    target_refs: list[str] = Form(default=[]),
    llm_model: str = Form(""),
    image_model: str = Form(""),
    publish_mode: str = Form(PipelinePublishMode.MODERATED.value),
    generation_backend: str = Form(PipelineGenerationBackend.CHAIN.value),
    generate_interval_minutes: int = Form(60),
    is_active: bool = Form(False),
    react_emoji: str = Form(""),
    filter_present: str = Form(""),
    filter_message_kinds: list[str] = Form(default=[]),
    filter_service_actions: list[str] = Form(default=[]),
    filter_media_types: list[str] = Form(default=[]),
    filter_sender_kinds: list[str] = Form(default=[]),
    filter_keywords: str = Form(""),
    filter_regex: str = Form(""),
    filter_has_text: str = Form(""),
    dag_source_channel_ids: list[int] = Form(default=[]),
    account_phone: str = Form(""),
):
    form = PipelineEditForm(
        name=name,
        prompt_template=prompt_template,
        source_channel_ids=source_channel_ids,
        target_refs=target_refs,
        llm_model=llm_model,
        image_model=image_model,
        publish_mode=publish_mode,
        generation_backend=generation_backend,
        generate_interval_minutes=generate_interval_minutes,
        is_active=is_active,
        react_emoji=react_emoji,
        filter_present=filter_present,
        filter_message_kinds=filter_message_kinds,
        filter_service_actions=filter_service_actions,
        filter_media_types=filter_media_types,
        filter_sender_kinds=filter_sender_kinds,
        filter_keywords=filter_keywords,
        filter_regex=filter_regex,
        filter_has_text=filter_has_text,
        dag_source_channel_ids=dag_source_channel_ids,
        account_phone=account_phone,
    )
    result = await handlers.edit_pipeline(request, pipeline_id, **form.__dict__)
    return pipeline_response(request, result)


@router.post("/{pipeline_id}/toggle")
async def toggle_pipeline(request: Request, pipeline_id: int):
    return pipeline_response(request, await handlers.toggle_pipeline(request, pipeline_id))


@router.post("/{pipeline_id}/delete")
async def delete_pipeline(request: Request, pipeline_id: int):
    return pipeline_response(request, await handlers.delete_pipeline(request, pipeline_id))


@router.post("/{pipeline_id}/run")
async def run_pipeline(request: Request, pipeline_id: int,
                       since_value: int = Form(24), since_unit: str = Form("h")):
    form = PipelineRunForm(since_value=since_value, since_unit=since_unit)
    result = await handlers.run_pipeline(
        request,
        pipeline_id,
        since_value=form.since_value,
        since_unit=form.since_unit,
    )
    return pipeline_response(request, result)


@router.post("/{pipeline_id}/dry-run")
async def dry_run_pipeline(request: Request, pipeline_id: int):
    return pipeline_response(request, await handlers.dry_run_pipeline(request, pipeline_id))


@router.get("/{pipeline_id}/edit", response_class=HTMLResponse)
async def edit_page(request: Request, pipeline_id: int):
    return pipeline_response(request, await handlers.edit_page(request, pipeline_id))


@router.get("/{pipeline_id}/generate", response_class=HTMLResponse)
async def generate_page(request: Request, pipeline_id: int):
    return pipeline_response(request, await handlers.generate_page(request, pipeline_id))


@router.get("/{pipeline_id}/generate-stream")
async def generate_stream(
    request: Request,
    pipeline_id: int,
    model: str = "",
    max_tokens: int = 256,
    temperature: float = 0.0,
):
    result = await handlers.generate_stream(
        request,
        pipeline_id,
        model=model,
        max_tokens=max_tokens,
        temperature=temperature,
    )
    return pipeline_response(request, result)


@router.post("/{pipeline_id}/generate")
async def generate_pipeline(
    request: Request,
    pipeline_id: int,
    model: str = Form(""),
    max_tokens: int = Form(256),
    temperature: float = Form(0.0),
):
    form = PipelineGenerateForm(model=model, max_tokens=max_tokens, temperature=temperature)
    result = await handlers.generate_pipeline(
        request,
        pipeline_id,
        model=form.model,
        max_tokens=form.max_tokens,
        temperature=form.temperature,
    )
    return pipeline_response(request, result)


@router.post("/{pipeline_id}/publish")
async def publish_pipeline(request: Request, pipeline_id: int, run_id: int | None = Form(None)):
    return pipeline_response(request, await handlers.publish_pipeline(request, pipeline_id, run_id=run_id))


@router.get("/{pipeline_id}/refinement-steps")
async def get_refinement_steps(request: Request, pipeline_id: int):
    return pipeline_response(request, await handlers.get_refinement_steps(request, pipeline_id))


@router.get("/dry-run-count", response_class=JSONResponse)
async def dry_run_count_new(request: Request, source_ids: str = "",
                             since_value: int = 6, since_unit: str = "h"):
    result = await handlers.dry_run_count_new(
        request,
        source_ids=source_ids,
        since_value=since_value,
        since_unit=since_unit,
    )
    return pipeline_response(request, result)


@router.get("/{pipeline_id}/dry-run-count", response_class=JSONResponse)
async def dry_run_count(request: Request, pipeline_id: int,
                        since_value: int = 6, since_unit: str = "h"):
    result = await handlers.dry_run_count(
        request,
        pipeline_id,
        since_value=since_value,
        since_unit=since_unit,
    )
    return pipeline_response(request, result)


@router.get("/templates", response_class=HTMLResponse)
async def templates_page(request: Request):
    return pipeline_response(request, await handlers.templates_page(request))


@router.get("/templates/json", response_class=JSONResponse)
async def templates_json(request: Request):
    return pipeline_response(request, await handlers.templates_json(request))


@router.post("/from-template")
async def create_from_template(
    request: Request,
    template_id: int | None = Form(None),
    name: str = Form(""),
    source_channel_ids: list[int] = Form(default=[]),
    target_refs: list[str] = Form(default=[]),
    llm_model: str = Form(""),
    image_model: str = Form(""),
    generate_interval_minutes: int = Form(60),
):
    form = PipelineTemplateCreateForm(
        template_id=template_id,
        name=name,
        source_channel_ids=source_channel_ids,
        target_refs=target_refs,
        llm_model=llm_model,
        image_model=image_model,
        generate_interval_minutes=generate_interval_minutes,
    )
    result = await handlers.create_from_template(request, **form.__dict__)
    return pipeline_response(request, result)


@router.get("/{pipeline_id}/export")
async def export_pipeline(request: Request, pipeline_id: int):
    return pipeline_response(request, await handlers.export_pipeline(request, pipeline_id))


@router.post("/import")
async def import_pipeline(
    request: Request,
    json_file: UploadFile | None = File(None),
    json_text: str = Form(""),
    name_override: str = Form(""),
):
    form = PipelineImportForm(json_file=json_file, json_text=json_text, name_override=name_override)
    result = await handlers.import_pipeline(
        request,
        json_file=form.json_file,
        json_text=form.json_text,
        name_override=form.name_override,
    )
    return pipeline_response(request, result)


@router.post("/{pipeline_id}/ai-edit")
async def ai_edit_pipeline(request: Request, pipeline_id: int):
    return pipeline_response(request, await handlers.ai_edit_pipeline(request, pipeline_id))


@router.post("/{pipeline_id}/refinement-steps")
async def set_refinement_steps(request: Request, pipeline_id: int):
    return pipeline_response(request, await handlers.set_refinement_steps(request, pipeline_id))
