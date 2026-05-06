from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.web.pipelines import handlers
from src.web.pipelines.forms import (
    CreateWizardForm,
    PipelineCreateForm,
    PipelineEditForm,
    PipelineGenerateForm,
    PipelineImportForm,
    PipelinePublishForm,
    PipelineRunForm,
    PipelineTemplateCreateForm,
    form_model_dependency,
)
from src.web.pipelines.responses import pipeline_response

router = APIRouter()

CreateWizardFormDep = Annotated[CreateWizardForm, Depends(form_model_dependency(CreateWizardForm))]
PipelineCreateFormDep = Annotated[PipelineCreateForm, Depends(form_model_dependency(PipelineCreateForm))]
PipelineEditFormDep = Annotated[PipelineEditForm, Depends(form_model_dependency(PipelineEditForm))]
PipelineRunFormDep = Annotated[PipelineRunForm, Depends(form_model_dependency(PipelineRunForm))]
PipelineGenerateFormDep = Annotated[PipelineGenerateForm, Depends(form_model_dependency(PipelineGenerateForm))]
PipelinePublishFormDep = Annotated[PipelinePublishForm, Depends(form_model_dependency(PipelinePublishForm))]
PipelineTemplateCreateFormDep = Annotated[
    PipelineTemplateCreateForm,
    Depends(form_model_dependency(PipelineTemplateCreateForm)),
]
PipelineImportFormDep = Annotated[PipelineImportForm, Depends(form_model_dependency(PipelineImportForm))]


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
    form: CreateWizardFormDep,
):
    result = await handlers.create_wizard_submit(request, form)
    return pipeline_response(request, result)


@router.post("/add")
async def add_pipeline(
    request: Request,
    form: PipelineCreateFormDep,
):
    result = await handlers.add_pipeline(request, form)
    return pipeline_response(request, result)


@router.post("/{pipeline_id}/edit")
async def edit_pipeline(
    request: Request,
    pipeline_id: int,
    form: PipelineEditFormDep,
):
    result = await handlers.edit_pipeline(request, pipeline_id, form)
    return pipeline_response(request, result)


@router.post("/{pipeline_id}/toggle")
async def toggle_pipeline(request: Request, pipeline_id: int):
    return pipeline_response(request, await handlers.toggle_pipeline(request, pipeline_id))


@router.post("/{pipeline_id}/delete")
async def delete_pipeline(request: Request, pipeline_id: int):
    return pipeline_response(request, await handlers.delete_pipeline(request, pipeline_id))


@router.post("/{pipeline_id}/run")
async def run_pipeline(request: Request, pipeline_id: int, form: PipelineRunFormDep):
    result = await handlers.run_pipeline(request, pipeline_id, form)
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
    form: PipelineGenerateFormDep,
):
    result = await handlers.generate_pipeline(request, pipeline_id, form)
    return pipeline_response(request, result)


@router.post("/{pipeline_id}/publish")
async def publish_pipeline(request: Request, pipeline_id: int, form: PipelinePublishFormDep):
    return pipeline_response(request, await handlers.publish_pipeline(request, pipeline_id, form))


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
    form: PipelineTemplateCreateFormDep,
):
    result = await handlers.create_from_template(request, form)
    return pipeline_response(request, result)


@router.get("/{pipeline_id}/export")
async def export_pipeline(request: Request, pipeline_id: int):
    return pipeline_response(request, await handlers.export_pipeline(request, pipeline_id))


@router.post("/import")
async def import_pipeline(
    request: Request,
    form: PipelineImportFormDep,
):
    result = await handlers.import_pipeline(request, form)
    return pipeline_response(request, result)


@router.post("/{pipeline_id}/ai-edit")
async def ai_edit_pipeline(request: Request, pipeline_id: int):
    return pipeline_response(request, await handlers.ai_edit_pipeline(request, pipeline_id))


@router.post("/{pipeline_id}/refinement-steps")
async def set_refinement_steps(request: Request, pipeline_id: int):
    return pipeline_response(request, await handlers.set_refinement_steps(request, pipeline_id))
