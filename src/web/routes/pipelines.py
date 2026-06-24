from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.web.pipelines import handlers
from src.web.pipelines.forms import (
    CreateWizardForm,
    PipelineAutoSelectVariantForm,
    PipelineCreateForm,
    PipelineEditForm,
    PipelineGenerateForm,
    PipelineImportForm,
    PipelinePublishForm,
    PipelineRunForm,
    PipelineSelectVariantForm,
    PipelineTemplateCreateForm,
    form_model_dependency,
)
from src.web.pipelines.responses import pipeline_response
from src.web.schemas.common import ErrorResponse
from src.web.schemas.pipelines import (
    ChannelSearchItem,
    PipelineDetailResponse,
    PipelineQueueResponse,
    PipelineRunDetailResponse,
    PipelineRunsResponse,
    PipelineTemplateItem,
    PipelineVariantsResponse,
)

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
PipelineSelectVariantFormDep = Annotated[
    PipelineSelectVariantForm,
    Depends(form_model_dependency(PipelineSelectVariantForm)),
]
PipelineAutoSelectVariantFormDep = Annotated[
    PipelineAutoSelectVariantForm,
    Depends(form_model_dependency(PipelineAutoSelectVariantForm)),
]


@router.get(
    "/api/channels/search",
    response_class=JSONResponse,
    response_model=list[ChannelSearchItem],
    status_code=200,
    tags=["pipelines"],
    summary="Searchable channel picker",
)
async def api_channels_search(request: Request, q: str = ""):
    """Return up to 50 channels matching *q* for the pipeline source/target picker."""
    return pipeline_response(request, await handlers.api_channels_search(request, q=q))


@router.get("/", response_class=HTMLResponse)
async def pipelines_page(request: Request):
    return pipeline_response(request, await handlers.pipelines_page(request))


@router.get("/fragments/list", response_class=HTMLResponse)
async def pipelines_list_fragment(request: Request):
    return pipeline_response(request, await handlers.pipelines_list_fragment(request))


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


@router.get(
    "/{pipeline_id}/variants/{run_id}",
    response_class=JSONResponse,
    response_model=PipelineVariantsResponse,
    status_code=200,
    tags=["pipelines"],
    summary="A/B variants of a run",
    responses={404: {"model": ErrorResponse, "description": "Run not found"}},
)
async def get_pipeline_variants(request: Request, pipeline_id: int, run_id: int):
    """A/B variants of a generation run as JSON (parity with CLI `pipeline variants`)."""
    return pipeline_response(
        request, await handlers.get_pipeline_variants(request, pipeline_id, run_id)
    )


@router.post("/{pipeline_id}/select-variant")
async def select_pipeline_variant(
    request: Request, pipeline_id: int, form: PipelineSelectVariantFormDep
):
    return pipeline_response(
        request, await handlers.select_pipeline_variant(request, pipeline_id, form)
    )


@router.post("/{pipeline_id}/auto-select-best")
async def auto_select_pipeline_variant(
    request: Request, pipeline_id: int, form: PipelineAutoSelectVariantFormDep
):
    return pipeline_response(
        request, await handlers.auto_select_pipeline_variant(request, pipeline_id, form)
    )


@router.get("/{pipeline_id}/refinement-steps")
async def get_refinement_steps(request: Request, pipeline_id: int):
    return pipeline_response(request, await handlers.get_refinement_steps(request, pipeline_id))


@router.get(
    "/{pipeline_id}/show",
    response_class=JSONResponse,
    response_model=PipelineDetailResponse,
    status_code=200,
    tags=["pipelines"],
    summary="Pipeline configuration detail",
    responses={404: {"model": ErrorResponse, "description": "Pipeline not found"}},
)
async def show_pipeline(request: Request, pipeline_id: int):
    """Pipeline details as JSON (parity with CLI `pipeline show`).

    Returns 404 with ``{"error": "pipeline_not_found"}`` for an unknown id.
    """
    return pipeline_response(request, await handlers.show_pipeline(request, pipeline_id))


@router.get(
    "/{pipeline_id}/runs",
    response_class=JSONResponse,
    response_model=PipelineRunsResponse,
    status_code=200,
    tags=["pipelines"],
    summary="Pipeline run history",
    responses={404: {"model": ErrorResponse, "description": "Pipeline not found"}},
)
async def list_pipeline_runs(
    request: Request,
    pipeline_id: int,
    limit: int = 20,
    offset: int = 0,
    status: str | None = None,
    moderation_status: str | None = None,
):
    """Run history for a pipeline (parity with CLI `pipeline runs`).

    Returns 404 with ``{"error": "pipeline_not_found"}`` for an unknown id.
    """
    return pipeline_response(
        request,
        await handlers.list_pipeline_runs(
            request,
            pipeline_id,
            limit=limit,
            offset=offset,
            status=status,
            moderation_status=moderation_status,
        ),
    )


@router.get(
    "/{pipeline_id}/runs/{run_id}",
    response_class=JSONResponse,
    response_model=PipelineRunDetailResponse,
    status_code=200,
    tags=["pipelines"],
    summary="Single pipeline run detail",
    responses={404: {"model": ErrorResponse, "description": "Run not found"}},
)
async def show_pipeline_run(request: Request, pipeline_id: int, run_id: int):
    """Run details incl. generated text and image URL (parity with CLI `pipeline run-show`).

    Returns 404 with ``{"error": "run_not_found"}`` if the run is missing or
    belongs to another pipeline.
    """
    return pipeline_response(request, await handlers.show_pipeline_run(request, pipeline_id, run_id))


@router.get(
    "/{pipeline_id}/queue",
    response_class=JSONResponse,
    response_model=PipelineQueueResponse,
    status_code=200,
    tags=["pipelines"],
    summary="Pipeline moderation queue",
    responses={404: {"model": ErrorResponse, "description": "Pipeline not found"}},
)
async def pipeline_queue(request: Request, pipeline_id: int, limit: int = 50):
    """Runs awaiting moderation for a pipeline (parity with CLI `pipeline queue`).

    Returns 404 with ``{"error": "pipeline_not_found"}`` for an unknown id.
    """
    return pipeline_response(request, await handlers.pipeline_queue(request, pipeline_id, limit=limit))


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


@router.get(
    "/templates/json",
    response_class=JSONResponse,
    response_model=list[PipelineTemplateItem],
    status_code=200,
    tags=["pipelines"],
    summary="List pipeline templates",
)
async def templates_json(request: Request):
    """List available pipeline templates with their serialized graph as JSON."""
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
