from __future__ import annotations

import logging

from fastapi import File, Form, Request, UploadFile
from fastapi.responses import JSONResponse, RedirectResponse, Response, StreamingResponse

from src.agent.prompt_template import ALLOWED_TEMPLATE_VARIABLES
from src.models import PipelineGenerationBackend, PipelinePublishMode
from src.services.pipeline_filters import filter_messages
from src.services.pipeline_llm_requirements import (
    get_dag_source_channel_ids,
    get_react_emoji_config,
    pipeline_is_dag,
    pipeline_needs_llm,
    pipeline_needs_publish_mode,
)
from src.services.pipeline_service import (
    PipelineService,
    PipelineValidationError,
)
from src.services.pipeline_service import (
    to_since_hours as _to_since_hours,
)
from src.utils.json import safe_json_dumps
from src.web import deps
from src.web.pipelines.forms import (
    build_filter_config_from_form,
    get_filter_config,
    parse_target_refs,
)
from src.web.pipelines.responses import PipelineRedirect

logger = logging.getLogger("src.web.routes.pipelines")


def _pipeline_redirect(
    code: str,
    *,
    error: bool = False,
    phone: str | None = None,
) -> PipelineRedirect:
    return PipelineRedirect(code=code, error=error, phone=phone)


def _target_refs(values):
    return parse_target_refs(values)


def _get_filter_config(pipeline) -> dict | None:
    return get_filter_config(pipeline)


def _build_filter_config_from_form(
    *,
    filter_present: str,
    filter_message_kinds: list[str],
    filter_service_actions: list[str],
    filter_media_types: list[str],
    filter_sender_kinds: list[str],
    filter_keywords: str,
    filter_regex: str,
    filter_has_text: str,
) -> dict | None:
    return build_filter_config_from_form(
        filter_present=filter_present,
        filter_message_kinds=filter_message_kinds,
        filter_service_actions=filter_service_actions,
        filter_media_types=filter_media_types,
        filter_sender_kinds=filter_sender_kinds,
        filter_keywords=filter_keywords,
        filter_regex=filter_regex,
        filter_has_text=filter_has_text,
    )


async def api_channels_search(request: Request, q: str = ""):
    """AJAX endpoint for searchable picker — returns up to 50 channels matching *q*."""
    db = deps.get_db(request)
    query = q.strip()
    if len(query) < 2:
        cur = await db.execute(
            "SELECT channel_id, title, username FROM channels ORDER BY id DESC LIMIT 50",
        )
        rows = await cur.fetchall()
        return [
            {
                "value": row["channel_id"],
                "title": row["title"] or str(row["channel_id"]),
                "username": row["username"] or "",
                "group": "channel",
            }
            for row in rows
        ]
    cur = await db.execute(
        """SELECT channel_id, title, username FROM channels
           WHERE (LOWER(title) LIKE ? OR LOWER(username) LIKE ? OR CAST(channel_id AS TEXT) LIKE ?)
           ORDER BY channel_id LIMIT 50""",
        (f"%{query.lower()}%", f"%{query.lower()}%", f"%{query}%"),
    )
    rows = await cur.fetchall()
    return [
        {
            "value": row["channel_id"],
            "title": row["title"] or str(row["channel_id"]),
            "username": row["username"] or "",
            "group": "channel",
        }
        for row in rows
    ]


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
            logger.warning("Failed to refresh dialog cache for %s", selected_phone, exc_info=True)
    cached_dialogs = await svc.list_cached_dialogs_by_phone()
    items = await svc.get_with_relations()
    # Annotate each item with whether the pipeline actually needs an LLM provider.
    # Used by the template to disable Generate/Run buttons per-pipeline instead of
    # globally — non-LLM pipelines (SOURCE→PUBLISH DAGs) remain runnable.
    needs_llm_map: dict[int, bool] = {}
    for item in items:
        pipeline = item.get("pipeline") if isinstance(item, dict) else None
        if pipeline is None or pipeline.id is None:
            continue
        try:
            needs_llm_map[pipeline.id] = pipeline_needs_llm(pipeline)
        except Exception:
            # Fail safe: assume LLM is needed on unexpected shapes.
            logger.warning("pipeline_needs_llm failed for pipeline_id=%s", pipeline.id, exc_info=True)
            needs_llm_map[pipeline.id] = True
    # gather next_run times for pipelines (batch query)
    next_runs = {}
    try:
        scheduler = deps.get_scheduler(request)
        all_jobs = scheduler.get_all_jobs_next_run()
        for item in items:
            pipeline = item.get("pipeline") if isinstance(item, dict) else None
            if pipeline is None or pipeline.id is None:
                continue
            job_id = f"pipeline_run_{pipeline.id}"
            nr = all_jobs.get(job_id)
            next_runs[pipeline.id] = nr.isoformat() if nr else None
    except Exception:
        next_runs = {}
    ctx = {
        "items": items,
        "channels": channels,
        "accounts": accounts,
        "cached_dialogs": cached_dialogs,
        "selected_phone": selected_phone,
        "prompt_variables": sorted(ALLOWED_TEMPLATE_VARIABLES),
        "publish_modes": list(PipelinePublishMode),
        "generation_backends": list(PipelineGenerationBackend),
        "next_runs": next_runs,
        "llm_configured": deps.get_llm_provider_service(request).has_providers(),
        "needs_llm_map": needs_llm_map,
    }
    # Only query DB for provider statuses when the banner is visible
    if not ctx["llm_configured"]:
        ctx["llm_provider_statuses"] = await deps.get_llm_provider_service(request).get_provider_status_list()
    return ctx


async def pipelines_page(request: Request):
    return deps.get_templates(request).TemplateResponse(
        request,
        "pipelines.html",
        await _page_context(request),
    )


async def create_wizard_page(request: Request):
    svc = deps.pipeline_service(request)
    accounts = await deps.get_account_bundle(request).list_accounts()
    cached_dialogs = await svc.list_cached_dialogs_by_phone()
    llm_provider_svc = deps.get_llm_provider_service(request)
    llm_configured = llm_provider_svc.has_providers()
    return deps.get_templates(request).TemplateResponse(
        request,
        "pipelines/create.html",
        {
            "accounts": accounts,
            "cached_dialogs": cached_dialogs,
            "llm_configured": llm_configured,
        },
    )


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
    if not name or not pipeline_json:
        return _pipeline_redirect("pipeline_invalid", error=True)
    import json as _json

    svc = deps.pipeline_service(request)
    try:
        graph_data = _json.loads(pipeline_json)
        # Extract llm_model from first LLM node config for pipeline-level setting
        llm_model = ""
        for node in graph_data.get("nodes", []):
            if node.get("type") in ("llm_generate", "llm_refine", "agent_loop"):
                llm_model = node.get("config", {}).get("model", "") or ""
                break
        data = {
            "name": name,
            "prompt_template": ".",
            "llm_model": llm_model or None,
            "source_ids": source_channel_ids,
            "target_refs": target_refs,
            "generate_interval_minutes": generate_interval_minutes,
            "pipeline_json": graph_data,
            "account_phone": account_phone or None,
        }
        pipeline_id = await svc.import_json(data)
    except PipelineValidationError as exc:
        return _pipeline_redirect(str(exc), error=True)
    except Exception as exc:
        logger.warning("create-wizard failed: %s", exc, exc_info=True)
        return _pipeline_redirect(f"Ошибка: {exc}", error=True)
    if is_active:
        await svc.toggle(pipeline_id)
    try:
        scheduler = deps.get_scheduler(request)
        await scheduler.sync_pipeline_jobs()
    except Exception:
        logger.warning("Scheduler sync failed", exc_info=True)
    if run_after:
        _since = _to_since_hours(since_value, since_unit)
        enqueuer = deps.get_task_enqueuer(request)
        await enqueuer.enqueue_pipeline_run(pipeline_id, since_hours=_since)
        return _pipeline_redirect("pipeline_run_with_since")
    return _pipeline_redirect("pipeline_added")


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
    if not name or not prompt_template:
        return _pipeline_redirect("pipeline_invalid", error=True)
    svc: PipelineService = deps.pipeline_service(request)
    try:
        new_pipeline_id = await svc.add(
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
    except PipelineValidationError:
        return _pipeline_redirect("pipeline_invalid", error=True)
    # sync scheduler jobs
    try:
        scheduler = deps.get_scheduler(request)
        await scheduler.sync_pipeline_jobs()
    except Exception:
        logger.warning("Scheduler sync failed", exc_info=True)
    # Warn if pipeline needs LLM but no provider is configured — still create it.
    try:
        created = await svc.get(new_pipeline_id)
        if created is not None and pipeline_needs_llm(created):
            if not deps.get_llm_provider_service(request).has_providers():
                return _pipeline_redirect("pipeline_added_no_llm")
    except Exception:
        logger.debug("pipeline_needs_llm check failed after add", exc_info=True)
    return _pipeline_redirect("pipeline_added")


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
    if not name:
        return _pipeline_redirect("pipeline_invalid", error=True)
    svc: PipelineService = deps.pipeline_service(request)
    phone = request.query_params.get("phone")
    existing = await svc.get(pipeline_id)
    if existing is None:
        return _pipeline_redirect("pipeline_invalid", error=True, phone=phone)
    # For DAG pipelines, hidden form fields send defaults — preserve existing values
    if pipeline_is_dag(existing):
        if not prompt_template:
            prompt_template = existing.prompt_template or ""
        if generation_backend == PipelineGenerationBackend.CHAIN.value and existing.generation_backend:
            generation_backend = existing.generation_backend.value
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
            react_emoji=react_emoji,
            filter_config=_build_filter_config_from_form(
                filter_present=filter_present,
                filter_message_kinds=filter_message_kinds,
                filter_service_actions=filter_service_actions,
                filter_media_types=filter_media_types,
                filter_sender_kinds=filter_sender_kinds,
                filter_keywords=filter_keywords,
                filter_regex=filter_regex,
                filter_has_text=filter_has_text,
            ),
            dag_source_channel_ids=dag_source_channel_ids,
            account_phone=account_phone,
        )
    except PipelineValidationError as exc:
        return _pipeline_redirect(str(exc), error=True, phone=phone)
    if not ok:
        return _pipeline_redirect("pipeline_invalid", error=True)
    # sync scheduler jobs
    try:
        scheduler = deps.get_scheduler(request)
        await scheduler.sync_pipeline_jobs()
    except Exception:
        logger.warning("Scheduler sync failed", exc_info=True)
    return _pipeline_redirect("pipeline_edited")


async def toggle_pipeline(request: Request, pipeline_id: int):
    ok = await deps.pipeline_service(request).toggle(pipeline_id)
    if not ok:
        return _pipeline_redirect("pipeline_invalid", error=True)
    try:
        scheduler = deps.get_scheduler(request)
        await scheduler.sync_pipeline_jobs()
    except Exception:
        logger.warning("Scheduler sync failed", exc_info=True)
    return _pipeline_redirect("pipeline_toggled")


async def delete_pipeline(request: Request, pipeline_id: int):
    await deps.pipeline_service(request).delete(pipeline_id)
    try:
        scheduler = deps.get_scheduler(request)
        await scheduler.sync_pipeline_jobs()
    except Exception:
        logger.warning("Scheduler sync failed", exc_info=True)
    return _pipeline_redirect("pipeline_deleted")


async def run_pipeline(request: Request, pipeline_id: int,
                       since_value: int = Form(24), since_unit: str = Form("h")):
    svc = deps.pipeline_service(request)
    pipeline = await svc.get(pipeline_id)
    if pipeline is None:
        return _pipeline_redirect("pipeline_invalid", error=True)
    # Per-pipeline LLM requirement: pure forward/publish DAGs run fine without a provider.
    if pipeline_needs_llm(pipeline) and not deps.get_llm_provider_service(request).has_providers():
        return _pipeline_redirect("llm_not_configured", error=True)
    try:
        since_hours = _to_since_hours(since_value, since_unit)
        enqueuer = deps.get_task_enqueuer(request)
        await enqueuer.enqueue_pipeline_run(pipeline_id, since_hours=since_hours)
    except Exception:
        logger.warning("Failed to enqueue pipeline run for pipeline_id=%d", pipeline_id, exc_info=True)
        return _pipeline_redirect("pipeline_run_failed", error=True)
    return _pipeline_redirect("pipeline_run_enqueued")


async def dry_run_pipeline(request: Request, pipeline_id: int):
    svc = deps.pipeline_service(request)
    pipeline = await svc.get(pipeline_id)
    if pipeline is None:
        return _pipeline_redirect("pipeline_invalid", error=True)
    if pipeline_needs_llm(pipeline) and not deps.get_llm_provider_service(request).has_providers():
        return _pipeline_redirect("llm_not_configured", error=True)
    try:
        enqueuer = deps.get_task_enqueuer(request)
        await enqueuer.enqueue_pipeline_run(pipeline_id, dry_run=True)
    except Exception:
        logger.warning("Failed to enqueue dry-run for pipeline_id=%d", pipeline_id, exc_info=True)
        return _pipeline_redirect("pipeline_run_failed", error=True)
    return _pipeline_redirect("pipeline_dry_run_enqueued")


async def edit_page(request: Request, pipeline_id: int):
    svc = deps.pipeline_service(request)
    pipeline = await svc.get(pipeline_id)
    if pipeline is None:
        return _pipeline_redirect("pipeline_invalid", error=True)
    db = deps.get_db(request)
    channels = await deps.get_channel_bundle(request).list_channels(include_filtered=True)
    accounts = await deps.get_account_bundle(request).list_accounts()
    selected_phone = request.query_params.get("phone") or (accounts[0].phone if accounts else "")
    if selected_phone and request.query_params.get("refresh") == "1":
        try:
            await deps.channel_service(request).get_my_dialogs(selected_phone, refresh=True)
        except Exception:
            logger.warning("Failed to refresh dialog cache for %s", selected_phone, exc_info=True)
    cached_dialogs = await svc.list_cached_dialogs_by_phone()
    sources = await db.repos.content_pipelines.list_sources(pipeline_id)
    targets = await db.repos.content_pipelines.list_targets(pipeline_id)
    source_ids = [s.channel_id for s in sources]
    target_refs = [f"{t.phone}|{t.dialog_id}" for t in targets]
    return deps.get_templates(request).TemplateResponse(
        request,
        "pipelines/edit.html",
        {
            "pipeline": pipeline,
            "channels": channels,
            "accounts": accounts,
            "cached_dialogs": cached_dialogs,
            "source_ids": source_ids,
            "target_refs": target_refs,
            "prompt_variables": sorted(ALLOWED_TEMPLATE_VARIABLES),
            "generation_backends": list(PipelineGenerationBackend),
            "publish_modes": list(PipelinePublishMode),
            "needs_llm": pipeline_needs_llm(pipeline),
            "needs_publish_mode": pipeline_needs_publish_mode(pipeline),
            "is_dag": pipeline_is_dag(pipeline),
            "react_emoji": get_react_emoji_config(pipeline),
            "filter_config": _get_filter_config(pipeline),
            "dag_source_channel_ids": get_dag_source_channel_ids(pipeline),
        },
    )


async def generate_page(request: Request, pipeline_id: int):
    svc = deps.pipeline_service(request)
    pipeline = await svc.get(pipeline_id)
    if pipeline is None:
        return _pipeline_redirect("pipeline_invalid", error=True)
    db = deps.get_db(request)
    runs = await db.repos.generation_runs.list_by_pipeline(pipeline_id)
    return deps.get_templates(request).TemplateResponse(
        request,
        "pipelines/generate.html",
        {"pipeline": pipeline, "runs": runs, "request": request},
    )


async def generate_stream(
    request: Request,
    pipeline_id: int,
    model: str = "",
    max_tokens: int = 256,
    temperature: float = 0.0,
):
    svc = deps.pipeline_service(request)
    pipeline = await svc.get(pipeline_id)
    if pipeline is None:
        return _pipeline_redirect("pipeline_invalid", error=True)
    db = deps.get_db(request)
    engine = deps.get_search_engine(request)

    provider_service = deps.get_llm_provider_service(request)
    if pipeline_needs_llm(pipeline) and not provider_service.has_providers():
        return _pipeline_redirect("llm_not_configured", error=True)
    if not pipeline_needs_llm(pipeline):
        return _pipeline_redirect("pipeline_no_llm_nodes", error=True)
    provider_callable = provider_service.get_provider_callable(pipeline.llm_model)

    from src.services.generation_service import GenerationService

    gen = GenerationService(engine, provider_callable=provider_callable)
    scope = await svc.get_retrieval_scope(pipeline)

    # persist run
    run_id = await db.repos.generation_runs.create_run(pipeline_id, pipeline.prompt_template)
    try:
        await db.repos.generation_runs.set_status(run_id, "running")
    except Exception:
        await db.repos.generation_runs.set_status(run_id, "failed")
        raise
    async def event_gen():
        last = None
        try:
            async for update in gen.generate_stream(
                query=scope.query,
                prompt_template=pipeline.prompt_template,
                model=(model or pipeline.llm_model),
                max_tokens=max_tokens,
                temperature=temperature,
                channel_id=scope.channel_id,
            ):
                last = update
                data = {
                    "delta": update.get("delta"),
                    "text": update.get("generated_text"),
                    "citations": update.get("citations"),
                }
                yield f"data: {safe_json_dumps(data)}\n\n"

            # finished successfully
            final_text = last.get("generated_text") if last else ""
            metadata = {"citations": last.get("citations", []) if last else []}
            await db.repos.generation_runs.save_result(run_id, final_text, metadata)
            await db.repos.generation_runs.set_status(run_id, "completed")
            yield f"event: done\ndata: {safe_json_dumps({'run_id': run_id})}\n\n"
        except Exception:
            logger.exception("Generation stream failed for pipeline_id=%d run_id=%d", pipeline_id, run_id)
            await db.repos.generation_runs.set_status(run_id, "failed")
            yield f"event: error\ndata: {safe_json_dumps({'error': 'Generation failed'})}\n\n"
        except BaseException:
            await db.repos.generation_runs.set_status(run_id, "failed")
            raise

    return StreamingResponse(event_gen(), media_type="text/event-stream")


async def generate_pipeline(
    request: Request,
    pipeline_id: int,
    model: str = Form(""),
    max_tokens: int = Form(256),
    temperature: float = Form(0.0),
):
    svc = deps.pipeline_service(request)
    pipeline = await svc.get(pipeline_id)
    if pipeline is None:
        return _pipeline_redirect("pipeline_invalid", error=True)
    db = deps.get_db(request)
    engine = deps.get_search_engine(request)

    provider_service = deps.get_llm_provider_service(request)
    if pipeline_needs_llm(pipeline) and not provider_service.has_providers():
        return _pipeline_redirect("llm_not_configured", error=True)

    from src.services.content_generation_service import ContentGenerationService
    from src.services.quality_scoring_service import QualityScoringService

    gen = ContentGenerationService(
        db,
        engine,
        config=request.app.state.config,
        quality_service=QualityScoringService(db, provider_service=provider_service),
        client_pool=deps.get_pool(request),
        provider_service=provider_service,
    )
    try:
        run = await gen.generate(
            pipeline=pipeline,
            model=(model or pipeline.llm_model),
            max_tokens=max_tokens,
            temperature=temperature,
        )
    except Exception:
        logger.exception("Generation failed for pipeline_id=%d", pipeline_id)
        runs = await db.repos.generation_runs.list_by_pipeline(pipeline_id)
        return deps.get_templates(request).TemplateResponse(
            request,
            "pipelines/generate.html",
            {"pipeline": pipeline, "runs": runs, "error": "Generation failed", "request": request},
        )
    return deps.get_templates(request).TemplateResponse(
        request,
        "pipelines/generate.html",
        {"pipeline": pipeline, "run": run, "request": request},
    )


async def publish_pipeline(request: Request, pipeline_id: int, run_id: int | None = Form(None)):
    if run_id is None:
        return _pipeline_redirect("pipeline_invalid", error=True)
    db = deps.get_db(request)
    run = await db.repos.generation_runs.get(run_id)
    if run is None or run.pipeline_id != pipeline_id:
        return _pipeline_redirect("pipeline_invalid", error=True)
    # Mark as published (no external publishing performed here)
    metadata = run.metadata or {}
    from datetime import datetime, timezone

    metadata["published"] = True
    metadata["published_at"] = datetime.now(timezone.utc).isoformat()
    await db.repos.generation_runs.save_result(run_id, run.generated_text or "", metadata)
    await db.repos.generation_runs.set_status(run_id, "published")
    return _pipeline_redirect("pipeline_published")


async def get_refinement_steps(request: Request, pipeline_id: int):
    db = deps.get_db(request)
    pipeline = await db.repos.content_pipelines.get_by_id(pipeline_id)
    if pipeline is None:
        return _pipeline_redirect("pipeline_not_found", error=True)
    return JSONResponse(content={"steps": pipeline.refinement_steps})


# ------------------------------------------------------------------
# Dry-run count endpoints
# ------------------------------------------------------------------


def _apply_pipeline_filter(pipeline, messages: list) -> int:
    """Count messages that would pass the filter node, if any."""
    if pipeline.pipeline_json is None:
        return len(messages)
    graph = pipeline.pipeline_json
    filter_node = next(
        (n for n in graph.nodes if n.type.value == "filter"),
        None,
    )
    if filter_node is None:
        return len(messages)
    return len(filter_messages(messages, filter_node.config))


async def dry_run_count_new(request: Request, source_ids: str = "",
                             since_value: int = 6, since_unit: str = "h"):
    since_hours = _to_since_hours(since_value, since_unit)
    ids = [int(x) for x in source_ids.split(",") if x.strip().isdigit()]
    db = deps.get_db(request)
    messages = await db.repos.messages.get_recent_for_channels(ids, since_hours)
    return {"total": len(messages), "after_filter": len(messages)}


async def dry_run_count(request: Request, pipeline_id: int,
                        since_value: int = 6, since_unit: str = "h"):
    since_hours = _to_since_hours(since_value, since_unit)
    svc = deps.pipeline_service(request)
    pipeline = await svc.get(pipeline_id)
    if pipeline is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    db = deps.get_db(request)
    if pipeline.pipeline_json:
        ids = get_dag_source_channel_ids(pipeline) or []
    else:
        sources = await db.repos.content_pipelines.list_sources(pipeline_id)
        ids = [s.channel_id for s in sources]
    messages = await db.repos.messages.get_recent_for_channels(ids, since_hours)
    after_filter = _apply_pipeline_filter(pipeline, messages)
    return {"total": len(messages), "after_filter": after_filter}


# ------------------------------------------------------------------
# Templates
# ------------------------------------------------------------------


async def templates_page(request: Request):
    svc: PipelineService = deps.pipeline_service(request)
    templates = await svc.list_templates()
    channels = await deps.get_channel_bundle(request).list_channels(include_filtered=True)
    accounts = await deps.get_account_bundle(request).list_accounts()
    cached_dialogs = await svc.list_cached_dialogs_by_phone()
    llm_configured = deps.get_llm_provider_service(request).has_providers()
    return deps.get_templates(request).TemplateResponse(
        request,
        "pipelines/templates.html",
        {
            "templates": templates,
            "channels": channels,
            "accounts": accounts,
            "cached_dialogs": cached_dialogs,
            "llm_configured": llm_configured,
        },
    )


async def templates_json(request: Request):
    svc: PipelineService = deps.pipeline_service(request)
    templates = await svc.list_templates()
    result = []
    import json as _json
    for tpl in templates:
        result.append({
            "id": tpl.id,
            "name": tpl.name,
            "description": tpl.description,
            "category": tpl.category,
            "template_json": _json.loads(tpl.template_json.to_json()),
        })
    return JSONResponse(content=result)


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
    if template_id is None or not name:
        return _pipeline_redirect("pipeline_invalid", error=True)
    svc: PipelineService = deps.pipeline_service(request)
    try:
        pipeline_id = await svc.create_from_template(
            template_id,
            name=name,
            source_ids=source_channel_ids,
            target_refs=_target_refs(target_refs),
            overrides={
                "llm_model": llm_model or None,
                "image_model": image_model or None,
                "generate_interval_minutes": generate_interval_minutes,
            },
        )
    except PipelineValidationError as exc:
        return _pipeline_redirect(str(exc), error=True)
    try:
        scheduler = deps.get_scheduler(request)
        await scheduler.sync_pipeline_jobs()
    except Exception:
        logger.warning("Scheduler sync failed", exc_info=True)
    return RedirectResponse(url=f"/pipelines/{pipeline_id}/edit", status_code=303)


# ------------------------------------------------------------------
# JSON import / export
# ------------------------------------------------------------------


async def export_pipeline(request: Request, pipeline_id: int):
    svc: PipelineService = deps.pipeline_service(request)
    data = await svc.export_json(pipeline_id)
    if data is None:
        return _pipeline_redirect("pipeline_invalid", error=True)
    filename = f"pipeline_{pipeline_id}.json"
    content = safe_json_dumps(data, ensure_ascii=False, indent=2)
    return Response(
        content=content,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


async def import_pipeline(
    request: Request,
    json_file: UploadFile | None = File(None),
    json_text: str = Form(""),
    name_override: str = Form(""),
):
    svc: PipelineService = deps.pipeline_service(request)
    import json as _json

    try:
        if json_file and json_file.filename:
            raw = await json_file.read()
            data = _json.loads(raw)
        elif json_text.strip():
            data = _json.loads(json_text.strip())
        else:
            return _pipeline_redirect("Не передан JSON файл или текст.", error=True)

        pipeline_id = await svc.import_json(data, name_override=name_override or None)
    except PipelineValidationError as exc:
        return _pipeline_redirect(str(exc), error=True)
    except Exception as exc:
        return _pipeline_redirect(f"Ошибка импорта: {exc}", error=True)

    try:
        scheduler = deps.get_scheduler(request)
        await scheduler.sync_pipeline_jobs()
    except Exception:
        logger.warning("Scheduler sync failed", exc_info=True)
    return RedirectResponse(url=f"/pipelines/{pipeline_id}/generate", status_code=303)


async def ai_edit_pipeline(request: Request, pipeline_id: int):
    """Accept JSON body: {"instruction": "..."}. Returns updated pipeline_json."""
    if not deps.get_llm_provider_service(request).has_providers():
        return JSONResponse(content={"ok": False, "error": "LLM not configured"}, status_code=400)
    svc: PipelineService = deps.pipeline_service(request)
    db = deps.get_db(request)
    try:
        body = await request.json()
        instruction = body.get("instruction", "").strip()
        if not instruction:
            return JSONResponse(content={"ok": False, "error": "instruction is required"}, status_code=400)
        result = await svc.edit_via_llm(pipeline_id, instruction, db, config=request.app.state.config)
        return JSONResponse(content=result)
    except Exception as exc:
        return JSONResponse(content={"ok": False, "error": str(exc)}, status_code=500)


async def set_refinement_steps(request: Request, pipeline_id: int):
    """Accept JSON body: {"steps": [{"name": "...", "prompt": "...{text}..."}]}."""
    db = deps.get_db(request)
    pipeline = await db.repos.content_pipelines.get_by_id(pipeline_id)
    if pipeline is None:
        return _pipeline_redirect("pipeline_not_found", error=True)
    try:
        body = await request.json()
        steps = body.get("steps", [])
        if not isinstance(steps, list):
            raise ValueError("steps must be a list")
        validated = [
            {"name": str(s.get("name", "")).strip(), "prompt": str(s.get("prompt", "")).strip()}
            for s in steps
            if isinstance(s, dict) and s.get("prompt", "").strip()
        ]
    except Exception as exc:
        return JSONResponse(content={"ok": False, "error": str(exc)}, status_code=400)
    await db.repos.content_pipelines.set_refinement_steps(pipeline_id, validated)
    return JSONResponse(content={"ok": True, "steps": validated})
