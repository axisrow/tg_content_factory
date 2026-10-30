"""Pipeline CLI generation operations."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from tg_agent.cli import runtime, worker_handoff

if TYPE_CHECKING:
    from tg_agent.telegram.client_pool import ClientPool
from tg_agent.cli.pipeline import requests, responses
from tg_agent.cli.pipeline.context import PipelineServices, default_services
from tg_agent.services.pipeline_llm_requirements import pipeline_needs_llm
from tg_agent.services.pipeline_service import (
    PipelineScopeError,
    PipelineService,
    to_since_hours,
)


async def _generation_pool(config, db) -> ClientPool | None:
    """Telegram pool for generation, or ``None`` while a daemon owns the sessions.

    Generation itself needs no Telegram; Telegram-side nodes (react/forward/
    delete-message/edit-message) fail loudly without a pool, so a pipeline
    containing them must run with the daemon stopped. Pipelines without them
    run pool-less next to a live daemon instead of being refused by
    ``init_pool`` (#1521).
    """
    if not worker_handoff.direct_requested() and worker_handoff.serve_is_running(config):
        print(
            "A running tg-agent daemon owns the Telegram sessions: generating without "
            "Telegram access. Telegram-side nodes (react/forward/delete-message/"
            "edit-message) will fail — stop the daemon (`tg-agent stop`) to run "
            "pipelines containing them.",
            file=sys.stderr,
        )
        return None
    _, pool = await runtime.init_pool(config, db)
    return pool


async def _pipeline_run(
    args: requests.PipelineRunCommand, db, config, svc: PipelineService, *, services: PipelineServices | None = None
) -> ClientPool | None:
    services = services or default_services()
    pipeline = await svc.get(args.id)
    if pipeline is None:
        print(f"Pipeline id={args.id} not found")
        return None
    engine = services.search_engine(db)

    from tg_agent.services.provider_service import build_provider_service
    from tg_agent.services.quality_scoring_service import QualityScoringService

    # build_provider_service snapshots os.environ once and loads DB
    # providers; the registry no longer reads env itself (#1050).
    provider_service = await build_provider_service(db, config)
    if pipeline_needs_llm(pipeline) and not provider_service.has_providers():
        print(
            "LLM provider is not configured. Add one in /settings or set an API key "
            "env var (e.g. OPENAI_API_KEY). Non-LLM pipelines run without a provider."
        )
        return None

    pool = await _generation_pool(config, db)
    client_pool = pool
    gen_svc = services.generation_service(
        db,
        engine,
        config=config,
        client_pool=client_pool,
        quality_service=QualityScoringService(db, provider_service=provider_service),
        provider_service=provider_service,
    )
    try:
        run_obj = await gen_svc.generate(
            pipeline=pipeline,
            model=pipeline.llm_model,
            max_tokens=args.max_tokens,
            temperature=args.temperature,
        )
        responses.render_pipeline_run_result(args, run_obj)
    except Exception as exc:
        print(f"Generation failed: {exc}")

    return pool


async def _pipeline_generate(
    args: requests.PipelineGenerateCommand,
    db,
    config,
    svc: PipelineService,
    *,
    services: PipelineServices | None = None,
) -> ClientPool | None:
    services = services or default_services()
    pipeline = await svc.get(args.id)
    if pipeline is None:
        print(f"Pipeline id={args.id} not found")
        return None
    # Per-run A/B overrides (issue #1068): --ab-variants / --auto-select
    # override the pipeline's stored A/B config for this single run only.
    ab_overrides: dict[str, object] = {}
    if getattr(args, "ab_variants", None) is not None:
        ab_overrides["ab_num_variants"] = args.ab_variants
    if getattr(args, "auto_select", False):
        ab_overrides["ab_auto_select"] = True
    if ab_overrides:
        pipeline = pipeline.model_copy(update=ab_overrides)
    engine = services.search_engine(db)
    from tg_agent.services.provider_service import build_provider_service
    from tg_agent.services.quality_scoring_service import QualityScoringService

    # build_provider_service snapshots os.environ once and loads DB
    # providers; the registry no longer reads env itself (#1050).
    provider_svc = await build_provider_service(db, config)
    if pipeline_needs_llm(pipeline) and not provider_svc.has_providers():
        print(
            "LLM provider is not configured. Add one in /settings or set an API key "
            "env var (e.g. OPENAI_API_KEY). Non-LLM pipelines run without a provider."
        )
        return None
    pool = await _generation_pool(config, db)
    client_pool = pool
    agent_manager = None
    if getattr(pipeline.generation_backend, "value", pipeline.generation_backend) == "deep_agents":
        from tg_agent.agent.manager import AgentManager

        agent_manager = AgentManager(db, config, client_pool=client_pool)
    gen_svc = services.generation_service(
        db,
        engine,
        config=config,
        client_pool=client_pool,
        agent_manager=agent_manager,
        quality_service=QualityScoringService(db, provider_service=provider_svc),
        provider_service=provider_svc,
    )
    try:
        run = await gen_svc.generate(
            pipeline=pipeline,
            model=args.model,
            max_tokens=args.max_tokens,
            temperature=args.temperature,
        )
        responses.render_pipeline_generate_result(run)
    except Exception as exc:
        print(f"Generation failed: {exc}")

    return pool


async def _pipeline_generate_stream(
    args: requests.PipelineGenerateStreamCommand,
    db,
    config,
    svc: PipelineService,
    *,
    services: PipelineServices | None = None,
) -> None:
    services = services or default_services()
    pipeline = await svc.get(args.id)
    if pipeline is None:
        print(f"Pipeline id={args.id} not found")
        return

    from tg_agent.services.generation_service import GenerationService
    from tg_agent.services.provider_service import build_provider_service

    # build_provider_service snapshots os.environ once and loads DB
    # providers; the registry no longer reads env itself (#1050).
    provider_service = await build_provider_service(db, config)
    if pipeline_needs_llm(pipeline) and not provider_service.has_providers():
        print("LLM provider is not configured. Add one in /settings or set an API key env var (e.g. OPENAI_API_KEY).")
        return
    if not pipeline_needs_llm(pipeline):
        print("Pipeline has no LLM nodes; nothing to stream.")
        return

    provider_callable = provider_service.get_provider_callable(pipeline.llm_model)
    engine = services.search_engine(db)
    gen = GenerationService(engine, provider_callable=provider_callable)
    # Fail-closed (#1077): never widen a failed source-scope lookup to
    # an all-channels retrieval. Abort before creating a run.
    try:
        scope = await svc.get_retrieval_scope(pipeline)
    except PipelineScopeError as exc:
        responses.emit_json_line({"event": "error", "error": str(exc)})
        return

    run_id = await db.repos.generation_runs.create_run(pipeline.id, pipeline.prompt_template)
    await db.repos.generation_runs.set_status(run_id, "running")
    last = None
    try:
        async for update in gen.generate_stream(
            query=scope.query,
            prompt_template=pipeline.prompt_template,
            model=(args.model or pipeline.llm_model),
            max_tokens=args.max_tokens,
            temperature=args.temperature,
            limit=args.limit,
            channel_id=scope.channel_id,
        ):
            last = update
            responses.emit_json_line(
                {
                    "delta": update.get("delta"),
                    "text": update.get("generated_text"),
                    "citations": update.get("citations"),
                }
            )
        # A mid-stream provider failure ends the generator gracefully
        # (partial text printed) but flags stream_error — it is NOT a
        # successful run. Persist it as failed with the error recorded
        # instead of saving truncated text as completed (issue #1034,
        # cycle-review).
        if last and last.get("stream_error"):
            await db.repos.generation_runs.set_status(run_id, "failed", metadata={"stream_error": last["stream_error"]})
            responses.emit_json_line({"event": "error", "error": last["stream_error"]})
            return
        final_text = last.get("generated_text") if last else ""
        metadata = {"citations": last.get("citations", []) if last else []}
        await db.repos.generation_runs.save_result(run_id, final_text, metadata)
        await db.repos.generation_runs.set_status(run_id, "completed")
        responses.emit_json_line({"event": "done", "run_id": run_id})
    except Exception as exc:
        await db.repos.generation_runs.set_status(run_id, "failed")
        responses.emit_json_line({"event": "error", "error": str(exc)})
    except BaseException:
        # Ctrl+C raises CancelledError (a BaseException, not Exception in
        # py3.11), which would otherwise leave the run stuck in "running".
        # Mirror the web handler: mark failed, then re-raise to abort. (#737)
        await db.repos.generation_runs.set_status(run_id, "failed")
        raise


async def _pipeline_runs(args: requests.PipelineRunsCommand, db, config, svc: PipelineService) -> None:
    pipeline = await svc.get(args.id)
    if pipeline is None:
        print(f"Pipeline id={args.id} not found")
        return
    runs = await db.repos.generation_runs.list_by_pipeline(args.id, limit=args.limit)
    return responses.render_pipeline_runs(args, runs)


async def _pipeline_run_show(args: requests.PipelineRunShowCommand, db, config, svc: PipelineService) -> None:
    run = await db.repos.generation_runs.get(args.run_id)
    return responses.render_pipeline_run_show(args, run)


async def _pipeline_variants(args: requests.PipelineVariantsCommand, db, config, svc: PipelineService) -> None:
    from tg_agent.services.ab_testing_service import ABTestingService

    ab_service = ABTestingService(db, config=config)
    ab_result = await ab_service.get_variants(args.run_id)
    return responses.render_pipeline_variants(ab_result, args)


async def _pipeline_select_variant(
    args: requests.PipelineSelectVariantCommand, db, config, svc: PipelineService
) -> None:
    from tg_agent.services.ab_testing_service import ABTestingService

    ab_service = ABTestingService(db, config=config)
    try:
        await ab_service.select_variant(args.run_id, args.index)
    except ValueError as exc:
        print(f"Error: {exc}")
        return
    print(f"Selected variant {args.index} for run id={args.run_id}")


async def _pipeline_auto_select(args: requests.PipelineAutoSelectCommand, db, config, svc: PipelineService) -> None:
    from tg_agent.services.ab_testing_service import ABTestingService
    from tg_agent.services.provider_service import build_provider_service
    from tg_agent.services.quality_scoring_service import QualityScoringService

    provider_svc = await build_provider_service(db, config)
    quality_service = QualityScoringService(db, provider_service=provider_svc)
    ab_service = ABTestingService(db, provider_service=provider_svc, config=config)
    best_index = await ab_service.auto_select_best(args.run_id, scoring_service=quality_service)
    print(f"Auto-selected variant {best_index} for run id={args.run_id}")


async def _pipeline_dry_run_count(args: requests.PipelineDryRunCountCommand, db, config, svc: PipelineService) -> None:
    since_h = to_since_hours(args.since_value, args.since_unit)
    msgs = await db.repos.messages.get_recent_for_channels(args.source, since_h)
    print(f"Messages found: {len(msgs)} (sources={args.source}, since={args.since_value}{args.since_unit})")


HANDLERS = {
    "run": _pipeline_run,
    "generate": _pipeline_generate,
    "generate-stream": _pipeline_generate_stream,
    "runs": _pipeline_runs,
    "run-show": _pipeline_run_show,
    "variants": _pipeline_variants,
    "select-variant": _pipeline_select_variant,
    "auto-select": _pipeline_auto_select,
    "dry-run-count": _pipeline_dry_run_count,
}
