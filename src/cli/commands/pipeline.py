from __future__ import annotations

import argparse
import asyncio

from src.cli import runtime
from src.search.engine import SearchEngine
from src.services.content_generation_service import ContentGenerationService
from src.services.generation_service import GenerationService
from src.services.pipeline_service import (
    PipelineService,
    PipelineTargetRef,
    PipelineValidationError,
)
from src.services.publish_service import PublishService


def _parse_target_refs(values: list[str]) -> list[PipelineTargetRef]:
    refs: list[PipelineTargetRef] = []
    for value in values:
        phone, separator, raw_dialog_id = value.partition("|")
        if not separator:
            raise PipelineValidationError("Target must be in PHONE|DIALOG_ID format.")
        try:
            dialog_id = int(raw_dialog_id)
        except ValueError as exc:
            raise PipelineValidationError("Target dialog id must be numeric.") from exc
        refs.append(PipelineTargetRef(phone=phone, dialog_id=dialog_id))
    return refs


def _preview_text(value: str | None, limit: int = 60) -> str:
    if not value:
        return "—"
    compact = " ".join(value.split())
    if len(compact) <= limit:
        return compact
    return f"{compact[: limit - 3]}..."


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
        pool = None
        try:
            svc = PipelineService(db)

            if args.pipeline_action == "list":
                items = await svc.get_with_relations()
                if not items:
                    print("No pipelines found.")
                    return
                print(
                    f"{'ID':<5} {'Name':<24} {'Backend':<8} "
                    f"{'Mode':<10} {'Active':<8} {'Src/Tgt':<10}"
                )
                print("-" * 76)
                for item in items:
                    pipeline = item["pipeline"]
                    counts = f"{len(item['sources'])}/{len(item['targets'])}"
                    print(
                        f"{pipeline.id:<5} {pipeline.name[:24]:<24} "
                        f"{pipeline.generation_backend.value:<8} "
                        f"{pipeline.publish_mode.value:<10} "
                        f"{str(pipeline.is_active):<8} {counts:<10}"
                    )

            elif args.pipeline_action == "show":
                detail = await svc.get_detail(args.id)
                if detail is None:
                    print(f"Pipeline id={args.id} not found")
                    return
                pipeline = detail["pipeline"]
                print(f"id={pipeline.id}")
                print(f"name={pipeline.name}")
                print(f"backend={pipeline.generation_backend.value}")
                print(f"publish_mode={pipeline.publish_mode.value}")
                print(f"interval={pipeline.generate_interval_minutes}m")
                print(f"active={pipeline.is_active}")
                print(f"llm_model={pipeline.llm_model or '—'}")
                print(f"image_model={pipeline.image_model or '—'}")
                print("sources:")
                for title in detail["source_titles"]:
                    print(f" - {title}")
                print("targets:")
                for target in detail["targets"]:
                    print(f" - {target.phone}:{target.dialog_id} ({target.title or '—'})")

            elif args.pipeline_action == "add":
                try:
                    pipeline_id = await svc.add(
                        name=args.name,
                        prompt_template=args.prompt_template,
                        source_channel_ids=args.source,
                        target_refs=_parse_target_refs(args.target),
                        llm_model=args.llm_model,
                        image_model=args.image_model,
                        publish_mode=args.publish_mode,
                        generation_backend=args.generation_backend,
                        generate_interval_minutes=args.interval,
                        is_active=not args.inactive,
                    )
                except PipelineValidationError as exc:
                    print(f"Error: {exc}")
                    return
                print(f"Added pipeline id={pipeline_id}: {args.name}")

            elif args.pipeline_action == "edit":
                existing = await svc.get(args.id)
                if existing is None:
                    print(f"Pipeline id={args.id} not found")
                    return
                current_sources = [source.channel_id for source in await svc.get_sources(args.id)]
                current_targets = [
                    PipelineTargetRef(phone=target.phone, dialog_id=target.dialog_id)
                    for target in await svc.get_targets(args.id)
                ]
                try:
                    ok = await svc.update(
                        args.id,
                        name=args.name or existing.name,
                        prompt_template=args.prompt_template or existing.prompt_template,
                        source_channel_ids=args.source if args.source else current_sources,
                        target_refs=(
                            _parse_target_refs(args.target) if args.target else current_targets
                        ),
                        llm_model=(
                            args.llm_model if args.llm_model is not None else existing.llm_model
                        ),
                        image_model=(
                            args.image_model
                            if args.image_model is not None
                            else existing.image_model
                        ),
                        publish_mode=args.publish_mode or existing.publish_mode,
                        generation_backend=args.generation_backend or existing.generation_backend,
                        generate_interval_minutes=(
                            args.interval
                            if args.interval is not None
                            else existing.generate_interval_minutes
                        ),
                        is_active=existing.is_active if args.active is None else args.active,
                    )
                except PipelineValidationError as exc:
                    print(f"Error: {exc}")
                    return
                if not ok:
                    print(f"Pipeline id={args.id} not found")
                    return
                print(f"Updated pipeline id={args.id}")

            elif args.pipeline_action == "toggle":
                ok = await svc.toggle(args.id)
                if not ok:
                    print(f"Pipeline id={args.id} not found")
                    return
                print(f"Toggled pipeline id={args.id}")

            elif args.pipeline_action == "delete":
                await svc.delete(args.id)
                print(f"Deleted pipeline id={args.id}")

            elif args.pipeline_action == "run":
                pipeline = await svc.get(args.id)
                if pipeline is None:
                    print(f"Pipeline id={args.id} not found")
                    return
                engine = SearchEngine(db)

                from src.services.provider_service import AgentProviderService

                provider_service = AgentProviderService(db)
                provider_callable = provider_service.get_provider_callable(pipeline.llm_model)

                gen_svc = GenerationService(engine, provider_callable=provider_callable)
                run_id = await db.repos.generation_runs.create_run(
                    pipeline.id, pipeline.prompt_template
                )
                await db.repos.generation_runs.set_status(run_id, "running")
                print(f"Created generation run id={run_id}")
                retrieval_query = pipeline.prompt_template or pipeline.name or ""
                try:
                    result = await gen_svc.generate(
                        query=retrieval_query,
                        prompt_template=pipeline.prompt_template,
                        limit=args.limit,
                        model=pipeline.llm_model,
                        max_tokens=args.max_tokens,
                        temperature=args.temperature,
                    )
                    await db.repos.generation_runs.save_result(
                        run_id,
                        result.get("generated_text", ""),
                        {"citations": result.get("citations", [])},
                    )
                    print(f"Generation completed for run id={run_id}")
                    if args.preview:
                        print("--- DRAFT PREVIEW ---")
                        print(result.get("generated_text"))
                    if args.publish:
                        print(
                            "Publish requested — publishing via targets is not implemented in CLI; "
                            "Use the web UI or implement account targets."
                        )
                except Exception as exc:
                    await db.repos.generation_runs.set_status(run_id, "failed")
                    print(f"Generation failed: {exc}")

            elif args.pipeline_action == "generate":
                pipeline = await svc.get(args.id)
                if pipeline is None:
                    print(f"Pipeline id={args.id} not found")
                    return
                engine = SearchEngine(db)
                agent_manager = None
                if getattr(pipeline.generation_backend, "value", pipeline.generation_backend) == "deep_agents":
                    from src.agent.manager import AgentManager

                    agent_manager = AgentManager(db, config)
                from src.services.quality_scoring_service import QualityScoringService

                gen_svc = ContentGenerationService(
                    db,
                    engine,
                    agent_manager=agent_manager,
                    quality_service=QualityScoringService(db),
                )
                try:
                    run = await gen_svc.generate(
                        pipeline=pipeline,
                        model=args.model,
                        max_tokens=args.max_tokens,
                        temperature=args.temperature,
                    )
                    print(f"Created generation run id={run.id}")
                    if run.generated_text:
                        print("--- DRAFT PREVIEW ---")
                        print(run.generated_text)
                except Exception as exc:
                    print(f"Generation failed: {exc}")

            elif args.pipeline_action == "runs":
                pipeline = await svc.get(args.id)
                if pipeline is None:
                    print(f"Pipeline id={args.id} not found")
                    return
                runs = await db.repos.generation_runs.list_by_pipeline(
                    args.id, limit=args.limit
                )
                if args.status:
                    runs = [r for r in runs if r.status == args.status]
                if not runs:
                    print("No generation runs found.")
                    return
                print(f"{'ID':<8} {'Status':<12} {'ModStatus':<12} {'Created':<20}")
                print("-" * 56)
                for r in runs:
                    created = r.created_at.isoformat() if r.created_at else "—"
                    print(f"{r.id:<8} {r.status:<12} {r.moderation_status:<12} {created:<20}")

            elif args.pipeline_action == "run-show":
                run = await db.repos.generation_runs.get(args.run_id)
                if run is None:
                    print(f"Run id={args.run_id} not found")
                    return
                print(f"id={run.id}")
                print(f"pipeline_id={run.pipeline_id}")
                print(f"status={run.status}")
                print(f"moderation_status={run.moderation_status}")
                print(f"created_at={run.created_at}")
                if run.generated_text:
                    print("--- GENERATED TEXT ---")
                    print(run.generated_text[:500])
                    if len(run.generated_text) > 500:
                        print(f"... ({len(run.generated_text) - 500} more chars)")
                if run.image_url:
                    print(f"image_url={run.image_url}")
                if run.published_at:
                    print(f"published_at={run.published_at}")

            elif args.pipeline_action == "queue":
                pipeline = await svc.get(args.id)
                if pipeline is None:
                    print(f"Pipeline id={args.id} not found")
                    return
                runs = await db.repos.generation_runs.list_pending_moderation(
                    pipeline_id=args.id,
                    limit=args.limit,
                )
                if not runs:
                    print(f"No pending moderation runs for pipeline id={args.id}")
                    return
                print(f"{'Run ID':<8} {'Status':<12} {'Created':<19} Preview")
                print("-" * 80)
                for run in runs:
                    created_at = (
                        run.created_at.strftime("%Y-%m-%d %H:%M:%S") if run.created_at else "—"
                    )
                    print(
                        f"{run.id or 0:<8} {run.moderation_status:<12} "
                        f"{created_at:<19} {_preview_text(run.generated_text)}"
                    )

            elif args.pipeline_action == "approve":
                run = await db.repos.generation_runs.get(args.run_id)
                if run is None:
                    print(f"Run id={args.run_id} not found")
                    return
                await db.repos.generation_runs.set_moderation_status(args.run_id, "approved")
                print(f"Approved run id={args.run_id}")

            elif args.pipeline_action == "reject":
                run = await db.repos.generation_runs.get(args.run_id)
                if run is None:
                    print(f"Run id={args.run_id} not found")
                    return
                await db.repos.generation_runs.set_moderation_status(args.run_id, "rejected")
                print(f"Rejected run id={args.run_id}")

            elif args.pipeline_action == "bulk-approve":
                approved = 0
                for run_id in args.run_ids:
                    run = await db.repos.generation_runs.get(run_id)
                    if run is None:
                        print(f"  Run id={run_id} not found, skipping.")
                        continue
                    await db.repos.generation_runs.set_moderation_status(run_id, "approved")
                    approved += 1
                print(f"Bulk approved: {approved}/{len(args.run_ids)}")

            elif args.pipeline_action == "bulk-reject":
                rejected = 0
                for run_id in args.run_ids:
                    run = await db.repos.generation_runs.get(run_id)
                    if run is None:
                        print(f"  Run id={run_id} not found, skipping.")
                        continue
                    await db.repos.generation_runs.set_moderation_status(run_id, "rejected")
                    rejected += 1
                print(f"Bulk rejected: {rejected}/{len(args.run_ids)}")

            elif args.pipeline_action == "publish":
                run = await db.repos.generation_runs.get(args.run_id)
                if run is None:
                    print(f"Run id={args.run_id} not found")
                    return
                if run.pipeline_id is None:
                    print(f"Run id={args.run_id} has no pipeline")
                    return

                pipeline = await svc.get(run.pipeline_id)
                if pipeline is None:
                    print(f"Pipeline id={run.pipeline_id} not found")
                    return

                _, pool = await runtime.init_pool(config, db)
                if not pool.clients:
                    print("ERROR: Нет доступных аккаунтов Telegram.")
                    return

                publish_service = PublishService(db, pool)
                results = await publish_service.publish_run(run, pipeline)
                if not results or not all(result.success for result in results):
                    print(f"Failed to publish run id={args.run_id}")
                    for result in results:
                        if not result.success:
                            print(f"  - {result.error or 'Unknown publish error'}")
                    return

                print(
                    f"Published run id={args.run_id} to {len(results)} target(s)"
                )
            elif args.pipeline_action == "refinement-steps":
                pipeline = await svc.get(args.id)
                if pipeline is None:
                    print(f"Pipeline id={args.id} not found")
                    return
                if args.steps_json:
                    import json

                    try:
                        steps = json.loads(args.steps_json)
                    except json.JSONDecodeError as exc:
                        print(f"Invalid JSON: {exc}")
                        return
                    if not isinstance(steps, list):
                        print("Refinement steps must be a JSON array.")
                        return
                    await db.repos.content_pipelines.set_refinement_steps(args.id, steps)
                    print(f"Set {len(steps)} refinement step(s) for pipeline id={args.id}.")
                else:
                    steps = pipeline.refinement_steps or []
                    if not steps:
                        print(f"Pipeline id={args.id} has no refinement steps.")
                    else:
                        import json

                        print(json.dumps(steps, ensure_ascii=False, indent=2))

            elif args.pipeline_action == "export":
                import json

                data = await svc.export_json(args.id)
                if data is None:
                    print(f"Pipeline id={args.id} not found")
                    return
                output = json.dumps(data, ensure_ascii=False, indent=2)
                if args.output:
                    with open(args.output, "w", encoding="utf-8") as fh:
                        fh.write(output)
                    print(f"Exported pipeline id={args.id} to {args.output}")
                else:
                    print(output)

            elif args.pipeline_action == "import":
                import json

                with open(args.file, encoding="utf-8") as fh:
                    data = json.load(fh)
                try:
                    pipeline_id = await svc.import_json(data, name_override=getattr(args, "name", None))
                    print(f"Imported pipeline (id={pipeline_id})")
                except PipelineValidationError as exc:
                    print(f"Validation error: {exc}")

            elif args.pipeline_action == "templates":
                import json

                templates = await svc.list_templates(category=getattr(args, "category", None))
                if not templates:
                    print("No templates found.")
                    return
                print(f"{'ID':<5} {'Category':<14} {'Name':<32} Description")
                print("-" * 80)
                for tpl in templates:
                    builtin = " [builtin]" if tpl.is_builtin else ""
                    print(f"{tpl.id or '—':<5} {tpl.category:<14} {tpl.name:<32} {tpl.description[:40]}{builtin}")

            elif args.pipeline_action == "from-template":
                try:
                    template_id = args.template_id
                    name = args.name
                    source_ids = (
                        [int(x.strip()) for x in args.source_ids.split(",") if x.strip()]
                        if args.source_ids else []
                    )
                    target_refs = _parse_target_refs(args.target_refs.split(",") if args.target_refs else [])
                    pipeline_id = await svc.create_from_template(
                        template_id,
                        name=name,
                        source_ids=source_ids,
                        target_refs=target_refs,
                    )
                    print(f"Created pipeline from template (id={pipeline_id})")
                except PipelineValidationError as exc:
                    print(f"Validation error: {exc}")

            elif args.pipeline_action == "ai-edit":
                instruction = args.instruction
                result = await svc.edit_via_llm(args.id, instruction, db)
                import json

                if result["ok"]:
                    print("Pipeline JSON updated successfully.")
                    if getattr(args, "show", False):
                        print(json.dumps(result["pipeline_json"], ensure_ascii=False, indent=2))
                else:
                    print(f"Error: {result['error']}")

        finally:
            if pool is not None:
                await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
