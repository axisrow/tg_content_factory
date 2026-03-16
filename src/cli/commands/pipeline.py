from __future__ import annotations

import argparse
import asyncio

from src.cli import runtime
from src.services.pipeline_service import (
    PipelineService,
    PipelineTargetRef,
    PipelineValidationError,
)


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


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        _, db = await runtime.init_db(args.config)
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
                    print(f"  - {title}")
                print("targets:")
                for target in detail["targets"]:
                    print(f"  - {target.phone}:{target.dialog_id} ({target.title or '—'})")

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
                            _parse_target_refs(args.target)
                            if args.target
                            else current_targets
                        ),
                        llm_model=(
                            args.llm_model
                            if args.llm_model is not None
                            else existing.llm_model
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
                        last_generated_id=existing.last_generated_id,
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
        finally:
            await db.close()

    asyncio.run(_run())
