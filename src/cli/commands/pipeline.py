from __future__ import annotations

import argparse
import asyncio

from pydantic import ValidationError

from src.cli import runtime
from src.database.bundles import PipelineBundle
from src.services.pipeline_service import PipelineService


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        _, db = await runtime.init_db(args.config)
        try:
            svc = PipelineService(PipelineBundle.from_database(db))

            if args.pipeline_action == "list":
                items = await svc.list()
                if not items:
                    print("No pipelines found.")
                    return
                fmt = "{:<5} {:<30} {:<15} {:<10} {:<8}"
                print(fmt.format("ID", "Name", "Phone", "Mode", "Active"))
                print("-" * 70)
                for p in items:
                    print(fmt.format(
                        p.id or 0,
                        p.name[:30],
                        p.phone[:15],
                        p.publish_mode.value,
                        "yes" if p.is_active else "no",
                    ))

            elif args.pipeline_action == "add":
                try:
                    pid = await svc.add(
                        args.name,
                        args.phone,
                        prompt_template=args.prompt_template,
                        llm_model=args.llm_model,
                    )
                except ValidationError as e:
                    print(f"Error: {e.errors()[0]['msg']}")
                    return
                print(f"Added pipeline id={pid}: {args.name}")

            elif args.pipeline_action == "edit":
                p = await svc.get(args.id)
                if not p:
                    print(f"Pipeline id={args.id} not found")
                    return
                try:
                    await svc.update(
                        args.id,
                        args.name if args.name else p.name,
                        p.phone,
                        source_channel_ids=p.source_channel_ids,
                        targets=p.targets,
                        prompt_template=(
                            args.prompt_template
                            if args.prompt_template is not None
                            else p.prompt_template
                        ),
                        llm_model=(
                            args.llm_model if args.llm_model is not None else p.llm_model
                        ),
                        publish_mode=p.publish_mode,
                    )
                except ValidationError as e:
                    print(f"Error: {e.errors()[0]['msg']}")
                    return
                print(f"Updated pipeline id={args.id}")

            elif args.pipeline_action == "delete":
                await svc.delete(args.id)
                print(f"Deleted pipeline id={args.id}")

            elif args.pipeline_action == "toggle":
                await svc.toggle(args.id)
                print(f"Toggled pipeline id={args.id}")

        finally:
            await db.close()

    asyncio.run(_run())
