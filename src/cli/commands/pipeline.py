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
                fmt = "{:<5} {:<40} {:<10} {:<20}"
                print(fmt.format("ID", "Name", "Active", "Created"))
                print("-" * 80)
                for pipeline in items:
                    created_at = (
                        pipeline.created_at.strftime("%Y-%m-%d %H:%M:%S")
                        if pipeline.created_at else "—"
                    )
                    print(fmt.format(
                        pipeline.id or 0,
                        pipeline.name[:40],
                        "yes" if pipeline.is_active else "no",
                        created_at,
                    ))

            elif args.pipeline_action == "add":
                try:
                    pipeline_id = await svc.add(args.name)
                except ValidationError as e:
                    print(f"Error: {e.errors()[0]['msg']}")
                    return
                print(f"Added pipeline id={pipeline_id}: {args.name}")

            elif args.pipeline_action == "edit":
                pipeline = await svc.get(args.id)
                if not pipeline:
                    print(f"Pipeline id={args.id} not found")
                    return
                try:
                    await svc.update(args.id, args.name if args.name else pipeline.name)
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
