"""CLI commands for image generation."""

from __future__ import annotations

import argparse
import asyncio

from src.cli import runtime
from src.services.image_generation_service import ImageGenerationService


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        action = args.image_action
        svc = ImageGenerationService()

        if action == "generate":
            if not await svc.is_available():
                print("No image providers configured. Set REPLICATE_API_TOKEN or similar env var.")
                return
            model = args.model or None
            prompt = args.prompt
            print(f"Generating image: model={model or 'default'}, prompt={prompt!r}")
            result = await svc.generate(model, prompt)
            if result:
                print(f"Result: {result}")
            else:
                print("Generation failed — check logs")

        elif action == "models":
            provider = args.provider
            query = args.query or ""
            models = await svc.search_models(provider, query)
            if not models:
                print(f"No models found for provider={provider} query={query!r}")
                return
            for m in models:
                runs = f" ({m['run_count']:,} runs)" if m.get("run_count") else ""
                print(f"  {m['model_string']}{runs}")
                if m.get("description"):
                    print(f"    {m['description']}")

        elif action == "providers":
            names = svc.adapter_names
            if not names:
                print("No providers configured. Set env vars: REPLICATE_API_TOKEN, TOGETHER_API_KEY, etc.")
                return
            for name in names:
                print(f"  {name}")

        elif action == "generated":
            _, db = await runtime.init_db(args.config)
            try:
                images = await db.repos.generated_images.list_recent(limit=args.limit)
                if not images:
                    print("No generated images found.")
                    return
                for img in images:
                    prompt = (img.prompt[:60] + "...") if len(img.prompt) > 60 else img.prompt
                    print(f"[{img.id}] {img.created_at} — {prompt}")
                    if img.local_path:
                        print(f"    file=/{img.local_path}")
                    if img.model:
                        print(f"    model={img.model}")
            finally:
                await db.close()

        else:
            print("Usage: image {generate|models|providers|generated}")

    asyncio.run(_run())
