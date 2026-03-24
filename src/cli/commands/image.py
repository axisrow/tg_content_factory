"""CLI commands for image generation."""

from __future__ import annotations

import argparse
import asyncio
import logging

from src.cli import runtime
from src.services.image_generation_service import ImageGenerationService

logger = logging.getLogger(__name__)


async def _build_service(config_path: str) -> ImageGenerationService:
    """Build ImageGenerationService with DB-configured providers + env fallback."""
    try:
        from src.services.image_provider_service import ImageProviderService

        config, db = await runtime.init_db(config_path)
        try:
            svc = ImageProviderService(db, config)
            configs = await svc.load_provider_configs()
            adapters = svc.build_adapters(configs)
            if adapters:
                return ImageGenerationService(adapters=adapters)
        finally:
            await db.close()
    except Exception:
        logger.warning("Failed to load image providers from DB, falling back to env vars", exc_info=True)
    return ImageGenerationService()


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        action = args.image_action
        svc = await _build_service(args.config)

        if action == "generate":
            if not await svc.is_available():
                print("No image providers configured. Set REPLICATE_API_TOKEN or similar env var, or configure via Settings UI.")
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
                print("No providers configured. Set env vars: REPLICATE_API_TOKEN, TOGETHER_API_KEY, etc., or configure via Settings UI.")
                return
            for name in names:
                print(f"  {name}")

        else:
            print("Usage: image {generate|models|providers}")

    asyncio.run(_run())
