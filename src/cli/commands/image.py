"""CLI commands for image generation."""

from __future__ import annotations

import argparse
import asyncio
import logging

from src.cli import runtime
from src.services.image_generation_service import ImageGenerationService

logger = logging.getLogger(__name__)


async def _build_image_service(db, config) -> ImageGenerationService:
    """Build with DB-configured image providers + env fallback.

    Mirrors web _get_image_service so the CLI sees providers configured only via
    the web UI (encrypted DB keys), not just env vars (audit #838/6).
    """
    try:
        from src.services.image_provider_service import ImageProviderService

        provider_svc = ImageProviderService(db, config)
        configs = await provider_svc.load_provider_configs()
        adapters = provider_svc.build_adapters(configs)
        if adapters:
            return ImageGenerationService(adapters=adapters)
    except Exception:
        logger.warning("Failed to load image providers from DB", exc_info=True)
    return ImageGenerationService()


def _generation_failure_text(svc: ImageGenerationService) -> str:
    failure = svc.last_failure
    if failure is not None and getattr(failure, "is_timeout", False):
        return failure.user_message(lang="en")
    return "Generation failed — check logs"


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        action = args.image_action
        config, db = await runtime.init_db(args.config)
        try:
            await _run_action(action, args, db, config)
        finally:
            await db.close()

    async def _run_action(action, args, db, config) -> None:
        svc = await _build_image_service(db, config)

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
                print(_generation_failure_text(svc))

        elif action == "models":
            provider = args.provider
            query = args.query or ""
            refresh = getattr(args, "refresh", False)
            models = await svc.search_models(provider, query, refresh=refresh)
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

        else:
            print("Usage: image {generate|models|providers|generated}")

    asyncio.run(_run())
