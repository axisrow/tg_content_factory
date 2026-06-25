"""Shared async bodies for the ``image`` CLI group (epic #959, Wave 2 — #1122).

Migrated off the argparse dispatcher onto the Typer ``app`` (see
``src/cli/typer_commands.py``). Each leaf sub-command is a plain ``async def
*_impl`` here — no local ``asyncio.run`` and no ``argparse.Namespace``.
"""

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


async def generate_impl(config_path: str, *, prompt: str, model: str | None = None) -> None:
    """Generate an image from *prompt* using *model* (or the default adapter)."""
    config, db = await runtime.init_db(config_path)
    try:
        svc = await _build_image_service(db, config)
        if not await svc.is_available():
            print("No image providers configured. Set REPLICATE_API_TOKEN or similar env var.")
            return
        print(f"Generating image: model={model or 'default'}, prompt={prompt!r}")
        result = await svc.generate(model, prompt)
        if result:
            print(f"Result: {result}")
        else:
            print(_generation_failure_text(svc))
    finally:
        await db.close()


async def models_impl(config_path: str, *, provider: str, query: str = "", refresh: bool = False) -> None:
    """Search available models for *provider*."""
    config, db = await runtime.init_db(config_path)
    try:
        svc = await _build_image_service(db, config)
        models = await svc.search_models(provider, query or "", refresh=refresh)
        if not models:
            print(f"No models found for provider={provider} query={query!r}")
            return
        for m in models:
            runs = f" ({m['run_count']:,} runs)" if m.get("run_count") else ""
            print(f"  {m['model_string']}{runs}")
            if m.get("description"):
                print(f"    {m['description']}")
    finally:
        await db.close()


async def providers_impl(config_path: str) -> None:
    """List configured image providers."""
    config, db = await runtime.init_db(config_path)
    try:
        svc = await _build_image_service(db, config)
        names = svc.adapter_names
        if not names:
            print("No providers configured. Set env vars: REPLICATE_API_TOKEN, TOGETHER_API_KEY, etc.")
            return
        for name in names:
            print(f"  {name}")
    finally:
        await db.close()


async def generated_impl(config_path: str, *, limit: int = 20) -> None:
    """List recently generated images."""
    _, db = await runtime.init_db(config_path)
    try:
        images = await db.repos.generated_images.list_recent(limit=limit)
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


def run(args: argparse.Namespace) -> None:
    """Thin argparse adapter over the ``*_impl`` bodies (legacy dispatch path).

    The production CLI routes ``image`` through the Typer ``app`` (#1122); this
    wrapper keeps the argparse leaf audit and command-level tests working.
    """
    action = args.image_action
    if action == "generate":
        asyncio.run(generate_impl(args.config, prompt=args.prompt, model=getattr(args, "model", None)))
    elif action == "models":
        asyncio.run(
            models_impl(
                args.config,
                provider=args.provider,
                query=getattr(args, "query", ""),
                refresh=getattr(args, "refresh", False),
            )
        )
    elif action == "providers":
        asyncio.run(providers_impl(args.config))
    elif action == "generated":
        asyncio.run(generated_impl(args.config, limit=getattr(args, "limit", 20)))
    else:
        # Unreachable via the real CLI (Typer/argparse reject unknown actions on
        # parse); kept so the legacy adapter degrades to usage help like the
        # original dispatcher did.
        print("Usage: image {generate|models|providers|generated}")
