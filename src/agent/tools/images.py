"""Agent tools for image generation."""

from __future__ import annotations

import logging
import os
from typing import Annotated

from claude_agent_sdk import tool

from src.agent.tools._registry import _text_response
from src.web.paths import DATA_IMAGE_DIR

logger = logging.getLogger(__name__)


GENERATE_IMAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "prompt": {
            "type": "string",
            "description": "Текстовый промпт для генерации изображения",
        },
        "model": {
            "anyOf": [{"type": "string"}, {"type": "null"}],
            "description": "Необязательная модель в формате provider:model_id",
        },
    },
    "required": ["prompt"],
    "additionalProperties": False,
}


def _default_model_for_adapters(adapter_names) -> str:
    from src.services.image_provider_service import IMAGE_PROVIDER_ORDER, image_provider_spec

    available = set(adapter_names)
    for provider in IMAGE_PROVIDER_ORDER:
        if provider not in available:
            continue
        spec = image_provider_spec(provider)
        if spec and spec.default_model:
            return spec.default_model
    return ""


def _no_default_model_message(adapter_names) -> str:
    names = sorted({str(name) for name in adapter_names if name})
    message = (
        "Не удалось выбрать модель изображения автоматически. "
        "Задайте default_image_model в настройках или передайте model явно "
        "в формате provider:model_id (например together:black-forest-labs/FLUX.1-schnell)."
    )
    if names:
        message += f" Доступные adapters: {', '.join(names)}."
    return message


def _is_model_provider_available(model: str, adapter_names) -> bool:
    provider, sep, _ = model.partition(":")
    if not sep:
        return True
    return provider in set(adapter_names)


async def resolve_default_image_model(requested_model, db, image_service) -> str:
    """Return the explicit image model to use for an agent generate_image call."""
    if isinstance(requested_model, str) and requested_model.strip():
        return requested_model.strip()

    if db is not None:
        try:
            value = await db.get_setting("default_image_model")
        except Exception:
            logger.warning("Failed to read default_image_model setting", exc_info=True)
            value = None
        # get_setting's contract is str | None; the isinstance guard also coerces
        # bare-MagicMock returns to "" on the deepagents-sync test path.
        if isinstance(value, str) and value.strip():
            saved_model = value.strip()
            if _is_model_provider_available(saved_model, image_service.adapter_names):
                return saved_model
            logger.warning(
                "Ignoring default_image_model=%s because its provider is not available; adapters=%s",
                saved_model,
                image_service.adapter_names,
            )

    return _default_model_for_adapters(image_service.adapter_names)


def register(db, client_pool, embedding_service, **kwargs):
    config = kwargs.get("config")
    tools = []

    async def _build_image_service():
        """Build ImageGenerationService with DB providers + env fallback."""
        from src.services.image_generation_service import ImageGenerationService

        if db and config:
            try:
                from src.services.image_provider_service import ImageProviderService

                svc = ImageProviderService(db, config)
                configs = await svc.load_provider_configs()
                adapters = svc.build_adapters(configs)
                if adapters:
                    return ImageGenerationService(adapters=adapters)
            except Exception:
                logger.warning("Failed to load image providers from DB", exc_info=True)
        return ImageGenerationService()

    @tool(
        "generate_image",
        "Generate an image from a text prompt. Optional model format: 'provider:model_id' "
        "(e.g. 'together:black-forest-labs/FLUX.1-schnell'). "
        "When omitted, the configured/default image model is used automatically.",
        GENERATE_IMAGE_SCHEMA,
    )
    async def generate_image(args):
        prompt = args.get("prompt", "")
        if not prompt:
            return _text_response("Ошибка: prompt обязателен.")
        model = args.get("model")
        try:
            svc = await _build_image_service()
            if not await svc.is_available():
                return _text_response("Генерация изображений не настроена. Добавьте провайдера в настройках.")
            resolved_model = await resolve_default_image_model(model, db, svc)
            if not resolved_model:
                return _text_response(_no_default_model_message(svc.adapter_names))
            result = await svc.generate(model=resolved_model, text=prompt)
            if result and (result.startswith("https://") or result.startswith("http://")):
                import hashlib
                from urllib.parse import urlparse

                import httpx

                url_path = urlparse(result).path
                _, dot, suffix = url_path.rpartition(".")
                ext = suffix[:4] if dot and suffix.isalnum() else "png"
                filename = hashlib.md5(result.encode()).hexdigest()[:12] + "." + ext
                local_path = str(DATA_IMAGE_DIR / filename)
                max_image_bytes = 50 * 1024 * 1024  # 50 MB
                async with httpx.AsyncClient() as http_client:
                    async with http_client.stream("GET", result, follow_redirects=True, timeout=30) as resp:
                        resp.raise_for_status()
                        try:
                            with open(local_path, "wb") as f:
                                total = 0
                                async for chunk in resp.aiter_bytes(chunk_size=65536):
                                    total += len(chunk)
                                    if total > max_image_bytes:
                                        raise ValueError("Image exceeds 50 MB limit")
                                    f.write(chunk)
                        except BaseException:
                            if os.path.exists(local_path):
                                os.unlink(local_path)
                            raise
                logger.info("Image downloaded to %s", local_path)
                if db:
                    await db.repos.generated_images.save(
                        prompt=prompt, model=resolved_model, image_url=result, local_path=local_path,
                    )
                return _text_response(
                    f"Изображение создано!\n\n"
                    f"![{prompt}](/data/image/{filename})"
                )
            if result:
                return _text_response(f"Изображение сгенерировано:\n{result}")
            failure = getattr(svc, "last_failure", None)
            if failure is not None and getattr(failure, "is_timeout", False):
                return _text_response(failure.user_message(lang="ru"))
            return _text_response("Генерация не вернула результат.")
        except Exception as e:
            return _text_response(f"Ошибка генерации изображения: {e}")

    tools.append(generate_image)

    @tool(
        "list_image_models",
        "Search available image models for a provider. "
        "Get provider name from list_image_providers first. query filters by model name substring.",
        {
            "provider": Annotated[str, "Название провайдера из list_image_providers"],
            "query": Annotated[str, "Подстрока для фильтрации моделей по имени"],
        },
    )
    async def list_image_models(args):
        provider = args.get("provider", "")
        if not provider:
            return _text_response("Ошибка: provider обязателен.")
        query = args.get("query", "")
        try:
            svc = await _build_image_service()
            models = await svc.search_models(provider=provider, query=query)
            if not models:
                return _text_response(f"Модели для {provider} не найдены.")
            lines = [f"Модели {provider} ({len(models)}):"]
            for m in models:
                name = m.get("id", m.get("name", "?"))
                parts = [f"- {name}"]
                rc = m.get("run_count")
                if rc:
                    parts.append(f"({rc:,} runs)")
                rank = m.get("rank")
                if rank is not None:
                    parts.append(f"[rank {rank}]")
                lines.append(" ".join(parts))
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка поиска моделей: {e}")

    tools.append(list_image_models)

    @tool("list_image_providers", "List configured image generation providers", {})
    async def list_image_providers(args):
        try:
            svc = await _build_image_service()
            names = svc.adapter_names
            if not names:
                return _text_response("Провайдеры изображений не настроены.")
            lines = [f"Провайдеры изображений ({len(names)}):"]
            for n in names:
                lines.append(f"- {n}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения провайдеров: {e}")

    tools.append(list_image_providers)

    @tool(
        "list_generated_images",
        "List recently generated images stored in the database. "
        "Returns image ID, prompt, model, local file path, and creation date.",
        {"limit": Annotated[int, "Максимальное количество результатов"]},
    )
    async def list_generated_images(args):
        limit = args.get("limit") or 20
        try:
            images = await db.repos.generated_images.list_recent(limit=limit)
            if not images:
                return _text_response("Нет сгенерированных изображений.")
            lines = [f"Последние {len(images)} изображений:"]
            for img in images:
                prompt_preview = (img.prompt[:60] + "...") if len(img.prompt) > 60 else img.prompt
                lines.append(f"  [{img.id}] {img.created_at} — {prompt_preview}")
                if img.local_path:
                    lines.append(f"       Файл: /{img.local_path}")
                if img.model:
                    lines.append(f"       Модель: {img.model}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения списка изображений: {e}")

    tools.append(list_generated_images)

    return tools
