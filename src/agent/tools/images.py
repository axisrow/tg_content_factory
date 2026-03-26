"""Agent tools for image generation."""

from __future__ import annotations

import logging

from claude_agent_sdk import tool

from src.agent.tools._registry import _text_response

logger = logging.getLogger(__name__)


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
        "Generate an image from a text prompt using configured image providers. "
        "Model format: 'provider:model_id' (e.g. 'together:black-forest-labs/FLUX.1-schnell'). "
        "If model is omitted, uses the first registered adapter.",
        {"prompt": str, "model": str},
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
            result = await svc.generate(model=model, text=prompt)
            if result and (result.startswith("https://") or result.startswith("http://")):
                import hashlib
                import os

                import httpx

                os.makedirs("data/image", exist_ok=True)
                from urllib.parse import urlparse

                url_path = urlparse(result).path
                _, dot, suffix = url_path.rpartition(".")
                ext = suffix[:4] if dot and suffix.isalnum() else "png"
                filename = hashlib.md5(result.encode()).hexdigest()[:12] + "." + ext
                local_path = os.path.join("data/image", filename)
                max_image_bytes = 50 * 1024 * 1024  # 50 MB
                async with httpx.AsyncClient() as http_client:
                    async with http_client.stream("GET", result, follow_redirects=True, timeout=30) as resp:
                        resp.raise_for_status()
                        with open(local_path, "wb") as f:
                            total = 0
                            async for chunk in resp.aiter_bytes(chunk_size=65536):
                                total += len(chunk)
                                if total > max_image_bytes:
                                    raise ValueError("Image exceeds 50 MB limit")
                                f.write(chunk)
                logger.info("Image downloaded to %s", local_path)
                await db.repos.generated_images.save(
                    prompt=prompt, model=model, image_url=result, local_path=local_path,
                )
                return _text_response(
                    f"Изображение сгенерировано:\n"
                    f"- URL: {result}\n"
                    f"- Файл: {local_path}"
                )
            if result:
                return _text_response(f"Изображение сгенерировано:\n{result}")
            return _text_response("Генерация не вернула результат.")
        except Exception as e:
            return _text_response(f"Ошибка генерации изображения: {e}")

    tools.append(generate_image)

    @tool(
        "list_image_models",
        "Search available image generation models for a provider",
        {"provider": str, "query": str},
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
            for m in models[:30]:
                name = m.get("id", m.get("name", "?"))
                lines.append(f"- {name}")
            if len(models) > 30:
                lines.append(f"... и ещё {len(models) - 30}")
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
        {"limit": int},
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
