"""Agent tools for image generation."""

from __future__ import annotations

from claude_agent_sdk import tool

from src.agent.tools._registry import _text_response


def register(db, client_pool, embedding_service, **kwargs):
    tools = []

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
            from src.services.image_generation_service import ImageGenerationService

            svc = ImageGenerationService(db)
            if not await svc.is_available():
                return _text_response("Генерация изображений не настроена. Добавьте провайдера в настройках.")
            result = await svc.generate(model=model, text=prompt)
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
            from src.services.image_generation_service import ImageGenerationService

            svc = ImageGenerationService(db)
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
            from src.services.image_generation_service import ImageGenerationService

            svc = ImageGenerationService(db)
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

    return tools
