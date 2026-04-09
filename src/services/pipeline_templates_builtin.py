"""Built-in pipeline templates that are seeded into the database on startup."""
from __future__ import annotations

from src.models import PipelineEdge, PipelineGraph, PipelineNode, PipelineNodeType, PipelineTemplate


def _node(node_id: str, ntype: PipelineNodeType, name: str, config: dict, x: float, y: float) -> PipelineNode:
    return PipelineNode(id=node_id, type=ntype, name=name, config=config, position={"x": x, "y": y})


def _edge(from_node: str, to_node: str) -> PipelineEdge:
    return PipelineEdge.model_validate({"from": from_node, "to": to_node})


def get_builtin_templates() -> list[PipelineTemplate]:
    return [
        # 1. Content generation
        PipelineTemplate(
            name="Контент-генерация",
            description="Генерация постов на основе источников: поиск контекста → LLM → публикация",
            category="content",
            is_builtin=True,
            template_json=PipelineGraph(
                nodes=[
                    _node("source_1", PipelineNodeType.SOURCE, "Источники", {"channel_ids": []}, 0, 0),
                    _node(
                        "retrieve_1", PipelineNodeType.RETRIEVE_CONTEXT,
                        "Поиск контекста", {"limit": 8, "method": "hybrid"}, 220, 0,
                    ),
                    _node(
                        "llm_1", PipelineNodeType.LLM_GENERATE, "Генерация текста",
                        {"prompt_template": "Напиши пост на основе: {source_messages}",
                         "max_tokens": 2000, "temperature": 0.7}, 440, 0,
                    ),
                    _node("publish_1", PipelineNodeType.PUBLISH, "Публикация",
                          {"targets": [], "mode": "moderated"}, 660, 0),
                ],
                edges=[
                    _edge("source_1", "retrieve_1"),
                    _edge("retrieve_1", "llm_1"),
                    _edge("llm_1", "publish_1"),
                ],
            ),
        ),
        # 2. Content generation with image
        PipelineTemplate(
            name="Контент-генерация с картинкой",
            description="Генерация поста + изображения: поиск контекста → LLM → изображение → публикация",
            category="content",
            is_builtin=True,
            template_json=PipelineGraph(
                nodes=[
                    _node("source_1", PipelineNodeType.SOURCE, "Источники", {"channel_ids": []}, 0, 0),
                    _node(
                        "retrieve_1", PipelineNodeType.RETRIEVE_CONTEXT,
                        "Поиск контекста", {"limit": 8, "method": "hybrid"}, 220, 0,
                    ),
                    _node(
                        "llm_1", PipelineNodeType.LLM_GENERATE, "Генерация текста",
                        {"prompt_template": "Напиши пост на основе: {source_messages}",
                         "max_tokens": 2000, "temperature": 0.7}, 440, 0,
                    ),
                    _node(
                        "image_1", PipelineNodeType.IMAGE_GENERATE, "Генерация картинки",
                        {"model": "together:black-forest-labs/FLUX.1-schnell"}, 440, 120,
                    ),
                    _node("publish_1", PipelineNodeType.PUBLISH, "Публикация",
                          {"targets": [], "mode": "moderated"}, 660, 60),
                ],
                edges=[
                    _edge("source_1", "retrieve_1"),
                    _edge("retrieve_1", "llm_1"),
                    _edge("llm_1", "image_1"),
                    _edge("llm_1", "publish_1"),
                    _edge("image_1", "publish_1"),
                ],
            ),
        ),
        # 3. Notification on search query match
        PipelineTemplate(
            name="Уведомление по поисковому запросу",
            description="Отправка уведомления в бот при совпадении сообщений с поисковым запросом",
            category="automation",
            is_builtin=True,
            template_json=PipelineGraph(
                nodes=[
                    _node(
                        "trigger_1", PipelineNodeType.SEARCH_QUERY_TRIGGER,
                        "Триггер запроса", {"query": ""}, 0, 0,
                    ),
                    _node(
                        "notify_1", PipelineNodeType.NOTIFY, "Уведомление",
                        {"message_template": "Найдено совпадение: {text}"}, 220, 0,
                    ),
                ],
                edges=[
                    _edge("trigger_1", "notify_1"),
                ],
            ),
        ),
        # 4. React to messages with delay
        PipelineTemplate(
            name="Реакции на сообщения",
            description="Автоматически ставит реакции на сообщения в канале/чате со случайной задержкой",
            category="automation",
            is_builtin=True,
            template_json=PipelineGraph(
                nodes=[
                    _node("source_1", PipelineNodeType.SOURCE, "Источник", {"channel_ids": []}, 0, 0),
                    _node(
                        "delay_1", PipelineNodeType.DELAY, "Случайная задержка",
                        {"min_seconds": 5, "max_seconds": 60}, 220, 0,
                    ),
                    _node(
                        "react_1", PipelineNodeType.REACT, "Реакция",
                        {"emoji": "👍", "random_emojis": []}, 440, 0,
                    ),
                ],
                edges=[
                    _edge("source_1", "delay_1"),
                    _edge("delay_1", "react_1"),
                ],
            ),
        ),
        # 5. Delete join/leave messages
        PipelineTemplate(
            name="Удаление join/leave сообщений",
            description="Удаляет сообщения о вступлении и выходе пользователей из чата",
            category="moderation",
            is_builtin=True,
            template_json=PipelineGraph(
                nodes=[
                    _node("source_1", PipelineNodeType.SOURCE, "Источник", {"channel_ids": []}, 0, 0),
                    _node(
                        "filter_1", PipelineNodeType.FILTER, "Фильтр join/leave",
                        {"type": "service_message", "service_types": ["user_joined", "user_left"]}, 220, 0,
                    ),
                    _node("delete_1", PipelineNodeType.DELETE_MESSAGE, "Удаление", {}, 440, 0),
                ],
                edges=[
                    _edge("source_1", "filter_1"),
                    _edge("filter_1", "delete_1"),
                ],
            ),
        ),
        # 6. Forward messages
        PipelineTemplate(
            name="Пересылка сообщений",
            description="Пересылает сообщения из источника в целевой канал/чат",
            category="automation",
            is_builtin=True,
            template_json=PipelineGraph(
                nodes=[
                    _node("source_1", PipelineNodeType.SOURCE, "Источник", {"channel_ids": []}, 0, 0),
                    _node("forward_1", PipelineNodeType.FORWARD, "Пересылка", {"targets": []}, 220, 0),
                ],
                edges=[
                    _edge("source_1", "forward_1"),
                ],
            ),
        ),
        # 7. Rewrite via LLM
        PipelineTemplate(
            name="Рерайт через LLM",
            description="Рерайтит сообщения из источника и публикует в целевой канал",
            category="content",
            is_builtin=True,
            template_json=PipelineGraph(
                nodes=[
                    _node("source_1", PipelineNodeType.SOURCE, "Источник", {"channel_ids": []}, 0, 0),
                    _node(
                        "refine_1", PipelineNodeType.LLM_REFINE, "Рерайт",
                        {"prompt": "Перепиши следующий текст своими словами, сохраняя смысл:\n\n{text}",
                         "max_tokens": 1000}, 220, 0,
                    ),
                    _node("publish_1", PipelineNodeType.PUBLISH, "Публикация",
                          {"targets": [], "mode": "moderated"}, 440, 0),
                ],
                edges=[
                    _edge("source_1", "refine_1"),
                    _edge("refine_1", "publish_1"),
                ],
            ),
        ),
        # 8. Delete messages by keywords
        PipelineTemplate(
            name="Удаление по ключевым словам",
            description="Удаляет сообщения, содержащие определённые ключевые слова или ссылки",
            category="moderation",
            is_builtin=True,
            template_json=PipelineGraph(
                nodes=[
                    _node("source_1", PipelineNodeType.SOURCE, "Источник", {"channel_ids": []}, 0, 0),
                    _node(
                        "filter_1", PipelineNodeType.FILTER, "Фильтр по ключевым словам",
                        {"type": "keywords", "keywords": [], "match_links": False}, 220, 0,
                    ),
                    _node("delete_1", PipelineNodeType.DELETE_MESSAGE, "Удаление", {}, 440, 0),
                ],
                edges=[
                    _edge("source_1", "filter_1"),
                    _edge("filter_1", "delete_1"),
                ],
            ),
        ),
        # 9. Delete anonymous messages
        PipelineTemplate(
            name="Удаление анонимных сообщений",
            description="Удаляет сообщения от анонимных пользователей (пишущих от имени канала)",
            category="moderation",
            is_builtin=True,
            template_json=PipelineGraph(
                nodes=[
                    _node("source_1", PipelineNodeType.SOURCE, "Источник", {"channel_ids": []}, 0, 0),
                    _node(
                        "filter_1", PipelineNodeType.FILTER, "Фильтр анонимных",
                        {"type": "anonymous_sender"}, 220, 0,
                    ),
                    _node("delete_1", PipelineNodeType.DELETE_MESSAGE, "Удаление", {}, 440, 0),
                ],
                edges=[
                    _edge("source_1", "filter_1"),
                    _edge("filter_1", "delete_1"),
                ],
            ),
        ),
        # 10. Competitor monitoring
        PipelineTemplate(
            name="Мониторинг конкурентов",
            description="Мониторит каналы по ключевым словам и отправляет уведомления при совпадении",
            category="monitoring",
            is_builtin=True,
            template_json=PipelineGraph(
                nodes=[
                    _node("source_1", PipelineNodeType.SOURCE, "Каналы конкурентов",
                          {"channel_ids": []}, 0, 0),
                    _node(
                        "filter_1", PipelineNodeType.FILTER, "Фильтр по темам",
                        {"type": "keywords", "keywords": []}, 220, 0,
                    ),
                    _node(
                        "notify_1", PipelineNodeType.NOTIFY, "Уведомление",
                        {"message_template": "Конкурент: {channel_title}\n{text}"}, 440, 0,
                    ),
                ],
                edges=[
                    _edge("source_1", "filter_1"),
                    _edge("filter_1", "notify_1"),
                ],
            ),
        ),
        # 11. Auto-responder
        PipelineTemplate(
            name="Автоответчик",
            description="Отвечает на сообщения по ключевым словам с помощью LLM",
            category="automation",
            is_builtin=True,
            template_json=PipelineGraph(
                nodes=[
                    _node("source_1", PipelineNodeType.SOURCE, "Источник", {"channel_ids": []}, 0, 0),
                    _node(
                        "filter_1", PipelineNodeType.FILTER, "Фильтр по ключевым словам",
                        {"type": "keywords", "keywords": []}, 220, 0,
                    ),
                    _node(
                        "llm_1", PipelineNodeType.LLM_GENERATE, "Генерация ответа",
                        {
                            "prompt_template": "Напиши ответ на сообщение:\n\n{source_messages}",
                            "max_tokens": 500,
                        }, 440, 0,
                    ),
                    _node(
                        "publish_1", PipelineNodeType.PUBLISH, "Публикация ответа",
                        {"targets": [], "mode": "auto", "reply": True}, 660, 0,
                    ),
                ],
                edges=[
                    _edge("source_1", "filter_1"),
                    _edge("filter_1", "llm_1"),
                    _edge("llm_1", "publish_1"),
                ],
            ),
        ),
        # 12. Summarization
        PipelineTemplate(
            name="Суммаризация постов",
            description="Собирает сообщения из источников и создаёт краткую саммари с помощью LLM",
            category="content",
            is_builtin=True,
            template_json=PipelineGraph(
                nodes=[
                    _node("source_1", PipelineNodeType.SOURCE, "Источники", {"channel_ids": []}, 0, 0),
                    _node(
                        "retrieve_1", PipelineNodeType.RETRIEVE_CONTEXT,
                        "Поиск контекста", {"limit": 12, "method": "hybrid"}, 220, 0,
                    ),
                    _node(
                        "llm_1", PipelineNodeType.LLM_GENERATE, "Суммаризация",
                        {"prompt_template": (
                             "Сделай краткую выжимку из следующих сообщений,"
                             " выдели основные темы и факты:\n\n{source_messages}"
                         ),
                         "max_tokens": 1000, "temperature": 0.3}, 440, 0,
                    ),
                    _node("publish_1", PipelineNodeType.PUBLISH, "Публикация",
                          {"targets": [], "mode": "moderated"}, 660, 0),
                ],
                edges=[
                    _edge("source_1", "retrieve_1"),
                    _edge("retrieve_1", "llm_1"),
                    _edge("llm_1", "publish_1"),
                ],
            ),
        ),
        # 13. Translation
        PipelineTemplate(
            name="Перевод постов",
            description="Переводит сообщения из источников на русский язык и публикует в целевой канал",
            category="content",
            is_builtin=True,
            template_json=PipelineGraph(
                nodes=[
                    _node("source_1", PipelineNodeType.SOURCE, "Источники", {"channel_ids": []}, 0, 0),
                    _node(
                        "llm_1", PipelineNodeType.LLM_GENERATE, "Перевод",
                        {"prompt_template": (
                             "Переведи следующий текст на русский язык,"
                             " сохранив стиль и форматирование:\n\n{source_messages}"
                         ),
                         "max_tokens": 2000, "temperature": 0.3}, 220, 0,
                    ),
                    _node("publish_1", PipelineNodeType.PUBLISH, "Публикация",
                          {"targets": [], "mode": "moderated"}, 440, 0),
                ],
                edges=[
                    _edge("source_1", "llm_1"),
                    _edge("llm_1", "publish_1"),
                ],
            ),
        ),
        # 14. Smart assistant (agent loop)
        PipelineTemplate(
            name="Умный ассистент",
            description="Агентный пайплайн: анализирует сообщения с помощью LLM-ассистента и публикует результат",
            category="automation",
            is_builtin=True,
            template_json=PipelineGraph(
                nodes=[
                    _node("source_1", PipelineNodeType.SOURCE, "Источники", {"channel_ids": []}, 0, 0),
                    _node(
                        "fetch_1", PipelineNodeType.FETCH_MESSAGES,
                        "Загрузка сообщений", {}, 220, 0,
                    ),
                    _node(
                        "agent_1", PipelineNodeType.AGENT_LOOP, "Агент",
                        {"system_prompt": (
                             "Ты контент-ассистент. Проанализируй сообщения"
                             " и создай интересный пост на их основе."
                         ),
                         "max_tokens": 2000, "temperature": 0.7}, 440, 0,
                    ),
                    _node("publish_1", PipelineNodeType.PUBLISH, "Публикация",
                          {"targets": [], "mode": "moderated"}, 660, 0),
                ],
                edges=[
                    _edge("source_1", "fetch_1"),
                    _edge("fetch_1", "agent_1"),
                    _edge("agent_1", "publish_1"),
                ],
            ),
        ),
        # 15. Agent moderation
        PipelineTemplate(
            name="Агент-модерация",
            description=(
                "Агентный пайплайн для модерации: агент анализирует сообщения,"
                " затем условие проверяет решение об удалении"
            ),
            category="moderation",
            is_builtin=True,
            template_json=PipelineGraph(
                nodes=[
                    _node("source_1", PipelineNodeType.SOURCE, "Источник", {"channel_ids": []}, 0, 0),
                    _node(
                        "fetch_1", PipelineNodeType.FETCH_MESSAGES,
                        "Загрузка сообщений", {}, 220, 0,
                    ),
                    _node(
                        "agent_1", PipelineNodeType.AGENT_LOOP, "Модератор",
                        {"system_prompt": "Ты модератор чата. Проанализируй сообщение и реши, нарушает ли оно правила. "
                         "Ответь DELETE если нужно удалить, или OK если сообщение допустимое.",
                         "max_tokens": 50, "temperature": 0.1}, 440, 0,
                    ),
                    _node(
                        "condition_1", PipelineNodeType.CONDITION, "Проверка решения",
                        {"field": "generated_text", "operator": "contains", "value": "DELETE"}, 660, 0,
                    ),
                    _node("delete_1", PipelineNodeType.DELETE_MESSAGE, "Удаление", {}, 880, 0),
                ],
                edges=[
                    _edge("source_1", "fetch_1"),
                    _edge("fetch_1", "agent_1"),
                    _edge("agent_1", "condition_1"),
                    _edge("condition_1", "delete_1"),
                ],
            ),
        ),
    ]
