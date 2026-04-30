from __future__ import annotations

import re
from datetime import date as date_cls
from string import Formatter

AGENT_PROMPT_TEMPLATE_SETTING = "agent_prompt_template"
ALLOWED_TEMPLATE_VARIABLES = frozenset({"source_messages", "channel_title", "topic", "date"})
DEFAULT_AGENT_PROMPT_TEMPLATE = (
    "Ты — ИИ-Телеграм клиент с полным доступом ко всем функциям Telegram.\n"
    "У тебя подключены реальные Telegram-аккаунты и более 100 инструментов.\n\n"
    "## Твои возможности\n"
    "- Поиск: сообщения в базе (search_messages), семантический (semantic_search), "
    "по Telegram API (search_telegram, search_my_chats, search_in_channel)\n"
    "- Сообщения: отправлять, редактировать, удалять, пересылать, закреплять (send_message, "
    "edit_message, forward_messages, read_messages)\n"
    "- Диалоги и сущности: поиск чатов (search_dialogs), определение любой сущности по "
    "@username/ссылке/ID (resolve_entity)\n"
    "- Каналы: добавлять, собирать сообщения, получать статистику, создавать новые "
    "(add_channel, collect_channel, create_telegram_channel)\n"
    "- Контент: генерация текстов через пайплайны (run_pipeline, generate_draft), "
    "модерация и публикация (approve_run, publish_pipeline_run)\n"
    "- Изображения: генерация через AI (generate_image), список провайдеров и моделей\n"
    "- Аналитика: тренды, пиковые часы, топ-сообщения, скорость сообщений "
    "(get_trending_topics, get_peak_hours, get_top_messages)\n"
    "- Управление: участники групп, админы, права, кик "
    "(get_participants, edit_admin, edit_permissions, kick_participant)\n"
    "- Автоматизация: планировщик сбора, уведомления, фото-рассылки, "
    "поисковые запросы с мониторингом\n"
    "- Веб: поиск в интернете и получение веб-страниц (WebSearch, WebFetch)\n\n"
    "## Как работать\n"
    "1. Определи интент пользователя из контекста — не задавай лишних вопросов, действуй.\n"
    "2. Если запрос содержит @username, t.me/ ссылку или ID — сначала resolve_entity, "
    "чтобы понять тип сущности (юзер, бот, канал, группа), затем действуй по ситуации.\n"
    "3. На вопросы про подключение аккаунта, доступность номера или reconnect сначала используй "
    "get_account_info. Не делай выводы про SMS, 2FA, disabled/not connected без результата live tool "
    "и признаков missing/invalid saved session.\n"
    "4. Выстраивай цепочки инструментов для сложных задач: "
    "например, контент-план = search_telegram + WebSearch + generate_draft + generate_image.\n"
    "5. Будь проактивным: предлагай идеи, если пользователю не хватает фантазии — помоги придумать.\n"
    "6. На вопрос «Что ты можешь?» — расскажи о своих возможностях.\n\n"
    "Отвечай на русском языке. Будь точным и структурированным."
)
_VALIDATION_SAMPLE_VALUES = {
    "source_messages": "sample message",
    "channel_title": "Sample Channel",
    "topic": "Sample Topic",
    "date": "2024-01-01",
}

_CONTEXT_HEADER_RE = re.compile(
    r'^\[КОНТЕКСТ:\s*(?P<channel_title>.+?)(?:,\s*тема\s+(?P<topic>".*?"|#\d+))?,\s*\d+\s+сообщений\]$'
)


class PromptTemplateError(ValueError):
    """Raised when a prompt template contains unsupported placeholders or syntax."""


def validate_prompt_template(template: str) -> None:
    formatter = Formatter()
    try:
        parsed = list(formatter.parse(template))
    except ValueError as exc:
        raise PromptTemplateError(
            "Шаблон содержит некорректный синтаксис фигурных скобок."
        ) from exc

    for _literal_text, field_name, _format_spec, _conversion in parsed:
        if field_name is None:
            continue
        if field_name not in ALLOWED_TEMPLATE_VARIABLES:
            raise PromptTemplateError(f"Недопустимая переменная: {field_name}")

    try:
        template.format_map(_VALIDATION_SAMPLE_VALUES)
    except ValueError as exc:
        raise PromptTemplateError(
            "Шаблон содержит некорректный синтаксис фигурных скобок."
        ) from exc


def build_prompt_template_context(
    history: list[dict],
    *,
    today: date_cls | None = None,
) -> dict[str, str]:
    context_message = next(
        (
            str(msg.get("content") or "")
            for msg in reversed(history)
            if msg.get("role") == "user" and str(msg.get("content") or "").startswith("[КОНТЕКСТ:")
        ),
        "",
    )
    channel_title = ""
    topic = ""
    source_messages = ""
    if context_message:
        lines = context_message.splitlines()
        header = lines[0] if lines else context_message
        body = "\n".join(lines[1:]).strip() if len(lines) > 1 else ""
        match = _CONTEXT_HEADER_RE.match(header)
        if match:
            channel_title = match.group("channel_title") or ""
            topic = match.group("topic") or ""
            if len(topic) >= 2 and topic.startswith('"') and topic.endswith('"'):
                topic = topic[1:-1]
            source_messages = body
        else:
            source_messages = context_message

    resolved_today = today or date_cls.today()
    return {
        "source_messages": source_messages,
        "channel_title": channel_title,
        "topic": topic,
        "date": resolved_today.isoformat(),
    }


def render_prompt_template(template: str, values: dict[str, str]) -> str:
    rendered_values = {name: str(values.get(name, "")) for name in ALLOWED_TEMPLATE_VARIABLES}
    return template.format_map(rendered_values)
