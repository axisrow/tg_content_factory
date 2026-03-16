from __future__ import annotations

import re
from datetime import date as date_cls
from string import Formatter

AGENT_PROMPT_TEMPLATE_SETTING = "agent_prompt_template"
ALLOWED_TEMPLATE_VARIABLES = frozenset({"source_messages", "channel_title", "topic", "date"})
DEFAULT_AGENT_PROMPT_TEMPLATE = (
    "Ты — аналитический ассистент для работы с данными из Telegram-каналов.\n"
    "Используй search_messages для поиска сообщений и get_channels для списка каналов.\n"
    "Основной use-case: анализ вопросов и ответов из каналов для создания учебного курса.\n"
    "Отвечай на русском языке. Будь точным и структурированным."
)

_CONTEXT_HEADER_RE = re.compile(
    r'^\[КОНТЕКСТ:\s*(?P<channel_title>.+?)(?:,\s*тема\s+(?P<topic>".*?"|#\d+))?,\s*\d+\s+сообщений\]$'
)


class PromptTemplateError(ValueError):
    """Raised when a prompt template contains unsupported placeholders or syntax."""


def validate_prompt_template(template: str) -> None:
    formatter = Formatter()
    try:
        parsed = list(formatter.parse(template))
        template.format_map({name: name for name in ALLOWED_TEMPLATE_VARIABLES})
    except ValueError as exc:
        raise PromptTemplateError(
            "Шаблон содержит некорректный синтаксис фигурных скобок."
        ) from exc
    except KeyError as exc:
        raise PromptTemplateError(f"Недопустимая переменная: {exc.args[0]}") from exc

    for _literal_text, field_name, _format_spec, _conversion in parsed:
        if field_name is None:
            continue
        if field_name not in ALLOWED_TEMPLATE_VARIABLES:
            raise PromptTemplateError(f"Недопустимая переменная: {field_name}")


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
            if topic.startswith('"') and topic.endswith('"'):
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
    validate_prompt_template(template)
    rendered_values = {name: str(values.get(name, "")) for name in ALLOWED_TEMPLATE_VARIABLES}
    return template.format_map(rendered_values)
