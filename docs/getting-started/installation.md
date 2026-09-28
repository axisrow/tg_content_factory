# Установка

## Требования

- Python 3.11+
- Telegram API ключи ([получить на my.telegram.org](https://my.telegram.org/apps))
- SQLite (встроен в Python)

## Установка

```bash
pip install tg-agent
```

### Для агентов кодирования

После установки добавьте скилл агенту — он описывает каталог команд и правила безопасности:

- **Claude Code**: `/plugin marketplace add axisrow/tg_content_factory` → `/plugin install tg-agent@tg-agent-marketplace`, либо скопировать каталог: `cp -r skills/tg-agent ~/.claude/skills/tg-agent`
- **OpenCode / Codex / другие**: отдайте [`skills/tg-agent/SKILL.md`](https://github.com/axisrow/tg_content_factory/blob/main/skills/tg-agent/SKILL.md) как инструкцию

Или из исходников:

```bash
git clone https://github.com/axisrow/tg_content_factory.git
cd tg_content_factory
pip install -e ".[dev]"
# Confirm the editable install; isolated tools such as mutmut need this.
python -m pip show tg-agent
```

## Опциональные зависимости

> **Важно для инструментов разработки.** Команда `pip install -e ".[dev]"` обязательна
> перед запуском инструментов, которые копируют исходники в отдельный каталог
> (например, `mutmut`). Запуск таких инструментов из корня репозитория может
> случайно работать за счёт текущего каталога, но не проверяет, что пакет
> импортируется в изолированном окружении. Распространяемое имя пакета —
> `tg-agent`, поэтому проверяйте установку командой `python -m pip show tg-agent`.

```bash
# Семантический поиск (numpy-based KNN)
pip install numpy

# Провайдеры LLM (для контент-пайплайнов)
pip install openai cohere

# Документация
pip install -e ".[docs]"
```
