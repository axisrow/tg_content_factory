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

- **Claude Code**: `/plugin marketplace add https://github.com/axisrow/tg_content_factory` → `/plugin install tg-agent@tg-agent-marketplace` (короткая форма `axisrow/tg_content_factory` клонирует по SSH и падает без ключей), либо скопировать каталог: `cp -r skills/tg-agent ~/.claude/skills/tg-agent`
- **OpenCode / Codex / другие**: отдайте [`skills/tg-agent/SKILL.md`](https://github.com/axisrow/tg_content_factory/blob/main/skills/tg-agent/SKILL.md) как инструкцию

Пакет `tg-agent` — это ядро (CLI + воркер). Устаревшие поверхности — веб-панель (`serve`) и интерактивный TUI (`agent chat` без промпта) — вынесены в опциональные пакеты `tg-webui` и `tg-tui` (#1720); на PyPI их нет, они ставятся из checkout репозитория (ниже).

Или из исходников:

```bash
git clone https://github.com/axisrow/tg_content_factory.git
cd tg_content_factory
pip install -e ".[dev]"
# Confirm the editable install; isolated tools such as mutmut need this.
python -m pip show tg-agent
# The core imports the monorepo shared package at runtime.
pip install -e packages/tg_shared
# Optional legacy surfaces: the web panel (`serve`) and the interactive TUI.
pip install -e packages/tg_webui -e packages/tg_tui
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
