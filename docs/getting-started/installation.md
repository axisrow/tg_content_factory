# Установка

## Требования

- Python 3.11.5+
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

Пакет `tg-agent` — это CLI и воркер-демон. Веб-панель (`serve`) и интерактивный агент-чат (TUI) — опциональные пакеты `tg-webui` и `tg-tui`, они ставятся дополнительно (`pip install tg-webui`, `pip install tg-tui`). Обе — устаревшие поверхности: работают, но не развиваются. Stdio-мост MCP (`mcp-server`) и агентные бэкенды Codex/ADK удалены в #1868.

При обновлении с прежней версии перезапустите работающий демон один раз (`tg-agent restart`) и общий gateway, если он используется (`tg-agent gateway restart --shared`): старый демон/gateway не обслуживает relay, и живые команды рядом с ним сразу падают с этой подсказкой.

Или из исходников:

```bash
git clone https://github.com/axisrow/tg_content_factory.git
cd tg_content_factory
# Install both packages in one transaction: tg-shared is not on PyPI.
pip install -e packages/tg_shared -e ".[dev]"
# Confirm the editable install; isolated tools such as mutmut need this.
python -m pip show tg-agent
```

## Опциональные зависимости

> **Важно для инструментов разработки.** Команда `pip install -e packages/tg_shared -e ".[dev]"` обязательна
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
