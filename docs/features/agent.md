# AI-агент

Встроенный AI-агент с 100+ MCP-инструментами, покрывающими все операции системы.

## Backends

| Backend | Условие активации |
|---------|------------------|
| `claude-agent-sdk` | `ANTHROPIC_API_KEY` или `CLAUDE_CODE_OAUTH_TOKEN` в окружении |
| `deepagents` | Fallback, если Claude недоступен |
| `codex` | Codex SDK (`pip install -e ".[codex]"`) + авторизованный Codex CLI; только через Developer Override |
| `adk` | Google ADK (`pip install -e ".[adk]"`) + `GOOGLE_API_KEY` / `GEMINI_API_KEY` (модели Gemini); только через Developer Override |

Автовыбор использует только `claude-agent-sdk` и `deepagents`. Бэкенды `codex` и `adk` запускают тяжёлый out-of-process `mcp-server` subprocess, поэтому в авто-цепочку не входят и включаются только вручную через Developer Override.

Переключение: Web UI → Settings → Agent → Developer Override.

## Запуск

=== "CLI"
    ```bash
    python -m src.main agent chat "собери сообщения из @channel"
    python -m src.main agent threads          # список тредов
    python -m src.main agent thread-create --title "Анализ"
    python -m src.main agent messages 1       # сообщения треда
    ```

=== "Web"
    `GET /agent/` — чат-интерфейс с тредами

## Категории инструментов

| Категория | Кол-во | Примеры |
|-----------|--------|---------|
| READ | 40+ | `search_messages`, `list_channels`, `get_participants` |
| WRITE | 50+ | `collect_channel`, `send_message`, `generate_draft` |
| DELETE | 10+ | `delete_message`, `kick_participant`, `delete_channel` |

## Права доступа

Для каждого Telegram-аккаунта можно настроить отдельные права на инструменты:

- **READ** — только чтение (безопасно)
- **WRITE** — запись и изменения
- **DELETE** — удаление (требует явного включения)

Настройка: Web UI → Settings → Agent → Tool Permissions (вкладка по каждому аккаунту).

## Полный список инструментов

Смотрите [Agent Tools Reference](../reference/agent-tools.md).
