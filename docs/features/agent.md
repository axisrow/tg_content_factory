# AI-агент

Встроенный AI-агент с 100+ MCP-инструментами, покрывающими все операции системы.

## Backends

| Backend | Условие активации |
|---------|------------------|
| `claude-agent-sdk` | `ANTHROPIC_API_KEY` или `CLAUDE_CODE_OAUTH_TOKEN` в окружении |
| `deepagents` | Fallback, если Claude недоступен |

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
