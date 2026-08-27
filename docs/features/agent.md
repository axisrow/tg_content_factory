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

## ADK dev UI и eval flow

ADK получает тот же реестр инструментов через MCP. Для локальной разработки
установите optional extra и запустите штатный ADK Web UI из корня репозитория:

```bash
pip install -e ".[adk]"
TG_CONFIG_PATH=/path/to/config.yaml adk web adk
```

В UI откройте вкладку **Eval**, создайте eval set и сохраните текущую сессию
как eval case. После редактирования кейсов тот же набор можно запускать без UI:

```bash
adk eval adk/tg_content_factory path/to/evalset.evalset.json --print_detailed_results
```

`adk/tg_content_factory/agent.py` — тонкий ADK entrypoint: он экспортирует `root_agent` и
переиспользует MCP wiring из `AdkSdkBackend`. Dev UI запускается без
in-process Telegram pool (`--no-pool`), поэтому database-backed tools доступны,
а pool-dependent tools возвращают штатный ответ о недоступности. Eval runs
подхватывают сохранённый `agent_prompt_template` (переменные контекста без
сессии заменяются пустыми значениями) и следует выполнять с тестовыми данными
и отдельным конфигом. Это opt-in flow и не входит в обычный CI без
установленного ADK и Google credentials.

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
