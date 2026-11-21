# Агенты и Telegram

## Нативный путь: CLI + скилл для агентов кодирования

tg-agent спроектирован как зависимость для агентов кодирования (Claude Code, OpenCode, Codex): агент получает полный спектр действий в Telegram через `tg-agent` CLI, а скилл обучает его каталогу команд и правилам безопасности. Каждая команда самодостаточна — делает работу и завершается; демон не нужен. Пока работает воркер-демон, живые команды выполняются на его Telegram-подключении через локальный relay (#1868) — тот же вывод, без второго входа и без очереди.

- Установка и настройка: [Быстрый старт](../getting-started/quickstart.md)
- Каталог команд для агентов: [`skills/tg-agent/reference.md`](https://github.com/axisrow/tg_content_factory/blob/main/skills/tg-agent/reference.md)
- Безопасность: реальные действия агент выполняет только после явного подтверждения пользователя — правила зашиты в скилл
- Сбор по расписанию: воркер-демон (`tg-agent restart`); разовые действия — обычный вызов CLI

## Удалено в #1868

MCP-сервер (`mcp-server`) и построенные на нём агентные бэкенды Codex SDK и Google ADK (dev-точка `adk/` и extra `adk`) удалены без замены. Встроенный агент-чат (`agent chat`, интерактивный TUI — пакет `tg-tui`, треды, контекст, остановка; web `/agent` — пакет `tg-webui`) и инструменты управления тредами (`list/create/delete/rename_agent_thread`, `get_thread_messages`) остаются устаревшей опциональной поверхностью. Codex как **провайдер изображений** (`image generate --model codex:…`, extra `codex`) остаётся.

## Агент контент-пайплайнов

Движок агента (`AgentManager` с реестром агент-инструментов) остаётся для контент-пайплайнов: пайплайн с `--generation-backend deep_agents` (`pipeline add` / `pipeline edit`) генерирует текст через него.

```bash
python -m tg_agent.main pipeline add NAME --prompt-template TEXT --source CHANNEL_ID --target DIALOG_ID --generation-backend deep_agents
python -m tg_agent.main pipeline generate ID
python -m tg_agent.main settings agent                       # показать текущий бэкенд и шаблон промпта
python -m tg_agent.main settings agent --prompt-template TEXT
```

## Backends

| Backend | Условие активации |
|---------|------------------|
| `deepagents` | Есть рабочие конфиги провайдеров в БД (`tg-agent provider add`); иначе — fallback по `AGENT_FALLBACK_MODEL` / `AGENT_FALLBACK_API_KEY` |
| `claude-agent-sdk` | `ANTHROPIC_API_KEY` или `CLAUDE_CODE_OAUTH_TOKEN` в окружении |

Автовыбор: `deepagents` при рабочих конфигах провайдеров в БД, затем `claude-agent-sdk`, затем `deepagents` как fallback. `ANTHROPIC_API_KEY` и `CLAUDE_CODE_OAUTH_TOKEN` не переиспользуются `deepagents`.

Переключение: `tg-agent settings agent --backend auto|claude|deepagents` (старое написание `claude-agent-sdk` принимается как `claude`). Override действует только в режиме разработчика: `tg-agent settings set agent_dev_mode_enabled 1`.

## Категории инструментов

| Категория | Кол-во | Примеры |
|-----------|--------|---------|
| READ | 80 | `search_messages`, `list_channels`, `get_participants` |
| WRITE | 94 | `collect_channel`, `send_message`, `generate_draft` |
| DELETE | 17 | `delete_message`, `kick_participant`, `delete_channel` |

## Права доступа

Для каждого Telegram-аккаунта можно настроить отдельные права на инструменты:

- **READ** — только чтение (безопасно)
- **WRITE** — запись и изменения
- **DELETE** — удаление (требует явного включения)

Права хранятся в DB-настройке `agent_tool_permissions` (JSON; без неё все инструменты разрешены) — читаются и пишутся через `tg-agent settings get` / `tg-agent settings set`. Интерактивный запрос доступа (`PermissionGate`) есть только в сессии устаревшего агент-чата (TUI/Web); в пайплайнах его нет: если ACL сохранён, инструмент без явной записи (`REQUESTABLE`) скрыт от модели и не выполняется, пока доступ не выдан явно.

## Полный список инструментов

Смотрите [Agent Tools Reference](../reference/agent-tools.md).
