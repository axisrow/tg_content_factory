# Архитектура

> **CLI — единственная поверхность** (агенты кодирования: `tg-agent` CLI + скилл). Веб-панель, встроенный агент-чат с TUI и MCP-сервер удалены в #1868. См. Positioning в корневом `CLAUDE.md`.

## Три слоя

```
┌─────────────────────────────────────────────┐
│        CLI (tg-agent)  ·  воркер-демон      │
│   (демон владеет Telegram-подключениями;    │
│    CLI работает на них через relay)         │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│              Сервисный слой                  │
│  ClientPool · Collector · SearchEngine       │
│  ContentGenerationService · Scheduler        │
│  PipelineService · NotificationService       │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│           SQLite (aiosqlite)                 │
│  channels · messages · accounts · settings  │
│  pipelines · generation_runs · photo_items  │
└─────────────────────────────────────────────┘
```

## Ключевые компоненты

### ClientPool
Управляет пулом Telethon-клиентов (multi-account). Ротация flood wait: пропускает аккаунты с активным `flood_wait_until`. StringSession теряет entity cache между рестартами — поэтому `collect_all_channels()` вызывает `client.get_dialogs()` перед итерацией.

### Воркер-демон
`tg-agent restart` (или `tg-agent worker` под tmux/systemd) запускает рантайм воркера: `tg_agent/runtime/bootstrap.py` собирает `RuntimeContainer`, `tg_agent/runtime/worker.py` его исполняет с управляемым PID-файлом. Демон владеет живым `ClientPool`, `CollectionQueue`, `UnifiedDispatcher`, `TelegramCommandDispatcher`, `SchedulerManager`, DM-слушателем и публикует `runtime_snapshots`, которые читают CLI-команды (`scheduler status` и др.).

### Session-owner relay (#1868)
`tg_agent/telegram/relay.py`: одним Telegram-подключением аккаунта владеет один процесс — воркер-демон или общий gateway. Он слушает loopback-relay (127.0.0.1, случайный порт, токен; endpoint-файл `<db>.relay.json`, 0600). Любой другой процесс строит обычный `ClientPool(relay=...)`: его клиенты без auth key сериализуют каждый MTProto-запрос, владелец выполняет его на своём подключении за своими floodgate-гардами, а сырой ответ разбирается на стороне клиента. Одна авторизация, одно подключение, один flood-бюджет, тот же нативный код. `cli/runtime.init_pool` выбирает путь: проект в режиме gateway → relay gateway; живой локальный демон → relay демона; иначе CLI подключается сам. Старый демон/gateway без relay — жёсткая ошибка с просьбой перезапустить (`tg-agent restart`, `tg-agent gateway restart --shared`). Запросы, которые завершили бы общую сессию (`auth.*`, смена телефона/2FA/TTL, сброс сессий), relay отклоняет; диалоги с BotFather (`notification setup/delete`) выполняются на владельце — только он получает ответы BotFather.

### Queue wake (#1868)
`tg_agent/runtime/wake.py`: постановка задачи в очередь (`telegram_commands`, `collection_tasks`) будит циклы воркера сразу — в том же процессе через событие, из CLI-процесса через relay демона (операция `wake`). Поллинг остался только как страховка.

### Collector
Инкрементальный сбор: `min_id = channel.last_collected_id`, `reverse=True`. После цикла `last_collected_id = max(seen message_ids)`. Batch insert через `INSERT OR IGNORE` + `UNIQUE(channel_id, message_id)`.

### SearchEngine
- **FTS5**: полнотекстовый поиск с wildcard matching
- **Semantic**: NumPy KNN без sqlite-vec (portable fallback)
- **AISearchEngine**: LLM-powered поиск (собирается в контейнере, но после #1868 ни одна поверхность его не вызывает — режим был в веб-панели)

### UnifiedDispatcher
Поллит БД для generic tasks (CONTENT_GENERATE, CONTENT_PUBLISH, PIPELINE_RUN, PHOTO_DUE) и диспетчеризирует к handler-методам. Восстанавливает прерванные задачи при старте.

### Агент пайплайнов
Движок агента остаётся только для контент-пайплайнов (`--generation-backend deep_agents`): `AgentManager.get_runtime_status()` (`tg_agent/agent/manager.py`) выбирает backend:
- `deepagents`, если в БД есть рабочие конфиги провайдеров
- `claude-agent-sdk` при наличии `ANTHROPIC_API_KEY` / `CLAUDE_CODE_OAUTH_TOKEN`
- `deepagents` как fallback (`AGENT_FALLBACK_MODEL`)

`agent_backend_override` (режим разработчика; `tg-agent settings agent --backend auto|claude|deepagents`) принудительно выбирает один. Реестр агент-инструментов (`tg_agent/agent/tools/`) — инструменты этого движка; соответствие CLI — в [CLI / Agent Tool Parity](reference/parity.md). Встроенный чат (`agent …`, TUI, web `/agent`), `mcp-server` и бэкенды Codex/ADK удалены в #1868.

## Database Access Pattern

`db.repos.<repo>.<method>()` — **канонический** низкоуровневый доступ к БД. Новый код
обращается к репозиториям напрямую через `db.repos`, а не через pass-through-методы
фасада/бандлов.

```python
# Репозитории через db.repos (канон)
db.repos.channels.get_channels(active_only=True, include_filtered=False)
db.repos.generation_runs.list_pending_moderation(pipeline_id=1)
db.repos.settings.get_setting("key")
```

### Bundles и facade — compat-шимы

- `tg_agent/database/facade.py` (`Database`) исторически экспонирует ~90 методов вида
  `self._require(); return await self._<repo>.<method>(...)` — это механические
  pass-through к репозиториям. Они сохраняются как **compat-шимы** на время окна
  совместимости; новый код их не наращивает (`db.get_setting(k)` → `db.repos.settings.get_setting(k)`,
  `db.get_channels(...)` → `db.repos.channels.get_channels(...)`).
- `tg_agent/database/bundles.py` (`AccountBundle`, `ChannelBundle`, …) группирует несколько
  репозиториев под сервисную границу (`ChannelBundle` агрегирует `channels` + `channel_stats`
  + `tasks`). Бандлы остаются там, где дают осмысленную агрегацию; чисто
  переименовывающие 1:1-методы — тоже compat-шимы.

Структурный guard `tests/test_db_access_conventions.py` фиксирует текущее число
pass-through-методов (ratchet «только вниз») — добавление новых pass-through падает в CI,
вынуждая использовать `db.repos`.

## CLI Pipelines

- `tg_agent/cli/commands/pipeline.py` — Typer-входные точки и совместимый `run(argparse.Namespace)`; адаптация аргументов выполняется на границе CLI.
- `tg_agent/cli/pipeline/requests.py` — явные dataclass-запросы для каждой команды.
- `tg_agent/cli/pipeline/` — отдельные обработчики CRUD, генерации, модерации и графа; форматирование результатов и JSONL вынесено в `responses.py`.
- Общий dispatcher владеет одним lifecycle БД и пула; фабрики сервисов передаются обработчикам через `PipelineServices`.

## Runtime Wiring

- `tg_agent/runtime/bootstrap.py` — сборка сервисов, восстановление при старте и остановка воркера; результат — `RuntimeContainer` (`tg_agent/runtime/container.py`).
- `tg_agent/runtime/worker.py` — исполнение контейнера под `restart`/`worker`: PID-файл, heartbeat и snapshot-публикация, relay владельца сессий.
- `tg_agent/cli/runtime.py` — сторона CLI: `init_db`, `init_pool` (relay gateway → relay демона → собственное подключение) и пробуждение очередей демона после постановки задач.

## Ключевые паттерны

| Паттерн | Описание |
|---------|----------|
| **Entity cache** | `get_dialogs()` перед итерацией каналов (StringSession теряет кеш) |
| **Flood wait rotation** | `get_available_client()` пропускает аккаунты с активным flood wait |
| **Config key dropping** | Если `${ENV_VAR}` пустой — ключ удаляется из конфига |
| **Incremental collection** | `min_id` + `reverse=True` + обновление `last_collected_id` |
| **Batch insert** | `INSERT OR IGNORE` + `UNIQUE(channel_id, message_id)` |
| **Cancellation** | `asyncio.Event` проверяется каждые 10 сообщений |
| **Session-owner relay** | Одно подключение на аккаунт: демон/gateway выполняет MTProto-запросы остальных процессов |
| **Queue wake** | Постановка в очередь будит воркер сразу; поллинг — страховка |
| **CollectionQueue** | `asyncio.Queue` + single worker, статусы в БД |
| **DB migrations** | `PRAGMA table_info` + `ALTER TABLE ADD COLUMN` |

## Тестирование

```bash
# Параллельные тесты (все CPU минус один)
pytest tests/ -v -m "not aiosqlite_serial" -n auto

# Серийные тесты (aiosqlite)
pytest tests/ -v -m aiosqlite_serial
```

Тесты используют `:memory:` SQLite через фикстуру `db`. Real Telegram тесты требуют `RUN_REAL_TELEGRAM_SAFE=1`.
