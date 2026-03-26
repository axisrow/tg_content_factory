# Архитектура

## Три слоя

```
┌─────────────────────────────────────────────┐
│           CLI  /  Web UI  /  Agent          │
│  (параллельные входные точки к одной логике) │
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

### Collector
Инкрементальный сбор: `min_id = channel.last_collected_id`, `reverse=True`. После цикла `last_collected_id = max(seen message_ids)`. Batch insert через `INSERT OR IGNORE` + `UNIQUE(channel_id, message_id)`.

### SearchEngine
- **FTS5**: полнотекстовый поиск с wildcard matching
- **Semantic**: NumPy KNN без sqlite-vec (portable fallback)
- **AISearchEngine**: LLM-powered поиск

### UnifiedDispatcher
Поллит БД для generic tasks (CONTENT_GENERATE, CONTENT_PUBLISH, PIPELINE_RUN, PHOTO_DUE) и диспетчеризирует к handler-методам. Восстанавливает прерванные задачи при старте.

### Agent System
`AgentProviderService` выбирает backend:
- `claude-agent-sdk` при наличии `ANTHROPIC_API_KEY` / `CLAUDE_CODE_OAUTH_TOKEN`
- `deepagents` как fallback с provider adapters

100+ MCP-инструментов покрывают все CLI/API операции.

## Database Access Pattern

```python
# Репозитории через db.repos
db.repos.channels.get_all()
db.repos.generation_runs.list_pending_moderation(pipeline_id=1)
db.repos.settings.get("key")
```

## Web App Wiring

- `src/web/assembly.py` — `register_routes()` монтирует все роутеры; `configure_app()` привязывает `AppContainer` к `app.state.*`
- `src/web/container.py` — `AppContainer` агрегирует все сервисы
- `src/web/deps.py` — хелперы `deps.get_db()`, `deps.get_pool()` и др.

## Ключевые паттерны

| Паттерн | Описание |
|---------|----------|
| **Entity cache** | `get_dialogs()` перед итерацией каналов (StringSession теряет кеш) |
| **Flood wait rotation** | `get_available_client()` пропускает аккаунты с активным flood wait |
| **Config key dropping** | Если `${ENV_VAR}` пустой — ключ удаляется из конфига |
| **Incremental collection** | `min_id` + `reverse=True` + обновление `last_collected_id` |
| **Batch insert** | `INSERT OR IGNORE` + `UNIQUE(channel_id, message_id)` |
| **Cancellation** | `asyncio.Event` проверяется каждые 10 сообщений |
| **Session tokens** | HMAC-SHA256, payload `{user, exp}`, cookie 30 дней |
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
