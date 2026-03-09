# Release Notes

## v0.1.7

### Features

- **AI agent with Claude SDK** — SSE streaming, in-process MCP tools (`search_messages`, `get_channels`), contextual channel search (#25)
- **Forum topics support** — `topic_id` field, `get_forum_topics()` API, topic filtering in agent context
- **Context panel in agent UI** — channel, topic, and message limit selection
- **Context format** — structured message lines: `[msg_id=...][date][author] text`

### Fixes

- Entity-cache fix in `get_forum_topics` — reuse `get_dialogs` caching to avoid `PeerChannel` lookup failures
- Server-side `channel_id` validation — return 400 instead of 500, limit capped at 500

### Internal

- `topic_id` added to schema, migration, and `insert_message`
- Scheduler refactor: move `datetime` import to module level
- Add `aiohttp` dependency

## v0.1.6

### Новое

- **FTS5 полнотекстовый поиск** — поисковые запросы с флагом `is_fts` используют SQLite FTS5 для быстрого и точного поиска по сообщениям
- **Фильтры исключений** (`exclude_patterns`) — позволяют исключать нежелательные результаты из выдачи по регулярным выражениям
- **Ограничение длины сообщений** (`max_length`) — фильтрация слишком длинных сообщений в результатах поиска
- **Миграции БД** — автоматическое добавление новых колонок (`is_fts`, `exclude_patterns`, `max_length`) при обновлении
- **Расширенный CLI для search queries** — команда `edit` и новые флаги `--fts`, `--exclude-patterns`, `--max-length`

### UI

- Таблицы channels и search_queries используют `table-layout: fixed` для стабильной ширины колонок
- Кнопки действий больше не переносятся на новую строку (`white-space: nowrap`)
- Длинные FTS-запросы обрезаются с ellipsis в таблице
- Ребрендинг: TG Post Search → TG Agent во всех шаблонах
- Мелкие правки шаблонов: filter_report, settings, base

### Тесты

- Расширенное покрытие search queries: FTS-режим, exclude_patterns, max_length
- Новые web-тесты для обновлённых маршрутов

### Внутреннее

- Рефакторинг `messages` repository — выделена логика FTS5-поиска
- Обновлён `collector` для поддержки новых параметров запросов
- Обновлён `scheduler/manager` для корректной работы с FTS-запросами
