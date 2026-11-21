# Карта сервисов проекта

> **CLI — основная поверхность.** MCP-сервер удалён в #1868; веб-панель (FastAPI REST + Web, пакет `tg-webui`), TUI (пакет `tg-tui`) и встроенный агент-чат — устаревшие опциональные поверхности. Карта остаётся инвентарём сервисов; из прежних «пяти поверхностей» в таблицах оставлены CLI и Agent-tools (реестр инструментов движка контент-пайплайнов). См. «Positioning» в корневом `CLAUDE.md`.

> **Статус:** финальная документация (эпик #1022), поверхности обновлены после #1868. Дополняет [`architecture.md`](architecture.md) — высокоуровневую трёхслойную схему — практической картой «что есть и через какие поверхности доступно».
> **Что это:** инвентарь функциональных сервисов продукта + основа для системного баг-ханта (эпик #1024).
> **Метод:** read-only анализ поверхностей проекта (см. ниже) + сверка статуса по real-TG suite (`tests/cli_real_tg_integration/`); e2e веб-панели, на которые раньше опиралась часть статусов, ушли вместе с панелью.

## Что такое «сервис» здесь

Сервис = **функциональная возможность продукта с точки зрения пользователя** («собрать посты в базу», «постить картинки», «AI-генерация контента»…). Это НЕ файл в `tg_agent/services/` и НЕ пункт меню. Один сервис обычно размазан по нескольким поверхностям + нескольким backend-модулям.

## Поверхности доступа

| # | Поверхность | Для кого | Где живёт |
|---|-------------|----------|-----------|
| 1 | **CLI** | агенты кодирования и скрипты — единственный внешний контракт | `tg_agent/cli/commands/` (тела `*_impl`) + `tg_agent/cli/typer_commands.py` (Typer-приложение `app`, ~270 leaf-команд) |
| 2 | **Agent-tools** | инструменты движка контент-пайплайнов (`AgentManager`) | `tg_agent/agent/tools/` (191 tools в 20 группах) |

**Удалён в #1868:** MCP-сервер `mcp-server`. FastAPI REST и Web (веб-панель, пакет `tg-webui`), TUI (`AgentTuiApp`, пакет `tg-tui`) и встроенный агент-чат — устаревшие опциональные поверхности; колонок для них в таблицах ниже нет.

## Что означает «Статус»

| Метка | Смысл |
|-------|-------|
| 🟢 **проверено вживую** | есть прошедшие real-TG тесты (`tests/cli_real_tg_integration/`) — доказано боем |
| 🟡 **только код** | написано и покрыто unit/integration-тестами с фейками, но реальной работой (live Telegram / live provider / реальная нагрузка) не доказано |
| 🔴 **заглушка/частично** | код есть, но не подключён к поверхностям, либо явно неполный/экспериментальный |

> Калибровка: **постинг картинок** доказан real-TG тестами (3 passed) → 🟢. **Сбор постов** — основная рабочая функция, real-TG heavy → 🟢. Многие сервисы **гибридны**: read-путь 🟢, write/мутирующий путь 🟡 (помечено внутри строки).

> В колонках поверхностей: **✅** — поверхность есть; **—** — нет; статус-эмодзи рядом уточняет качество (если отличается от общего).

---

## Сервисы

### 1. 📥 Сбор постов в базу (Collection)

Инкрементально вытягивает сообщения из подписанных каналов в локальную SQLite.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `collect`, `collect sample`, `channel collect`, `scheduler trigger` |
| Agent-tools | ✅ | `collect_channel`, `collect_all_channels`, `collect_channel_stats`, `collect_all_stats` |

**Backend:** `CollectionService`, `Collector` (telegram/collector.py — **2584 строки, горячая зона**), `CollectionQueue`.
**Статус:** 🟢 **проверено вживую** — real-TG `heavy/test_channel_collect_first.py`, `heavy/test_collect_default_all.py`.

### 2. 🔎 Поиск (Search) — *объединяет поиск-по-базе + live-поиск в Telegram*

Полнотекстовый (FTS5), семантический (KNN), гибридный и AI-поиск по собранным сообщениям + поиск через Telegram-native API (чужие каналы, свои чаты, premium global).

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `search` (+ `--mode telegram`), `messages read` |
| Agent-tools | ✅ | `search_messages`, `semantic_search`, `search_hybrid`, `index_messages`, `purge_search_cache`, `search_telegram`, `search_my_chats`, `search_in_channel` |

**Backend:** `SearchService`, `SearchEngine`, `LocalSearch`, `AISearchEngine`, `EmbeddingService`, `TelegramSearch` (search/telegram_search.py, 621 строка).
**Статус:** 🟢 **проверено вживую** — base FTS: real-TG `safe_ro/test_search_basic.py`; live-поиск TG: `mutation_safe/test_search_telegram_premium.py`. ⚠️ Семантика — 🟡 только код (offline `test_local_search_semantic_paths.py`); AI-поиск (`AISearchEngine`, offline `test_ai_search.py`) после #1868 недоступен ни с одной поверхности (был в веб-панели и агент-чате).

### 3. 🏷 Управление каналами (Channels)

Добавление/удаление/импорт каналов-источников, теги, метаданные, типы, карантин/ревью.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `channel list/add/delete/toggle/import/add-bulk/list-for-import/refresh-types/refresh-meta/review-*`, `channel tag {list,add,delete,set,get}` |
| Agent-tools | ✅ | `list_channels`, `add_channel`, `delete_channel`, `toggle_channel`, `import_channels`, `refresh_channel_*`, `*_tag*`, `add_channels_bulk`, `list_dialogs_for_import` |

**Backend:** `ChannelService`, `ChannelOnboarding`.
**Статус:** 🟢 **проверено вживую** — real-TG `safe_write/test_channel_add_first.py`, `test_channel_add_bulk_first.py`, `safe_ro/test_channel_list.py`, `test_channel_tag_*`, `heavy/test_channel_refresh_*`.

### 4. 🧹 Очистка / фильтрация каналов (Filter)

Скоринг каналов по уникальности/спаму/языку, пометка и purge/hard-delete отфильтрованного.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `filter analyze/apply/reset/precheck/toggle/purge/purge-messages/hard-delete` |
| Agent-tools | ✅ | `analyze_filters`, `apply_filters`, `reset_filters`, `toggle_channel_filter`, `purge_filtered_channels`, `hard_delete_channels`, `precheck_filters`, `purge_channel_messages` |

**Backend:** `ChannelAnalyzer` (filters/analyzer.py), `FilterDeletionService`.
**Статус:** 🟢 **проверено вживую** (анализ) — real-TG `safe_ro/test_filter_analyze.py`, `test_filter_precheck.py`. ⚠️ purge/hard-delete (деструктив) — 🟡 только код (manifested как local-mutation).

### 5. ⭐ Рейтинг каналов (Channel Rating)

LLM-judge оценивает канал по 2 осям (полезность × жанр), пишет `channel_ratings`.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `analytics channel-rate`, `analytics channel-rating` |
| Agent-tools | ✅ | `rate_channel`, `get_channel_ratings` |

**Backend:** `ChannelAnalysisService`.
**Статус:** 🟡 **только код** — `channel-rate` manifested как «LLM-judge, provider spend, без live TG», покрыт integration-тестами с фейк-провайдером (#994). Генерация рейтингов боем не доказана.

### 6. 🤖 AI-генерация контента (Content Generation)

RAG/deep-agent генерация постов из собранного контекста; запись в `generation_runs`.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `pipeline generate`, `pipeline generate-stream`, `pipeline run`, `pipeline ai-edit` |
| Agent-tools | ✅ | `generate_draft`, `run_pipeline`, `ai_edit_pipeline` |

**Backend:** `ContentGenerationService`, `GenerationService`, `QualityScoringService` (подключён, пишет `quality_score`).
**Статус:** 🟡 **только код** — manifested как «provider spend/write», integration-тесты с фейк-провайдером (`test_content_generation_service.py`, `test_generation_service_streaming.py`); реальная LLM-генерация боем не зафиксирована.

### 7. 🖼 Генерация изображений (Image Generation)

Генерация картинок через провайдеры (Together/HuggingFace/OpenAI/Replicate/Codex) по `provider:model_id`.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `image generate/models/providers/generated` |
| Agent-tools | ✅ | `generate_image`, `list_image_models`, `list_image_providers`, `list_generated_images` |

**Backend:** `ImageGenerationService`, `ImageProviderService`, `S3Store` (опц.), `provider_adapters.py`.
**Статус:** 🟡 **только код** (генерация) — `image generate/models` manifested как provider-spend; листинг 🟢 (real-TG `safe_ro/test_image_providers.py`, `test_image_generated.py`). Opt-in live Codex-тесты (`codex_image_live/`) gated.

### 8. 🚰 Пайплайны контента (Pipelines)

DAG-конвейер source→filter→llm_generate→image→publish; шаблоны, импорт/экспорт, узлы/рёбра.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `pipeline list/show/add/edit/delete/toggle/run/dry-run-count/runs/run-show/export/import/templates/from-template/refinement-steps`, `pipeline node {add,replace,remove}`, `edge {add,remove}`, `filter {set,show,clear}`, `graph` |
| Agent-tools | ✅ | 20 tools: `list_pipelines`, `add_pipeline`, `edit_pipeline`, `run_pipeline`, `*_pipeline_json`, `*_pipeline_template*`, `get_pipeline_dry_run_count`… |

**Backend:** `PipelineService` (**809 строк**), `PipelineExecutor`, `PipelineNodeHandlers`, `PipelineFilters`.
**Статус:** 🟢 **проверено вживую** (чтение/конфиг) — real-TG `safe_ro/test_pipeline_*` (list/show/graph/dry-run-count/templates), `safe_write/test_pipeline_export_first.py`. ⚠️ Полный прогон с генерацией — 🟡 (см. сервис #6).

### 9. ✅ Модерация контента (Moderation)

Ручная очередь approve/reject сгенерированного контента перед публикацией.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `pipeline moderation-list/moderation-view/approve/reject/bulk-approve/bulk-reject/queue` |
| Agent-tools | ✅ | `list_pending_moderation`, `view_moderation_run`, `approve_run`, `reject_run`, `bulk_approve_runs`, `bulk_reject_runs` |

**Backend:** репозиторий `generation_runs`, `DraftNotificationService` (push о новых драфтах).
**Статус:** 🟢 **проверено вживую** (чтение) — real-TG `safe_ro/test_pipeline_moderation_list.py`, `moderation_view_first.py`. ⚠️ approve/reject (мутации) — 🟡 только код.

### 10. 📤 Публикация текста (Publishing)

Отправка сгенерированного/готового текста в целевой Telegram-канал/диалог.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `pipeline publish`, `dialogs send` |
| Agent-tools | ✅ | `publish_pipeline_run`, `send_message` |

**Backend:** `PublishService`.
**Статус:** 🟢 **проверено вживую** — real-TG `mutation_safe/test_pipeline_publish_sandbox.py`, `dialogs send` в `mutation_safe`.

### 11. 📸 Постинг картинок / Photo Loader (Photo Publishing)

Отправка фото в диалоги: разовая, batch, авто-загрузка по расписанию из папки.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `photo-loader dialogs/refresh/send/schedule-send/batch-*/publish/items/auto-*/run-due` |
| Agent-tools | ✅ | `send_photos_now`, `schedule_photos`, `create_photo_batch`, `publish_photo_batch`, `create_auto_upload`, `run_photo_due`… |

**Backend:** `PhotoPublishService`, `PhotoTaskService`, `PhotoAutoUploadService`.
**Статус:** 🟢 **проверено вживую** — real-TG `mutation_safe/test_photo_loader_{send,schedule_send,run_due}_sandbox.py` (3 passed); `safe_write/test_photo_loader_refresh.py`.

### 12. 💬 Диалоги Telegram (Dialogs / chat management)

Управление чатами: список/refresh диалогов, send/edit/delete/forward/pin/react, участники, права, archive/kick, создание каналов/групп.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `dialogs list/refresh/resolve/leave/join/topics/cache-*/send/forward/edit-message/delete-message/create-channel/create-group/pin-message/react/unpin-message/download-media/participants/edit-admin/edit-permissions/kick/broadcast-stats/archive/unarchive/mark-read`, `dialogs queue {status,cancel,clear-pending}` |
| Agent-tools | ✅ | `dialogs.py` (11) + `messaging.py` (22) — send/react/forward/pin/admin/kick/archive… (большинство phone-bound) |

**Backend:** `TelegramActionService`, `TelegramCommandService`, `TelegramCommandDispatcher` (**1161 строка, горячая зона**).
**Статус:** 🟢 **проверено вживую** — обширное real-TG: `mutation_safe/test_dialogs_*` (archive/delete/edit/forward/mark-read/participants/pin/react/unpin), `manual/test_dialogs_create_*`, `safe_ro/test_dialogs_*`.

### 13. 👤 Управление аккаунтами (Accounts)

Подключение/toggle/удаление Telegram-аккаунтов, primary, flood-статус, auth-флоу.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `account list/info/toggle/set-primary/delete/send-code/verify-code/add/flood-status/flood-clear` |
| Agent-tools | ✅ | `list_accounts`, `toggle_account`, `delete_account`, `get_flood_status`, `clear_flood_status`, `get_account_info`, `get_account_availability`, `get_runtime_diagnostics` |

**Backend:** `AccountService`, `AccountAvailability`, `ClientPool` (**2107 строк, горячая зона**), `TelegramAuth`.
**Статус:** 🟢 **проверено вживую** (чтение) — real-TG `safe_ro/test_account_{list,info,flood_status}.py`. ⚠️ Auth-флоу (`send-code`/`verify-code`/`add`) и `delete` — 🟡 только код (manifested как ручной onboarding / dangerous-blocked).

### 14. 🔔 Уведомления (Notifications)

Персональный бот через BotFather; матчинг сообщений с запросами; push о драфтах.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `notification setup/status/delete/test/dry-run/set-account` |
| Agent-tools | ✅ | `setup_notification_bot`, `delete_notification_bot`, `test_notification`, `get_notification_status`, `notification_dry_run` |

**Backend:** `NotificationService`, `NotificationMatcher`, `NotificationTargetService`, `DraftNotificationService`, `Notifier`.
**Статус:** 🟢 **проверено вживую** (статус/dry-run/test) — real-TG `safe_ro/test_notification_{status,dry_run}.py`, `mutation_safe/test_notification_test_sandbox.py`. ⚠️ `setup`/`delete` (BotFather) — 🟡 только код.

### 15. 🔁 Сохранённые поисковые запросы (Search Queries)

Именованные запросы для мониторинга трендов; запуск по расписанию; статистика срабатываний.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `search-query list/get/add/edit/delete/toggle/run/stats` |
| Agent-tools | ✅ | `list_search_queries`, `add_search_query`, `run_search_query`, `get_search_query_stats`… (8 шт.) |

**Backend:** `SearchQueryService`.
**Статус:** 🟢 **проверено вживую** (чтение) — real-TG `safe_ro/test_search_query_{list,get_first,stats_first}.py`. ⚠️ add/edit/run — 🟡 только код.

### 16. 📊 Аналитика (Analytics) — *объединяет аналитику + тренды*

Статистика по каналам/контенту (top-сообщения, engagement, hourly, velocity, peak-hours, heatmap, cross-citations) + trending темы (TF-IDF)/каналы/эмодзи.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `analytics top/content-types/hourly/summary/daily/pipeline-stats/velocity/peak-hours/calendar/channel/trending-topics/trending-channels/trending-emojis` |
| Agent-tools | ✅ | 15 tools (14 read + `rate_channel` write): `get_analytics_summary`, `get_top_messages`, `get_hourly_activity`, `get_peak_hours`, `get_channel_analytics`, `get_trending_*`… |

**Backend:** `ChannelAnalyticsService`, `ContentAnalyticsService`, `TrendService`.
**Статус:** 🟢 **проверено вживую** — real-TG `safe_ro/test_analytics_*` (15 кейсов: top/daily/hourly/summary/velocity/peak-hours/content-types/pipeline-stats/channel/trending-{topics,channels,emojis}…).

### 17. 📅 Календарь публикаций (Calendar) — *отдельный сервис*

Визуализация запланированных публикаций по дням.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `analytics calendar` |
| Agent-tools | ✅ | `get_calendar` |

**Backend:** `ContentCalendarService`.
**Статус:** 🟢 **проверено вживую** (чтение) — real-TG `safe_ro/test_analytics_calendar.py`.

### 18. ⏰ Планировщик / расписание (Scheduler)

APScheduler: периодический сбор, запуск search-queries, пауза/резюм очереди, управление job'ами.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `scheduler start/trigger/status/stop/job-toggle/set-interval/task-cancel/clear-pending/queue-pause/queue-resume` |
| Agent-tools | ✅ | `get_scheduler_status`, `start_scheduler`, `trigger_collection`, `toggle_scheduler_job`, `set_scheduler_interval`… (8 шт.) |

**Backend:** `SchedulerManager` (scheduler/manager.py).
**Статус:** 🟢 **проверено вживую** (статус/trigger/start) — real-TG `safe_ro/test_scheduler_status.py`, `mutating/test_proc_scheduler_{start,trigger}.py`.

### 19. 🗂 Фоновые задачи / Jobs (Background Jobs)

Единый read-only дашборд всех фоновых задач (collection tasks, telegram commands, photo tasks). Есть только в опциональной веб-панели (пакет `tg-webui`: `/jobs`, `/jobs/api/list`).

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ⚠️ — | **нет агрегированной команды** (частично: `scheduler status`, `scheduler queue-status`, `dialogs queue status`) |
| Agent-tools | — | (косвенно `get_telegram_queue_status`) |

**Backend:** `JobsReadModel`, `UnifiedDispatcher`, `TaskEnqueuer`, `task_handlers/`.
**Статус:** 🔴 **заглушка/частично** — в ядре `JobsReadModel` не подключён ни к одной поверхности: его читает только опциональная веб-панель (e2e `/jobs` живёт в репозитории `tg-webui`; здесь остаётся integration `test_jobs_read_model.py`). Агрегированного вида очередей в CLI нет — кандидат на `jobs list`.

### 20. 🌍 Перевод (Translation)

Детекция языка + LLM-перевод сообщений (batch и одиночный).

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `translate stats/detect/run/message` |
| Agent-tools | ✅ | `translate_message` |

**Backend:** `TranslationService`.
**Статус:** 🟢 **проверено вживую** (stats) — real-TG `safe_ro/test_translate_stats.py`. ⚠️ `run`/`detect`/`message` — 🟡 только код (provider/network translation).

### 21. 📦 Экспорт (Export)

Экспорт собранных сообщений в JSON/CSV/RSS/Telegram-Desktop формат.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `export json/csv/rss/telegram` |
| Agent-tools | ✅ | `export_messages` |

**Backend:** `ExportService`, `TelegramExportBuilder`.
**Статус:** 🟢 **проверено вживую** — real-TG `safe_ro/test_export_{json,csv,rss}.py`. `export telegram` — 🟡 только код (offline DB→file, покрыт integration).

### 22. 🔌 Провайдеры LLM/Image (Providers)

Регистрация/проба/refresh провайдеров (OpenAI, Cohere, Ollama, Anthropic, image-провайдеры).

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `provider list/add/delete/probe/refresh/test-all` |
| Agent-tools | ⚠️ — | прямого tool нет; косвенно `list_image_providers` |

**Backend:** `ProviderService`, `AgentProviderService`, `ImageProviderService`, `ProviderModelCache`, `provider_adapters.py` (**677 строк**).
**Статус:** 🟢 **проверено вживую** (list) — real-TG `safe_ro/test_provider_list.py`. ⚠️ probe/test-all/refresh — 🟡 только код (live-API проба, gated `real_provider_smoke`).

### 23. 🧠 Агент пайплайнов (Agent engine)

Движок агента для контент-пайплайнов (`--generation-backend deep_agents`): 2 backend'а — deepagents и claude-agent-sdk; реестр tools, permissions. Встроенный чат (треды, контекст; TUI — пакет `tg-tui`, web `/agent` — пакет `tg-webui`) — устаревшая опциональная поверхность. MCP-сервер и backend'ы Codex/ADK удалены в #1868.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `settings agent` (backend `auto/claude/deepagents` + шаблон промпта); запуск — `pipeline generate/run` пайплайна с `--generation-backend deep_agents`; устаревший чат — `agent threads/chat/messages/context` |
| Agent-tools | ✅ | `agent_threads.py` (5: list/create/delete/rename/get_messages) + сам реестр (191 tools) |

**Backend:** `AgentManager`, `AgentProviderService`, react-agent fallback.
**Статус:** 🟢 **проверено вживую** (чтение/one-shot чата) — real-TG `safe_ro/test_agent_threads.py`, `test_agent_messages_first.py`, `safe_write/test_proc_agent_chat_oneshot.py`. ⚠️ TUI-режим — 🟡 только код (интерактив без авто-теста); генерация через пайплайн — 🟡 (см. сервис #6).

### 24. ⚙️ Настройки и диагностика (Settings & Debug)

Конфиг (credentials, фильтр-пороги, agent, reactions, semantic); системная диагностика; debug-логи/память/тайминги.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `settings get/set/info/server-time/agent/filter-criteria/reactions/semantic`, `debug logs/memory/timing`, `test all/read/write/telegram/benchmark` |
| Agent-tools | ✅ | `get_settings`, `save_scheduler_settings`, `save_agent_settings`, `save_filter_settings`, `get_system_info`, `get_server_time` (6 шт.) |

**Backend:** репозиторий `settings`, `RuntimeDiagnostics`.
**Статус:** 🟢 **проверено вживую** — real-TG `safe_ro/test_settings_{get,info,server-time}.py`, `test_debug_{logs,memory,timing}.py`, `test_proc_test_read.py`.

### 25. 🛡 Лимиты и качество (Production Limits & Quality)

Rate/cost-лимиты на провайдеров, учёт дневных затрат; LLM-оценка качества контента.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ⚠️ — | через `settings`/config, без выделенной команды |
| Agent-tools | — | |

**Backend:** `ProductionLimitsService` (подключён в `image_generation_service`, опц., off-by-default), `QualityScoringService` (подключён в content-generation, пишет `quality_score`), `ErrorRecoveryService` (**нигде не импортируется** — 🔴 изолирован).
**Статус:** 🟡 **только код** — лимиты подключены, но off-by-default; `ErrorRecoveryService` изолирован. Реальная работа лимитов боем не доказана.
**Vulture-аудит #1044:** `ErrorRecoveryService` верифицирован как изолированный (grep `tg_agent/` = только сам файл; `CircuitBreaker` в `notifier.py` — это pybreaker #955, другая реализация; нет DI/динамической регистрации). Решение владельца — **НЕ удалять, подключить как фичу** (отдельная задача, не #1044). ⚠️ При подключении сверять с #958: `execute_with_recovery` ретраит любую `func` (max_retries=3) → подключать только к idempotent-операциям, НЕ к платным image POST (осознанный `max_retries=0` против дубль-биллинга).

### 26. 🧪 A/B-тестирование контента (A/B Testing)

Генерация нескольких вариантов поста и выбор лучшего.

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | — | |
| Agent-tools | — | |

**Backend:** `ABTestingService` (ab_testing_service.py) — **не импортируется нигде, кроме своего файла + тестов**.
**Статус:** 🔴 **заглушка/частично** — сервисный код есть, но НЕ подключён ни к одной поверхности. Висит изолированно.
**Vulture-аудит #1044:** изоляция верифицирована (grep `tg_agent/` = только сам файл; нет DI/динамической регистрации; репо-методы `generation_runs.set_variants`/`select_variant` зовутся ТОЛЬКО из `ABTestingService` — ни web, ни CLI вариантов не показывают). Решение владельца — **НЕ удалять, подключить как фичу** (отдельная задача-эпик «подключить неиспользуемое», не #1044).

### 27. ✉️ DM-цикл: входящие личные сообщения (DM journal / drafts / archive)

Журнал входящих DM (окно TTL 24ч, планка приватности `dialogs_history`) + черновики ответов (статусная машина draft→approved→sending→sent; отправка только через очередь воркера) + постоянный архив переписки (обе стороны, без TTL).

| Поверхность | Есть | Детали |
|---|:---:|---|
| CLI | ✅ | `dm drafts list/create/approve/reject/send`, `dm journal list`, `dm archive list` (#1580) |
| Agent-tools | ✅ | `dm.py` (7) — журнал/архив-чтение, черновики list/create, approve/reject (WRITE); отправка вне tool-контракта (#1580) |

**Backend:** репозитории `incoming_dms` (#1427), `dm_reply_drafts` (#1429), `dm_messages` (#1453); слушатель и догон (#1426/#1428); хендлер `dm.send_reply` в `TelegramCommandDispatcher`.
**Статус:** 🟡 **только код** — SQL-пути покрыты integration-тестами на :memory: (журнал/черновики/архив/agent-tools #1580); живой Telegram (слушатель, отправка) auto-suite не гоняет.

---

## Сводка

**Всего сервисов: 27** (после слияний: Поиск = поиск-по-базе + live-TG; Аналитика = аналитика + тренды; Календарь — отдельно; DM-цикл добавлен #1580).

Инвентарь поверхностей:
- **CLI** — 33 top-level команды, ~270 leaf-команд
- **Agent-tools** — 20 групп, 191 tools (80 read / 94 write / 17 delete) — авторитетный счёт из `TOOL_CATEGORIES`/`MODULE_GROUPS` (`tg_agent/agent/tools/permissions.py`, инвариант `tests/test_agent_tools_permissions.py`)
- **Устаревшие опциональные пакеты (вне ядра):** FastAPI REST (JSON parity-зеркала) и Web (HTML/HTMX-панель) — `tg-webui`; TUI (`AgentTuiApp`) — `tg-tui`
- **Удалён в #1868:** MCP-сервер

Распределение по статусу:

Каждый из 27 сервисов получает РОВНО один общий статус (по основному, обычно read-, пути). Числа ниже складываются в 27.

| Статус | Кол-во | Сервисы |
|--------|--------|---------|
| 🟢 **проверено вживую** | **20** | Сбор (1), Поиск (2, base FTS+live-TG), Каналы (3), Очистка/анализ (4), Пайплайны-чтение (8), Модерация-чтение (9), Публикация текста (10), Постинг картинок (11), Диалоги (12), Аккаунты-чтение (13), Уведомления-статус (14), Search-queries-чтение (15), Аналитика+Тренды (16), Календарь (17), Планировщик (18), Перевод-stats (20), Экспорт (21), Провайдеры-list (22), Агент (23, чтение/one-shot чата), Настройки (24) |
| 🟡 **только код** | **5** | Рейтинг каналов (5), AI-генерация (6), Генерация картинок (7), Production Limits & Quality (25), DM-цикл (27) |
| 🔴 **заглушка/частично** | **2** | Jobs (19, дашборд есть только в опциональной веб-панели), A/B-тестирование (26); внутри #25 `ErrorRecoveryService` тоже мёртв |

**Итого: 20 + 5 + 2 = 27.**

> ⚠️ Числа выше — по ОБЩЕМУ статусу сервиса. Отдельно от них: многие из 20 «зелёных» сервисов **гибридные** — read-путь 🟢, а write/мутирующий путь 🟡 (помечено в строке статуса каждой секции). Это НЕ отдельная категория в таблице (иначе сумма сломается) — это под-статус внутри зелёных. «Тёмные половины» (деструктив/auth/BotFather/provider-проба) у сервисов 4/9/13/14/20/22: read тривиально гоняется в `safe_ro`, мутации намеренно вынесены из авто-suite. Для баг-ханта именно эти write-половины — приоритет.

---

## Кандидаты на наибольший риск багов

> Этот раздел — вход для эпика баг-ханта (#1024). Ранжировано: статус «только код» + сложность/горячая зона + бизнес-критичность.

**Тир 1 — критично (горячие зоны + слабое live-покрытие мутаций):**

1. **Сбор постов (1)** — `Collector` 2584 строки, исторически `_collect_channel` имел цикломатическую сложность F(100) (`code_health.py` baseline). Статус 🟢, но объём + сложность = главный источник регрессий. Инкрементальная логика (`min_id`/`last_collected_id`), дедуп, cancellation, entity-cache — тонкие места.
2. **Аккаунты / ClientPool (13)** — `ClientPool` 2107 строк. Flood-wait ротация, primary-гонки (партиал-индекс #733), auth-флоу StringSession. Auth-половина 🟡. Прецеденты гонок pool↔DB.
3. **Диалоги / TelegramCommandDispatcher (12)** — `TelegramCommandDispatcher` 1161 строка. 33 tools, много phone-bound мутаций, очередь команд. Широкая mutation-поверхность.

**Тир 2 — высокий риск (контентный цикл, 🟡 целиком):**

4. **AI-генерация контента (6)** — provider-spend, стриминг генерации (`pipeline generate-stream`), RAG-контекст, deep-agent ветка. Боем не доказано. Дубль-биллинг при retry — известный класс багов (#958).
5. **Генерация картинок (7)** — несколько SDK-адаптеров, S3, retry/billing (прецедент #958: non-idempotent POST + max_retries=2 → дубль-биллинг). Каждый адаптер — свой lifecycle.
6. **Пайплайны — полный прогон (8)** — `PipelineService`+`PipelineExecutor` 809 строк, DAG, топосортировка, узлы/рёбра. Конфиг 🟢, исполнение генерации 🟡.
7. **Рейтинг каналов (5)** — LLM-judge, запись `channel_ratings`. Прецедент: бинарный AI-детектор провалил слепую проверку (recall 0) → переход на channel-slop. Риск ложных вердиктов.

**Тир 3 — «мёртвый/изолированный код» (риск тихих поломок, не покрыт):**

8. **A/B-тестирование (26)** — 🔴 не подключён ни к одной поверхности. Vulture-аудит #1044 подтвердил изоляцию; решение владельца — **подключить как фичу** (отдельная задача-эпик), НЕ удалять. Пока — bit-rot.
9. **ProductionLimits / ErrorRecovery (25)** — `ErrorRecoveryService` не импортируется; `ProductionLimitsService` off-by-default. Vulture-аудит #1044 подтвердил изоляцию `ErrorRecoveryService`; решение владельца — **подключить как фичу** (отдельная задача), НЕ удалять (⚠️ retry сверять с #958). Если лимиты реально нужны — по умолчанию не работают (риск перерасхода на провайдерах).

**Тир 4 — нарушения инвариантов (структурные баги):**

10. **Jobs (19)** — в ядре у `JobsReadModel` нет поверхности (дашборд — только в опциональной веб-панели); агрегированного CLI-вида очередей нет (частично `scheduler status`/`queue-status`, `dialogs queue status`). Read-модель уже ловила баг сортировки naive/aware (`test_jobs_read_model.py`).
11. **Деструктивные write-пути сервисов 4/9/13/14** — purge/hard-delete, account delete, BotFather setup/delete: 🟡 только код, manifested как «опасно для авто-suite». Не гоняются живьём → слепая зона.
