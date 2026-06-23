# Карта сервисов проекта

> **Статус:** финальная документация (эпик #1022). Дополняет [`architecture.md`](architecture.md) — высокоуровневую трёхслойную схему — практической картой «что есть и через какие поверхности доступно».
> **Что это:** инвентарь функциональных сервисов продукта + основа для системного баг-ханта (эпик #1024).
> **Метод:** read-only анализ пяти поверхностей проекта (см. ниже) + сверка статуса по real-TG suite (`tests/cli_real_tg_integration/`) и e2e (`tests/e2e/`).

## Что такое «сервис» здесь

Сервис = **функциональная возможность продукта с точки зрения пользователя** («собрать посты в базу», «постить картинки», «AI-генерация контента»…). Это НЕ файл в `src/services/` и НЕ пункт меню. Один сервис обычно размазан по нескольким поверхностям + нескольким backend-модулям.

## Пять поверхностей доступа

Проект экспонирует функциональность через **пять РАЗНЫХ поверхностей**. Ключевое: **FastAPI REST ≠ Web** — это две разные вещи, не путать.

| # | Поверхность | Для кого | Где живёт |
|---|-------------|----------|-----------|
| 1 | **FastAPI REST** (`/api/*`, JSON) | для **программ** (interop, внешние воркеры) | `src/web/routes/tasks.py` (`/api/tasks`, #829) + машинные JSON parity-эндпоинты внутри HTML-роутеров (CLI↔Web parity) |
| 2 | **Web** (HTML/HTMX) | для **человека** в браузере | `src/web/routes/` — Jinja2-шаблоны, HTMX-фрагменты, формы |
| 3 | **CLI** | команды терминала | `src/cli/commands/` + `src/cli/parser_domains/` (~209 leaf-команд) |
| 4 | **Agent-tools** | тулы AI-агента (MCP) | `src/agent/tools/` (184 tools в 18 модулях) |
| 5 | **TUI** (Textual) | интерактивный терминальный UI | `src/cli/commands/agent_tui.py` + `.tcss` (dep `textual[syntax]`); запуск — `agent chat` без `--prompt`. **УЗКАЯ**: только AI-чат, см. ниже |

### Важно про FastAPI vs Web

- **FastAPI REST** — JSON-эндпоинты, потребляемые **программами**. Единственный полноценный REST-контур для внешних интеграций — `/api/tasks` (interop #829: create/claim/complete/fail). Остальные JSON-эндпоинты (`/api/list`, `/api/calendar`, `/messages/{id}`, `/{id}/runs`, `/flood-status`, `/rss.xml`…) — это **машинные parity-зеркала** внутри HTML-роутеров, дающие CLI↔Web паритет и доступ для скриптов.
- **Web** — то, что видит человек: HTML-страницы, HTMX-частичные обновления, SSE-стримы (agent chat, generate-stream), формы. Это НЕ JSON и НЕ для программ.
- Один и тот же роутер часто отдаёт **обе** поверхности (HTML-страница + JSON parity-эндпоинт). В таблицах ниже они разнесены.

### Важно про TUI

TUI — это **единственный** Textual-app в проекте: `AgentTuiApp` (`src/cli/commands/agent_tui.py`, стили в `agent_tui.tcss`). Запускается через `python -m src.main agent chat` в интерактивном режиме (без `--prompt`). Проверено grep'ом по `src/`: больше нигде Textual/`.tcss`/`ComposeResult` не используется. Поэтому **TUI покрывает ровно один сервис — «AI-агент»**; во всех остальных строках TUF-колонка = «—».

## Что означает «Статус»

| Метка | Смысл |
|-------|-------|
| 🟢 **проверено вживую** | есть прошедшие real-TG тесты (`tests/cli_real_tg_integration/`) и/или e2e-панель (`tests/e2e/console_smoke.py`) — доказано боем |
| 🟡 **только код** | написано и покрыто unit/integration-тестами с фейками, но реальной работой (live Telegram / live provider / реальная нагрузка) не доказано |
| 🔴 **заглушка/частично** | код есть, но не подключён к поверхностям, либо явно неполный/экспериментальный |

> Калибровка: **постинг картинок** доказан real-TG тестами (3 passed) → 🟢. **Сбор постов** — основная рабочая функция, real-TG heavy + e2e collection flow → 🟢. Многие сервисы **гибридны**: read-путь 🟢, write/мутирующий путь 🟡 (помечено внутри строки).

> В колонках поверхностей: **✅** — поверхность есть; **—** — нет; статус-эмодзи рядом уточняет качество (если отличается от общего).

---

## Сервисы

### 1. 📥 Сбор постов в базу (Collection)

Инкрементально вытягивает сообщения из подписанных каналов в локальную SQLite.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | — | (статус сбора виден через `/jobs/api/list`) |
| Web | ✅ | `/channels` (кнопки collect), `POST /channels/collect-all`, `/channels/{pk}/collect`, `/channels/{pk}/collect/full` |
| CLI | ✅ | `collect`, `collect sample`, `channel collect`, `scheduler trigger` |
| Agent-tools | ✅ | `collect_channel`, `collect_all_channels`, `collect_channel_stats`, `collect_all_stats` |
| TUI | — | |

**Backend:** `CollectionService`, `Collector` (telegram/collector.py — **2584 строки, горячая зона**), `CollectionQueue`.
**Статус:** 🟢 **проверено вживую** — real-TG `heavy/test_channel_collect_first.py`, `heavy/test_collect_default_all.py`; e2e `test_collection_flow.py` (полный tract через embedded worker).

### 2. 🔎 Поиск (Search) — *объединяет поиск-по-базе + live-поиск в Telegram*

Полнотекстовый (FTS5), семантический (KNN), гибридный и AI-поиск по собранным сообщениям + поиск через Telegram-native API (чужие каналы, свои чаты, premium global).

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | ✅ | `GET /messages/{identifier}` (JSON parity) |
| Web | ✅ | `/search`, `/`, `GET /search/fragments/results`; режим `telegram` для live-поиска |
| CLI | ✅ | `search` (+ `--mode telegram`), `messages read` |
| Agent-tools | ✅ | `search_messages`, `semantic_search`, `search_hybrid`, `index_messages`, `purge_search_cache`, `search_telegram`, `search_my_chats`, `search_in_channel` |
| TUI | — | |

**Backend:** `SearchService`, `SearchEngine`, `LocalSearch`, `AISearchEngine`, `EmbeddingService`, `TelegramSearch` (search/telegram_search.py, 621 строка).
**Статус:** 🟢 **проверено вживую** — base FTS: real-TG `safe_ro/test_search_basic.py`, e2e `/search`; live-поиск TG: `mutation_safe/test_search_telegram_premium.py`. ⚠️ Семантика/AI-поиск — 🟡 только код (offline `test_local_search_semantic_paths.py`, `test_ai_search.py`).

### 3. 🏷 Управление каналами (Channels)

Добавление/удаление/импорт каналов-источников, теги, метаданные, типы, карантин/ревью.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | — | |
| Web | ✅ | `/channels`, `POST /channels/{add,add-bulk,import,refresh-types,refresh-meta}`, `/channels/tags*`, `/channels/review*` |
| CLI | ✅ | `channel list/add/delete/toggle/import/add-bulk/list-for-import/refresh-types/refresh-meta/review-*`, `channel tag {list,add,delete,set,get}` |
| Agent-tools | ✅ | `list_channels`, `add_channel`, `delete_channel`, `toggle_channel`, `import_channels`, `refresh_channel_*`, `*_tag*`, `add_channels_bulk`, `list_dialogs_for_import` |
| TUI | — | |

**Backend:** `ChannelService`, `ChannelOnboarding`.
**Статус:** 🟢 **проверено вживую** — real-TG `safe_write/test_channel_add_first.py`, `test_channel_add_bulk_first.py`, `safe_ro/test_channel_list.py`, `test_channel_tag_*`, `heavy/test_channel_refresh_*`; e2e `/channels`.

### 4. 🧹 Очистка / фильтрация каналов (Filter)

Скоринг каналов по уникальности/спаму/языку, пометка и purge/hard-delete отфильтрованного.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | — | |
| Web | ✅ | `/channels/filter/manage`, `POST /channels/filter/{analyze,apply,purge-*,hard-delete-*,reset*,precheck}` |
| CLI | ✅ | `filter analyze/apply/reset/precheck/toggle/purge/purge-messages/hard-delete` |
| Agent-tools | ✅ | `analyze_filters`, `apply_filters`, `reset_filters`, `toggle_channel_filter`, `purge_filtered_channels`, `hard_delete_channels`, `precheck_filters`, `purge_channel_messages` |
| TUI | — | |

**Backend:** `ChannelAnalyzer` (filters/analyzer.py), `FilterDeletionService`.
**Статус:** 🟢 **проверено вживую** (анализ) — real-TG `safe_ro/test_filter_analyze.py`, `test_filter_precheck.py`; e2e `/channels/filter/manage`. ⚠️ purge/hard-delete (деструктив) — 🟡 только код (manifested как local-mutation).

### 5. ⭐ Рейтинг каналов (Channel Rating)

LLM-judge оценивает канал по 2 осям (полезность × жанр), пишет `channel_ratings`.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | ✅ | `GET /analytics/channels/api/ratings` (JSON) |
| Web | ✅ | `POST /analytics/channels/rate`, `GET /analytics/channels/ratings` |
| CLI | ✅ | `analytics channel-rate`, `analytics channel-rating` |
| Agent-tools | ✅ | `rate_channel`, `get_channel_ratings` |
| TUI | — | |

**Backend:** `ChannelAnalysisService`.
**Статус:** 🟡 **только код** — `channel-rate` manifested как «LLM-judge, provider spend, без live TG», покрыт integration-тестами с фейк-провайдером (#994). e2e-панель `/analytics/channels/ratings` рендерится 🟢, но генерация рейтингов боем не доказана.

### 6. 🤖 AI-генерация контента (Content Generation)

RAG/deep-agent генерация постов из собранного контекста; запись в `generation_runs`.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | ✅ | `GET /pipelines/{id}/runs`, `/{id}/show` (JSON результата прогона) |
| Web | ✅ | `/pipelines/{id}/generate`, `/pipelines/{id}/generate-stream` (**SSE**), `POST /pipelines/{id}/run` |
| CLI | ✅ | `pipeline generate`, `pipeline generate-stream`, `pipeline run`, `pipeline ai-edit` |
| Agent-tools | ✅ | `generate_draft`, `run_pipeline`, `ai_edit_pipeline` |
| TUI | — | |

**Backend:** `ContentGenerationService`, `GenerationService`, `QualityScoringService` (подключён, пишет `quality_score`).
**Статус:** 🟡 **только код** — manifested как «provider spend/write», integration-тесты с фейк-провайдером (`test_content_generation_service.py`, `test_generation_service_streaming.py`); реальная LLM-генерация боем не зафиксирована.

### 7. 🖼 Генерация изображений (Image Generation)

Генерация картинок через провайдеры (Together/HuggingFace/OpenAI/Replicate/Codex) по `provider:model_id`.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | ✅ | `GET /images/generated`, `/images/models/search` (JSON parity) |
| Web | ✅ | `/images`, `POST /images/generate` |
| CLI | ✅ | `image generate/models/providers/generated` |
| Agent-tools | ✅ | `generate_image`, `list_image_models`, `list_image_providers`, `list_generated_images` |
| TUI | — | |

**Backend:** `ImageGenerationService`, `ImageProviderService`, `S3Store` (опц.), `provider_adapters.py`.
**Статус:** 🟡 **только код** (генерация) — `image generate/models` manifested как provider-spend; листинг 🟢 (real-TG `safe_ro/test_image_providers.py`, `test_image_generated.py`), e2e `/images`. Opt-in live Codex-тесты (`codex_image_live/`) gated.

### 8. 🚰 Пайплайны контента (Pipelines)

DAG-конвейер source→filter→llm_generate→image→publish; шаблоны, импорт/экспорт, узлы/рёбра.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | ✅ | `GET /pipelines/api/channels/search`, `/{id}/{runs,show,queue,dry-run-count}`, `/templates/json` (JSON) |
| Web | ✅ | `/pipelines`, `/pipelines/create`, `/pipelines/{id}/edit`, `POST /pipelines/add`, `/pipelines/templates`, `/pipelines/import`, `/pipelines/{id}/export` |
| CLI | ✅ | `pipeline list/show/add/edit/delete/toggle/run/dry-run-count/runs/run-show/export/import/templates/from-template/refinement-steps`, `pipeline node {add,replace,remove}`, `edge {add,remove}`, `filter {set,show,clear}`, `graph` |
| Agent-tools | ✅ | 20 tools: `list_pipelines`, `add_pipeline`, `edit_pipeline`, `run_pipeline`, `*_pipeline_json`, `*_pipeline_template*`, `get_pipeline_dry_run_count`… |
| TUI | — | |

**Backend:** `PipelineService` (**809 строк**), `PipelineExecutor`, `PipelineNodeHandlers`, `PipelineFilters`.
**Статус:** 🟢 **проверено вживую** (чтение/конфиг) — real-TG `safe_ro/test_pipeline_*` (list/show/graph/dry-run-count/templates), `safe_write/test_pipeline_export_first.py`; e2e `/pipelines`. ⚠️ Полный прогон с генерацией — 🟡 (см. сервис #6).

### 9. ✅ Модерация контента (Moderation)

Ручная очередь approve/reject сгенерированного контента перед публикацией.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | — | |
| Web | ✅ | `/moderation`, `POST /moderation/{run}/{approve,reject,publish}`, `/moderation/bulk-{approve,reject}` |
| CLI | ✅ | `pipeline moderation-list/moderation-view/approve/reject/bulk-approve/bulk-reject/queue` |
| Agent-tools | ✅ | `list_pending_moderation`, `view_moderation_run`, `approve_run`, `reject_run`, `bulk_approve_runs`, `bulk_reject_runs` |
| TUI | — | |

**Backend:** репозиторий `generation_runs`, `DraftNotificationService` (push о новых драфтах).
**Статус:** 🟢 **проверено вживую** (чтение) — real-TG `safe_ro/test_pipeline_moderation_list.py`, `moderation_view_first.py`; e2e `/moderation`. ⚠️ approve/reject (мутации) — 🟡 только код.

### 10. 📤 Публикация текста (Publishing)

Отправка сгенерированного/готового текста в целевой Telegram-канал/диалог.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | — | |
| Web | ✅ | `POST /pipelines/{id}/publish`, `POST /moderation/{run}/publish`, `POST /dialogs/send` |
| CLI | ✅ | `pipeline publish`, `dialogs send` |
| Agent-tools | ✅ | `publish_pipeline_run`, `send_message` |
| TUI | — | |

**Backend:** `PublishService`.
**Статус:** 🟢 **проверено вживую** — real-TG `mutation_safe/test_pipeline_publish_sandbox.py`, `dialogs send` в `mutation_safe`.

### 11. 📸 Постинг картинок / Photo Loader (Photo Publishing)

Отправка фото в диалоги: разовая, batch, авто-загрузка по расписанию из папки.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | ✅ | `GET /dialogs/photos/{batches,auto,items}` (JSON parity) |
| Web | ✅ | `/dialogs/photos`, `POST /dialogs/photos/{send,schedule,batch,auto,run-due}` |
| CLI | ✅ | `photo-loader dialogs/refresh/send/schedule-send/batch-*/items/auto-*/run-due` (14 шт.) |
| Agent-tools | ✅ | `send_photos_now`, `schedule_photos`, `create_photo_batch`, `create_auto_upload`, `run_photo_due`… (14 шт., 6 phone-bound) |
| TUI | — | |

**Backend:** `PhotoPublishService`, `PhotoTaskService`, `PhotoAutoUploadService`.
**Статус:** 🟢 **проверено вживую** — real-TG `mutation_safe/test_photo_loader_{send,schedule_send,run_due}_sandbox.py` (3 passed); `safe_write/test_photo_loader_refresh.py`.

### 12. 💬 Диалоги Telegram (Dialogs / chat management)

Управление чатами: список/refresh диалогов, send/edit/delete/forward/pin/react, участники, права, archive/kick, создание каналов/групп.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | — | (всё через HTML-формы/HTMX) |
| Web | ✅ | `/dialogs` + ~25 POST-экшенов |
| CLI | ✅ | `dialogs list/refresh/resolve/leave/join/topics/cache-*/send/forward/edit-message/delete-message/create-channel/create-group/pin-message/react/unpin-message/download-media/participants/edit-admin/edit-permissions/kick/broadcast-stats/archive/unarchive/mark-read`, `dialogs queue {status,cancel,clear-pending}` |
| Agent-tools | ✅ | `dialogs.py` (11) + `messaging.py` (22) — send/react/forward/pin/admin/kick/archive… (большинство phone-bound) |
| TUI | — | |

**Backend:** `TelegramActionService`, `TelegramCommandService`, `TelegramCommandDispatcher` (**1161 строка, горячая зона**).
**Статус:** 🟢 **проверено вживую** — обширное real-TG: `mutation_safe/test_dialogs_*` (archive/delete/edit/forward/mark-read/participants/pin/react/unpin), `manual/test_dialogs_create_*`, `safe_ro/test_dialogs_*`; e2e `/dialogs`.

### 13. 👤 Управление аккаунтами (Accounts)

Подключение/toggle/удаление Telegram-аккаунтов, primary, flood-статус, auth-флоу.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | ✅ | `GET /settings/flood-status`, `/settings/{id}/info` (JSON + live-диагностика) |
| Web | ✅ | `/auth/login` (multi-step), `POST /settings/{id}/{toggle,delete,set-primary,flood-clear}` |
| CLI | ✅ | `account list/info/toggle/set-primary/delete/send-code/verify-code/add/flood-status/flood-clear` |
| Agent-tools | ✅ | `list_accounts`, `toggle_account`, `delete_account`, `get_flood_status`, `clear_flood_status`, `get_account_info`, `get_account_availability`, `get_runtime_diagnostics` |
| TUI | — | |

**Backend:** `AccountService`, `AccountAvailability`, `ClientPool` (**2107 строк, горячая зона**), `TelegramAuth`.
**Статус:** 🟢 **проверено вживую** (чтение) — real-TG `safe_ro/test_account_{list,info,flood_status}.py`. ⚠️ Auth-флоу (`send-code`/`verify-code`/`add`) и `delete` — 🟡 только код (manifested как ручной onboarding / dangerous-blocked).

### 14. 🔔 Уведомления (Notifications)

Персональный бот через BotFather; матчинг сообщений с запросами; push о драфтах.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | — | |
| Web | ✅ | `POST /settings/notifications/{setup,delete,test}`, `GET /settings/notifications/status`, `/scheduler/{test-notification,dry-run-notifications}` |
| CLI | ✅ | `notification setup/status/delete/test/dry-run/set-account` |
| Agent-tools | ✅ | `setup_notification_bot`, `delete_notification_bot`, `test_notification`, `get_notification_status`, `notification_dry_run` |
| TUI | — | |

**Backend:** `NotificationService`, `NotificationMatcher`, `NotificationTargetService`, `DraftNotificationService`, `Notifier`.
**Статус:** 🟢 **проверено вживую** (статус/dry-run/test) — real-TG `safe_ro/test_notification_{status,dry_run}.py`, `mutation_safe/test_notification_test_sandbox.py`. ⚠️ `setup`/`delete` (BotFather) — 🟡 только код.

### 15. 🔁 Сохранённые поисковые запросы (Search Queries)

Именованные запросы для мониторинга трендов; запуск по расписанию; статистика срабатываний.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | ✅ | `GET /search-queries/{id}`, `/{id}/stats` (JSON) |
| Web | ✅ | `/search-queries`, `POST /search-queries/{add,{id}/toggle,{id}/edit,{id}/delete,{id}/run}` |
| CLI | ✅ | `search-query list/get/add/edit/delete/toggle/run/stats` |
| Agent-tools | ✅ | `list_search_queries`, `add_search_query`, `run_search_query`, `get_search_query_stats`… (8 шт.) |
| TUI | — | |

**Backend:** `SearchQueryService`.
**Статус:** 🟢 **проверено вживую** (чтение) — real-TG `safe_ro/test_search_query_{list,get_first,stats_first}.py`; e2e `/search-queries`. ⚠️ add/edit/run — 🟡 только код.

### 16. 📊 Аналитика (Analytics) — *объединяет аналитику + тренды*

Статистика по каналам/контенту (top-сообщения, engagement, hourly, velocity, peak-hours, heatmap, cross-citations) + trending темы (TF-IDF)/каналы/эмодзи.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | ✅ | широкий JSON parity: `/analytics/{messages/top,messages/hourly,messages/velocity,peak-hours,pipelines/stats}`, `/analytics/content/api/*`, `/analytics/trends/{topics,channels,emojis}`, `/analytics/channels/api/*` |
| Web | ✅ | `/analytics`, `/analytics/content`, `/analytics/channels`, `/analytics/trends` + HTMX-фрагменты |
| CLI | ✅ | `analytics top/content-types/hourly/summary/daily/pipeline-stats/velocity/peak-hours/calendar/channel/trending-topics/trending-channels/trending-emojis` |
| Agent-tools | ✅ | 15 read-tools: `get_analytics_summary`, `get_top_messages`, `get_hourly_activity`, `get_peak_hours`, `get_channel_analytics`, `get_trending_*`… |
| TUI | — | |

**Backend:** `ChannelAnalyticsService`, `ContentAnalyticsService`, `TrendService`.
**Статус:** 🟢 **проверено вживую** — real-TG `safe_ro/test_analytics_*` (15 кейсов: top/daily/hourly/summary/velocity/peak-hours/content-types/pipeline-stats/channel/trending-{topics,channels,emojis}…); e2e `/analytics`, `/analytics/channels`, `/analytics/trends`.

### 17. 📅 Календарь публикаций (Calendar) — *отдельный сервис*

Визуализация запланированных публикаций по дням.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | ✅ | `GET /calendar/api/{calendar,upcoming,stats}` (JSON) |
| Web | ✅ | `/calendar` + HTMX-фрагменты `{stats,grid,upcoming}` |
| CLI | ✅ | `analytics calendar` |
| Agent-tools | ✅ | `get_calendar` |
| TUI | — | |

**Backend:** `ContentCalendarService`.
**Статус:** 🟢 **проверено вживую** (рендер/чтение) — real-TG `safe_ro/test_analytics_calendar.py`; e2e `/calendar`.

### 18. ⏰ Планировщик / расписание (Scheduler)

APScheduler: периодический сбор, запуск search-queries, пауза/резюм очереди, управление job'ами.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | — | (фрагменты health/jobs/tasks — HTMX) |
| Web | ✅ | `/scheduler`, `POST /scheduler/{start,stop,pause,resume,trigger,trigger-warm}`, `/scheduler/jobs/{id}/{toggle,set-interval}`, `/scheduler/tasks/*` |
| CLI | ✅ | `scheduler start/trigger/status/stop/job-toggle/set-interval/task-cancel/clear-pending/queue-pause/queue-resume` |
| Agent-tools | ✅ | `get_scheduler_status`, `start_scheduler`, `trigger_collection`, `toggle_scheduler_job`, `set_scheduler_interval`… (8 шт.) |
| TUI | — | |

**Backend:** `SchedulerManager` (scheduler/manager.py).
**Статус:** 🟢 **проверено вживую** (статус/trigger/start) — real-TG `safe_ro/test_scheduler_status.py`, `mutating/test_proc_scheduler_{start,trigger}.py`; e2e `/scheduler` (регресс-гард на 0/0-баг).

### 19. 🗂 Фоновые задачи / Jobs (Background Jobs)

Единый read-only дашборд всех фоновых задач (collection tasks, telegram commands, photo tasks).

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | ✅ | `GET /jobs/api/list` (JSON, filterable по source/status) |
| Web | ✅ | `/jobs`, `GET /jobs/fragments/list` |
| CLI | ⚠️ — | **нет прямой команды** (видно частично через `scheduler status`, `dialogs queue status`) |
| Agent-tools | — | (косвенно `get_telegram_queue_status`) |
| TUI | — | |

**Backend:** `JobsReadModel`, `UnifiedDispatcher`, `TaskEnqueuer`, `task_handlers/`.
**Статус:** 🟢 **проверено вживую** (рендер) — e2e `/jobs`; integration `test_jobs_read_model.py` (регресс HTTP 500 naive/aware sort). ⚠️ **Нет CLI-парити** — кандидат на нарушение CLI↔Web инварианта.

### 20. 🌍 Перевод (Translation)

Детекция языка + LLM-перевод сообщений (batch и одиночный).

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | — | |
| Web | ✅ | `POST /settings/{save-translation,translation-backfill,translation-run}`, `POST /search/search/translate/{id}` |
| CLI | ✅ | `translate stats/detect/run/message` |
| Agent-tools | ✅ | `translate_message` |
| TUI | — | |

**Backend:** `TranslationService`.
**Статус:** 🟢 **проверено вживую** (stats) — real-TG `safe_ro/test_translate_stats.py`. ⚠️ `run`/`detect`/`message` — 🟡 только код (provider/network translation).

### 21. 📦 Экспорт (Export)

Экспорт собранных сообщений в JSON/CSV/RSS/Telegram-Desktop формат.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | ✅ | `GET /rss.xml`, `/atom.xml`; `POST /channels/{id}/export` → JSON `{task_id, status}` |
| Web | ✅ | кнопка экспорта на `/channels` |
| CLI | ✅ | `export json/csv/rss/telegram` |
| Agent-tools | ✅ | `export_messages` |
| TUI | — | |

**Backend:** `ExportService`, `TelegramExportBuilder`.
**Статус:** 🟢 **проверено вживую** — real-TG `safe_ro/test_export_{json,csv,rss}.py`. `export telegram` — 🟡 только код (offline DB→file, покрыт integration).

### 22. 🔌 Провайдеры LLM/Image (Providers)

Регистрация/проба/refresh провайдеров (OpenAI, Cohere, Ollama, Anthropic, image-провайдеры).

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | — | (фрагменты test-all-status — HTMX) |
| Web | ✅ | `POST /settings/agent-providers/{add,save,delete,refresh,probe,test-all}`, `/settings/image-providers/*` |
| CLI | ✅ | `provider list/add/delete/probe/refresh/test-all` |
| Agent-tools | ⚠️ — | прямого tool нет; косвенно `list_image_providers` |
| TUI | — | |

**Backend:** `ProviderService`, `AgentProviderService`, `ImageProviderService`, `ProviderModelCache`, `provider_adapters.py` (**677 строк**).
**Статус:** 🟢 **проверено вживую** (list) — real-TG `safe_ro/test_provider_list.py`. ⚠️ probe/test-all/refresh — 🟡 только код (live-API проба, gated `real_provider_smoke`).

### 23. 🧠 AI-агент (Agent / chat)

Чат-агент над всеми tools (4 backend'а: claude-agent-sdk, deepagents, Codex, ADK); треды, контекст, permissions. **Единственный сервис с TUI.**

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | ✅ | `GET /agent/threads/{id}/messages`, `/agent/channels-json` (JSON) |
| Web | ✅ | `/agent`, `POST /agent/threads`, `/agent/threads/{id}/chat` (**SSE**), `/agent/threads/{id}/{stop,context,permission/{req}}` |
| CLI | ✅ | `agent threads/thread-create/thread-delete/chat/thread-rename/thread-stop/messages/context/test-escaping/test-tools` |
| Agent-tools | ✅ | `agent_threads.py` (5: list/create/delete/rename/get_messages) + сам реестр 184 tools |
| TUI | ✅ | `AgentTuiApp` (`agent_tui.py` + `.tcss`); запуск `agent chat` без `--prompt` |

**Backend:** `AgentManager`, `AgentProviderService`, react-agent fallback, MCP-server.
**Статус:** 🟢 **проверено вживую** (чтение/one-shot) — real-TG `safe_ro/test_agent_threads.py`, `test_agent_messages_first.py`, `safe_write/test_proc_agent_chat_oneshot.py`; e2e `/agent`. ⚠️ Codex/ADK backend'ы и TUI-режим — 🟡 только код (opt-in / gated / интерактив без авто-теста).

### 24. ⚙️ Настройки и диагностика (Settings & Debug)

Конфиг (credentials, фильтр-пороги, agent, reactions, semantic); системная диагностика; debug-логи/память/тайминги.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | ✅ | `GET /debug/memory` (JSON); `/telegram-commands/{id}` (JSON-статус команды, redacted) |
| Web | ✅ | `/settings` (агрегатор), `POST /settings/save-*`, `/debug`, `/debug/{logs,timing,memory}` |
| CLI | ✅ | `settings get/set/info/server-time/agent/filter-criteria/reactions/semantic`, `debug logs/memory/timing`, `test all/read/write/telegram/benchmark` |
| Agent-tools | ✅ | `get_settings`, `save_scheduler_settings`, `save_agent_settings`, `save_filter_settings`, `get_system_info`, `get_server_time` (6 шт.) |
| TUI | — | |

**Backend:** репозиторий `settings`, `RuntimeDiagnostics`.
**Статус:** 🟢 **проверено вживую** — real-TG `safe_ro/test_settings_{get,info,server-time}.py`, `test_debug_{logs,memory,timing}.py`, `test_proc_test_read.py`; e2e `/settings`. ⚠️ `/debug` НЕ в e2e console-smoke (dev-only).

### 25. 🛡 Лимиты и качество (Production Limits & Quality)

Rate/cost-лимиты на провайдеров, учёт дневных затрат; LLM-оценка качества контента.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | — | |
| Web | — | (косвенно через `/settings` config) |
| CLI | ⚠️ — | через `settings`/config, без выделенной команды |
| Agent-tools | — | |
| TUI | — | |

**Backend:** `ProductionLimitsService` (подключён в `image_generation_service`, опц., off-by-default), `QualityScoringService` (подключён в content-generation, пишет `quality_score`), `ErrorRecoveryService` (**нигде не импортируется** — 🔴 изолирован).
**Статус:** 🟡 **только код** — лимиты подключены, но off-by-default; `ErrorRecoveryService` мёртв. Реальная работа лимитов боем не доказана.

### 26. 🧪 A/B-тестирование контента (A/B Testing)

Генерация нескольких вариантов поста и выбор лучшего.

| Поверхность | Есть | Детали |
|---|:---:|---|
| FastAPI REST | — | |
| Web | — | |
| CLI | — | |
| Agent-tools | — | |
| TUI | — | |

**Backend:** `ABTestingService` (ab_testing_service.py) — **не импортируется нигде, кроме своего файла + тестов**.
**Статус:** 🔴 **заглушка/частично** — сервисный код есть, но НЕ подключён ни к одной из пяти поверхностей. Висит изолированно.

---

## Сводка

**Всего сервисов: 26** (после слияний: Поиск = поиск-по-базе + live-TG; Аналитика = аналитика + тренды; Календарь — отдельно).

Инвентарь поверхностей:
- **FastAPI REST** — 1 полноценный REST-контур (`/api/tasks`, interop #829) + машинные JSON parity-эндпоинты в ~14 роутерах
- **Web** (HTML/HTMX) — 27 роутеров, 18 UI-страниц меню
- **CLI** — 26 top-level команд, ~209 leaf-команд
- **Agent-tools** — 18 модулей, 184 tools (86 read / 83 write / 15 delete)
- **TUI** — 1 app (`AgentTuiApp`), покрывает только сервис «AI-агент»

Распределение по статусу:

| Статус | Кол-во | Сервисы |
|--------|--------|---------|
| 🟢 **проверено вживую** | **19** | Сбор (1), Поиск (2, base FTS+live-TG), Каналы (3), Очистка/анализ (4), Пайплайны-чтение (8), Модерация-чтение (9), Публикация текста (10), Постинг картинок (11), Диалоги (12), Аккаунты-чтение (13), Уведомления-статус (14), Search-queries-чтение (15), Аналитика+Тренды (16), Календарь (17), Планировщик (18), Jobs (19), Перевод-stats (20), Экспорт (21), Провайдеры-list (22), Агент-чтение (23), Настройки (24) |
| 🟡 **только код** | **6** | Рейтинг каналов (5), AI-генерация (6), Генерация картинок (7), Production Limits & Quality (25) — плюс «тёмные половины» гибридных сервисов 4/9/13/14/20/22 (деструктив/auth/BotFather/provider-проба) |
| 🔴 **заглушка/частично** | **1** | A/B-тестирование (26); внутри #25 `ErrorRecoveryService` тоже мёртв |

> ⚠️ Многие сервисы **гибридные**: read-путь 🟢, write/мутирующий путь 🟡 (помечено в строке). Read тривиально гоняется в `safe_ro`, мутации намеренно вынесены из авто-suite. Для баг-ханта именно write-половины — приоритет.

---

## Кандидаты на наибольший риск багов

> Этот раздел — вход для эпика баг-ханта (#1024). Ранжировано: статус «только код» + сложность/горячая зона + бизнес-критичность.

**Тир 1 — критично (горячие зоны + слабое live-покрытие мутаций):**

1. **Сбор постов (1)** — `Collector` 2584 строки, исторически `_collect_channel` имел цикломатическую сложность F(100) (`code_health.py` baseline). Статус 🟢, но объём + сложность = главный источник регрессий. Инкрементальная логика (`min_id`/`last_collected_id`), дедуп, cancellation, entity-cache — тонкие места.
2. **Аккаунты / ClientPool (13)** — `ClientPool` 2107 строк. Flood-wait ротация, primary-гонки (партиал-индекс #733), auth-флоу StringSession. Auth-половина 🟡. Прецеденты гонок pool↔DB.
3. **Диалоги / TelegramCommandDispatcher (12)** — `TelegramCommandDispatcher` 1161 строка. 33 tools, ~25 web-экшенов, много phone-bound мутаций, очередь команд. Широкая mutation-поверхность.

**Тир 2 — высокий риск (контентный цикл, 🟡 целиком):**

4. **AI-генерация контента (6)** — provider-spend, SSE-стриминг, RAG-контекст, deep-agent ветка. Боем не доказано. Дубль-биллинг при retry — известный класс багов (#958).
5. **Генерация картинок (7)** — несколько SDK-адаптеров, S3, retry/billing (прецедент #958: non-idempotent POST + max_retries=2 → дубль-биллинг). Каждый адаптер — свой lifecycle.
6. **Пайплайны — полный прогон (8)** — `PipelineService`+`PipelineExecutor` 809 строк, DAG, топосортировка, узлы/рёбра. Конфиг 🟢, исполнение генерации 🟡.
7. **Рейтинг каналов (5)** — LLM-judge, запись `channel_ratings`. Прецедент: бинарный AI-детектор провалил слепую проверку (recall 0) → переход на channel-slop. Риск ложных вердиктов.

**Тир 3 — «мёртвый/изолированный код» (риск тихих поломок, не покрыт):**

8. **A/B-тестирование (26)** — 🔴 не подключён ни к одной поверхности. Либо удалить, либо доделать; сейчас — bit-rot.
9. **ProductionLimits / ErrorRecovery (25)** — `ErrorRecoveryService` не импортируется; `ProductionLimitsService` off-by-default. Если лимиты реально нужны — по умолчанию не работают (риск перерасхода на провайдерах).

**Тир 4 — нарушения инвариантов (структурные баги):**

10. **Jobs (19)** — нет CLI-парити (инвариант «каждая web-операция = CLI-эквивалент»). Уже ловил HTTP 500 (naive/aware sort, `test_jobs_read_model.py`). Read-only, но единая точка агрегации всех очередей.
11. **Деструктивные write-пути сервисов 4/9/13/14** — purge/hard-delete, account delete, BotFather setup/delete: 🟡 только код, manifested как «опасно для авто-suite». Не гоняются живьём → слепая зона.
