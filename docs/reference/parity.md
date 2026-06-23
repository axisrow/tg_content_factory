# CLI / Web / Agent Parity

Каждая операция по возможности доступна через CLI, Web и agent tools. Для управления диалогами каноническое имя во всех интерфейсах теперь `dialogs`.

## Каналы

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Список каналов | `channel list` | `GET /channels/` | `list_channels` |
| Добавить канал | `channel add` | `POST /channels/add` | `add_channel` |
| Удалить канал | `channel delete` | `POST /channels/{pk}/delete` | `delete_channel` |
| Вкл/выкл канал | `channel toggle` | `POST /channels/{pk}/toggle` | `toggle_channel` |
| Статистика канала | `channel stats` | `POST /channels/{pk}/stats` | `collect_channel_stats`, `get_channel_stats` |
| Импорт каналов | `channel import` | `POST /channels/import` | `import_channels` |
| Обновить типы | `channel refresh-types` | `POST /channels/refresh-types` | `refresh_channel_types` |
| Обновить метаданные | `channel refresh-meta` | `POST /channels/refresh-meta` | `refresh_channel_meta` |
| Список на ревью | `channel review-list` | `GET /channels/review` | `list_channels_for_review` |
| Подтвердить удаление | `channel review-confirm` | `POST /channels/{pk}/review-confirm` | `confirm_channel_dead` |
| Оставить активным | `channel review-keep` | `POST /channels/{pk}/review-keep` | `review_keep_channel` |
| Массовое добавление из диалогов | `channel add-bulk` | `POST /channels/add-bulk` | `add_channels_bulk` |
| Список тегов | `channel tag list` | `GET /channels/tags` | `list_tags` |
| Создать тег | `channel tag add` | `POST /channels/tags` | `create_tag` |
| Удалить тег | `channel tag delete` | `DELETE /channels/tags/{name}` | `delete_tag` |
| Получить теги канала | `channel tag get` | `GET /channels/{pk}/tags` | `get_channel_tags` |
| Обновить теги канала | `channel tag set` | `POST /channels/{pk}/tags` | `set_channel_tags` |
| Список диалогов для импорта | `channel list-for-import` | `GET /channels/dialogs` | `list_dialogs_for_import` |

## Переименования каналов

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Список переименований | *исключение: web-only UI модерации* | `GET /channels/renames` | *исключение: web-only UI модерации* |
| Отфильтровать переименование | *исключение: web-only UI модерации* | `POST /channels/renames/{event_id}/filter` | *исключение: web-only UI модерации* |
| Оставить переименование | *исключение: web-only UI модерации* | `POST /channels/renames/{event_id}/keep` | *исключение: web-only UI модерации* |

## Сбор сообщений

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Собрать все каналы | `collect` | `POST /channels/collect-all` | `collect_all_channels` |
| Собрать один канал | `channel collect` | `POST /channels/{pk}/collect` | `collect_channel` |
| Статистика всех | `channel stats --all` | `POST /channels/stats/all` | `collect_all_stats` |
| Превью без сохранения | `collect sample` | *исключение: превью без сохранения, UI-only* | *исключение: превью без сохранения, UI-only* |
| Отменить задачу | `scheduler task-cancel` | `POST /scheduler/tasks/{id}/cancel` | `cancel_scheduler_task` |
| Очистить очередь | `scheduler clear-pending` | `POST /scheduler/tasks/clear-pending-collect` | `clear_pending_tasks` |

## Поиск

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Поиск сообщений | `search` | `GET /search` | `search_messages` |
| Семантический поиск | `search --mode semantic` | `GET /search?mode=semantic` | `semantic_search` |
| Гибридный поиск | `search --mode hybrid` | `GET /search?mode=hybrid` | `search_hybrid` |
| Telegram-поиск | `search --mode telegram` | `GET /search?mode=telegram` | `search_telegram` |
| Поиск по чатам | `search --mode my_chats` | `GET /search?mode=my_chats` | `search_my_chats` |
| Поиск в канале | `search --mode channel` | `GET /search?mode=channel` | `search_in_channel` |
| Индексация | `search --index-now` | `POST /settings/semantic-index` | `index_messages` |
| Очистка кэша Premium-поиска | `search --purge-cache` | `POST /search/purge-cache` | `purge_search_cache` |

## Сообщения

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Чтение сообщений | `messages read` | `GET /messages/{identifier}` | `read_messages` |
| Перевод одного сообщения | `translate message` | `POST /search/translate/{message_db_id}` | `translate_message` |
| Экспорт сообщений | `export json|csv|rss` | *исключение: файловый экспорт вне web* | *исключение: файловый экспорт вне tool-контракта* |
| Экспорт в формат Telegram | `export telegram` | `POST /channels/{id}/export` | `export_messages` |

## Поисковые запросы

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Список | `search-query list` | `GET /search-queries/` | `list_search_queries` |
| Добавить | `search-query add` | `POST /search-queries/add` | `add_search_query` |
| Редактировать | `search-query edit` | `POST /search-queries/{id}/edit` | `edit_search_query` |
| Удалить | `search-query delete` | `POST /search-queries/{id}/delete` | `delete_search_query` |
| Вкл/выкл | `search-query toggle` | `POST /search-queries/{id}/toggle` | `toggle_search_query` |
| Запустить вручную | `search-query run` | `POST /search-queries/{id}/run` | `run_search_query` |
| Получить | `search-query get` | `GET /search-queries/{id}` | `get_search_query` |
| Статистика | `search-query stats` | `GET /search-queries/{id}/stats` | `get_search_query_stats` |

## Фильтры

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Анализировать | `filter analyze` | `POST /channels/filter/analyze` | `analyze_filters` |
| Применить | `filter apply` | `POST /channels/filter/apply` | `apply_filters` |
| Сбросить | `filter reset` | `POST /channels/filter/reset` | `reset_filters` |
| Pre-check | `filter precheck` | `POST /channels/filter/precheck` | `precheck_filters` |
| Вкл/выкл фильтр | `filter toggle` | `POST /channels/{pk}/filter-toggle` | `toggle_channel_filter` |
| Очистить filtered-каналы | `filter purge` | `POST /channels/filter/purge-all` | `purge_filtered_channels` |
| Выбранные очистить | *исключение: batch UI-выбор* | `POST /channels/filter/purge-selected` | *исключение: batch UI-выбор* |
| Hard delete | `filter hard-delete` | `POST /channels/filter/hard-delete-selected` | `hard_delete_channels` |
| Очистить сообщения канала | `filter purge-messages` | `POST /channels/{id}/purge-messages` | `purge_channel_messages` |

## Пайплайны

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Список | `pipeline list` | `GET /pipelines/` | `list_pipelines` |
| Детали | `pipeline show` | `GET /pipelines/{id}/show` | `get_pipeline_detail` |
| Добавить | `pipeline add` | `POST /pipelines/add` | `add_pipeline` |
| Редактировать | `pipeline edit` | `POST /pipelines/{id}/edit` | `edit_pipeline` |
| Удалить | `pipeline delete` | `POST /pipelines/{id}/delete` | `delete_pipeline` |
| Вкл/выкл | `pipeline toggle` | `POST /pipelines/{id}/toggle` | `toggle_pipeline` |
| Запустить | `pipeline run` | `POST /pipelines/{id}/run` | `run_pipeline` |
| Генерация контента | `pipeline generate` | `POST /pipelines/{id}/generate` | `generate_draft` |
| История запусков | `pipeline runs` | `GET /pipelines/{id}/runs` | `list_pipeline_runs` |
| Детали запуска | `pipeline run-show` | `GET /pipelines/{id}/runs/{run_id}` | `get_pipeline_run` |
| Очередь модерации | `pipeline queue` | `GET /pipelines/{id}/queue` | `get_pipeline_queue` |
| Опубликовать | `pipeline publish` | `POST /pipelines/{id}/publish` | `publish_pipeline_run` |
| Одобрить | `pipeline approve` | `POST /moderation/{id}/approve` | `approve_run` |
| Отклонить | `pipeline reject` | `POST /moderation/{id}/reject` | `reject_run` |
| Одобрить (bulk) | `pipeline bulk-approve` | `POST /moderation/bulk-approve` | `bulk_approve_runs` |
| Отклонить (bulk) | `pipeline bulk-reject` | `POST /moderation/bulk-reject` | `bulk_reject_runs` |
| Шаги refinement | `pipeline refinement-steps` | `GET/POST /pipelines/{id}/refinement-steps` | `get_refinement_steps`, `set_refinement_steps` |
| Подсчёт dry-run | `pipeline dry-run-count` | `GET /pipelines/{id}/dry-run-count` | `get_pipeline_dry_run_count` |
| Узлы графа (CRUD) | `pipeline node` | *исключение: графовый CRUD только-CLI* | *исключение: графовый CRUD только-CLI* |
| Связи графа (CRUD) | `pipeline edge` | *исключение: графовый CRUD только-CLI* | *исключение: графовый CRUD только-CLI* |
| Граф (ASCII) | `pipeline graph` | *исключение: ASCII-вывод терминал* | *исключение: ASCII-вывод терминал* |
| Экспорт JSON | `pipeline export` | `GET /pipelines/{id}/export` | `export_pipeline_json` |
| Импорт JSON | `pipeline import` | `POST /pipelines/import` | `import_pipeline_json` |
| Список шаблонов | `pipeline templates` | `GET /pipelines/templates` | `list_pipeline_templates` |
| Создать из шаблона | `pipeline from-template` | `POST /pipelines/from-template` | `create_pipeline_from_template` |
| AI edit | `pipeline ai-edit` | `POST /pipelines/{id}/ai-edit` | `ai_edit_pipeline` |
| Страница модерации | `pipeline moderation-list` | `GET /moderation/` | `list_pending_moderation` |
| Просмотр модерации | `pipeline moderation-view` | `GET /moderation/{id}/view` | `view_moderation_run` |
| Стрим генерации | `pipeline generate-stream` | `GET /pipelines/{id}/generate-stream` | *исключение: SSE вне tool-контракта* |
| Фильтр контента: задать | `pipeline filter set` | *исключение: filter CRUD только-CLI* | *исключение: filter CRUD только-CLI* |
| Фильтр контента: показать | `pipeline filter show` | *исключение: filter CRUD только-CLI* | *исключение: filter CRUD только-CLI* |
| Фильтр контента: очистить | `pipeline filter clear` | *исключение: filter CRUD только-CLI* | *исключение: filter CRUD только-CLI* |
| Мастер создания | *исключение: web-only (CLI≈pipeline add)* | `POST /pipelines/create-wizard` | *исключение: web-only мастер (CLI≈pipeline add)* |
| Превью dry-run | *исключение: web-only (CLI≈dry-run-count)* | `POST /pipelines/{id}/dry-run` | *исключение: web-only превью (CLI≈dry-run-count)* |

## Планировщик

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Статус | `scheduler status` | `GET /scheduler/` | `get_scheduler_status` |
| Запустить | `scheduler start` | `POST /scheduler/start` | `start_scheduler` |
| Остановить | `scheduler stop` | `POST /scheduler/stop` | `stop_scheduler` |
| Триггер | `scheduler trigger` | `POST /scheduler/trigger` | `trigger_collection` |
| Вкл/выкл job | `scheduler job-toggle` | `POST /scheduler/jobs/{id}/toggle` | `toggle_scheduler_job` |
| Изменить интервал | `scheduler set-interval` | `POST /scheduler/jobs/{id}/set-interval` | `set_scheduler_interval` |
| Сохранить настройки | `settings set` | `POST /settings/save-scheduler` | `save_scheduler_settings` |
| Пауза очереди сбора | `scheduler queue-pause` | `POST /scheduler/pause` | *исключение: служебное управление очередью* |
| Возобновить очередь | `scheduler queue-resume` | `POST /scheduler/resume` | *исключение: служебное управление очередью* |
| Прогрев диалогов | — | `POST /scheduler/trigger-warm` | *исключение: служебный прогрев кеша* |

## Уведомления

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Настроить бота | `notification setup` | `POST /settings/notifications/setup` | `setup_notification_bot` |
| Статус | `notification status` | `GET /settings/notifications/status` | `get_notification_status` |
| Удалить | `notification delete` | `POST /settings/notifications/delete` | `delete_notification_bot` |
| Тест | `notification test` | `POST /settings/notifications/test`, `POST /scheduler/test-notification` | `test_notification` |
| Dry-run | `notification dry-run` | `POST /scheduler/dry-run-notifications` | `notification_dry_run` |
| Выбрать аккаунт | `notification set-account` | `POST /settings/save-notification-account` | *исключение: привязка аккаунта, чувствительно* |

## Аккаунты

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Список | `account list` | `GET /settings/` | `list_accounts` |
| Вкл/выкл | `account toggle` | `POST /settings/{id}/toggle` | `toggle_account` |
| Сделать Primary | `account set-primary` | `POST /settings/{id}/set-primary` | *исключение: системное управление primary* |
| Удалить | `account delete` | `POST /settings/{id}/delete` | `delete_account` |
| Flood статус | `account flood-status` | `GET /settings/flood-status` | `get_flood_status` |
| Сбросить flood | `account flood-clear` | `POST /settings/{id}/flood-clear` | `clear_flood_status` |
| Инфо | `account info` | `GET /settings/{id}/info` | `get_account_info` |
| Доступность | `account list` | `GET /settings/` | `get_account_availability` |
| Диагностика рантайма | `scheduler status` | `GET /settings/` | `get_runtime_diagnostics` |
| Добавить аккаунт | `account add` | `POST /auth/send-code`, `POST /auth/verify-code` | *исключение: интерактивный 2FA-flow* |
| Отправить код авторизации | `account send-code` | `POST /auth/send-code` | *исключение: интерактивный 2FA-flow* |
| Подтвердить код авторизации | `account verify-code` | `POST /auth/verify-code` | *исключение: интерактивный 2FA-flow* |

## Аналитика

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Сводка | `analytics summary` | `GET /analytics/content/api/summary` | `get_analytics_summary` |
| Топ сообщений | `analytics top` | `GET /analytics/messages/top` | `get_top_messages` |
| Типы контента | `analytics content-types` | `GET /analytics/content/api/types` | `get_content_type_stats` |
| Почасовая активность | `analytics hourly` | `GET /analytics/messages/hourly` | `get_hourly_activity` |
| Ежедневная статистика | `analytics daily` | `GET /analytics/content/api/daily` | `get_daily_stats` |
| Статистика пайплайнов | `analytics pipeline-stats` | `GET /analytics/pipelines/stats` | `get_pipeline_stats` |
| Трендовые темы | `analytics trending-topics` | `GET /analytics/trends/topics` | `get_trending_topics` |
| Топ каналов | `analytics trending-channels` | `GET /analytics/trends/channels` | `get_trending_channels` |
| Трендовые эмодзи | `analytics trending-emojis` | `GET /analytics/trends/emojis` | `get_trending_emojis` |
| Скорость сообщений | `analytics velocity` | `GET /analytics/messages/velocity` | `get_message_velocity` |
| Пиковые часы | `analytics peak-hours` | `GET /analytics/peak-hours` | `get_peak_hours` |
| Календарь | `analytics calendar` | `GET /calendar/api/calendar` | `get_calendar` |
| Аналитика канала | `analytics channel` | `GET /analytics/channels/api/overview` | `get_channel_analytics` |
| Рейтинг каналов | `analytics channel-rating` | _планируется (#968)_ | _планируется (#969)_ |

## Dialogs

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Список диалогов | `dialogs list` | `GET /dialogs/` | `search_dialogs` |
| Обновить кеш | `dialogs refresh` | `POST /dialogs/refresh` | `refresh_dialogs` |
| Покинуть диалоги | `dialogs leave` | `POST /dialogs/leave` | `leave_dialogs` |
| Подписаться/вступить | `dialogs join` | `POST /dialogs/join` | `join_channel`, `join_chat`, `subscribe_channel` |
| Resolve entity | `dialogs resolve` | `POST /dialogs/resolve` | `resolve_entity` |
| Статус кеша | `dialogs cache-status` | `GET /dialogs/cache-status` | `get_cache_status` |
| Очистить кеш | `dialogs cache-clear` | `POST /dialogs/cache-clear` | `clear_dialog_cache` |
| Топики форума | `dialogs topics` | `GET /agent/forum-topics` | `get_forum_topics` |
| Создать канал | `dialogs create-channel` | `POST /dialogs/create-channel` | `create_telegram_channel` |
| Отправить сообщение | `dialogs send` | `POST /dialogs/send` | `send_message` |
| Переслать сообщение | `dialogs forward` | `POST /dialogs/forward-messages` | `forward_messages` |
| Редактировать | `dialogs edit-message` | `POST /dialogs/edit-message` | `edit_message` |
| Удалить | `dialogs delete-message` | `POST /dialogs/delete-message` | `delete_message` |
| Закрепить | `dialogs pin-message` | `POST /dialogs/pin-message` | `pin_message` |
| Открепить | `dialogs unpin-message` | `POST /dialogs/unpin-message` | `unpin_message` |
| Реакция | `dialogs react` | `POST /dialogs/react` | `send_reaction`, `send_reactions` |
| Скачать медиа | `dialogs download-media` | `POST /dialogs/download-media` | `download_media` |
| Отметить прочитанным | `dialogs mark-read` | `POST /dialogs/mark-read` | `mark_read` |
| Список участников | `dialogs participants` | `GET /dialogs/participants` | `get_participants` |
| Права администратора | `dialogs edit-admin` | `POST /dialogs/edit-admin` | `edit_admin` |
| Ограничения | `dialogs edit-permissions` | `POST /dialogs/edit-permissions` | `edit_permissions` |
| Кик | `dialogs kick` | `POST /dialogs/kick` | `kick_participant` |
| Статистика канала | `dialogs broadcast-stats` | `GET /dialogs/broadcast-stats` | `get_broadcast_stats` |
| Архивировать | `dialogs archive` | `POST /dialogs/archive` | `archive_chat` |
| Разархивировать | `dialogs unarchive` | `POST /dialogs/unarchive` | `unarchive_chat` |

## Фото-загрузчик

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Список диалогов | `photo-loader dialogs` | `GET /dialogs/photos/` | `list_photo_dialogs` |
| Обновить | `photo-loader refresh` | `POST /dialogs/photos/refresh` | `refresh_photo_dialogs` |
| Отправить фото | `photo-loader send` | `POST /dialogs/photos/send` | `send_photos_now` |
| Запланировать | `photo-loader schedule-send` | `POST /dialogs/photos/schedule` | `schedule_photos` |
| Создать батч | `photo-loader batch-create` | `POST /dialogs/photos/batch` | `create_photo_batch` |
| Список батчей | `photo-loader batch-list` | `GET /dialogs/photos/batches` | `list_photo_batches` |
| Отменить батч | `photo-loader batch-cancel` | `POST /dialogs/photos/items/{id}/cancel` | `cancel_photo_item` |
| Авто-загрузка | `photo-loader auto-create` | `POST /dialogs/photos/auto` | `create_auto_upload` |
| Список авто | `photo-loader auto-list` | `GET /dialogs/photos/auto` | `list_auto_uploads` |
| Обновить авто | `photo-loader auto-update` | `POST /dialogs/photos/auto/{id}/update` | `update_auto_upload` |
| Вкл/выкл авто | `photo-loader auto-toggle` | `POST /dialogs/photos/auto/{id}/toggle` | `toggle_auto_upload` |
| Удалить авто | `photo-loader auto-delete` | `POST /dialogs/photos/auto/{id}/delete` | `delete_auto_upload` |
| Запустить due | `photo-loader run-due` | `POST /dialogs/photos/run-due` | `run_photo_due` |
| Список items | `photo-loader items` | `GET /dialogs/photos/items` | `list_photo_items` |

## Изображения

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Генерация | `image generate` | `POST /images/generate` | `generate_image` |
| Поиск моделей | `image models` | `GET /images/models/search` | `list_image_models` |
| Поиск моделей (живой запрос) | `image models --refresh` | `GET /images/models/search?refresh=1` | `list_image_models` |
| Список провайдеров | `image providers` | `GET /images/` | `list_image_providers` |
| Список генераций | `image generated` | `GET /images/generated` | `list_generated_images` |

## LLM-провайдеры

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Список | `provider list` | `GET /settings/` | *исключение: управление ключами провайдеров* |
| Добавить | `provider add` | `POST /settings/agent-providers/add` | *исключение: управление ключами провайдеров* |
| Удалить | `provider delete` | `POST /settings/agent-providers/{name}/delete` | *исключение: управление ключами провайдеров* |
| Probe | `provider probe` | `POST /settings/agent-providers/{name}/probe` | *исключение: управление ключами провайдеров* |
| Обновить модели | `provider refresh` | `POST /settings/agent-providers/{name}/refresh`, `POST /settings/agent-providers/refresh-all` | *исключение: управление ключами провайдеров* |
| Тест всех | `provider test-all` | `POST /settings/agent-providers/test-all` | *исключение: управление ключами провайдеров* |

## AI-агент

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Список тредов | `agent threads` | `GET /agent/` | `list_agent_threads` |
| Создать тред | `agent thread-create` | `POST /agent/threads` | `create_agent_thread` |
| Удалить тред | `agent thread-delete` | `DELETE /agent/threads/{id}` | `delete_agent_thread` |
| Переименовать | `agent thread-rename` | `POST /agent/threads/{id}/rename` | `rename_agent_thread` |
| Сообщения треда | `agent messages` | `GET /agent/threads/{id}/messages` | `get_thread_messages` |
| Чат (многоходовой) | `agent chat` | `POST /agent/threads/{id}/chat` | *исключение: self-control агента (рекурсия)* |
| Статус Telegram-очереди | `dialogs queue status` | `POST /agent/threads/{id}/chat` | `get_telegram_queue_status` |
| Отменить задание очереди | `dialogs queue cancel` | `POST /dialogs/queue/{id}/cancel` | `cancel_telegram_command` |
| Очистить ожидающие в очереди | `dialogs queue clear-pending` | `POST /dialogs/queue/clear-pending` | `clear_pending_telegram_commands` |
| Контекст | `agent context` | `POST /agent/threads/{id}/context` | *исключение: self-control агента* |
| Остановить | `agent thread-stop` | `POST /agent/threads/{id}/stop` | *исключение: self-control агента* |

> **Многоходовой чат (паритет CLI ↔ Web).** Обе стороны идут через единый
> `AgentManager.chat_stream`, который на каждом ходе загружает полную историю треда
> (`get_agent_messages`). В web многоходовость реализована как повторные `POST` в один и тот же
> `thread_id`: контекст между ходами и события `tool_start`/`tool_end` в SSE на паритете с
> интерактивным циклом CLI `agent chat`.

## Настройки

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Получить | `settings get` | `GET /settings/` | `get_settings` |
| Установить raw key/value | `settings set` | `POST /settings/save-*` | *исключение: raw/чувствительные настройки* |
| Диагностика | `settings info` | *исключение: debug read-only* | `get_system_info` |
| Время сервера | `settings server-time` | *исключение: debug read-only* | `get_server_time` |
| Агент | `settings agent` | `POST /settings/save-agent` | `save_agent_settings` |
| Фильтры | `settings filter-criteria` | `POST /settings/save-filters` | `save_filter_settings` |
| Интервал реакций | `settings reactions` | `POST /settings/save-scheduler` | *исключение: чувствительные настройки* |
| Семантический поиск | `settings semantic` | `POST /settings/save-semantic-search` | *исключение: чувствительные настройки* |

## Сервер и диагностика

| Операция | CLI | Web Endpoint |
|----------|-----|-------------|
| Запустить | `serve` | — |
| Фоновый worker | `worker` | — |
| MCP-сервер инструментов (stdio) | `mcp-server` | — |
| Остановить | `stop` | — |
| Перезапустить | `restart` | — |
| Health check | — | `GET /health` |
| Вход | — | `GET/POST /login` |
| Выход | — | `GET /logout` |
| Логи | `debug logs` | `GET /debug/logs` |
| Тайминги | `debug timing` | `GET /debug/timing` |
| Память | `debug memory` | `GET /debug/memory` |
