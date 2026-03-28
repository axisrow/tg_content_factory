# CLI / Web / Agent Parity

Каждая операция доступна через все три интерфейса.

## Каналы

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Список каналов | `channel list` | `GET /channels/` | `list_channels` |
| Добавить канал | `channel add` | `POST /channels/add` | `add_channel` |
| Удалить канал | `channel delete` | `POST /channels/{pk}/delete` | `delete_channel` |
| Вкл/выкл канал | `channel toggle` | `POST /channels/{pk}/toggle` | `toggle_channel` |
| Статистика канала | `channel stats` | `POST /channels/{pk}/stats` | `collect_channel_stats` |
| Импорт каналов | `channel import` | `POST /channels/import` | `import_channels` |
| Обновить типы | `channel refresh-types` | `POST /channels/refresh-types` | `refresh_channel_types` |
| Обновить метадата | `channel refresh-meta` | — | `refresh_channel_meta` |
| Список диалогов | — | `GET /channels/dialogs` | — |
| Массовое добавление | — | `POST /channels/add-bulk` | — |

## Сбор сообщений

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Собрать все каналы | `collect` | `POST /channels/collect-all` | `collect_all_channels` |
| Собрать один канал | `channel collect` | `POST /channels/{pk}/collect` | `collect_channel` |
| Статистика всех | — | `POST /channels/stats/all` | `collect_all_stats` |
| Превью (без сохранения) | `collect sample` | — | — |
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
| Индексация | — | `POST /settings/semantic-index` | `index_messages` |

## Поисковые запросы

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Список | `search-query list` | `GET /search-queries/` | `list_search_queries` |
| Добавить | `search-query add` | `POST /search-queries/add` | `add_search_query` |
| Редактировать | `search-query edit` | `POST /search-queries/{id}/edit` | `edit_search_query` |
| Удалить | `search-query delete` | `POST /search-queries/{id}/delete` | `delete_search_query` |
| Вкл/выкл | `search-query toggle` | `POST /search-queries/{id}/toggle` | `toggle_search_query` |
| Запустить вручную | `search-query run` | `POST /search-queries/{id}/run` | `run_search_query` |
| Получить | — | — | `get_search_query` |
| Статистика | `search-query stats` | — | `get_search_query_stats` |

## Фильтры

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Анализировать | `filter analyze` | `POST /channels/filter/analyze` | `analyze_filters` |
| Применить | `filter apply` | `POST /channels/filter/apply` | `apply_filters` |
| Сбросить | `filter reset` | `POST /channels/filter/reset` | `reset_filters` |
| Pre-check | `filter precheck` | `POST /channels/filter/precheck` | `precheck_filters` |
| Вкл/выкл фильтр | `filter toggle` | `POST /channels/{pk}/filter-toggle` | `toggle_channel_filter` |
| Очистить сообщения | `filter purge` | `POST /channels/filter/purge-all` | `purge_filtered_channels` |
| Выбранные очистить | — | `POST /channels/filter/purge-selected` | — |
| Hard delete | `filter hard-delete` | `POST /channels/filter/hard-delete-selected` | `hard_delete_channels` |
| Очистить сообщения канала | — | `POST /channels/{id}/purge-messages` | — |

## Пайплайны

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Список | `pipeline list` | `GET /pipelines/` | `list_pipelines` |
| Детали | `pipeline show` | — | `get_pipeline_detail` |
| Добавить | `pipeline add` | `POST /pipelines/add` | `add_pipeline` |
| Редактировать | `pipeline edit` | `POST /pipelines/{id}/edit` | `edit_pipeline` |
| Удалить | `pipeline delete` | `POST /pipelines/{id}/delete` | `delete_pipeline` |
| Вкл/выкл | `pipeline toggle` | `POST /pipelines/{id}/toggle` | `toggle_pipeline` |
| Запустить | `pipeline run` | `POST /pipelines/{id}/run` | `run_pipeline` |
| Генерация контента | `pipeline generate` | `POST /pipelines/{id}/generate` | `generate_draft` |
| История запусков | `pipeline runs` | — | `list_pipeline_runs` |
| Детали запуска | `pipeline run-show` | — | `get_pipeline_run` |
| Опубликовать | `pipeline publish` | `POST /pipelines/{id}/publish` | `publish_pipeline_run` |
| Очередь | `pipeline queue` | — | `get_pipeline_queue` |
| Одобрить | `pipeline approve` | `POST /moderation/{id}/approve` | `approve_run` |
| Отклонить | `pipeline reject` | `POST /moderation/{id}/reject` | `reject_run` |
| Одобрить (bulk) | `pipeline bulk-approve` | `POST /moderation/bulk-approve` | `bulk_approve_runs` |
| Отклонить (bulk) | `pipeline bulk-reject` | `POST /moderation/bulk-reject` | `bulk_reject_runs` |
| Страница модерации | — | `GET /moderation/` | `list_pending_moderation` |
| Просмотр модерации | — | `GET /moderation/{id}/view` | `view_moderation_run` |
| Стрим генерации | — | `GET /pipelines/{id}/generate-stream` | — |

## Планировщик

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Статус | `scheduler status` | `GET /scheduler/` | `get_scheduler_status` |
| Запустить | `scheduler start` | `POST /scheduler/start` | `start_scheduler` |
| Остановить | `scheduler stop` | `POST /scheduler/stop` | `stop_scheduler` |
| Триггер | `scheduler trigger` | `POST /scheduler/trigger` | `trigger_collection` |
| Вкл/выкл job | `scheduler job-toggle` | `POST /scheduler/jobs/{id}/toggle` | `toggle_scheduler_job` |
| Изменить интервал | `scheduler set-interval` | `POST /scheduler/jobs/{id}/set-interval` | `set_scheduler_interval` |

## Уведомления

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Настроить бота | `notification setup` | `POST /settings/notifications/setup` | `setup_notification_bot` |
| Статус | `notification status` | `GET /settings/notifications/status` | `get_notification_status` |
| Удалить | `notification delete` | `POST /settings/notifications/delete` | `delete_notification_bot` |
| Тест | `notification test` | `POST /settings/notifications/test` | `test_notification` |
| Dry-run | `notification dry-run` | `POST /scheduler/dry-run-notifications` | `notification_dry_run` |
| Тест из scheduler | — | `POST /scheduler/test-notification` | — |

## Аккаунты

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Список | `account list` | `GET /settings/` | `list_accounts` |
| Вкл/выкл | `account toggle` | `POST /settings/{id}/toggle` | `toggle_account` |
| Удалить | `account delete` | `POST /settings/{id}/delete` | `delete_account` |
| Flood статус | `account flood-status` | — | `get_flood_status` |
| Сбросить flood | `account flood-clear` | — | `clear_flood_status` |
| Инфо | `account info` | — | `get_account_info` |
| Авторизация | — | `POST /auth/send-code` | — |
| Верификация | — | `POST /auth/verify-code` | — |

## Аналитика

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Сводка | `analytics summary` | `GET /analytics/content/api/summary` | `get_analytics_summary` |
| Топ сообщений | `analytics top` | — | `get_top_messages` |
| Типы контента | `analytics content-types` | `GET /analytics/content` | `get_content_type_stats` |
| Почасовая активность | `analytics hourly` | — | `get_hourly_activity` |
| Ежедневная статистика | `analytics daily` | `GET /analytics/content/api/pipelines` | `get_daily_stats` |
| Статистика пайплайнов | `analytics pipeline-stats` | — | `get_pipeline_stats` |
| Трендовые темы | `analytics trending-topics` | `GET /analytics/trends` | `get_trending_topics` |
| Топ каналов | `analytics trending-channels` | — | `get_trending_channels` |
| Скорость сообщений | `analytics velocity` | — | `get_message_velocity` |
| Пиковые часы | `analytics peak-hours` | — | `get_peak_hours` |
| Календарь | `analytics calendar` | `GET /calendar/api/calendar` | `get_calendar` |
| Экспорт | `analytics export` | — | — |

## My Telegram — Диалоги

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Список диалогов | `my-telegram list` | `GET /my-telegram/` | `search_my_telegram` |
| Обновить кеш | `my-telegram refresh` | `POST /my-telegram/refresh` | `refresh_dialogs` |
| Покинуть диалоги | `my-telegram leave` | `POST /my-telegram/leave` | `leave_dialogs` |
| Статус кеша | `my-telegram cache-status` | `GET /my-telegram/cache-status` | `get_cache_status` |
| Очистить кеш | `my-telegram cache-clear` | `POST /my-telegram/cache-clear` | `clear_dialog_cache` |
| Топики форума | `my-telegram topics` | `GET /agent/forum-topics` | `get_forum_topics` |
| Создать канал | `my-telegram create-channel` | `POST /my-telegram/create-channel` | `create_telegram_channel` |

## My Telegram — Сообщения

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Отправить сообщение | `my-telegram send` | `POST /my-telegram/send` | `send_message` |
| Редактировать | `my-telegram edit-message` | `POST /my-telegram/edit-message` | `edit_message` |
| Удалить | `my-telegram delete-message` | `POST /my-telegram/delete-message` | `delete_message` |
| Закрепить | `my-telegram pin-message` | `POST /my-telegram/pin-message` | `pin_message` |
| Открепить | `my-telegram unpin-message` | `POST /my-telegram/unpin-message` | `unpin_message` |
| Скачать медиа | `my-telegram download-media` | `POST /my-telegram/download-media` | `download_media` |
| Отметить прочитанным | `my-telegram mark-read` | `POST /my-telegram/mark-read` | `mark_read` |

## My Telegram — Участники

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Список участников | `my-telegram participants` | `GET /my-telegram/participants` | `get_participants` |
| Права администратора | `my-telegram edit-admin` | `POST /my-telegram/edit-admin` | `edit_admin` |
| Ограничения | `my-telegram edit-permissions` | `POST /my-telegram/edit-permissions` | `edit_permissions` |
| Кик | `my-telegram kick` | `POST /my-telegram/kick` | `kick_participant` |
| Статистика канала | `my-telegram broadcast-stats` | `GET /my-telegram/broadcast-stats` | `get_broadcast_stats` |
| Архивировать | `my-telegram archive` | `POST /my-telegram/archive` | `archive_chat` |
| Разархивировать | `my-telegram unarchive` | `POST /my-telegram/unarchive` | `unarchive_chat` |

## Фото-загрузчик

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Список диалогов | `photo-loader dialogs` | `GET /my-telegram/photos/` | `list_photo_dialogs` |
| Обновить | `photo-loader refresh` | `POST /my-telegram/photos/refresh` | `refresh_photo_dialogs` |
| Отправить фото | `photo-loader send` | `POST /my-telegram/photos/send` | `send_photos_now` |
| Запланировать | `photo-loader schedule-send` | `POST /my-telegram/photos/schedule` | `schedule_photos` |
| Создать батч | `photo-loader batch-create` | `POST /my-telegram/photos/batch` | `create_photo_batch` |
| Список батчей | `photo-loader batch-list` | — | `list_photo_batches` |
| Отменить батч | `photo-loader batch-cancel` | `POST /my-telegram/photos/items/{id}/cancel` | `cancel_photo_item` |
| Авто-загрузка | `photo-loader auto-create` | `POST /my-telegram/photos/auto` | `create_auto_upload` |
| Список авто | `photo-loader auto-list` | — | `list_auto_uploads` |
| Обновить авто | `photo-loader auto-update` | — | `update_auto_upload` |
| Вкл/выкл авто | `photo-loader auto-toggle` | `POST /my-telegram/photos/auto/{id}/toggle` | `toggle_auto_upload` |
| Удалить авто | `photo-loader auto-delete` | `POST /my-telegram/photos/auto/{id}/delete` | `delete_auto_upload` |
| Запустить due | `photo-loader run-due` | `POST /my-telegram/photos/run-due` | `run_photo_due` |
| Список items | — | — | `list_photo_items` |

## Изображения

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Генерация | `image generate` | `POST /images/generate` | `generate_image` |
| Поиск моделей | `image models` | `GET /images/models/search` | `list_image_models` |
| Список провайдеров | `image providers` | — | `list_image_providers` |

## AI-агент

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Список тредов | `agent threads` | `GET /agent/` | `list_agent_threads` |
| Создать тред | `agent thread-create` | `POST /agent/threads` | `create_agent_thread` |
| Удалить тред | `agent thread-delete` | `DELETE /agent/threads/{id}` | `delete_agent_thread` |
| Переименовать | `agent thread-rename` | `POST /agent/threads/{id}/rename` | `rename_agent_thread` |
| Сообщения треда | `agent messages` | — | `get_thread_messages` |
| Чат | `agent chat` | `POST /agent/threads/{id}/chat` | — |
| Контекст | `agent context` | `POST /agent/threads/{id}/context` | — |
| Остановить | — | `POST /agent/threads/{id}/stop` | — |

## Настройки

| Операция | CLI | Web Endpoint | Agent Tool |
|----------|-----|-------------|------------|
| Получить | `settings get` | `GET /settings/` | `get_settings` |
| Установить | `settings set` | `POST /settings/save-*` | — |
| Диагностика | `settings info` | — | `get_system_info` |
| Планировщик | — | `POST /settings/save-scheduler` | `save_scheduler_settings` |
| Агент | — | `POST /settings/save-agent` | `save_agent_settings` |
| Фильтры | — | `POST /settings/save-filters` | `save_filter_settings` |
| Семантический поиск | — | `POST /settings/save-semantic-search` | — |

## Сервер

| Операция | CLI | Web Endpoint |
|----------|-----|-------------|
| Запустить | `serve` | — |
| Остановить | `stop` | — |
| Перезапустить | `restart` | — |
| Health check | — | `GET /health` |
| Вход | — | `GET/POST /login` |
| Выход | — | `GET /logout` |
| Debug logs | — | `GET /debug/logs` |
| Timing | — | `GET /debug/timing` |
| Memory | — | `GET /debug/memory` |
