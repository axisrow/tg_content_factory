# Agent Tools Reference

Все инструменты AI-агента, сгруппированные по модулям. Категории: **READ** (чтение), **WRITE** (запись), **DELETE** (удаление).

## Поиск

| Tool | Категория | Описание |
|------|-----------|----------|
| `search_messages` | READ | Полнотекстовый поиск по FTS5 |
| `semantic_search` | READ | Семантический поиск через векторные эмбеддинги |
| `index_messages` | WRITE | Индексация сообщений для семантического поиска |

## Каналы

| Tool | Категория | Описание |
|------|-----------|----------|
| `list_channels` | READ | Список всех каналов |
| `get_channel_stats` | READ | Статистика канала |
| `add_channel` | WRITE | Добавить канал |
| `delete_channel` | DELETE | Удалить канал |
| `toggle_channel` | WRITE | Вкл/выкл канал |
| `import_channels` | WRITE | Массовый импорт |
| `refresh_channel_types` | WRITE | Обновить типы каналов |

## Сбор

| Tool | Категория | Описание |
|------|-----------|----------|
| `collect_channel` | WRITE | Собрать один канал |
| `collect_all_channels` | WRITE | Собрать все каналы |
| `collect_channel_stats` | READ | Статистика сбора канала |
| `collect_all_stats` | READ | Статистика сбора всех |

## Пайплайны

| Tool | Категория | Описание |
|------|-----------|----------|
| `list_pipelines` | READ | Список пайплайнов |
| `get_pipeline_detail` | READ | Детали пайплайна |
| `add_pipeline` | WRITE | Создать пайплайн |
| `edit_pipeline` | WRITE | Редактировать |
| `toggle_pipeline` | WRITE | Вкл/выкл |
| `delete_pipeline` | DELETE | Удалить |
| `run_pipeline` | WRITE | Запустить |
| `generate_draft` | WRITE | Генерировать контент |
| `list_pipeline_runs` | READ | История запусков |
| `get_pipeline_run` | READ | Детали запуска |
| `publish_pipeline_run` | WRITE | Опубликовать |
| `get_pipeline_queue` | READ | Очередь модерации |

## Модерация

| Tool | Категория | Описание |
|------|-----------|----------|
| `list_pending_moderation` | READ | Очередь на модерацию |
| `view_moderation_run` | READ | Просмотр элемента |
| `approve_run` | WRITE | Одобрить |
| `reject_run` | WRITE | Отклонить |
| `bulk_approve_runs` | WRITE | Массовое одобрение |
| `bulk_reject_runs` | WRITE | Массовое отклонение |

## Поисковые запросы

| Tool | Категория | Описание |
|------|-----------|----------|
| `list_search_queries` | READ | Список запросов |
| `get_search_query` | READ | Получить запрос |
| `add_search_query` | WRITE | Добавить запрос |
| `edit_search_query` | WRITE | Редактировать |
| `delete_search_query` | DELETE | Удалить |
| `toggle_search_query` | WRITE | Вкл/выкл |
| `run_search_query` | WRITE | Запустить вручную |

## Аккаунты

| Tool | Категория | Описание |
|------|-----------|----------|
| `list_accounts` | READ | Список аккаунтов |
| `toggle_account` | WRITE | Вкл/выкл |
| `delete_account` | DELETE | Удалить |
| `get_flood_status` | READ | Статус flood wait |
| `clear_flood_status` | WRITE | Сбросить flood wait |

## Фильтры

| Tool | Категория | Описание |
|------|-----------|----------|
| `analyze_filters` | READ | Анализ каналов |
| `apply_filters` | WRITE | Применить фильтры |
| `reset_filters` | WRITE | Сбросить фильтры |
| `toggle_channel_filter` | WRITE | Вкл/выкл фильтр |
| `purge_filtered_channels` | DELETE | Очистить сообщения |
| `hard_delete_channels` | DELETE | Удалить из БД |
| `precheck_filters` | WRITE | Pre-check |

## Аналитика

| Tool | Категория | Описание |
|------|-----------|----------|
| `get_analytics_summary` | READ | Общая сводка |
| `get_pipeline_stats` | READ | Статистика пайплайнов |
| `get_daily_stats` | READ | Ежедневная статистика |
| `get_trending_topics` | READ | Трендовые темы |
| `get_trending_channels` | READ | Топ каналов |
| `get_message_velocity` | READ | Скорость сообщений |
| `get_peak_hours` | READ | Пиковые часы |
| `get_calendar` | READ | Календарь |

## Планировщик

| Tool | Категория | Описание |
|------|-----------|----------|
| `get_scheduler_status` | READ | Статус планировщика |
| `start_scheduler` | WRITE | Запустить |
| `stop_scheduler` | WRITE | Остановить |
| `trigger_collection` | WRITE | Запустить сбор |
| `toggle_scheduler_job` | WRITE | Вкл/выкл job |

## Уведомления

| Tool | Категория | Описание |
|------|-----------|----------|
| `get_notification_status` | READ | Статус бота |
| `setup_notification_bot` | WRITE | Настроить бота |
| `delete_notification_bot` | DELETE | Удалить бота |
| `test_notification` | WRITE | Тест уведомления |

## Фото

| Tool | Категория | Описание |
|------|-----------|----------|
| `list_photo_batches` | READ | Список батчей |
| `list_photo_items` | READ | Список элементов |
| `send_photos_now` | WRITE | Отправить сейчас |
| `schedule_photos` | WRITE | Запланировать |
| `cancel_photo_item` | WRITE | Отменить |
| `list_auto_uploads` | READ | Список авто-задач |
| `toggle_auto_upload` | WRITE | Вкл/выкл авто |
| `delete_auto_upload` | DELETE | Удалить авто |
| `create_photo_batch` | WRITE | Создать батч |
| `run_photo_due` | WRITE | Запустить due |
| `create_auto_upload` | WRITE | Создать авто |
| `update_auto_upload` | WRITE | Обновить авто |

## Мой Telegram

| Tool | Категория | Описание |
|------|-----------|----------|
| `list_dialogs` | READ | Список диалогов |
| `refresh_dialogs` | WRITE | Обновить кеш |
| `leave_dialogs` | DELETE | Покинуть диалоги |
| `create_telegram_channel` | WRITE | Создать канал |
| `get_forum_topics` | READ | Топики форума |
| `clear_dialog_cache` | WRITE | Очистить кеш |
| `get_cache_status` | READ | Статус кеша |

## Сообщения

| Tool | Категория | Описание |
|------|-----------|----------|
| `send_message` | WRITE | Отправить сообщение |
| `edit_message` | WRITE | Редактировать |
| `delete_message` | DELETE | Удалить (⚠️ destructive) |
| `pin_message` | WRITE | Закрепить сообщение |
| `unpin_message` | WRITE | Открепить |
| `download_media` | READ | Скачать медиа |

## Управление чатом

| Tool | Категория | Описание |
|------|-----------|----------|
| `get_participants` | READ | Список участников |
| `edit_admin` | WRITE | Права администратора |
| `edit_permissions` | WRITE | Ограничения пользователя |
| `kick_participant` | DELETE | Кик (⚠️ destructive) |
| `get_broadcast_stats` | READ | Статистика канала |
| `archive_chat` | WRITE | Архивировать чат |
| `unarchive_chat` | WRITE | Разархивировать |
| `mark_read` | WRITE | Отметить прочитанными |

## Изображения

| Tool | Категория | Описание |
|------|-----------|----------|
| `list_image_models` | READ | Поиск моделей |
| `list_image_providers` | READ | Список провайдеров |
| `generate_image` | WRITE | Генерация изображения |

## Настройки

| Tool | Категория | Описание |
|------|-----------|----------|
| `get_settings` | READ | Получить настройки |
| `save_scheduler_settings` | WRITE | Настройки планировщика |
| `save_agent_settings` | WRITE | Настройки агента |
| `save_filter_settings` | WRITE | Настройки фильтров |
| `get_system_info` | READ | Диагностика системы |

## Треды агента

| Tool | Категория | Описание |
|------|-----------|----------|
| `list_agent_threads` | READ | Список тредов |
| `create_agent_thread` | WRITE | Создать тред |
| `delete_agent_thread` | DELETE | Удалить тред |
| `rename_agent_thread` | WRITE | Переименовать |
| `get_thread_messages` | READ | Сообщения треда |
