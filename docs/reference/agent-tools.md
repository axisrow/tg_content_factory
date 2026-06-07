# Agent Tools Reference

Все инструменты AI-агента, сгруппированные по модулям. Категории: **READ** (чтение), **WRITE** (запись), **DELETE** (удаление).

Разрешения в настройках агента трактуются так: явный `on` разрешает выполнение, явный `off` запрещает инструмент, а отсутствующая запись для нового инструмента в интерактивной Web/TUI-сессии должна запрашивать доступ через `PermissionGate`.

## Поиск

| Tool | Категория | Описание |
|------|-----------|----------|
| `search_messages` | READ | Полнотекстовый поиск по FTS5 |
| `semantic_search` | READ | Семантический поиск через векторные эмбеддинги |
| `index_messages` | WRITE | Индексация сообщений для семантического поиска |
| `search_telegram` | READ | Глобальный поиск через Telegram API (Premium) |
| `search_my_chats` | READ | Поиск по личным чатам аккаунта |
| `search_in_channel` | READ | Поиск внутри конкретного канала через Telegram |
| `search_hybrid` | READ | Гибридный поиск (FTS + семантика) |
| `purge_search_cache` | DELETE | Очистка кэша Premium-поиска по запросу |

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
| `refresh_channel_meta` | WRITE | Обновить метаданные (about, linked_chat_id, has_comments) |
| `list_tags` | READ | Список тегов |
| `create_tag` | WRITE | Создать тег |
| `delete_tag` | DELETE | Удалить тег |
| `set_channel_tags` | WRITE | Обновить теги канала |
| `get_channel_tags` | READ | Получить теги конкретного канала |
| `add_channels_bulk` | WRITE | Массовое добавление каналов из кеша диалогов |
| `list_dialogs_for_import` | READ | Список диалогов для импорта (с флагом already_added) |

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
| `get_refinement_steps` | READ | Шаги refinement |
| `set_refinement_steps` | WRITE | Сохранить шаги refinement |
| `export_pipeline_json` | READ | Экспорт JSON |
| `import_pipeline_json` | WRITE | Импорт JSON |
| `list_pipeline_templates` | READ | Список шаблонов |
| `create_pipeline_from_template` | WRITE | Создать pipeline из шаблона |
| `ai_edit_pipeline` | WRITE | AI-редактирование pipeline |
| `get_pipeline_dry_run_count` | READ | Подсчёт сообщений-кандидатов dry-run |

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
| `get_search_query_stats` | READ | Статистика совпадений за период |

## Аккаунты

| Tool | Категория | Описание |
|------|-----------|----------|
| `list_accounts` | READ | Список аккаунтов |
| `toggle_account` | WRITE | Вкл/выкл |
| `delete_account` | DELETE | Удалить |
| `get_flood_status` | READ | Статус flood wait |
| `get_account_availability` | READ | Доступность аккаунта (как в Settings UI): available/flood/disconnected/inactive/session_unavailable |
| `get_runtime_diagnostics` | READ | Диагностика рантайма: runtime_kind (live/snapshot/none), live-пул отдельно от DB-флагов, свежесть снапшота воркера |
| `clear_flood_status` | WRITE | Сбросить flood wait |
| `get_account_info` | READ | Информация об аккаунте (имя, username, premium) |

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
| `purge_channel_messages` | DELETE | Очистить сообщения конкретного канала (по channel_id) |

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
| `get_top_messages` | READ | Топ сообщений по реакциям |
| `get_content_type_stats` | READ | Статистика по типам контента |
| `get_hourly_activity` | READ | Почасовая активность |
| `get_trending_emojis` | READ | Трендовые эмодзи за период |
| `get_channel_analytics` | READ | Обзорная аналитика одного канала |

## Планировщик

| Tool | Категория | Описание |
|------|-----------|----------|
| `get_scheduler_status` | READ | Статус планировщика |
| `start_scheduler` | WRITE | Запустить |
| `stop_scheduler` | WRITE | Остановить |
| `trigger_collection` | WRITE | Запустить сбор |
| `toggle_scheduler_job` | WRITE | Вкл/выкл job |
| `set_scheduler_interval` | WRITE | Установить интервал сбора |
| `cancel_scheduler_task` | WRITE | Отменить задачу сбора |
| `clear_pending_tasks` | WRITE | Очистить очередь ожидающих |

## Уведомления

| Tool | Категория | Описание |
|------|-----------|----------|
| `get_notification_status` | READ | Статус бота |
| `setup_notification_bot` | WRITE | Настроить бота |
| `delete_notification_bot` | DELETE | Удалить бота |
| `test_notification` | WRITE | Тест уведомления |
| `notification_dry_run` | READ | Превью совпадений без отправки |

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
| `list_photo_dialogs` | READ | Список диалогов для фото |
| `refresh_photo_dialogs` | WRITE | Обновить кеш диалогов |

## Диалоги

| Tool | Категория | Описание |
|------|-----------|----------|
| `search_dialogs` | READ | Поиск диалогов по названию (search, type, limit) |
| `refresh_dialogs` | WRITE | Обновить кеш |
| `leave_dialogs` | DELETE | Покинуть диалоги |
| `join_channel` | WRITE | Подписаться/вступить в канал или группу |
| `join_chat` | WRITE | Alias: вступить в группу/чат |
| `subscribe_channel` | WRITE | Alias: подписаться на канал |
| `create_telegram_channel` | WRITE | Создать канал |
| `get_forum_topics` | READ | Топики форума |
| `clear_dialog_cache` | WRITE | Очистить кеш |
| `get_cache_status` | READ | Статус кеша |
| `resolve_entity` | READ | Resolve @username, t.me ссылку или numeric ID |

## Сообщения

| Tool | Категория | Описание |
|------|-----------|----------|
| `read_messages` | READ | Читать сообщения из чата |
| `send_message` | WRITE | Отправить сообщение |
| `send_reaction` | WRITE | Поставить emoji-реакцию на сообщение |
| `send_reactions` | WRITE | Поставить emoji-реакции на несколько сообщений одного чата (batch) |
| `get_telegram_queue_status` | READ | Статус очереди Telegram-заданий и реакций |
| `cancel_telegram_command` | WRITE | Отменить ожидающее задание очереди по id |
| `clear_pending_telegram_commands` | WRITE | Массовая отмена ожидающих заданий очереди (по типу/телефону) |
| `forward_messages` | WRITE | Переслать сообщения |
| `edit_message` | WRITE | Редактировать |
| `delete_message` | DELETE | Удалить (⚠️ destructive) |
| `pin_message` | WRITE | Закрепить сообщение |
| `unpin_message` | WRITE | Открепить |
| `download_media` | READ | Скачать медиа |
| `translate_message` | WRITE | Перевод одного сообщения по его DB id |

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
| `list_generated_images` | READ | История сгенерированных изображений |

## Настройки

| Tool | Категория | Описание |
|------|-----------|----------|
| `get_settings` | READ | Получить настройки |
| `save_scheduler_settings` | WRITE | Настройки планировщика |
| `save_agent_settings` | WRITE | Настройки агента |
| `save_filter_settings` | WRITE | Настройки фильтров |
| `get_system_info` | READ | Диагностика системы |
| `get_server_time` | READ | Текущее время сервера (UTC) |

## Треды агента

| Tool | Категория | Описание |
|------|-----------|----------|
| `list_agent_threads` | READ | Список тредов |
| `create_agent_thread` | WRITE | Создать тред |
| `delete_agent_thread` | DELETE | Удалить тред |
| `rename_agent_thread` | WRITE | Переименовать |
| `get_thread_messages` | READ | Сообщения треда |
