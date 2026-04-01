# Web API Reference

Базовый URL: `http://localhost:8000`

## Auth

| Method | Path | Описание |
|--------|------|----------|
| GET | `/login` | Страница входа |
| POST | `/login` | Войти |
| GET | `/logout` | Выйти |
| GET | `/health` | Health check |

## Auth (Telegram)

| Method | Path | Описание |
|--------|------|----------|
| GET | `/auth/login` | Страница авторизации |
| POST | `/auth/save-credentials` | Сохранить api_id/api_hash |
| POST | `/auth/send-code` | Отправить код |
| POST | `/auth/resend-code` | Переотправить код |
| POST | `/auth/verify-code` | Верифицировать код |

## Channels

| Method | Path | Описание |
|--------|------|----------|
| GET | `/channels/` | Список каналов |
| POST | `/channels/add` | Добавить канал |
| POST | `/channels/add-bulk` | Массовое добавление |
| GET | `/channels/dialogs` | Список диалогов |
| POST | `/channels/{pk}/toggle` | Вкл/выкл |
| POST | `/channels/{pk}/delete` | Удалить |
| POST | `/channels/refresh-types` | Обновить типы |
| POST | `/channels/collect-all` | Собрать все |
| POST | `/channels/{pk}/collect` | Собрать один |
| POST | `/channels/stats/all` | Статистика всех |
| POST | `/channels/{pk}/stats` | Статистика одного |
| GET | `/channels/import` | Страница импорта |
| POST | `/channels/import` | Импортировать |

## Filters

| Method | Path | Описание |
|--------|------|----------|
| GET | `/channels/filter/manage` | Управление фильтрами |
| POST | `/channels/filter/analyze` | Анализ |
| POST | `/channels/filter/apply` | Применить |
| POST | `/channels/filter/reset` | Сбросить |
| POST | `/channels/filter/precheck` | Pre-check |
| POST | `/channels/filter/purge-selected` | Очистить выбранные |
| POST | `/channels/filter/purge-all` | Очистить все |
| POST | `/channels/filter/hard-delete-selected` | Hard delete |
| POST | `/channels/{pk}/filter-toggle` | Переключить фильтр |
| POST | `/channels/{id}/purge-messages` | Очистить сообщения канала |

## Search

| Method | Path | Описание |
|--------|------|----------|
| GET | `/` | Главная / поиск |
| GET | `/search` | Выполнить поиск |

## Search Queries

| Method | Path | Описание |
|--------|------|----------|
| GET | `/search-queries/` | Список запросов |
| POST | `/search-queries/add` | Добавить |
| POST | `/search-queries/{id}/toggle` | Вкл/выкл |
| POST | `/search-queries/{id}/edit` | Редактировать |
| POST | `/search-queries/{id}/delete` | Удалить |
| POST | `/search-queries/{id}/run` | Запустить |

## Pipelines

| Method | Path | Описание |
|--------|------|----------|
| GET | `/pipelines/` | Список |
| POST | `/pipelines/add` | Добавить |
| POST | `/pipelines/{id}/edit` | Редактировать |
| POST | `/pipelines/{id}/toggle` | Вкл/выкл |
| POST | `/pipelines/{id}/delete` | Удалить |
| POST | `/pipelines/{id}/run` | Запустить |
| GET | `/pipelines/{id}/generate` | Страница генерации |
| GET | `/pipelines/{id}/generate-stream` | SSE стрим |
| POST | `/pipelines/{id}/generate` | Генерировать |
| POST | `/pipelines/{id}/publish` | Опубликовать |

## Moderation

| Method | Path | Описание |
|--------|------|----------|
| GET | `/moderation/` | Очередь модерации |
| GET | `/moderation/{id}/view` | Просмотр |
| POST | `/moderation/{id}/approve` | Одобрить |
| POST | `/moderation/{id}/reject` | Отклонить |
| POST | `/moderation/{id}/publish` | Опубликовать |
| POST | `/moderation/bulk-approve` | Массовое одобрение |
| POST | `/moderation/bulk-reject` | Массовое отклонение |

## Scheduler

| Method | Path | Описание |
|--------|------|----------|
| GET | `/scheduler/` | Страница планировщика |
| POST | `/scheduler/start` | Запустить |
| POST | `/scheduler/stop` | Остановить |
| POST | `/scheduler/trigger` | Триггер |
| POST | `/scheduler/test-notification` | Тест уведомления |
| POST | `/scheduler/dry-run-notifications` | Dry-run |
| POST | `/scheduler/jobs/{id}/toggle` | Вкл/выкл job |
| POST | `/scheduler/jobs/{id}/set-interval` | Интервал |
| POST | `/scheduler/tasks/{id}/cancel` | Отменить задачу |
| POST | `/scheduler/tasks/clear-pending-collect` | Очистить очередь |

## Settings

| Method | Path | Описание |
|--------|------|----------|
| GET | `/settings/` | Страница настроек |
| POST | `/settings/save-scheduler` | Настройки планировщика |
| POST | `/settings/save-semantic-search` | Семантический поиск |
| POST | `/settings/semantic-index` | Запустить индексацию |
| POST | `/settings/save-agent` | Настройки агента |
| POST | `/settings/save-filters` | Настройки фильтров |
| POST | `/settings/save-notification-account` | Аккаунт уведомлений |
| POST | `/settings/save-credentials` | Credentials |
| POST | `/settings/notifications/setup` | Настроить бота |
| GET | `/settings/notifications/status` | Статус бота |
| POST | `/settings/notifications/delete` | Удалить бота |
| POST | `/settings/notifications/test` | Тест |
| POST | `/settings/agent-providers/add` | Добавить провайдер |
| POST | `/settings/agent-providers/save` | Сохранить провайдер |
| POST | `/settings/agent-providers/{name}/delete` | Удалить |
| POST | `/settings/agent-providers/{name}/refresh` | Обновить |
| POST | `/settings/agent-providers/refresh-all` | Обновить все |
| POST | `/settings/agent-providers/{name}/probe` | Проверить |
| POST | `/settings/agent-providers/test-all` | Тест всех |
| GET | `/settings/agent-providers/test-all/status` | Статус теста |
| POST | `/settings/image-providers/add` | Добавить провайдер изображений |
| POST | `/settings/image-providers/save` | Сохранить |
| POST | `/settings/image-providers/{name}/delete` | Удалить |
| POST | `/settings/{id}/toggle` | Вкл/выкл аккаунт |
| POST | `/settings/{id}/delete` | Удалить аккаунт |

## Dialogs

| Method | Path | Описание |
|--------|------|----------|
| GET | `/dialogs/` | Главная страница |
| POST | `/dialogs/refresh` | Обновить диалоги |
| GET | `/dialogs/cache-status` | Статус кеша |
| POST | `/dialogs/cache-clear` | Очистить кеш |
| POST | `/dialogs/leave` | Покинуть диалоги |
| POST | `/dialogs/send` | Отправить сообщение |
| POST | `/dialogs/edit-message` | Редактировать |
| POST | `/dialogs/delete-message` | Удалить |
| POST | `/dialogs/pin-message` | Закрепить |
| POST | `/dialogs/unpin-message` | Открепить |
| POST | `/dialogs/download-media` | Скачать медиа |
| GET | `/dialogs/participants` | Список участников (JSON) |
| POST | `/dialogs/edit-admin` | Права администратора |
| POST | `/dialogs/edit-permissions` | Ограничения |
| POST | `/dialogs/kick` | Кик |
| GET | `/dialogs/broadcast-stats` | Статистика канала (JSON) |
| POST | `/dialogs/archive` | Архивировать |
| POST | `/dialogs/unarchive` | Разархивировать |
| POST | `/dialogs/mark-read` | Отметить прочитанными |
| GET | `/dialogs/create-channel` | Страница создания канала |
| POST | `/dialogs/create-channel` | Создать канал |

## Photo Loader

| Method | Path | Описание |
|--------|------|----------|
| GET | `/dialogs/photos/` | Страница фото-загрузчика |
| POST | `/dialogs/photos/refresh` | Обновить |
| POST | `/dialogs/photos/send` | Отправить |
| POST | `/dialogs/photos/schedule` | Запланировать |
| POST | `/dialogs/photos/batch` | Батч |
| POST | `/dialogs/photos/auto` | Авто-загрузка |
| POST | `/dialogs/photos/run-due` | Запустить due |
| POST | `/dialogs/photos/items/{id}/cancel` | Отменить |
| POST | `/dialogs/photos/auto/{id}/toggle` | Вкл/выкл авто |
| POST | `/dialogs/photos/auto/{id}/delete` | Удалить авто |

## Agent

| Method | Path | Описание |
|--------|------|----------|
| GET | `/agent/` | Чат-страница |
| POST | `/agent/threads` | Создать тред |
| DELETE | `/agent/threads/{id}` | Удалить тред |
| POST | `/agent/threads/{id}/rename` | Переименовать |
| POST | `/agent/threads/{id}/context` | Контекст |
| POST | `/agent/threads/{id}/stop` | Остановить |
| POST | `/agent/threads/{id}/chat` | Отправить сообщение |
| GET | `/agent/channels-json` | Каналы (JSON) |
| GET | `/agent/forum-topics` | Топики форума |

## Images

| Method | Path | Описание |
|--------|------|----------|
| GET | `/images/` | Страница изображений |
| POST | `/images/generate` | Генерировать |
| GET | `/images/models/search` | Поиск моделей |

## Analytics

| Method | Path | Описание |
|--------|------|----------|
| GET | `/analytics/` | Главная аналитики |
| GET | `/analytics/content` | Контент-аналитика |
| GET | `/analytics/content/api/summary` | API: сводка |
| GET | `/analytics/content/api/pipelines` | API: пайплайны |
| GET | `/analytics/trends` | Тренды |

## Calendar

| Method | Path | Описание |
|--------|------|----------|
| GET | `/calendar/` | Календарь |
| GET | `/calendar/api/calendar` | API: данные |
| GET | `/calendar/api/upcoming` | API: upcoming |
| GET | `/calendar/api/stats` | API: статистика |

## Dashboard

| Method | Path | Описание |
|--------|------|----------|
| GET | `/dashboard/` | Дашборд |

## Debug

| Method | Path | Описание |
|--------|------|----------|
| GET | `/debug/` | Главная debug |
| GET | `/debug/logs` | Логи |
| GET | `/debug/timing` | Тайминги запросов |
| GET | `/debug/timing/rows` | Строки тайминга |
| GET | `/debug/memory` | Статистика памяти |
