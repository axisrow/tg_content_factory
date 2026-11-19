# Обзор функций

> **CLI — единственная поверхность** (агенты кодирования: `tg-agent` CLI + скилл). Веб-панель, встроенный агент-чат с TUI и MCP-сервер удалены в #1868. Agent tools — реестр инструментов движка контент-пайплайнов. См. Positioning в корневом `CLAUDE.md`.

Все операции доступны через CLI. Пока работает воркер-демон (`tg-agent restart`), живые команды выполняются на его Telegram-подключении через локальный relay; без демона команда подключает аккаунт сама.

| Модуль | CLI команды | Agent tools |
|--------|-------------|-------------|
| Каналы | `channel list/add/delete/toggle/import/refresh-types/refresh-meta` | 20 tools |
| Сбор | `collect`, `channel collect/stats` | 4 tools |
| Поиск | `search` | 8 tools |
| Поисковые запросы | `search-query list/add/edit/delete/toggle/run/stats` | 8 tools |
| Фильтры | `filter analyze/apply/reset/precheck/toggle/purge/hard-delete` | 8 tools |
| Пайплайны | `pipeline list/show/add/edit/delete/toggle/run/generate/runs/…` | 24 tools |
| Модерация | `pipeline moderation-list/moderation-view/approve/reject/bulk-approve/bulk-reject` | 6 tools |
| Планировщик | `scheduler start/stop/trigger/status/job-toggle/set-interval/…` | 8 tools |
| Уведомления | `notification setup/status/delete/test/dry-run` | 5 tools |
| Аккаунты | `account list/info/toggle/delete/flood-status/flood-clear` | 10 tools |
| Аналитика | `analytics top/content-types/hourly/summary/daily/…` | 15 tools |
| Dialogs | `dialogs list/refresh/leave/send/edit-message/…` | 37 tools |
| Личные сообщения (DM) | `dm journal/archive/drafts …` | 7 tools |
| Фото-загрузчик | `photo-loader send/schedule-send/batch-*/auto-*` | 15 tools |
| Изображения | `image generate/models/providers/generated` | 4 tools |
| Экспорт | `export json/csv/rss/telegram` | 1 tool |
| Настройки | `settings get/set/info/agent/…` | 6 tools |

Подробная таблица соответствия: [CLI / Agent Tool Parity](../reference/parity.md)
