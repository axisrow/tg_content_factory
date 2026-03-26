# Обзор функций

Все операции доступны через CLI, Web UI и AI-агента.

| Модуль | CLI команды | Web endpoints | Agent tools |
|--------|-------------|---------------|-------------|
| Каналы | `channel list/add/delete/toggle/import/refresh-types/refresh-meta` | `/channels/*` | 7 tools |
| Сбор | `collect`, `channel collect/stats` | `/channels/collect-*` | 4 tools |
| Поиск | `search` | `/`, `/search` | 3 tools |
| Поисковые запросы | `search-query list/add/edit/delete/toggle/run/stats` | `/search-queries/*` | 7 tools |
| Фильтры | `filter analyze/apply/reset/precheck/toggle/purge/hard-delete` | `/channels/filter/*` | 7 tools |
| Пайплайны | `pipeline list/show/add/edit/delete/toggle/run/generate/runs/…` | `/pipelines/*` | 12 tools |
| Модерация | — | `/moderation/*` | 6 tools |
| Планировщик | `scheduler start/stop/trigger/status/job-toggle/set-interval/…` | `/scheduler/*` | 5 tools |
| Уведомления | `notification setup/status/delete/test/dry-run` | `/settings/notifications/*` | 4 tools |
| Аккаунты | `account list/info/toggle/delete/flood-status/flood-clear` | `/settings/*` | 5 tools |
| Аналитика | `analytics top/content-types/hourly/summary/daily/…` | `/analytics/*` | 8 tools |
| My Telegram | `my-telegram list/refresh/leave/send/edit-message/…` | `/my-telegram/*` | 18+ tools |
| Фото-загрузчик | `photo-loader send/schedule/batch-*/auto-*` | `/my-telegram/photos/*` | 12 tools |
| AI-агент | `agent threads/chat/…` | `/agent/*` | — |
| Изображения | `image generate/models/providers` | `/images/*` | 3 tools |
| Настройки | `settings get/set/info` | `/settings/*` | 5 tools |

Подробная таблица соответствия: [CLI / Web Parity](../reference/parity.md)
