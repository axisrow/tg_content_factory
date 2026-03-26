# Планировщик

Автоматический периодический сбор сообщений и проверка поисковых запросов.

## Управление

=== "CLI"
    ```bash
    python -m src.main scheduler start          # запустить (foreground)
    python -m src.main scheduler stop           # отключить автостарт
    python -m src.main scheduler trigger        # разовый запуск коллекции
    python -m src.main scheduler status         # статус и конфигурация
    python -m src.main scheduler job-toggle collection_job
    python -m src.main scheduler set-interval collection_job 60
    python -m src.main scheduler task-cancel 42
    python -m src.main scheduler clear-pending
    ```

=== "Web"
    `GET /scheduler/` · `POST /scheduler/start` · `POST /scheduler/stop`
    `POST /scheduler/trigger` · `POST /scheduler/jobs/{job_id}/toggle`
    `POST /scheduler/jobs/{job_id}/set-interval`

## Jobs

| Job | Описание |
|-----|----------|
| `collection_job` | Инкрементальный сбор всех каналов |
| `search_query_job` | Проверка поисковых запросов и отправка уведомлений |

## Уведомления

Тестирование:

=== "CLI"
    ```bash
    python -m src.main notification dry-run    # превью совпадений без отправки
    python -m src.main notification test       # тестовое уведомление
    ```

=== "Web"
    `POST /scheduler/dry-run-notifications` · `POST /scheduler/test-notification`

## Задачи коллекции

Статус задач (`pending/running/completed/failed/cancelled`) отслеживается в БД через `CollectionQueue`.
