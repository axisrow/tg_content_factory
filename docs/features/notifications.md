# Уведомления

Личный бот для получения уведомлений о совпадениях поисковых запросов.

## Настройка

=== "CLI"
    ```bash
    python -m src.main notification setup      # создать бота через BotFather
    python -m src.main notification status     # статус бота
    python -m src.main notification test       # тестовое сообщение
    python -m src.main notification dry-run    # превью совпадений без отправки
    python -m src.main notification delete     # удалить бота
    ```

=== "Web"
    `POST /settings/notifications/setup` · `GET /settings/notifications/status`
    `POST /settings/notifications/test` · `POST /settings/notifications/delete`

## Как работает

1. Создаётся личный бот через BotFather (используя подключённый Telegram аккаунт)
2. Планировщик проверяет активные поисковые запросы при каждом запуске
3. При новых совпадениях — отправляется уведомление с текстом и ссылкой `t.me/channel/msg_id`

## Dry-run

Проверить текущие совпадения без отправки уведомлений:

```bash
python -m src.main notification dry-run
```

Web: `POST /scheduler/dry-run-notifications`
